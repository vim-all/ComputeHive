// Package netproxy implements the optional, per-job, allowlist-only network access for contributor
// mode. A job never gets a real network: it gets an internal Docker network whose only reachable
// peer is this proxy, and the proxy only talks to the hosts the contributor allowed, after checking
// that they resolve to public addresses.
package netproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"
	"time"
)

// Resolver looks up addresses; tests inject a fake.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Config configures one job's proxy.
type Config struct {
	Allow    func(host string) bool // may this job reach this host name?
	Resolver Resolver               // default: net.DefaultResolver
	Dial     func(ctx context.Context, network, addr string) (net.Conn, error)
	Log      *slog.Logger
	Job      string
	MaxConns int           // concurrent tunnels (default 32)
	Idle     time.Duration // idle timeout per tunnel (default 60s)
}

var blockedPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, c := range []string{
		"0.0.0.0/8",       // "this network"
		"100.64.0.0/10",   // carrier-grade NAT (includes Alibaba metadata 100.100.100.200)
		"192.0.0.0/24",    // IETF protocol assignments
		"192.0.2.0/24",    // documentation
		"198.18.0.0/15",   // benchmarking
		"198.51.100.0/24", // documentation
		"203.0.113.0/24",  // documentation
		"240.0.0.0/4",     // reserved and broadcast
		"64:ff9b::/96",    // NAT64: can embed any IPv4 address, including private ones
		"2001:db8::/32",   // documentation
	} {
		out = append(out, netip.MustParsePrefix(c))
	}
	return out
}()

// IsBlockedIP reports whether the proxy must never connect to ip: loopback, private (RFC 1918 and
// IPv6 ULA, which includes the AWS IPv6 metadata address), link-local (which includes the IPv4 cloud
// metadata address 169.254.169.254), carrier-grade NAT, multicast, unspecified, reserved, and any
// IPv4 address smuggled inside an IPv6 one.
func IsBlockedIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Proxy is an http.Handler speaking the HTTP proxy protocol (CONNECT for HTTPS, absolute-URI for HTTP).
type Proxy struct {
	cfg     Config
	sem     chan struct{}
	tr      *http.Transport
	allowed atomic.Int64
	denied  atomic.Int64
	log     *slog.Logger
}

func New(cfg Config) *Proxy {
	if cfg.Resolver == nil {
		cfg.Resolver = net.DefaultResolver
	}
	if cfg.Dial == nil {
		d := &net.Dialer{Timeout: 10 * time.Second}
		cfg.Dial = d.DialContext
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 32
	}
	if cfg.Idle <= 0 {
		cfg.Idle = 60 * time.Second
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	p := &Proxy{cfg: cfg, sem: make(chan struct{}, cfg.MaxConns), log: log.With("job_id", cfg.Job, "component", "netproxy")}
	p.tr = &http.Transport{
		Proxy: nil, // never chain to another proxy
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return p.dialChecked(ctx, host, port)
		},
		DisableKeepAlives: true, ResponseHeaderTimeout: 30 * time.Second,
	}
	return p
}

// Stats returns how many requests were allowed and denied.
func (p *Proxy) Stats() (allowed, denied int64) { return p.allowed.Load(), p.denied.Load() }

func (p *Proxy) deny(w http.ResponseWriter, host, reason string) {
	p.denied.Add(1)
	p.log.Warn("network request denied", "event", "net_denied", "host", host, "reason", reason)
	http.Error(w, "ComputeHive: network access denied: "+reason, http.StatusForbidden)
}

func allowedPort(port string) bool { return port == "80" || port == "443" }

// dialChecked resolves host itself, refuses the connection if ANY answer is a blocked address, and
// then dials the resolved IP rather than the name, so a DNS answer cannot change between the check and
// the connection (rebinding).
func (p *Proxy) dialChecked(ctx context.Context, host, port string) (net.Conn, error) {
	if !allowedPort(port) {
		return nil, fmt.Errorf("port %s is not allowed (only 80 and 443)", port)
	}
	var addrs []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{ip}
	} else {
		var lerr error
		addrs, lerr = p.cfg.Resolver.LookupNetIP(ctx, "ip", host)
		if lerr != nil || len(addrs) == 0 {
			return nil, fmt.Errorf("cannot resolve %s: %v", host, lerr)
		}
	}
	for _, a := range addrs {
		if IsBlockedIP(a) {
			return nil, fmt.Errorf("%s resolves to %s, a private, loopback, link-local or metadata address", host, a.Unmap())
		}
	}
	// Every candidate has passed the filter. Try them in order (a v6 answer is useless on a v4-only bridge).
	var lastErr error
	for _, a := range addrs {
		conn, err := p.cfg.Dial(ctx, "tcp", net.JoinHostPort(a.Unmap().String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("could not connect to %s: %v", host, lastErr)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	default:
		http.Error(w, "too many concurrent connections", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodConnect {
		p.connect(w, r)
		return
	}
	p.forward(w, r)
}

func (p *Proxy) hostAllowed(host string) bool {
	if _, err := netip.ParseAddr(host); err == nil {
		return false // jobs name hosts, never raw addresses
	}
	return p.cfg.Allow != nil && p.cfg.Allow(strings.ToLower(strings.TrimSuffix(host, ".")))
}

func (p *Proxy) connect(w http.ResponseWriter, r *http.Request) {
	host, port, err := net.SplitHostPort(r.Host)
	if err != nil {
		p.deny(w, r.Host, "malformed CONNECT target")
		return
	}
	if !p.hostAllowed(host) {
		p.deny(w, host, "host is not on the allow list for this job")
		return
	}
	upstream, err := p.dialChecked(r.Context(), host, port)
	if err != nil {
		p.deny(w, host, err.Error())
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
		return
	}
	client, _, err := hj.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	p.allowed.Add(1)
	p.log.Info("network tunnel opened", "event", "net_allowed", "host", host, "port", port)
	_, _ = client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	pipe(client, upstream, p.cfg.Idle)
}

func pipe(a, b net.Conn, idle time.Duration) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			_ = src.SetReadDeadline(time.Now().Add(idle))
			n, err := src.Read(buf)
			if n > 0 {
				_ = dst.SetWriteDeadline(time.Now().Add(idle))
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) {
	if !r.URL.IsAbs() || r.URL.Scheme != "http" {
		p.deny(w, r.Host, "only absolute http:// requests and CONNECT are proxied")
		return
	}
	host := r.URL.Hostname()
	if !p.hostAllowed(host) {
		p.deny(w, host, "host is not on the allow list for this job")
		return
	}
	out := r.Clone(r.Context())
	out.RequestURI = ""
	for _, h := range []string{"Proxy-Connection", "Proxy-Authorization", "Connection", "Keep-Alive", "Te", "Trailer", "Upgrade"} {
		out.Header.Del(h)
	}
	resp, err := p.tr.RoundTrip(out)
	if err != nil {
		p.deny(w, host, err.Error())
		return
	}
	defer resp.Body.Close()
	p.allowed.Add(1)
	p.log.Info("network request forwarded", "event", "net_allowed", "host", host)
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<30))
}
