package netproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "127.53.1.1", "::1", // loopback
		"10.0.0.1", "10.255.255.255", "172.16.0.1", "172.31.255.255", "192.168.0.1", "192.168.255.254", // RFC 1918
		"169.254.169.254", "169.254.0.1", "fe80::1", // link-local, including the cloud metadata address
		"fd00:ec2::254", "fc00::1", // IPv6 ULA, including the AWS IPv6 metadata address
		"100.64.0.1", "100.100.100.200", "100.127.255.255", // carrier-grade NAT, including Alibaba metadata
		"0.0.0.0", "0.1.2.3", "::", // unspecified / this-network
		"224.0.0.1", "239.255.255.250", "ff02::1", // multicast
		"255.255.255.255", "240.0.0.1", // broadcast / reserved
		"::ffff:127.0.0.1", "::ffff:10.0.0.1", "::ffff:169.254.169.254", // IPv4 hidden in IPv6
		"64:ff9b::a00:1",                                         // NAT64 mapping of 10.0.0.1
		"192.0.2.1", "198.51.100.7", "203.0.113.9", "198.18.0.1", // documentation / benchmarking
	}
	allowed := []string{"8.8.8.8", "1.1.1.1", "93.184.216.34", "172.15.255.255", "172.32.0.1", "192.167.1.1", "100.63.255.255", "100.128.0.1", "2606:4700:4700::1111", "::ffff:8.8.8.8"}
	for _, s := range blocked {
		if !IsBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("%s must be blocked", s)
		}
	}
	for _, s := range allowed {
		if IsBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("%s is public and must be allowed", s)
		}
	}
}

type fakeResolver struct {
	mu      sync.Mutex
	answers map[string][]string
	calls   map[string]int
}

func (f *fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[host]++
	ips, ok := f.answers[host]
	if !ok {
		return nil, fmt.Errorf("no such host %s", host)
	}
	var out []netip.Addr
	for _, s := range ips {
		out = append(out, netip.MustParseAddr(s))
	}
	return out, nil
}

// rig starts a proxy whose "internet" is a local echo/HTTP server, and records what it dialed.
type rig struct {
	proxy  *httptest.Server
	px     *Proxy
	dialed []string
	mu     sync.Mutex
	remote net.Listener
}

func newRig(t *testing.T, allow []string, dns map[string][]string) *rig {
	t.Helper()
	remote, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { // a trivial "origin": answers a line with a greeting
		for {
			c, err := remote.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				line, _ := bufio.NewReader(c).ReadString('\n')
				fmt.Fprintf(c, "origin says hello to %s", strings.TrimSpace(line))
			}()
		}
	}()
	r := &rig{remote: remote}
	set := map[string]bool{}
	for _, h := range allow {
		set[h] = true
	}
	r.px = New(Config{
		Allow:    func(h string) bool { return set[h] },
		Resolver: &fakeResolver{answers: dns},
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			r.mu.Lock()
			r.dialed = append(r.dialed, addr)
			r.mu.Unlock()
			return (&net.Dialer{}).DialContext(ctx, "tcp", remote.Addr().String()) // everything lands on the local origin
		},
		Idle: 2 * time.Second,
	})
	r.proxy = httptest.NewServer(r.px)
	t.Cleanup(func() { r.proxy.Close(); remote.Close() })
	return r
}

func (r *rig) dials() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.dialed...)
}

// connect issues a CONNECT through the proxy and returns the status line plus, if tunnelled, the origin's greeting.
func (r *rig) connect(t *testing.T, target string) (status string, body string) {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(r.proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	br := bufio.NewReader(conn)
	line, _ := br.ReadString('\n')
	status = strings.TrimSpace(line)
	if strings.Contains(status, " 200 ") {
		for { // skip headers
			l, err := br.ReadString('\n')
			if err != nil || l == "\r\n" {
				break
			}
		}
		fmt.Fprint(conn, "ping\n")
		b, _ := io.ReadAll(br)
		body = string(b)
	}
	return status, body
}

func TestConnectToAnAllowedPublicHostWorksAndDialsTheResolvedIP(t *testing.T) {
	r := newRig(t, []string{"pypi.org"}, map[string][]string{"pypi.org": {"151.101.0.223"}})
	status, body := r.connect(t, "pypi.org:443")
	if !strings.Contains(status, "200") || !strings.Contains(body, "origin says hello") {
		t.Fatalf("tunnel failed: %q %q", status, body)
	}
	if d := r.dials(); len(d) != 1 || d[0] != "151.101.0.223:443" {
		t.Fatalf("the proxy must dial the IP it validated, not the name: %v", d)
	}
}

func TestConnectDeniedCases(t *testing.T) {
	r := newRig(t, []string{"pypi.org", "internal.example", "mixed.example", "meta.example", "dns64.example"}, map[string][]string{
		"pypi.org":         {"151.101.0.223"},
		"internal.example": {"10.0.0.5"},                // allowed name, private address
		"mixed.example":    {"8.8.8.8", "192.168.1.10"}, // one good answer, one private: all or nothing
		"meta.example":     {"169.254.169.254"},         // the cloud metadata service
		"dns64.example":    {"64:ff9b::a00:5"},          // NAT64-embedded 10.0.0.5
		"evil.example":     {"8.8.8.8"},                 // public, but not on the allow list
	})
	for name, target := range map[string]string{
		"not on the allow list":          "evil.example:443",
		"allowed name -> private IP":     "internal.example:443",
		"mixed public/private answers":   "mixed.example:443",
		"cloud metadata":                 "meta.example:80",
		"NAT64-embedded private":         "dns64.example:443",
		"metadata IP literal":            "169.254.169.254:80",
		"loopback literal":               "127.0.0.1:443",
		"private literal":                "10.0.0.1:443",
		"public IP literal (names only)": "8.8.8.8:443",
		"ssh port":                       "pypi.org:22",
		"database port":                  "pypi.org:5432",
		"unresolvable":                   "nope.example:443",
		"malformed":                      "pypi.org",
	} {
		t.Run(name, func(t *testing.T) {
			status, body := r.connect(t, target)
			if !strings.Contains(status, "403") {
				t.Fatalf("%s: got %q, want 403", target, status)
			}
			if body != "" {
				t.Fatalf("data flowed through a denied tunnel: %q", body)
			}
		})
	}
	if d := r.dials(); len(d) != 0 {
		t.Fatalf("a denied request caused a connection attempt: %v", d)
	}
	if _, denied := r.px.Stats(); denied < 13 {
		t.Fatalf("denials not counted: %d", denied)
	}
}

// DNS rebinding: the name resolves to a public address when checked and a private one when
// connecting. Because the proxy dials the validated IP, the second answer never matters.
func TestDNSRebindingCannotRedirectTheConnection(t *testing.T) {
	res := &fakeResolver{answers: map[string][]string{"rebind.example": {"8.8.8.8"}}}
	r := newRig(t, []string{"rebind.example"}, nil)
	r.px.cfg.Resolver = res
	status, _ := r.connect(t, "rebind.example:443")
	if !strings.Contains(status, "200") {
		t.Fatalf("first request: %q", status)
	}
	res.mu.Lock()
	res.answers["rebind.example"] = []string{"127.0.0.1"} // attacker flips the record
	res.mu.Unlock()
	status, _ = r.connect(t, "rebind.example:443")
	if !strings.Contains(status, "403") {
		t.Fatalf("after the record flipped to loopback the proxy must refuse, got %q", status)
	}
	for _, d := range r.dials() {
		if strings.HasPrefix(d, "127.") {
			t.Fatalf("proxy connected to loopback: %v", r.dials())
		}
	}
}

func TestPlainHTTPForwardingIsFilteredToo(t *testing.T) {
	r := newRig(t, []string{"pypi.org", "meta.example"}, map[string][]string{"pypi.org": {"151.101.0.223"}, "meta.example": {"169.254.169.254"}})
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: func(*http.Request) (*url.URL, error) { return url.Parse(r.proxy.URL) }}}

	resp, err := client.Get("http://meta.example/latest/meta-data/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("metadata via plain http: %d", resp.StatusCode)
	}
	resp, err = client.Get("http://evil.example/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unlisted host via plain http: %d", resp.StatusCode)
	}
	if d := r.dials(); len(d) != 0 {
		t.Fatalf("denied plain-http requests dialed out: %v", d)
	}
}

func TestConcurrencyIsBounded(t *testing.T) {
	px := New(Config{MaxConns: 1, Allow: func(string) bool { return false }})
	px.sem <- struct{}{} // the only slot is taken
	rec := httptest.NewRecorder()
	px.ServeHTTP(rec, httptest.NewRequest(http.MethodConnect, "http://pypi.org:443", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("over-limit request got %d", rec.Code)
	}
}
