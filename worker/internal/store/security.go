package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/config"
)

// BuildTLS returns the TLS configuration for talking to the coordinator, or nil for plaintext
// (development only). With a CA the server's certificate is verified; with a client certificate as
// well, the worker also proves its identity (mutual TLS).
func BuildTLS(cfg config.Config) (*tls.Config, error) {
	if cfg.CoordinatorCA == "" {
		return nil, nil
	}
	pemBytes, err := os.ReadFile(cfg.CoordinatorCA)
	if err != nil {
		return nil, fmt.Errorf("read coordinator CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("%s contains no valid certificates", cfg.CoordinatorCA)
	}
	t := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	host := cfg.CoordinatorTLSServerName
	if host == "" {
		h, _, err := net.SplitHostPort(cfg.CoordinatorAddr)
		if err != nil {
			return nil, fmt.Errorf("coordinator-addr %q: %w", cfg.CoordinatorAddr, err)
		}
		host = h
	}
	t.ServerName = host
	if cfg.CoordinatorTLSCert != "" || cfg.CoordinatorTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CoordinatorTLSCert, cfg.CoordinatorTLSKey)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		t.Certificates = []tls.Certificate{cert}
	}
	return t, nil
}

// credentialStore keeps the worker's credential in memory and in a private file, so the worker
// keeps its identity across restarts without ever being given a token again.
type credentialStore struct {
	mu   sync.RWMutex
	path string
	cred string
}

func loadCredential(path string) *credentialStore {
	c := &credentialStore{path: path}
	if raw, err := os.ReadFile(path); err == nil {
		c.cred = strings.TrimSpace(string(raw))
	}
	return c
}

func (c *credentialStore) get() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cred
}

func (c *credentialStore) set(cred string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(c.path), ".credential-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.WriteString(cred + "\n"); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), c.path); err != nil {
		return err
	}
	c.cred = cred
	return nil
}

// perRPC attaches the worker credential to every gRPC call.
type perRPC struct{ c *credentialStore }

func (p perRPC) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if cred := p.c.get(); cred != "" {
		return map[string]string{"authorization": "Bearer " + cred}, nil
	}
	return nil, nil
}

// RequireTransportSecurity is false so development without TLS still works; production setups use
// TLS (the coordinator refuses to start with authentication over plaintext unless told to).
func (perRPC) RequireTransportSecurity() bool { return false }

var errNoEnrollment = errors.New("this worker has no credential and no enrollment token: ask the operator for a token (`computehive admin enroll`) and start the worker with ENROLLMENT_TOKEN or `computehive worker join --token`")

// ArtifactAuth returns a function that authenticates requests to the coordinator's artifact store
// as this worker. It must only be applied to requests for the coordinator's own URL.
func (s *Store) ArtifactAuth() func(setHeader func(k, v string)) {
	return func(setHeader func(k, v string)) {
		if cred := s.cred.get(); cred != "" {
			setHeader("Authorization", "Bearer "+cred)
			setHeader("X-Worker-Id", s.cfg.WorkerID)
		}
	}
}

// HTTPClient returns an HTTP client that trusts the cluster CA, for talking to the coordinator's
// HTTP API (artifacts and outputs).
func (s *Store) HTTPClient(timeout time.Duration) *http.Client {
	c := &http.Client{Timeout: timeout}
	if s.tlsCfg != nil {
		clone := s.tlsCfg.Clone()
		clone.ServerName = "" // verify the host in each request's URL
		c.Transport = &http.Transport{TLSClientConfig: clone, Proxy: http.ProxyFromEnvironment}
	}
	return c
}
