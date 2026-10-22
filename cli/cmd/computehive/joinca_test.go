package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newCA returns a PEM-encoded self-signed certificate (a CA unless isCA is false) and its SHA-256 fingerprint.
func newCA(t *testing.T, name string, isCA bool) (pemBytes []byte, fingerprint string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: isCA, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(der)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), hex.EncodeToString(sum[:])
}

func serve(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewTLSServer(handler) // a self-signed server: the fetch must not depend on verifying it
	t.Cleanup(srv.Close)
	return srv.URL + "/ca.crt"
}

func TestFetchCAReturnsTheCertificateOnlyWhenTheFingerprintMatches(t *testing.T) {
	good, fp := newCA(t, "cluster CA", true)
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(good) })

	got, err := fetchCA(context.Background(), url, fp)
	if err != nil || string(got) != string(good) {
		t.Fatalf("matching fingerprint: %v", err)
	}
	// Fingerprint spelling variants are accepted: upper case and colon-separated, as openssl prints them.
	colon := strings.ToUpper(strings.Join(splitEvery(fp, 2), ":"))
	if _, err := fetchCA(context.Background(), url, colon); err != nil {
		t.Fatalf("colon-separated upper-case fingerprint: %v", err)
	}
}

func splitEvery(s string, n int) []string {
	var out []string
	for len(s) > 0 {
		k := min(n, len(s))
		out = append(out, s[:k])
		s = s[k:]
	}
	return out
}

func TestFetchCARefusesAnImpostorsCertificate(t *testing.T) {
	_, expected := newCA(t, "the real cluster CA", true)
	impostor, _ := newCA(t, "attacker's CA", true) // a perfectly valid CA, just not the expected one
	url := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(impostor) })

	_, err := fetchCA(context.Background(), url, expected)
	if err == nil || !strings.Contains(err.Error(), "REFUSING TO TRUST") {
		t.Fatalf("a CA with a different fingerprint must be refused, got %v", err)
	}
}

func TestFetchCARejectsAnythingThatIsNotAnExpectedCACertificate(t *testing.T) {
	_, fp := newCA(t, "x", true)
	leaf, _ := newCA(t, "not a CA", false)
	cases := map[string]struct {
		h    http.HandlerFunc
		want string
	}{
		"not PEM":  {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("<html>login</html>")) }, "not a PEM certificate"},
		"not a CA": {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(leaf) }, "not a CA certificate"},
		"404":      {func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }, "404"},
		"too large": {func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat("A", maxCABytes+10)))
		}, "too large"},
		"garbage certificate": {func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not der")}))
		}, "malformed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := fetchCA(context.Background(), serve(t, tc.h), fp); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
	for _, bad := range []string{"", "abc", strings.Repeat("z", 64)} {
		if _, err := fetchCA(context.Background(), "https://127.0.0.1:1/ca.crt", bad); err == nil {
			t.Errorf("fingerprint %q was accepted", bad)
		}
	}
}

func TestCAFingerprintMatchesWhatFetchCAChecksAndHandlesMultipleBlocks(t *testing.T) {
	pemBytes, fp := newCA(t, "ca", true)
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, append([]byte("junk before\n"), pemBytes...), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := caFingerprint(path); err != nil || got != fp {
		t.Fatalf("got %q, %v, want %q", got, err, fp)
	}
	if err := os.WriteFile(path, []byte("no certificate here"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := caFingerprint(path); err == nil {
		t.Fatal("a file without a certificate must be an error")
	}
	if _, err := caFingerprint(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a missing file must be an error")
	}
}

func TestJoinInstructionsCarryTheTokenAndTheRealFingerprint(t *testing.T) {
	pemBytes, fp := newCA(t, "ca", true)
	path := filepath.Join(t.TempDir(), "ca.crt")
	_ = os.WriteFile(path, pemBytes, 0o600)

	out := joinInstructions(globals{coordinator: "10.1.2.3:50051", ca: path}, "chet_secret")
	for _, want := range []string{"--coordinator 10.1.2.3:50051", "--token chet_secret", "--ca-sha256 " + fp} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	local := joinInstructions(globals{coordinator: "localhost:50051", ca: path}, "t")
	if !strings.Contains(local, "<coordinator-host>:50051") || !strings.Contains(local, "Replace <coordinator-host>") {
		t.Errorf("a loopback coordinator address is useless to another machine and must be a placeholder:\n%s", local)
	}
}
