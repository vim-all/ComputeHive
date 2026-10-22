package store

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/config"
)

// selfSigned writes a CA-style certificate and key into dir and returns their paths.
func selfSigned(t *testing.T, dir, name string) (crt, key string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(k)
	crt, key = filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	_ = os.WriteFile(crt, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
	_ = os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600)
	return crt, key
}

func TestBuildTLS(t *testing.T) {
	dir := t.TempDir()
	ca, _ := selfSigned(t, dir, "ca")
	ccrt, ckey := selfSigned(t, dir, "client")

	if got, err := BuildTLS(config.Config{CoordinatorAddr: "x:1"}); got != nil || err != nil {
		t.Fatalf("no CA means plaintext (dev): %v %v", got, err)
	}
	cfg, err := BuildTLS(config.Config{CoordinatorAddr: "coord.example:50051", CoordinatorCA: ca})
	if err != nil || cfg == nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "coord.example" || cfg.MinVersion < 0x0304 || cfg.RootCAs == nil || len(cfg.Certificates) != 0 {
		t.Fatalf("server-auth config: name=%q minver=%x certs=%d", cfg.ServerName, cfg.MinVersion, len(cfg.Certificates))
	}
	cfg, err = BuildTLS(config.Config{CoordinatorAddr: "10.0.0.5:50051", CoordinatorCA: ca, CoordinatorTLSServerName: "coordinator", CoordinatorTLSCert: ccrt, CoordinatorTLSKey: ckey})
	if err != nil || cfg.ServerName != "coordinator" || len(cfg.Certificates) != 1 {
		t.Fatalf("mTLS config: %v %+v", err, cfg)
	}

	bad := filepath.Join(dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("not a certificate"), 0o644)
	for name, c := range map[string]config.Config{
		"missing CA file":      {CoordinatorAddr: "x:1", CoordinatorCA: filepath.Join(dir, "nope.crt")},
		"CA with no certs":     {CoordinatorAddr: "x:1", CoordinatorCA: bad},
		"client cert mismatch": {CoordinatorAddr: "x:1", CoordinatorCA: ca, CoordinatorTLSCert: ccrt, CoordinatorTLSKey: filepath.Join(dir, "ca.key")},
		"bad address":          {CoordinatorAddr: "nohostport", CoordinatorCA: ca},
	} {
		if _, err := BuildTLS(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCredentialPersistsPrivatelyAndSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "credential")
	c := loadCredential(path)
	if c.get() != "" {
		t.Fatal("a new worker has no credential")
	}
	if err := c.set("chwc_test"); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("credential file: %v %v (must be 0600)", st, err)
	}
	if dst, _ := os.Stat(filepath.Dir(path)); dst.Mode().Perm() != 0o700 {
		t.Fatalf("state dir mode %v, want 0700", dst.Mode().Perm())
	}
	if again := loadCredential(path); again.get() != "chwc_test" {
		t.Fatalf("credential lost across a restart: %q", again.get())
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".credential-*")); len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

func TestEveryCallCarriesTheCredentialOnceEnrolled(t *testing.T) {
	c := loadCredential(filepath.Join(t.TempDir(), "credential"))
	p := perRPC{c}
	if md, _ := p.GetRequestMetadata(context.Background()); len(md) != 0 {
		t.Fatalf("an unenrolled worker has nothing to send, got %v", md)
	}
	_ = c.set("chwc_abc")
	md, _ := p.GetRequestMetadata(context.Background())
	if md["authorization"] != "Bearer chwc_abc" {
		t.Fatalf("metadata %v", md)
	}
}

func TestEnrollmentNeedsATokenWhenThereIsNoCredential(t *testing.T) {
	if !strings.Contains(errNoEnrollment.Error(), "enrollment token") {
		t.Fatal("the error must tell the operator what to do")
	}
}

func TestArtifactAuthHeaders(t *testing.T) {
	s := New(config.Config{WorkerID: "w1", StateDir: t.TempDir()})
	got := map[string]string{}
	s.ArtifactAuth()(func(k, v string) { got[k] = v })
	if len(got) != 0 {
		t.Fatalf("no credential yet, no headers: %v", got)
	}
	_ = s.cred.set("chwc_zzz")
	s.ArtifactAuth()(func(k, v string) { got[k] = v })
	if got["Authorization"] != "Bearer chwc_zzz" || got["X-Worker-Id"] != "w1" {
		t.Fatalf("headers %v", got)
	}
}
