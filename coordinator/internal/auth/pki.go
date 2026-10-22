package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// A tiny private PKI for a ComputeHive cluster: one CA, a server certificate for the coordinator,
// and optional client certificates for mTLS. ECDSA P-256 throughout.

type keyPair struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte // certificate PEM
}

func newSerial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		panic(err)
	}
	return n
}

func encodeKey(k *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}

func sign(tmpl, parent *x509.Certificate, pub *ecdsa.PublicKey, parentKey *ecdsa.PrivateKey) (*x509.Certificate, []byte, error) {
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, parentKey)
	if err != nil {
		return nil, nil, err
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return c, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

func newCA(name string, validFor time.Duration) (*keyPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: newSerial(), Subject: pkix.Name{CommonName: name, Organization: []string{"ComputeHive"}},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(validFor),
		IsCA: true, BasicConstraintsValid: true, MaxPathLen: 0, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	c, p, err := sign(tmpl, tmpl, &key.PublicKey, key)
	return &keyPair{cert: c, key: key, pem: p}, err
}

func (ca *keyPair) issue(cn string, hosts []string, usage x509.ExtKeyUsage, validFor time.Duration) (*keyPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: newSerial(), Subject: pkix.Name{CommonName: cn, Organization: []string{"ComputeHive"}},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(validFor),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	c, p, err := sign(tmpl, ca.cert, &key.PublicKey, ca.key)
	return &keyPair{cert: c, key: key, pem: p}, err
}

// GenerateCertSet writes a CA, a server certificate valid for hosts, and one client certificate per
// name in clients, into dir. Private keys are mode 0600. If ca.crt/ca.key already exist they are
// reused, so new client certificates can be issued later under the same CA.
func GenerateCertSet(dir string, hosts, clients []string) error {
	if len(hosts) == 0 {
		return errors.New("at least one host name or IP is required for the server certificate")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	var ca *keyPair
	caCrt, caKey := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "ca.key")
	if crtPEM, err := os.ReadFile(caCrt); err == nil {
		keyPEM, err := os.ReadFile(caKey)
		if err != nil {
			return fmt.Errorf("ca.crt exists but ca.key cannot be read: %w", err)
		}
		pair, err := tls.X509KeyPair(crtPEM, keyPEM)
		if err != nil {
			return fmt.Errorf("existing CA: %w", err)
		}
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return err
		}
		ec, ok := pair.PrivateKey.(*ecdsa.PrivateKey)
		if !ok {
			return errors.New("existing CA key is not ECDSA")
		}
		ca = &keyPair{cert: leaf, key: ec, pem: crtPEM}
	} else {
		var err error
		if ca, err = newCA("ComputeHive cluster CA", 5*365*24*time.Hour); err != nil {
			return err
		}
		k, _ := encodeKey(ca.key)
		if err := os.WriteFile(caCrt, ca.pem, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(caKey, k, 0o600); err != nil {
			return err
		}
	}
	srv, err := ca.issue("computehive-coordinator", hosts, x509.ExtKeyUsageServerAuth, 365*24*time.Hour)
	if err != nil {
		return err
	}
	k, _ := encodeKey(srv.key)
	if err := os.WriteFile(filepath.Join(dir, "server.crt"), srv.pem, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "server.key"), k, 0o600); err != nil {
		return err
	}
	for _, name := range clients {
		if !WorkerIDPattern.MatchString(name) {
			return fmt.Errorf("client name %q is not a valid worker id", name)
		}
		c, err := ca.issue(name, nil, x509.ExtKeyUsageClientAuth, 365*24*time.Hour)
		if err != nil {
			return err
		}
		k, _ := encodeKey(c.key)
		if err := os.WriteFile(filepath.Join(dir, "client-"+name+".crt"), c.pem, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "client-"+name+".key"), k, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// ServerTLS builds the coordinator's TLS configuration. If clientCAFile is set, every client must
// present a certificate signed by that CA (mutual TLS).
func ServerTLS(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, NextProtos: []string{"h2", "http/1.1"}}
	if clientCAFile != "" {
		pool, err := loadPool(clientCAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs, cfg.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func loadPool(file string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("%s contains no valid certificates", file)
	}
	return pool, nil
}
