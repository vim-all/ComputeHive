package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxCABytes bounds the download: a CA certificate is a kilobyte or two, so anything large is not one.
const maxCABytes = 64 << 10

// fetchCA downloads the cluster CA certificate from the coordinator and returns it only if its SHA-256
// fingerprint equals want.
//
// The connection is deliberately made WITHOUT verifying the server's certificate: at this point the worker has
// no CA to verify it with (that is what it is fetching). Trust comes entirely from the fingerprint, which the
// operator gives the contributor over a channel they already trust (`computehive admin enroll` prints it). A man
// in the middle can serve a different certificate but cannot make it match the fingerprint. Everything after this
// (gRPC, artifacts, outputs) is verified against the CA obtained here.
func fetchCA(ctx context.Context, url, want string) ([]byte, error) {
	want = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(want), ":", ""))
	if len(want) != 64 {
		return nil, fmt.Errorf("--ca-sha256 must be 64 hex characters (the SHA-256 fingerprint printed by `computehive admin enroll`), got %d", len(want))
	}
	if _, err := hex.DecodeString(want); err != nil {
		return nil, fmt.Errorf("--ca-sha256 is not hexadecimal: %w", err)
	}
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}}, //nolint:gosec // trust comes from the fingerprint check below
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download the cluster CA from %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download the cluster CA from %s: %s (does the coordinator run with TLS and publish /ca.crt?)", url, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCABytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxCABytes {
		return nil, fmt.Errorf("the response from %s is too large to be a CA certificate", url)
	}
	block, _ := pem.Decode(body)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("the response from %s is not a PEM certificate", url)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("the certificate from %s is malformed: %w", url, err)
	}
	if !cert.IsCA {
		return nil, fmt.Errorf("the certificate from %s is not a CA certificate", url)
	}
	sum := sha256.Sum256(block.Bytes)
	if got := hex.EncodeToString(sum[:]); got != want {
		return nil, fmt.Errorf("REFUSING TO TRUST the CA from %s: its fingerprint is %s but you expected %s. "+
			"Either the fingerprint is wrong or something is answering in the coordinator's place", url, got, want)
	}
	return body, nil
}
