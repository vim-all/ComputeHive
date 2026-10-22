package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// credentials resolution -------------------------------------------------------------------------

// firstExisting returns the first path that exists.
func firstExisting(paths ...string) string {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func configDirs() []string {
	dirs := []string{".computehive"}
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".config", "computehive"))
	}
	return dirs
}

// apiToken: --token, COMPUTEHIVE_TOKEN, then .computehive/api.token or ~/.config/computehive/api.token.
func (g globals) apiToken() string {
	if g.token != "" {
		return strings.TrimSpace(g.token)
	}
	var cands []string
	for _, d := range configDirs() {
		cands = append(cands, filepath.Join(d, "api.token"))
	}
	if f := firstExisting(cands...); f != "" {
		if raw, err := os.ReadFile(f); err == nil {
			return strings.TrimSpace(string(raw))
		}
	}
	return ""
}

// caFile: --ca, COMPUTEHIVE_CA, then .computehive/ca.crt or ~/.config/computehive/ca.crt.
func (g globals) caFile() string {
	if g.ca != "" {
		return g.ca
	}
	var cands []string
	for _, d := range configDirs() {
		cands = append(cands, filepath.Join(d, "ca.crt"))
	}
	return firstExisting(cands...)
}

func (g globals) tlsConfig() (*tls.Config, error) {
	ca := g.caFile()
	if ca == "" {
		return nil, nil
	}
	pemBytes, err := os.ReadFile(ca)
	if err != nil {
		return nil, fmt.Errorf("read CA %s: %w", ca, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("%s contains no valid certificates", ca)
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}, nil
}

type bearer struct{ token string }

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if b.token == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }

func (g globals) dial() (*grpc.ClientConn, error) {
	tc, err := g.tlsConfig()
	if err != nil {
		return nil, err
	}
	transport := insecure.NewCredentials()
	if tc != nil {
		transport = credentials.NewTLS(tc)
	}
	return grpc.NewClient(g.coordinator, grpc.WithTransportCredentials(transport), grpc.WithPerRPCCredentials(bearer{g.apiToken()}))
}

func (g globals) httpClient() (*http.Client, error) {
	tc, err := g.tlsConfig()
	if err != nil {
		return nil, err
	}
	c := &http.Client{}
	if tc != nil {
		c.Transport = &http.Transport{TLSClientConfig: tc, Proxy: http.ProxyFromEnvironment}
	}
	return c, nil
}

// friendly turns an auth failure into the next thing to do.
func friendly(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Unauthenticated") || strings.Contains(msg, "missing or invalid credentials"):
		return fmt.Errorf("%w\n  hint: the coordinator requires an API token. Pass --token, set COMPUTEHIVE_TOKEN, or put it in .computehive/api.token", err)
	case strings.Contains(msg, "certificate") || strings.Contains(msg, "x509") || strings.Contains(msg, "tls:"):
		return fmt.Errorf("%w\n  hint: TLS verification failed. Pass --ca <ca.crt> (or COMPUTEHIVE_CA / .computehive/ca.crt)", err)
	case strings.Contains(msg, "first record does not look like a TLS handshake") || strings.Contains(msg, "authentication handshake failed"):
		return fmt.Errorf("%w\n  hint: the coordinator speaks TLS but no CA is configured (--ca)", err)
	}
	return err
}

// admin commands -----------------------------------------------------------------------------------

const adminUsage = `usage:
  computehive admin enroll [--ttl 1h] [--uses 1] [--note TEXT]   mint an expiring worker enrollment token
  computehive admin revoke <worker-id>                            cut a worker off and requeue its jobs
`

func cmdAdmin(ctx context.Context, g globals, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, adminUsage)
		return errors.New("missing admin subcommand")
	}
	conn, err := g.dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewAdminServiceClient(conn)

	switch args[0] {
	case "enroll":
		fs := flag.NewFlagSet("admin enroll", flag.ContinueOnError)
		fs.SetOutput(errOut)
		ttl := fs.Duration("ttl", time.Hour, "how long the token stays valid (max 168h)")
		uses := fs.Int("uses", 1, "how many workers may enrol with it")
		note := fs.String("note", "", "label to remember who it is for")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		resp, err := c.CreateEnrollmentToken(ctx, &pb.CreateEnrollmentTokenRequest{TtlSeconds: int64(ttl.Seconds()), MaxUses: int32(*uses), Note: *note})
		if err != nil {
			return friendly(err)
		}
		fmt.Fprintln(errOut, "Enrollment token (shown once; it expires "+time.Unix(resp.GetExpiresAtUnix(), 0).UTC().Format(time.RFC3339)+", "+fmt.Sprint(resp.GetMaxUses())+" use(s)):")
		fmt.Fprintln(out, resp.GetToken())
		fmt.Fprintln(errOut, "Give it to the contributor:  computehive worker join --coordinator <host>:50051 --ca ca.crt --token "+resp.GetToken())
		return nil
	case "revoke":
		if len(args) != 2 {
			fmt.Fprint(errOut, adminUsage)
			return errors.New("usage: computehive admin revoke <worker-id>")
		}
		resp, err := c.RevokeWorker(ctx, &pb.RevokeWorkerRequest{WorkerId: &pb.WorkerID{Value: args[1]}})
		if err != nil {
			return friendly(err)
		}
		fmt.Fprintf(out, "worker %s revoked; %d job(s) it held were requeued\n", args[1], resp.GetRequeuedJobs())
		return nil
	default:
		fmt.Fprint(errOut, adminUsage)
		return fmt.Errorf("unknown admin subcommand %q", args[0])
	}
}
