package grpcserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"coordinator/internal/auth"
	"coordinator/internal/config"
	"coordinator/internal/httpapi"
	"coordinator/internal/queue"
	"coordinator/internal/testutil"
	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const apiToken = "test-api-token-0123456789abcdef"

type env struct {
	addr    string
	redis   *testutil.Redis
	auth    *auth.Service
	q       *queue.Queue
	certDir string
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type opts struct {
	tls      bool
	mtls     bool
	acfg     func(*auth.Config)
	disabled bool
}

func start(t *testing.T, o opts) *env {
	t.Helper()
	r := testutil.Start(t)
	rdb := r.Client()
	t.Cleanup(func() { rdb.Close() })
	qcfg := queue.DefaultConfig()
	qcfg.DeadAfter, qcfg.ReapInterval, qcfg.BackoffBase, qcfg.BackoffMax = 2*time.Second, 100*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond
	q := queue.New(rdb, qcfg)

	acfg := auth.DefaultConfig()
	acfg.APIToken, acfg.Disabled = apiToken, o.disabled
	if o.acfg != nil {
		o.acfg(&acfg)
	}
	a := auth.New(rdb, acfg, quiet())
	svc := NewServer(q, config.Config{HeartbeatInterval: 100 * time.Millisecond, WorkerDeadAfter: 2 * time.Second}, quiet()).WithAuth(a)

	e := &env{redis: r, auth: a, q: q}
	var tlsCfg *tls.Config
	if o.tls {
		e.certDir = t.TempDir()
		if err := auth.GenerateCertSet(e.certDir, []string{"localhost", "127.0.0.1"}, []string{"worker-a"}); err != nil {
			t.Fatal(err)
		}
		clientCA := ""
		if o.mtls {
			clientCA = filepath.Join(e.certDir, "ca.crt")
		}
		var err error
		tlsCfg, err = auth.ServerTLS(filepath.Join(e.certDir, "server.crt"), filepath.Join(e.certDir, "server.key"), clientCA)
		if err != nil {
			t.Fatal(err)
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e.addr = l.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = Serve(ctx, l, svc, tlsCfg); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return e
}

func (e *env) conn(t *testing.T, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	c, err := grpc.NewClient(e.addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func withBearer(ctx context.Context, tok string) context.Context {
	if tok == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func code(err error) codes.Code { return status.Code(err) }

func wid(id string) *pb.WorkerID { return &pb.WorkerID{Value: id} }

func admin(t *testing.T, e *env) pb.AdminServiceClient {
	return pb.NewAdminServiceClient(e.conn(t, insecure.NewCredentials()))
}

func mintToken(t *testing.T, e *env, ttl int64, uses int32) string {
	t.Helper()
	resp, err := admin(t, e).CreateEnrollmentToken(withBearer(context.Background(), apiToken), &pb.CreateEnrollmentTokenRequest{TtlSeconds: ttl, MaxUses: uses, Note: "test"})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	return resp.GetToken()
}

func register(e *env, t *testing.T, id, token, cred string) (*pb.RegisterWorkerResponse, error) {
	w := pb.NewWorkerServiceClient(e.conn(t, insecure.NewCredentials()))
	return w.RegisterWorker(withBearer(context.Background(), cred), &pb.RegisterWorkerRequest{
		WorkerId: id, EnrollmentToken: token, MaxJobs: 1, AvailableResources: &pb.ResourceSpec{CpuCores: 4, MemoryMb: 8192},
	})
}

func TestClientAndAdminCallsRequireTheAPIToken(t *testing.T) {
	e := start(t, opts{})
	c := pb.NewClientServiceClient(e.conn(t, insecure.NewCredentials()))
	ctx := context.Background()
	req := &pb.SubmitJobRequest{ContainerImage: "img", Command: []string{"x"}, RequiredResources: &pb.ResourceSpec{CpuCores: 1}}

	if _, err := c.SubmitJob(ctx, req); code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	if _, err := c.SubmitJob(withBearer(ctx, "wrong-token-wrong-token"), req); code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v", err)
	}
	if _, err := c.ListWorkers(ctx, &pb.ListWorkersRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("ListWorkers without a token: %v", err)
	}
	if _, err := c.SubmitJob(withBearer(ctx, apiToken), req); err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if _, err := admin(t, e).CreateEnrollmentToken(ctx, &pb.CreateEnrollmentTokenRequest{TtlSeconds: 60, MaxUses: 1}); code(err) != codes.Unauthenticated {
		t.Fatalf("admin RPC without a token: %v", err)
	}
}

func TestEnrollmentTokensExpireAndAreLimitedUse(t *testing.T) {
	e := start(t, opts{})

	// One-use token: the first worker enrols, the second is refused.
	tok := mintToken(t, e, 60, 1)
	resp, err := register(e, t, "worker-1", tok, "")
	if err != nil || !strings.HasPrefix(resp.GetCredential(), "chwc_") {
		t.Fatalf("first enrolment: %v %v", resp, err)
	}
	if _, err := register(e, t, "worker-2", tok, ""); code(err) != codes.Unauthenticated {
		t.Fatalf("a used-up token enrolled a second worker: %v", err)
	}

	// Multi-use token works up to its limit.
	multi := mintToken(t, e, 60, 2)
	for _, id := range []string{"m-1", "m-2"} {
		if _, err := register(e, t, id, multi, ""); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	if _, err := register(e, t, "m-3", multi, ""); code(err) != codes.Unauthenticated {
		t.Fatalf("third use of a 2-use token: %v", err)
	}

	// Expiry.
	short := mintToken(t, e, 1, 5)
	time.Sleep(1300 * time.Millisecond)
	if _, err := register(e, t, "late", short, ""); code(err) != codes.Unauthenticated {
		t.Fatalf("an expired token enrolled a worker: %v", err)
	}

	// Nonsense and missing tokens.
	for name, tk := range map[string]string{"made up": "chet_" + strings.Repeat("0", 48), "empty": ""} {
		if _, err := register(e, t, "x-"+strings.ReplaceAll(name, " ", ""), tk, ""); code(err) != codes.Unauthenticated {
			t.Fatalf("%s token: %v", name, err)
		}
	}
	// Invalid worker ids.
	for _, bad := range []string{"has space", "../etc", strings.Repeat("a", 65), "-lead"} {
		tk := mintToken(t, e, 60, 1)
		if _, err := register(e, t, bad, tk, ""); err == nil {
			t.Fatalf("worker id %q accepted", bad)
		}
	}
}

func TestAdminRPCValidatesItsInputs(t *testing.T) {
	e := start(t, opts{})
	ctx := withBearer(context.Background(), apiToken)
	a := admin(t, e)
	for name, req := range map[string]*pb.CreateEnrollmentTokenRequest{
		"zero ttl":     {TtlSeconds: 0, MaxUses: 1},
		"ttl too long": {TtlSeconds: 8 * 24 * 3600, MaxUses: 1},
		"no uses":      {TtlSeconds: 60, MaxUses: 0},
		"too many":     {TtlSeconds: 60, MaxUses: 5000},
		"huge note":    {TtlSeconds: 60, MaxUses: 1, Note: strings.Repeat("x", 500)},
	} {
		if _, err := a.CreateEnrollmentToken(ctx, req); code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want InvalidArgument", name, err)
		}
	}
	if _, err := a.RevokeWorker(ctx, &pb.RevokeWorkerRequest{WorkerId: wid("../../x")}); code(err) != codes.InvalidArgument {
		t.Errorf("revoke with a bad id: %v", err)
	}
}

func TestWorkerCredentialsAuthenticateEveryCallAndCannotBeBorrowed(t *testing.T) {
	e := start(t, opts{})
	credA := mustEnrol(t, e, "worker-a")
	credB := mustEnrol(t, e, "worker-b")
	w := pb.NewWorkerServiceClient(e.conn(t, insecure.NewCredentials()))
	ctx := context.Background()
	hb := func(c context.Context, id string) error {
		_, err := w.Heartbeat(c, &pb.HeartbeatRequest{WorkerId: wid(id), AvailableResources: &pb.ResourceSpec{CpuCores: 4}})
		return err
	}

	if err := hb(withBearer(ctx, credA), "worker-a"); err != nil {
		t.Fatalf("a worker with its own credential: %v", err)
	}
	if err := hb(ctx, "worker-a"); code(err) != codes.Unauthenticated {
		t.Fatalf("no credential: %v", err)
	}
	if err := hb(withBearer(ctx, "chwc_deadbeef"), "worker-a"); code(err) != codes.Unauthenticated {
		t.Fatalf("wrong credential: %v", err)
	}
	if err := hb(withBearer(ctx, credA), "worker-b"); code(err) != codes.Unauthenticated {
		t.Fatalf("worker A's credential must not act as worker B: %v", err)
	}
	if err := hb(withBearer(ctx, credB), "worker-b"); err != nil {
		t.Fatalf("worker B: %v", err)
	}
	if err := hb(withBearer(ctx, apiToken), "worker-a"); code(err) != codes.Unauthenticated {
		t.Fatalf("the API token is not a worker credential: %v", err)
	}
	// Every worker RPC is protected, not only Heartbeat.
	if _, err := w.RequestJob(ctx, &pb.RequestJobRequest{WorkerId: wid("worker-a")}); code(err) != codes.Unauthenticated {
		t.Fatalf("RequestJob without a credential: %v", err)
	}
	if _, err := w.SubmitResult(ctx, &pb.SubmitResultRequest{WorkerId: wid("worker-a"), LeaseToken: "x", Result: &pb.Result{JobId: &pb.JobID{Value: "j"}, Status: pb.Status_STATUS_SUCCEEDED}}); code(err) != codes.Unauthenticated {
		t.Fatalf("SubmitResult without a credential: %v", err)
	}
	// A worker's credential is not valid for the client API.
	c := pb.NewClientServiceClient(e.conn(t, insecure.NewCredentials()))
	if _, err := c.ListWorkers(withBearer(ctx, credA), &pb.ListWorkersRequest{}); code(err) != codes.Unauthenticated {
		t.Fatalf("a worker credential used as an API token: %v", err)
	}
}

func mustEnrol(t *testing.T, e *env, id string) string {
	t.Helper()
	resp, err := register(e, t, id, mintToken(t, e, 60, 1), "")
	if err != nil {
		t.Fatalf("enrol %s: %v", id, err)
	}
	return resp.GetCredential()
}

func TestAnEnrolledIdentityCannotBeTakenOverAndReRegistrationNeedsTheCredential(t *testing.T) {
	e := start(t, opts{})
	cred := mustEnrol(t, e, "worker-a")

	// A second valid token does NOT let someone claim worker-a's identity.
	if _, err := register(e, t, "worker-a", mintToken(t, e, 60, 1), ""); code(err) != codes.AlreadyExists {
		t.Fatalf("identity takeover with a fresh token: %v", err)
	}
	// The real worker re-registers (restart) with its credential and no token.
	resp, err := register(e, t, "worker-a", "", cred)
	if err != nil || resp.GetCredential() != "" {
		t.Fatalf("re-registration: %v (must not issue a second credential: %q)", err, resp.GetCredential())
	}
	// A wrong credential is not enough.
	if _, err := register(e, t, "worker-a", "", "chwc_wrong"); code(err) != codes.Unauthenticated {
		t.Fatalf("re-registration with a wrong credential: %v", err)
	}
}

func TestSecretsAreStoredOnlyAsHashes(t *testing.T) {
	e := start(t, opts{})
	tok := mintToken(t, e, 60, 1)
	resp, err := register(e, t, "worker-a", tok, "")
	if err != nil {
		t.Fatal(err)
	}
	rdb := e.redis.Client()
	defer rdb.Close()
	keys, _ := rdb.Keys(context.Background(), "ch:enroll:*").Result()
	ck, _ := rdb.Keys(context.Background(), "ch:cred:*").Result()
	if len(keys) != 1 || len(ck) != 1 {
		t.Fatalf("keys: %v %v", keys, ck)
	}
	dump := strings.Join(append(keys, ck...), "\n")
	cv, _ := rdb.Get(context.Background(), ck[0]).Result()
	hv, _ := rdb.HGetAll(context.Background(), keys[0]).Result()
	dump += "\n" + cv + "\n" + hv["note"]
	for _, secret := range []string{tok, resp.GetCredential(), strings.TrimPrefix(tok, "chet_"), strings.TrimPrefix(resp.GetCredential(), "chwc_")} {
		if strings.Contains(dump, secret) {
			t.Fatalf("a plaintext secret is in Redis: %q", secret)
		}
	}
	if ttl, _ := rdb.PTTL(context.Background(), keys[0]).Result(); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("enrollment token must carry a TTL, got %v", ttl)
	}
}

func TestRevokingAWorkerCutsItOffAndRequeuesItsJobs(t *testing.T) {
	e := start(t, opts{})
	cred := mustEnrol(t, e, "worker-a")
	ctx := context.Background()
	w := pb.NewWorkerServiceClient(e.conn(t, insecure.NewCredentials()))
	c := pb.NewClientServiceClient(e.conn(t, insecure.NewCredentials()))

	if _, err := w.Heartbeat(withBearer(ctx, cred), &pb.HeartbeatRequest{WorkerId: wid("worker-a"), AvailableResources: &pb.ResourceSpec{CpuCores: 4, MemoryMb: 8192},
		Usage: &pb.ResourceUsage{MemTotalMb: 8192, MemUsedPercent: 10}}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.SubmitJob(withBearer(ctx, apiToken), &pb.SubmitJobRequest{ContainerImage: "img", Command: []string{"x"}, RequiredResources: &pb.ResourceSpec{CpuCores: 1, MemoryMb: 64}})
	if err != nil {
		t.Fatal(err)
	}
	job, err := w.RequestJob(withBearer(ctx, cred), &pb.RequestJobRequest{WorkerId: wid("worker-a")})
	if err != nil || !job.GetHasJob() {
		t.Fatalf("worker-a did not get the job: %v %v", job, err)
	}

	rev, err := admin(t, e).RevokeWorker(withBearer(ctx, apiToken), &pb.RevokeWorkerRequest{WorkerId: wid("worker-a")})
	if err != nil || rev.GetRequeuedJobs() != 1 {
		t.Fatalf("revoke: %v %v", rev, err)
	}

	if _, err := w.Heartbeat(withBearer(ctx, cred), &pb.HeartbeatRequest{WorkerId: wid("worker-a")}); code(err) != codes.PermissionDenied {
		t.Fatalf("revoked worker's heartbeat: %v", err)
	}
	if _, err := w.SubmitResult(withBearer(ctx, cred), &pb.SubmitResultRequest{WorkerId: wid("worker-a"), LeaseToken: job.GetJob().GetLeaseToken(),
		Result: &pb.Result{JobId: sub.GetJobId(), Status: pb.Status_STATUS_SUCCEEDED}}); code(err) != codes.PermissionDenied {
		t.Fatalf("revoked worker's result: %v", err)
	}
	st, err := c.GetJobStatus(withBearer(ctx, apiToken), &pb.GetJobStatusRequest{JobId: sub.GetJobId()})
	if err != nil || st.GetStatus() != pb.Status_STATUS_REQUEUED {
		t.Fatalf("the revoked worker's job should be requeued, status %v err %v", st.GetStatus(), err)
	}
	if st.GetAttempt() != 0 && st.GetAttempt() != 1 {
		t.Fatalf("revocation must not burn attempts: %d", st.GetAttempt())
	}
	// The revoked id cannot enrol again, even with a fresh token.
	if _, err := register(e, t, "worker-a", mintToken(t, e, 60, 1), ""); code(err) != codes.PermissionDenied {
		t.Fatalf("a revoked id re-enrolled: %v", err)
	}
}

func TestRateLimitingThrottlesFloodsBeforeAuthentication(t *testing.T) {
	e := start(t, opts{acfg: func(c *auth.Config) { c.AnonRPS, c.AnonBurst = 5, 10 }})
	w := pb.NewWorkerServiceClient(e.conn(t, insecure.NewCredentials()))
	limited, other := 0, 0
	for i := 0; i < 60; i++ { // a token-guessing flood from one address
		_, err := w.Heartbeat(context.Background(), &pb.HeartbeatRequest{WorkerId: wid("worker-a")})
		switch code(err) {
		case codes.ResourceExhausted:
			limited++
		case codes.Unauthenticated:
			other++
		default:
			t.Fatalf("unexpected: %v", err)
		}
	}
	if limited < 40 || other > 20 {
		t.Fatalf("flood: %d throttled, %d reached authentication (burst 10)", limited, other)
	}
}

func TestPerWorkerRateLimitDoesNotStarveOtherWorkers(t *testing.T) {
	e := start(t, opts{acfg: func(c *auth.Config) { c.WorkerRPS, c.WorkerBurst = 5, 10; c.AnonRPS, c.AnonBurst = 1000, 2000 }})
	credA, credB := mustEnrol(t, e, "worker-a"), mustEnrol(t, e, "worker-b")
	w := pb.NewWorkerServiceClient(e.conn(t, insecure.NewCredentials()))
	hb := func(id, cred string) error {
		_, err := w.Heartbeat(withBearer(context.Background(), cred), &pb.HeartbeatRequest{WorkerId: wid(id), AvailableResources: &pb.ResourceSpec{CpuCores: 1}})
		return err
	}
	limited := 0
	for i := 0; i < 50; i++ {
		if code(hb("worker-a", credA)) == codes.ResourceExhausted {
			limited++
		}
	}
	if limited < 30 {
		t.Fatalf("a worker flooding the coordinator was not throttled (%d/50)", limited)
	}
	if err := hb("worker-b", credB); err != nil {
		t.Fatalf("a well-behaved worker was affected by another's flood: %v", err)
	}
}

func TestTLSIsRequiredToTalkAndTheServerIsVerified(t *testing.T) {
	e := start(t, opts{tls: true})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c := pb.NewClientServiceClient(e.conn(t, insecure.NewCredentials()))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err == nil {
		t.Fatal("plaintext client talked to a TLS server")
	}

	caPEM, _ := os.ReadFile(filepath.Join(e.certDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	good := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	c = pb.NewClientServiceClient(e.conn(t, good))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err != nil {
		t.Fatalf("TLS client with the right CA: %v", err)
	}

	// A client that trusts the system roots only (or a different CA) must refuse this server.
	other := credentials.NewTLS(&tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS13})
	c = pb.NewClientServiceClient(e.conn(t, other))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err == nil {
		t.Fatal("client accepted a server certificate from an unknown CA (man-in-the-middle possible)")
	}
	wrongName := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "evil.example", MinVersion: tls.VersionTLS13})
	c = pb.NewClientServiceClient(e.conn(t, wrongName))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err == nil {
		t.Fatal("client accepted a certificate for the wrong host name")
	}
}

func TestMutualTLSRequiresACertificateFromTheClusterCA(t *testing.T) {
	e := start(t, opts{tls: true, mtls: true})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	caPEM, _ := os.ReadFile(filepath.Join(e.certDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)

	noCert := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	c := pb.NewClientServiceClient(e.conn(t, noCert))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err == nil {
		t.Fatal("mTLS server accepted a client with no certificate")
	}

	cert, err := tls.LoadX509KeyPair(filepath.Join(e.certDir, "client-worker-a.crt"), filepath.Join(e.certDir, "client-worker-a.key"))
	if err != nil {
		t.Fatal(err)
	}
	withCert := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	c = pb.NewClientServiceClient(e.conn(t, withCert))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err != nil {
		t.Fatalf("client with a valid certificate: %v", err)
	}

	// A certificate from some other CA is no good, even if it is well-formed.
	rogue := t.TempDir()
	if err := auth.GenerateCertSet(rogue, []string{"localhost"}, []string{"worker-a"}); err != nil {
		t.Fatal(err)
	}
	rc, err := tls.LoadX509KeyPair(filepath.Join(rogue, "client-worker-a.crt"), filepath.Join(rogue, "client-worker-a.key"))
	if err != nil {
		t.Fatal(err)
	}
	rogueCreds := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{rc}, MinVersion: tls.VersionTLS13})
	c = pb.NewClientServiceClient(e.conn(t, rogueCreds))
	if _, err := c.ListWorkers(withBearer(ctx, apiToken), &pb.ListWorkersRequest{}); err == nil {
		t.Fatal("mTLS server accepted a certificate from a different CA")
	}
}

func TestArtifactStoreRequiresCredentials(t *testing.T) {
	e := start(t, opts{})
	cred := mustEnrol(t, e, "worker-a")
	h, err := httpapi.New(t.TempDir(), 1<<20, nil, e.auth)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	sha := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" // sha256("hello")
	do := func(method, tok, worker string, body string) int {
		req, _ := http.NewRequest(method, srv.URL+"/artifacts/"+sha, strings.NewReader(body))
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		if worker != "" {
			req.Header.Set("X-Worker-Id", worker)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := do("PUT", "", "", "hello"); code != http.StatusUnauthorized {
		t.Fatalf("anonymous upload: %d", code)
	}
	if code := do("PUT", cred, "worker-a", "hello"); code != http.StatusUnauthorized {
		t.Fatalf("a worker must not be able to upload artifacts: %d", code)
	}
	if code := do("PUT", apiToken, "", "hello"); code != http.StatusCreated {
		t.Fatalf("upload with the API token: %d", code)
	}
	if code := do("GET", "", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("anonymous download: %d", code)
	}
	if code := do("GET", cred, "worker-b", ""); code != http.StatusUnauthorized {
		t.Fatalf("worker-a's credential presented as worker-b: %d", code)
	}
	if code := do("GET", cred, "worker-a", ""); code != http.StatusOK {
		t.Fatalf("an enrolled worker's download: %d", code)
	}
	if code := do("GET", apiToken, "", ""); code != http.StatusOK {
		t.Fatalf("download with the API token: %d", code)
	}
}

func TestDevModeDisablesAuthLoudlyButStillWorks(t *testing.T) {
	e := start(t, opts{disabled: true})
	c := pb.NewClientServiceClient(e.conn(t, insecure.NewCredentials()))
	if _, err := c.ListWorkers(context.Background(), &pb.ListWorkersRequest{}); err != nil {
		t.Fatalf("auth disabled: %v", err)
	}
	if _, err := admin(t, e).CreateEnrollmentToken(context.Background(), &pb.CreateEnrollmentTokenRequest{TtlSeconds: 60, MaxUses: 1}); code(err) != codes.FailedPrecondition {
		t.Fatalf("enrollment makes no sense without auth: %v", err)
	}
}
