// Package auth is the coordinator's trust layer: expiring enrollment tokens, per-worker revocable
// credentials, the operator API token, TLS/mTLS certificates, and rate limiting. Secrets are only
// ever stored as SHA-256 hashes, so a Redis dump does not yield usable credentials.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Errors returned by Enroll.
var (
	ErrBadToken        = errors.New("enrollment token is invalid, expired or used up")
	ErrAlreadyEnrolled = errors.New("this worker id is already enrolled; present its credential instead of a token")
	ErrRevoked         = errors.New("this worker id has been revoked")
)

// WorkerIDPattern is the only form of worker id the coordinator accepts.
var WorkerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Config controls authentication and rate limits.
type Config struct {
	APIToken string // secret required for submitters and operators; empty with Disabled=false is invalid
	Disabled bool   // dev mode only: no authentication at all (logged loudly)

	// Token-bucket limits, requests per second and burst, per identity.
	WorkerRPS, WorkerBurst float64 // a worker, keyed by worker id
	ClientRPS, ClientBurst float64 // a submitter/operator, keyed by API token
	AnonRPS, AnonBurst     float64 // anyone, keyed by source address, applied before any authentication
}

// DefaultConfig: generous for honest workers (they send about 4 requests a second), tight for strangers.
func DefaultConfig() Config {
	return Config{WorkerRPS: 50, WorkerBurst: 100, ClientRPS: 50, ClientBurst: 100, AnonRPS: 20, AnonBurst: 40}
}

// Service holds the auth state, which lives in Redis so every coordinator replica agrees.
type Service struct {
	rdb *redis.Client
	cfg Config
	log *slog.Logger

	mu       sync.Mutex
	limiters map[string]*limiterEntry
}

// New returns a Service.
func New(rdb *redis.Client, cfg Config, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Disabled {
		log.Warn("AUTHENTICATION IS DISABLED: anyone who can reach this coordinator can submit jobs, enrol workers and read results. Development only.")
	}
	return &Service{rdb: rdb, cfg: cfg, log: log, limiters: map[string]*limiterEntry{}}
}

// Disabled reports whether authentication is switched off.
func (s *Service) Disabled() bool { return s.cfg.Disabled }

func hashHex(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

func randomSecret(prefix string) string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return prefix + hex.EncodeToString(b[:])
}

// CreateEnrollmentToken makes a token that lets up to uses new workers enrol before it expires.
// The token is returned once and never stored; Redis holds only its hash, with a TTL.
func (s *Service) CreateEnrollmentToken(ctx context.Context, ttl time.Duration, uses int, note string) (string, time.Time, error) {
	if ttl < time.Second || ttl > 7*24*time.Hour {
		return "", time.Time{}, errors.New("ttl must be between 1 second and 7 days")
	}
	if uses < 1 || uses > 1000 {
		return "", time.Time{}, errors.New("uses must be between 1 and 1000")
	}
	if len(note) > 200 {
		note = note[:200]
	}
	token := randomSecret("chet_")
	key := "ch:enroll:" + hashHex(token)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, "uses_left", uses, "note", note, "created_ms", time.Now().UnixMilli())
	pipe.PExpire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return "", time.Time{}, fmt.Errorf("store enrollment token: %w", err)
	}
	return token, time.Now().Add(ttl), nil
}

var enrollScript = redis.NewScript(`
-- ARGV: token_hash, worker_id, credential_hash
if redis.call('SISMEMBER', 'ch:revoked', ARGV[2]) == 1 then return 'REVOKED' end
-- A token holder must not be able to take over an identity that already has a credential.
if redis.call('EXISTS', 'ch:cred:' .. ARGV[2]) == 1 then return 'EXISTS' end
local tk = 'ch:enroll:' .. ARGV[1]
local left = tonumber(redis.call('HGET', tk, 'uses_left'))
if not left or left <= 0 then return 'BADTOKEN' end
redis.call('HINCRBY', tk, 'uses_left', -1)
redis.call('SET', 'ch:cred:' .. ARGV[2], ARGV[3])
return 'OK'
`)

// Enroll validates token and, if valid, issues workerID its credential. It is atomic: a token with one
// use left cannot enrol two workers, and an existing identity can never be re-enrolled.
func (s *Service) Enroll(ctx context.Context, workerID, token string) (string, error) {
	if !WorkerIDPattern.MatchString(workerID) {
		return "", errors.New("invalid worker id")
	}
	if token == "" {
		return "", ErrBadToken
	}
	cred := randomSecret("chwc_")
	res, err := enrollScript.Run(ctx, s.rdb, nil, hashHex(token), workerID, hashHex(cred)).Result()
	if err != nil {
		return "", fmt.Errorf("enroll: %w", err)
	}
	switch res {
	case "OK":
		s.log.Info("worker enrolled", "event", "worker_enrolled", "worker_id", workerID)
		return cred, nil
	case "REVOKED":
		return "", ErrRevoked
	case "EXISTS":
		return "", ErrAlreadyEnrolled
	default:
		return "", ErrBadToken
	}
}

// WorkerStatus is the outcome of checking a worker credential.
type WorkerStatus int

const (
	WorkerOK WorkerStatus = iota
	WorkerBadCredential
	WorkerRevoked
)

// VerifyWorker checks credential against the stored hash in constant time.
func (s *Service) VerifyWorker(ctx context.Context, workerID, credential string) (WorkerStatus, error) {
	if workerID == "" || credential == "" {
		return WorkerBadCredential, nil
	}
	revoked, err := s.rdb.SIsMember(ctx, "ch:revoked", workerID).Result()
	if err != nil {
		return WorkerBadCredential, fmt.Errorf("check revocation: %w", err)
	}
	if revoked {
		return WorkerRevoked, nil
	}
	want, err := s.rdb.Get(ctx, "ch:cred:"+workerID).Result()
	if errors.Is(err, redis.Nil) {
		return WorkerBadCredential, nil
	}
	if err != nil {
		return WorkerBadCredential, fmt.Errorf("read credential: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(want), []byte(hashHex(credential))) == 1 {
		return WorkerOK, nil
	}
	return WorkerBadCredential, nil
}

// Revoke destroys the worker's credential and bans its id. Later calls from it are refused.
func (s *Service) Revoke(ctx context.Context, workerID string) error {
	if !WorkerIDPattern.MatchString(workerID) {
		return errors.New("invalid worker id")
	}
	pipe := s.rdb.TxPipeline()
	pipe.SAdd(ctx, "ch:revoked", workerID)
	pipe.Del(ctx, "ch:cred:"+workerID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("revoke %s: %w", workerID, err)
	}
	s.log.Warn("worker revoked", "event", "worker_revoked", "worker_id", workerID)
	return nil
}

// VerifyAPIToken checks the operator/submitter token in constant time.
func (s *Service) VerifyAPIToken(token string) bool {
	if s.cfg.APIToken == "" {
		return false
	}
	token = strings.TrimSpace(token)
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.APIToken)) == 1
}

// HTTPWriteAllowed: uploading an artifact needs the API token.
func (s *Service) HTTPWriteAllowed(r *http.Request) bool {
	if s.cfg.Disabled {
		return true
	}
	tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return s.VerifyAPIToken(tok)
}

// HTTPReadAllowed: downloading an artifact needs the API token or an enrolled worker's credential.
func (s *Service) HTTPReadAllowed(r *http.Request) bool {
	if s.cfg.Disabled {
		return true
	}
	tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if s.VerifyAPIToken(tok) {
		return true
	}
	if id := r.Header.Get("X-Worker-Id"); id != "" {
		st, err := s.VerifyWorker(r.Context(), id, strings.TrimSpace(tok))
		return err == nil && st == WorkerOK
	}
	return false
}

// WorkerFromRequest authenticates an HTTP request as an enrolled worker (X-Worker-Id plus its bearer credential).
func (s *Service) WorkerFromRequest(r *http.Request) (string, bool) {
	if s.cfg.Disabled {
		return r.Header.Get("X-Worker-Id"), r.Header.Get("X-Worker-Id") != ""
	}
	id := r.Header.Get("X-Worker-Id")
	tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if id == "" {
		return "", false
	}
	st, err := s.VerifyWorker(r.Context(), id, strings.TrimSpace(tok))
	return id, err == nil && st == WorkerOK
}

// AllowAnon rate-limits unauthenticated HTTP clients by source address.
func (s *Service) AllowAnon(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	return s.allow("ip:"+host, s.cfg.AnonRPS, s.cfg.AnonBurst)
}
