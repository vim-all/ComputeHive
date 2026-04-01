package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

type Bundle struct {
	TempDir     string
	ArchivePath string
	SHA256      string
	SizeBytes   int64
}

type Fetcher struct {
	client         *http.Client
	maxBytes       int64
	sigV4          *v4.Signer
	sigV4Creds     aws.Credentials
	sigV4Region    string
	sigV4Service   string
	signRequests   bool
}

func NewFetcher(timeout time.Duration, maxBytes int64) *Fetcher {
	return newWithClient(&http.Client{Timeout: timeout}, maxBytes, SigV4Config{})
}

type SigV4Config struct {
	Enabled         bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Service         string
}

func NewFetcherWithSigV4(timeout time.Duration, maxBytes int64, sigV4Config SigV4Config) *Fetcher {
	return newWithClient(&http.Client{Timeout: timeout}, maxBytes, sigV4Config)
}

func newWithClient(client *http.Client, maxBytes int64, sigV4Config SigV4Config) *Fetcher {
	fetcher := &Fetcher{
		client:       client,
		maxBytes:     maxBytes,
		sigV4Region:  strings.TrimSpace(sigV4Config.Region),
		sigV4Service: strings.TrimSpace(sigV4Config.Service),
		signRequests: sigV4Config.Enabled,
	}

	if fetcher.sigV4Region == "" {
		fetcher.sigV4Region = "auto"
	}
	if fetcher.sigV4Service == "" {
		fetcher.sigV4Service = "s3"
	}

	if fetcher.signRequests {
		accessKeyID := strings.TrimSpace(sigV4Config.AccessKeyID)
		secretAccessKey := strings.TrimSpace(sigV4Config.SecretAccessKey)
		if accessKeyID == "" || secretAccessKey == "" {
			fetcher.signRequests = false
		} else {
			fetcher.sigV4 = v4.NewSigner()
			fetcher.sigV4Creds = aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				SessionToken:    strings.TrimSpace(sigV4Config.SessionToken),
				Source:          "worker-artifact-fetcher",
			}
		}
	}

	return fetcher
}

func (f *Fetcher) Fetch(ctx context.Context, job domain.Job) (Bundle, error) {
	job.Normalize()
	if err := job.Validate(); err != nil {
		return Bundle{}, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, job.ArtifactURL, nil)
	if err != nil {
		return Bundle{}, fmt.Errorf("create artifact request: %w", err)
	}
	if err := f.signIfConfigured(ctx, request); err != nil {
		return Bundle{}, fmt.Errorf("sign artifact request: %w", err)
	}

	response, err := f.client.Do(request)
	if err != nil {
		return Bundle{}, fmt.Errorf("download artifact: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return Bundle{}, fmt.Errorf("artifact download failed with status %s", response.Status)
	}
	if response.ContentLength > 0 && response.ContentLength > f.maxBytes {
		return Bundle{}, fmt.Errorf("artifact is too large: %d bytes exceeds limit %d", response.ContentLength, f.maxBytes)
	}

	tempDir, err := os.MkdirTemp("", "computehive-artifact-*")
	if err != nil {
		return Bundle{}, fmt.Errorf("create temp dir: %w", err)
	}

	bundle := Bundle{
		TempDir:     tempDir,
		ArchivePath: filepath.Join(tempDir, archiveName(job.ID)),
	}

	if err := f.downloadIntoBundle(response.Body, job.ArtifactSHA256, &bundle); err != nil {
		_ = Cleanup(bundle)
		return Bundle{}, err
	}

	return bundle, nil
}

func Cleanup(bundle Bundle) error {
	if strings.TrimSpace(bundle.TempDir) == "" {
		return nil
	}

	return os.RemoveAll(bundle.TempDir)
}

func (f *Fetcher) downloadIntoBundle(body io.Reader, expectedSHA256 string, bundle *Bundle) error {
	file, err := os.Create(bundle.ArchivePath)
	if err != nil {
		return fmt.Errorf("create artifact file: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	reader := io.Reader(body)
	if f.maxBytes > 0 {
		reader = io.LimitReader(body, f.maxBytes+1)
	}

	written, err := io.Copy(io.MultiWriter(file, hasher), reader)
	if err != nil {
		return fmt.Errorf("write artifact file: %w", err)
	}
	if f.maxBytes > 0 && written > f.maxBytes {
		return fmt.Errorf("artifact exceeds configured max size of %d bytes", f.maxBytes)
	}

	actualSHA256 := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(strings.TrimSpace(expectedSHA256), actualSHA256) {
		return fmt.Errorf("artifact sha256 mismatch: expected %s, got %s", expectedSHA256, actualSHA256)
	}

	bundle.SHA256 = actualSHA256
	bundle.SizeBytes = written
	return nil
}

func (f *Fetcher) signIfConfigured(ctx context.Context, request *http.Request) error {
	if !f.signRequests || f.sigV4 == nil {
		return nil
	}

	const payloadHash = "UNSIGNED-PAYLOAD"
	request.Header.Set("x-amz-content-sha256", payloadHash)
	return f.sigV4.SignHTTP(ctx, f.sigV4Creds, request, payloadHash, f.sigV4Service, f.sigV4Region, time.Now().UTC())
}

func archiveName(jobID string) string {
	clean := strings.Map(func(char rune) rune {
		switch {
		case char >= 'a' && char <= 'z':
			return char
		case char >= 'A' && char <= 'Z':
			return char
		case char >= '0' && char <= '9':
			return char
		case char == '-', char == '_':
			return char
		default:
			return '-'
		}
	}, jobID)
	if strings.TrimSpace(clean) == "" {
		clean = "artifact"
	}

	return clean + ".tar.gz"
}
