package output

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

const unsignedPayloadHash = "UNSIGNED-PAYLOAD"

type Config struct {
	BucketURL       string
	PublicBucketURL string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Service         string
	Timeout         time.Duration
	MaxFiles        int
	MaxBytes        int64

	// Self-hosted mode: with no object-storage bucket, outputs are stored on the coordinator.
	CoordinatorURL    string                            // e.g. https://coordinator:8080
	CoordinatorClient *http.Client                      // trusts the cluster CA
	CoordinatorAuth   func(setHeader func(k, v string)) // worker credential headers
}

type Manager struct {
	client          *http.Client
	signer          *v4.Signer
	creds           aws.Credentials
	endpointURL     string
	bucketName      string
	bucketAPIURL    string
	bucketPublicURL string
	region          string
	service         string
	maxFiles        int
	maxBytes        int64
	enabled         bool

	coordURL    string
	coordClient *http.Client
	coordAuth   func(setHeader func(k, v string))
}

type UploadResult struct {
	ManifestURI       string
	ManifestAPIURL    string
	ManifestPublicURL string
	TotalSizeBytes    int64
	Artifacts         []domain.OutputArtifact
}

func NewManager(cfg Config) (*Manager, error) {
	manager := &Manager{
		client:   &http.Client{Timeout: cfg.Timeout},
		region:   strings.TrimSpace(cfg.Region),
		service:  strings.TrimSpace(cfg.Service),
		maxFiles: cfg.MaxFiles,
		maxBytes: cfg.MaxBytes,
	}

	if manager.region == "" {
		manager.region = "auto"
	}
	if manager.service == "" {
		manager.service = "s3"
	}

	bucketURL := strings.TrimSpace(cfg.BucketURL)
	if bucketURL == "" {
		if u := strings.TrimRight(strings.TrimSpace(cfg.CoordinatorURL), "/"); u != "" {
			manager.coordURL, manager.coordAuth = u, cfg.CoordinatorAuth
			manager.coordClient = cfg.CoordinatorClient
			if manager.coordClient == nil {
				manager.coordClient = &http.Client{Timeout: cfg.Timeout}
			}
		}
		return manager, nil
	}

	if strings.TrimSpace(cfg.AccessKeyID) == "" || strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return nil, fmt.Errorf("object storage upload requires S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY")
	}

	endpointURL, bucketName, bucketAPIURL, err := parseBucketURL(bucketURL)
	if err != nil {
		return nil, err
	}

	manager.signer = v4.NewSigner()
	manager.creds = aws.Credentials{
		AccessKeyID:     strings.TrimSpace(cfg.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(cfg.SecretAccessKey),
		SessionToken:    strings.TrimSpace(cfg.SessionToken),
		Source:          "worker-output-uploader",
	}
	manager.endpointURL = endpointURL
	manager.bucketName = bucketName
	manager.bucketAPIURL = bucketAPIURL
	manager.bucketPublicURL = strings.TrimSpace(cfg.PublicBucketURL)
	if manager.bucketPublicURL == "" {
		manager.bucketPublicURL = bucketAPIURL
	}
	manager.enabled = true

	return manager, nil
}

func (m *Manager) CollectAndUpload(ctx context.Context, jobID, outputDir string) (UploadResult, error) {
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" {
		return UploadResult{}, nil
	}

	files, totalSize, err := m.collectFiles(outputDir)
	if err != nil {
		return UploadResult{}, err
	}
	if len(files) == 0 {
		return UploadResult{}, nil
	}
	if !m.enabled {
		if m.coordURL != "" {
			return m.uploadToCoordinator(ctx, jobID, files, totalSize)
		}
		return UploadResult{}, fmt.Errorf("job produced %d output file(s), but no output storage is configured (set S3_BUCKET, or point the worker at a coordinator with COORDINATOR_HTTP_URL)", len(files))
	}

	artifacts := make([]domain.OutputArtifact, 0, len(files))
	uploadedByHash := make(map[string]domain.OutputArtifact, len(files))
	for _, file := range files {
		blob, ok := uploadedByHash[file.SHA256]
		if !ok {
			blob, err = m.ensureBlob(ctx, file)
			if err != nil {
				return UploadResult{}, err
			}
			uploadedByHash[file.SHA256] = blob
		}

		artifacts = append(artifacts, domain.OutputArtifact{
			RelativePath: file.RelativePath,
			ObjectKey:    blob.ObjectKey,
			URI:          blob.URI,
			APIURL:       blob.APIURL,
			PublicURL:    blob.PublicURL,
			SHA256:       file.SHA256,
			SizeBytes:    file.SizeBytes,
			ContentType:  file.ContentType,
		})
	}

	manifestBytes, err := json.Marshal(struct {
		JobID           string                  `json:"job_id"`
		GeneratedAtUnix int64                   `json:"generated_at_unix"`
		TotalSizeBytes  int64                   `json:"total_size_bytes"`
		Artifacts       []domain.OutputArtifact `json:"artifacts"`
	}{
		JobID:           strings.TrimSpace(jobID),
		GeneratedAtUnix: time.Now().UTC().Unix(),
		TotalSizeBytes:  totalSize,
		Artifacts:       artifacts,
	})
	if err != nil {
		return UploadResult{}, fmt.Errorf("marshal output manifest: %w", err)
	}

	manifestSHA := sha256.Sum256(manifestBytes)
	manifestHex := hex.EncodeToString(manifestSHA[:])
	manifestKey := manifestObjectKey(jobID)
	manifestAPIURL, manifestPublicURL, err := m.uploadBytes(ctx, manifestKey, manifestBytes, manifestHex, "application/json")
	if err != nil {
		return UploadResult{}, err
	}

	return UploadResult{
		ManifestURI:       objectURI(m.bucketName, manifestKey),
		ManifestAPIURL:    manifestAPIURL,
		ManifestPublicURL: manifestPublicURL,
		TotalSizeBytes:    totalSize,
		Artifacts:         artifacts,
	}, nil
}

type collectedFile struct {
	Path         string
	RelativePath string
	SHA256       string
	SizeBytes    int64
	ContentType  string
}

func (m *Manager) collectFiles(root string) ([]collectedFile, int64, error) {
	files := make([]collectedFile, 0)
	var totalSize int64

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("read output file info %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if len(files) >= m.maxFiles {
			return fmt.Errorf("job outputs exceed configured limit of %d files", m.maxFiles)
		}

		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("build relative output path for %s: %w", path, err)
		}
		relativePath = filepath.ToSlash(filepath.Clean(relativePath))
		if strings.HasPrefix(relativePath, "../") || relativePath == "." {
			return fmt.Errorf("invalid output path %s", relativePath)
		}

		fileSHA, sizeBytes, contentType, err := inspectFile(path)
		if err != nil {
			return err
		}
		totalSize += sizeBytes
		if totalSize > m.maxBytes {
			return fmt.Errorf("job outputs exceed configured limit of %d bytes", m.maxBytes)
		}

		files = append(files, collectedFile{
			Path:         path,
			RelativePath: relativePath,
			SHA256:       fileSHA,
			SizeBytes:    sizeBytes,
			ContentType:  contentType,
		})
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].RelativePath < files[j].RelativePath
	})
	return files, totalSize, nil
}

func inspectFile(path string) (string, int64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, "", fmt.Errorf("open output file %s: %w", path, err)
	}
	defer file.Close()

	hasher := sha256.New()
	header := make([]byte, 512)
	headerLen := 0
	var size int64
	buffer := make([]byte, 64*1024)

	for {
		n, err := file.Read(buffer)
		if n > 0 {
			if headerLen < len(header) {
				copyLen := min(len(header)-headerLen, n)
				copy(header[headerLen:], buffer[:copyLen])
				headerLen += copyLen
			}
			if _, hashErr := hasher.Write(buffer[:n]); hashErr != nil {
				return "", 0, "", fmt.Errorf("hash output file %s: %w", path, hashErr)
			}
			size += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, "", fmt.Errorf("read output file %s: %w", path, err)
		}
	}

	contentType := mime.TypeByExtension(strings.ToLower(filepath.Ext(path)))
	if contentType == "" {
		contentType = http.DetectContentType(header[:headerLen])
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return hex.EncodeToString(hasher.Sum(nil)), size, contentType, nil
}

func (m *Manager) ensureBlob(ctx context.Context, file collectedFile) (domain.OutputArtifact, error) {
	objectKey := blobObjectKey(file.SHA256)
	exists, err := m.objectExists(ctx, objectKey)
	if err != nil {
		return domain.OutputArtifact{}, err
	}
	if !exists {
		if _, _, err := m.uploadFile(ctx, objectKey, file.Path, file.SHA256, file.ContentType); err != nil {
			return domain.OutputArtifact{}, err
		}
	}

	return domain.OutputArtifact{
		ObjectKey:   objectKey,
		URI:         objectURI(m.bucketName, objectKey),
		APIURL:      joinBucketAPIURL(m.bucketAPIURL, objectKey),
		PublicURL:   joinBucketAPIURL(m.bucketPublicURL, objectKey),
		SHA256:      file.SHA256,
		SizeBytes:   file.SizeBytes,
		ContentType: file.ContentType,
	}, nil
}

func (m *Manager) objectExists(ctx context.Context, objectKey string) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, joinBucketAPIURL(m.bucketAPIURL, objectKey), nil)
	if err != nil {
		return false, fmt.Errorf("create object HEAD request: %w", err)
	}
	if err := m.sign(ctx, request, unsignedPayloadHash); err != nil {
		return false, fmt.Errorf("sign object HEAD request: %w", err)
	}

	response, err := m.client.Do(request)
	if err != nil {
		return false, fmt.Errorf("check output object existence: %w", err)
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("output object HEAD failed with status %s", response.Status)
	}
}

func (m *Manager) uploadFile(ctx context.Context, objectKey, filePath, payloadSHA256, contentType string) (string, string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", fmt.Errorf("open output file for upload %s: %w", filePath, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", "", fmt.Errorf("inspect output file for upload %s: %w", filePath, err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, joinBucketAPIURL(m.bucketAPIURL, objectKey), file)
	if err != nil {
		return "", "", fmt.Errorf("create output upload request: %w", err)
	}
	request.ContentLength = info.Size()
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("x-amz-meta-file-sha256", payloadSHA256)
	if err := m.sign(ctx, request, payloadSHA256); err != nil {
		return "", "", fmt.Errorf("sign output upload request: %w", err)
	}

	response, err := m.client.Do(request)
	if err != nil {
		return "", "", fmt.Errorf("upload output file: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return "", "", fmt.Errorf("upload output file failed with status %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	return joinBucketAPIURL(m.bucketAPIURL, objectKey), joinBucketAPIURL(m.bucketPublicURL, objectKey), nil
}

func (m *Manager) uploadBytes(ctx context.Context, objectKey string, payload []byte, payloadSHA256, contentType string) (string, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, joinBucketAPIURL(m.bucketAPIURL, objectKey), bytes.NewReader(payload))
	if err != nil {
		return "", "", fmt.Errorf("create manifest upload request: %w", err)
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("x-amz-meta-file-sha256", payloadSHA256)
	if err := m.sign(ctx, request, payloadSHA256); err != nil {
		return "", "", fmt.Errorf("sign manifest upload request: %w", err)
	}

	response, err := m.client.Do(request)
	if err != nil {
		return "", "", fmt.Errorf("upload output manifest: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return "", "", fmt.Errorf("upload output manifest failed with status %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	return joinBucketAPIURL(m.bucketAPIURL, objectKey), joinBucketAPIURL(m.bucketPublicURL, objectKey), nil
}

func (m *Manager) sign(ctx context.Context, request *http.Request, payloadHash string) error {
	request.Header.Set("x-amz-content-sha256", payloadHash)
	return m.signer.SignHTTP(ctx, m.creds, request, payloadHash, m.service, m.region, time.Now().UTC())
}

func parseBucketURL(bucketURL string) (string, string, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(bucketURL))
	if err != nil {
		return "", "", "", fmt.Errorf("parse S3_BUCKET: %w", err)
	}
	host := parsed.Hostname()
	if strings.TrimSpace(host) == "" {
		return "", "", "", fmt.Errorf("S3_BUCKET must include a host")
	}

	var bucketName string
	for _, segment := range strings.Split(parsed.EscapedPath(), "/") {
		if strings.TrimSpace(segment) != "" {
			bucketName = strings.TrimSpace(segment)
			break
		}
	}
	if bucketName == "" {
		return "", "", "", fmt.Errorf("S3_BUCKET must include a bucket name in the path")
	}

	endpointURL := parsed.Scheme + "://" + parsed.Host
	bucketAPIURL := strings.TrimRight(endpointURL, "/") + "/" + bucketName
	return endpointURL, bucketName, bucketAPIURL, nil
}

func joinBucketAPIURL(baseURL, objectKey string) string {
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(objectKey, "/")
}

func objectURI(bucketName, objectKey string) string {
	return "s3://" + strings.TrimSpace(bucketName) + "/" + strings.TrimLeft(objectKey, "/")
}

func blobObjectKey(sha256Hex string) string {
	return "computehive/job-output-blobs/sha256/" + strings.TrimSpace(sha256Hex)
}

func manifestObjectKey(jobID string) string {
	return "computehive/job-results/" + sanitizePathComponent(jobID) + "/manifest.json"
}

func sanitizePathComponent(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "job"
	}

	var builder strings.Builder
	builder.Grow(len(value))
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z':
			builder.WriteRune(char)
		case char >= 'A' && char <= 'Z':
			builder.WriteRune(char)
		case char >= '0' && char <= '9':
			builder.WriteRune(char)
		case char == '-', char == '_', char == '.':
			builder.WriteRune(char)
		default:
			builder.WriteByte('-')
		}
	}

	clean := strings.Trim(builder.String(), "-")
	if clean == "" {
		return "job"
	}
	return clean
}

// uploadToCoordinator stores each output file on the coordinator. The coordinator accepts a file only
// from the worker that currently holds the job's lease and verifies the SHA-256 we send.
func (m *Manager) uploadToCoordinator(ctx context.Context, jobID string, files []collectedFile, total int64) (UploadResult, error) {
	artifacts := make([]domain.OutputArtifact, 0, len(files))
	for _, f := range files {
		uri, err := m.putOne(ctx, jobID, f)
		if err != nil {
			return UploadResult{}, err
		}
		artifacts = append(artifacts, domain.OutputArtifact{
			RelativePath: f.RelativePath, URI: uri, SHA256: f.SHA256, SizeBytes: f.SizeBytes, ContentType: f.ContentType,
		})
	}
	return UploadResult{TotalSizeBytes: total, Artifacts: artifacts}, nil
}

func (m *Manager) putOne(ctx context.Context, jobID string, f collectedFile) (string, error) {
	file, err := os.Open(f.Path)
	if err != nil {
		return "", fmt.Errorf("open output file %s: %w", f.RelativePath, err)
	}
	defer file.Close()
	target := m.coordURL + "/outputs/" + url.PathEscape(jobID) + "/" + escapePath(f.RelativePath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, file)
	if err != nil {
		return "", err
	}
	req.ContentLength = f.SizeBytes
	req.Header.Set("X-Content-SHA256", f.SHA256)
	req.Header.Set("Content-Type", "application/octet-stream")
	if m.coordAuth != nil {
		m.coordAuth(req.Header.Set)
	}
	resp, err := m.coordClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload %s to the coordinator: %w", f.RelativePath, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("coordinator refused output %s: %s: %s", f.RelativePath, resp.Status, strings.TrimSpace(string(body)))
	}
	var out struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.URI == "" {
		return "", fmt.Errorf("coordinator gave no URI for %s", f.RelativePath)
	}
	return out.URI, nil
}

func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}
