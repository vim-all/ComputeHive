package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

func digestOf(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func put(t *testing.T, c *Cache, body []byte) string {
	t.Helper()
	src := filepath.Join(t.TempDir(), "a.tar.gz")
	if err := os.WriteFile(src, body, 0o600); err != nil {
		t.Fatal(err)
	}
	d := digestOf(body)
	if err := c.Put(d, src); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCacheRoundTripsAndKeepsEntriesPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	c, err := NewCache(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("image bytes")
	d := put(t, c, body)
	f, size, ok := c.Open(d)
	if !ok {
		t.Fatal("miss right after put")
	}
	defer f.Close()
	got, _ := io.ReadAll(f)
	if !bytes.Equal(got, body) || size != int64(len(body)) {
		t.Fatalf("got %q size %d", got, size)
	}
	if info, _ := os.Stat(filepath.Join(dir, d)); info.Mode().Perm() != 0o600 {
		t.Fatalf("entry mode %v, want 0600", info.Mode().Perm())
	}
	if info, _ := os.Stat(dir); info.Mode().Perm() != 0o700 {
		t.Fatalf("cache dir mode %v, want 0700", info.Mode().Perm())
	}
	if ds := c.Digests(10); len(ds) != 1 || ds[0] != d {
		t.Fatalf("digests %v", ds)
	}
}

func TestCacheDiscardsAnEntryThatWasTamperedWith(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewCache(dir, 1<<20)
	d := put(t, c, []byte("original"))
	if err := os.WriteFile(filepath.Join(dir, d), []byte("evil"), 0o600); err != nil {
		t.Fatal(err)
	}
	if f, _, ok := c.Open(d); ok {
		f.Close()
		t.Fatal("a modified cache entry was served")
	}
	if _, err := os.Stat(filepath.Join(dir, d)); err == nil {
		t.Fatal("the corrupted entry was left in the cache")
	}
}

func TestCacheEvictsLeastRecentlyUsedFirst(t *testing.T) {
	c, _ := NewCache(t.TempDir(), 25) // room for two 10-byte entries
	a := put(t, c, bytes.Repeat([]byte("a"), 10))
	b := put(t, c, bytes.Repeat([]byte("b"), 10))
	past := time.Now().Add(-time.Hour)
	_ = os.Chtimes(filepath.Join(c.dir, a), past, past)
	_ = os.Chtimes(filepath.Join(c.dir, b), past.Add(time.Minute), past.Add(time.Minute))
	if f, _, ok := c.Open(a); ok { // touching a makes b the oldest
		f.Close()
	}
	d := put(t, c, bytes.Repeat([]byte("c"), 10))
	have := map[string]bool{}
	for _, x := range c.Digests(0) {
		have[x] = true
	}
	if have[b] || !have[a] || !have[d] {
		t.Fatalf("expected b evicted, a and c kept: %v", have)
	}
}

func TestCacheRefusesBadDigestsAndOversizedEntriesAndCanBeDisabled(t *testing.T) {
	c, _ := NewCache(t.TempDir(), 5)
	src := filepath.Join(t.TempDir(), "big")
	_ = os.WriteFile(src, []byte("0123456789"), 0o600)
	_ = c.Put(digestOf([]byte("0123456789")), src)
	if len(c.Digests(0)) != 0 {
		t.Fatal("an entry larger than the whole cache was kept")
	}
	for _, bad := range []string{"", "../../etc/passwd", "zz", strings.Repeat("A", 64) + "x"} {
		if err := c.Put(bad, src); err != nil {
			t.Errorf("Put(%q) = %v", bad, err)
		}
		if _, _, ok := c.Open(bad); ok {
			t.Errorf("Open(%q) hit", bad)
		}
	}
	if entries, _ := os.ReadDir(c.dir); len(entries) != 0 {
		t.Fatalf("junk written into the cache directory: %v", entries)
	}
	off, _ := NewCache(t.TempDir(), 0)
	d := put(t, off, []byte("x"))
	if _, _, ok := off.Open(d); ok || off.Digests(0) != nil {
		t.Fatal("a disabled cache stored something")
	}
	var nilCache *Cache
	if _, _, ok := nilCache.Open(d); ok || nilCache.Digests(0) != nil || nilCache.Put(d, src) != nil {
		t.Fatal("a nil cache must behave as disabled")
	}
}

func TestFetcherServesRepeatArtifactsFromTheCacheWithoutTheNetwork(t *testing.T) {
	body := []byte("the image archive")
	sum := digestOf(body)
	var requests atomic.Int32
	f := newWithClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Header: http.Header{}}, nil
	})}, 1024, SigV4Config{})
	c, _ := NewCache(t.TempDir(), 1<<20)
	f.WithCache(c)

	job := domain.Job{ID: "j1", ImageRef: "img", ArtifactURL: "https://artifact.test/x.tar.gz", ArtifactSHA256: sum}
	for i := 0; i < 3; i++ {
		b, err := f.Fetch(context.Background(), job)
		if err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		if got, _ := os.ReadFile(b.ArchivePath); !bytes.Equal(got, body) || b.SHA256 != sum {
			t.Fatalf("fetch %d: bundle holds %q (sha %s)", i, got, b.SHA256)
		}
		// A job that mutates its bundle must not poison the cache.
		_ = os.WriteFile(b.ArchivePath, []byte("scribble"), 0o600)
		_ = Cleanup(b)
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("%d downloads for three identical jobs, want 1", n)
	}
	if ds := f.CachedDigests(10); len(ds) != 1 || ds[0] != sum {
		t.Fatalf("advertised digests %v", ds)
	}
}

func TestFetcherDoesNotCacheADownloadThatFailsVerification(t *testing.T) {
	f := newWithClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("tampered")), Header: http.Header{}}, nil
	})}, 1024, SigV4Config{})
	c, _ := NewCache(t.TempDir(), 1<<20)
	f.WithCache(c)
	want := digestOf([]byte("what the job declared"))
	if _, err := f.Fetch(context.Background(), domain.Job{ID: "j", ImageRef: "img", ArtifactURL: "https://artifact.test/x", ArtifactSHA256: want}); err == nil {
		t.Fatal("a mismatching artifact was accepted")
	}
	if len(c.Digests(0)) != 0 {
		t.Fatal("unverified bytes reached the cache")
	}
}
