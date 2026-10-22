package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Cache is a content-addressed, size-bounded, least-recently-used store of verified artifacts. A job
// whose artifact is already here skips the download, and the worker advertises what it holds so the
// scheduler can prefer it (locality).
//
// The cache is keyed by SHA-256, and an entry is only ever written after its bytes were hashed, so a
// job cannot get one artifact served in place of another. Reads re-hash, so a file that was tampered
// with or corrupted on disk is discarded instead of being run.
type Cache struct {
	dir      string
	maxBytes int64

	mu sync.Mutex
}

// NewCache opens (creating, mode 0700) a cache holding at most maxBytes. maxBytes <= 0 disables it.
func NewCache(dir string, maxBytes int64) (*Cache, error) {
	c := &Cache{dir: dir, maxBytes: maxBytes}
	if maxBytes <= 0 {
		return c, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create artifact cache %s: %w", dir, err)
	}
	// Leftovers from a crash mid-write are never valid entries.
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".tmp-") {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
	return c, nil
}

func (c *Cache) enabled() bool { return c != nil && c.maxBytes > 0 }

func (c *Cache) path(digest string) string { return filepath.Join(c.dir, digest) }

// Open returns a verified handle on the entry for digest, or ok=false. The caller closes the file.
func (c *Cache) Open(digest string) (f *os.File, size int64, ok bool) {
	digest = strings.ToLower(strings.TrimSpace(digest))
	if !c.enabled() || !digestPattern.MatchString(digest) {
		return nil, 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	file, err := os.Open(c.path(digest))
	if err != nil {
		return nil, 0, false
	}
	h := sha256.New()
	n, err := io.Copy(h, file)
	if err != nil || hex.EncodeToString(h.Sum(nil)) != digest {
		file.Close()
		_ = os.Remove(c.path(digest)) // corrupted or tampered with: never serve it
		return nil, 0, false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, 0, false
	}
	now := time.Now()
	_ = os.Chtimes(c.path(digest), now, now) // mark as recently used
	return file, n, true
}

// Put stores the file at src (whose SHA-256 the caller has already verified to be digest) and evicts
// the least recently used entries until the cache fits. An artifact larger than the whole cache is not kept.
func (c *Cache) Put(digest, src string) error {
	digest = strings.ToLower(strings.TrimSpace(digest))
	if !c.enabled() || !digestPattern.MatchString(digest) {
		return nil
	}
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.Size() > c.maxBytes {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	tmp, err := os.CreateTemp(c.dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	in, err := os.Open(src)
	if err != nil {
		tmp.Close()
		return err
	}
	defer in.Close()
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, c.path(digest)); err != nil {
		return err
	}
	c.evictLocked(digest)
	return nil
}

type cacheEntry struct {
	digest string
	size   int64
	used   time.Time
}

func (c *Cache) entriesLocked() []cacheEntry {
	files, err := os.ReadDir(c.dir)
	if err != nil {
		return nil
	}
	var out []cacheEntry
	for _, f := range files {
		if !digestPattern.MatchString(f.Name()) {
			continue
		}
		if info, err := f.Info(); err == nil && info.Mode().IsRegular() {
			out = append(out, cacheEntry{f.Name(), info.Size(), info.ModTime()})
		}
	}
	return out
}

// evictLocked removes least-recently-used entries until the total fits, never removing keep.
func (c *Cache) evictLocked(keep string) {
	es := c.entriesLocked()
	var total int64
	for _, e := range es {
		total += e.size
	}
	if total <= c.maxBytes {
		return
	}
	sort.Slice(es, func(i, j int) bool { return es[i].used.Before(es[j].used) })
	for _, e := range es {
		if total <= c.maxBytes {
			return
		}
		if e.digest == keep {
			continue
		}
		if os.Remove(c.path(e.digest)) == nil {
			total -= e.size
		}
	}
}

// Digests lists what the cache holds, most recently used first and bounded, for the scheduler.
func (c *Cache) Digests(limit int) []string {
	if !c.enabled() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	es := c.entriesLocked()
	sort.Slice(es, func(i, j int) bool { return es[i].used.After(es[j].used) })
	var out []string
	for _, e := range es {
		if limit > 0 && len(out) >= limit {
			break
		}
		out = append(out, e.digest)
	}
	return out
}
