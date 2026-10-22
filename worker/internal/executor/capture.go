package executor

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"
)

// limitedBuffer keeps at most max bytes of a stream and silently discards the rest, while
// still consuming all of it so the job never blocks on a full pipe. A job that prints
// gigabytes therefore costs the worker a fixed amount of memory.
type limitedBuffer struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	max       int
	truncated bool
	total     int64
}

func newLimitedBuffer(max int) *limitedBuffer { return &limitedBuffer{max: max} }

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total += int64(len(p))
	if room := l.max - l.buf.Len(); room > 0 {
		n := min(len(p), room)
		l.buf.Write(p[:n])
		if n < len(p) {
			l.truncated = true
		}
	} else if len(p) > 0 {
		l.truncated = true
	}
	return len(p), nil
}

func (l *limitedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.truncated {
		return l.buf.String()
	}
	return fmt.Sprintf("%s\n[output truncated: kept the first %d of %d bytes]", l.buf.String(), l.max, l.total)
}

// dirUsage returns the total size of regular files under dir and the number of entries.
// Symlinks are not followed, so a link to a huge host file counts as a link.
func dirUsage(dir string) (bytes int64, entries int) {
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // vanishing files are normal while a job runs
		}
		entries++
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				bytes += info.Size()
			}
		}
		return nil
	})
	return bytes, entries
}

// watchDirSize polls dir and calls onExceed once if it grows past maxBytes or maxEntries. It is
// the disk quota for a job's writable work directory: Docker cannot size-limit a bind mount
// without root-level filesystem features, so the worker enforces it by killing the job.
// (RLIMIT_FSIZE additionally caps any single file; overshoot is bounded by poll interval x write speed.)
func watchDirSize(ctx context.Context, dir string, maxBytes int64, maxEntries int, interval time.Duration, onExceed func(bytes int64, entries int)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if b, n := dirUsage(dir); b > maxBytes || n > maxEntries {
				onExceed(b, n)
				return
			}
		}
	}
}
