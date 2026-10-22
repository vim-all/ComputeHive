package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

const (
	maxDigestFiles = 4096
	maxDigestBytes = 8 << 30
)

// OutputDigest is a fingerprint of everything a job produced: its exit code, its stdout, and every
// file it left in its output directory (path, size and content hash, in path order). Timestamps,
// ownership and creation order are deliberately ignored, so two correct runs of a deterministic job
// produce the same digest on different machines. The coordinator compares the digests of the two
// replicas of a verify=true job.
//
// It returns "" if the output cannot be fingerprinted (too many or too large files, unreadable
// files): an empty digest never matches anything, so such a job fails verification instead of passing.
func OutputDigest(exitCode int, stdout string, outDir string) string {
	h := sha256.New()
	fmt.Fprintf(h, "exit=%d\nstdout=%d\n", exitCode, len(stdout))
	h.Write([]byte(stdout))

	var files []string
	var total int64
	err := filepath.WalkDir(outDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() { // symlinks, devices, sockets: not part of the result
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		if len(files) >= maxDigestFiles || total > maxDigestBytes {
			return fmt.Errorf("output too large to fingerprint")
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return ""
	}
	sort.Strings(files)
	for _, path := range files {
		rel, err := filepath.Rel(outDir, path)
		if err != nil {
			return ""
		}
		f, err := os.Open(path)
		if err != nil {
			return ""
		}
		fh := sha256.New()
		n, err := io.Copy(fh, f)
		f.Close()
		if err != nil {
			return ""
		}
		fmt.Fprintf(h, "file=%s\x00size=%d\x00sha256=%x\n", filepath.ToSlash(rel), n, fh.Sum(nil))
	}
	return hex.EncodeToString(h.Sum(nil))
}
