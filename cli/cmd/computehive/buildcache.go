package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Re-submitting an unchanged directory (a parameter sweep, a retry, a re-run with other arguments) should
// not rebuild the image. It also should not produce a different artifact each time: Docker stamps the
// build time into the image, so two builds of identical sources are different tarballs, and a worker
// that already holds one could never be recognised as having the other. So the saved image is cached by
// a hash of the directory's contents, and an unchanged directory reuses the byte-identical tarball. That
// is what lets workers cache artifacts and the scheduler prefer one that already has it.
//
// The cache holds only images this user built from their own directories. Use `submit --rebuild` when a
// base image or something outside the directory changed.

const maxCachedImages = 16

var cachedTag = regexp.MustCompile(`^computehive-job:[0-9a-f]{16}$`)

// dirDigest hashes every file's relative path, permissions and contents (and symlink targets), so any
// change to the build context changes the digest. .git is skipped: it does not affect the image.
func dirDigest(root string) (string, error) {
	h := sha256.New()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if d.IsDir() {
			if d.Name() == ".git" && rel != "." {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s\x00%o\x00", filepath.ToSlash(rel), info.Mode().Perm())
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			fmt.Fprintf(h, "link\x00%s\x00", target)
		case info.Mode().IsRegular():
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()
			fmt.Fprintf(h, "file\x00%d\x00", info.Size())
			if _, err := io.Copy(h, f); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func imageCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, "computehive", "images")
	return dir, os.MkdirAll(dir, 0o700)
}

// prepareImage returns the image tag and the path of its saved tarball for the directory, building it
// only when needed. cleanup removes the tarball if it is not part of the cache.
func prepareImage(ctx context.Context, absDir string, rebuild bool, out io.Writer) (tag, tarPath string, cleanup func(), err error) {
	cleanup = func() {}
	digest, derr := dirDigest(absDir)
	cacheDir, cerr := imageCacheDir()
	useCache := derr == nil && cerr == nil
	if useCache && !rebuild {
		tarFile, tagFile := filepath.Join(cacheDir, digest+".tar"), filepath.Join(cacheDir, digest+".tag")
		if raw, err := os.ReadFile(tagFile); err == nil {
			if _, err := os.Stat(tarFile); err == nil && cachedTag.MatchString(strings.TrimSpace(string(raw))) {
				fmt.Fprintln(out, "directory unchanged since the last build: reusing the saved image (use --rebuild to force a rebuild)")
				now := time.Now()
				_ = os.Chtimes(tarFile, now, now) // most recently used
				return strings.TrimSpace(string(raw)), tarFile, cleanup, nil
			}
		}
	}

	fmt.Fprintf(out, "building image from %s ...\n", absDir)
	var built bytes.Buffer
	if err := runStreaming(ctx, &built, "docker", "build", "-q", absDir); err != nil {
		return "", "", cleanup, fmt.Errorf("docker build failed: %w", err)
	}
	imageID := strings.TrimPrefix(strings.TrimSpace(built.String()), "sha256:")
	if len(imageID) < 16 {
		return "", "", cleanup, fmt.Errorf("docker build returned an unexpected image id %q", built.String())
	}
	tag = "computehive-job:" + imageID[:16]
	if err := runStreaming(ctx, io.Discard, "docker", "tag", imageID, tag); err != nil {
		return "", "", cleanup, fmt.Errorf("docker tag failed: %w", err)
	}

	saveDir := os.TempDir()
	if useCache {
		saveDir = cacheDir
	}
	tmp, err := os.CreateTemp(saveDir, ".computehive-image-*.tar")
	if err != nil {
		return "", "", cleanup, err
	}
	tmp.Close()
	if err := runStreaming(ctx, io.Discard, "docker", "save", "-o", tmp.Name(), tag); err != nil {
		os.Remove(tmp.Name())
		return "", "", cleanup, fmt.Errorf("docker save failed: %w", err)
	}
	if !useCache {
		return tag, tmp.Name(), func() { os.Remove(tmp.Name()) }, nil
	}
	tarFile := filepath.Join(cacheDir, digest+".tar")
	if err := os.Rename(tmp.Name(), tarFile); err != nil {
		os.Remove(tmp.Name())
		return "", "", cleanup, err
	}
	if err := os.WriteFile(filepath.Join(cacheDir, digest+".tag"), []byte(tag+"\n"), 0o600); err != nil {
		return "", "", cleanup, err
	}
	pruneImageCache(cacheDir, maxCachedImages)
	return tag, tarFile, cleanup, nil
}

// pruneImageCache keeps the keep most recently used images.
func pruneImageCache(dir string, keep int) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type ent struct {
		base string
		mod  int64
	}
	var tars []ent
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".tar") && !strings.HasPrefix(f.Name(), ".") {
			if info, err := f.Info(); err == nil {
				tars = append(tars, ent{strings.TrimSuffix(f.Name(), ".tar"), info.ModTime().UnixNano()})
			}
		}
	}
	if len(tars) <= keep {
		return
	}
	sort.Slice(tars, func(i, j int) bool { return tars[i].mod > tars[j].mod })
	for _, e := range tars[keep:] {
		os.Remove(filepath.Join(dir, e.base+".tar"))
		os.Remove(filepath.Join(dir, e.base+".tag"))
	}
}
