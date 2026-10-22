package artifact

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const maxManifestBytes = 8 << 20

// ImageTar describes what `docker load` would do with an image archive.
type ImageTar struct {
	RepoTags []string // every tag the archive would create in the local image store
}

// InspectImageTar reads manifest.json from a `docker save` archive without loading it.
//
// `docker load` honours whatever tags the archive carries, so a hostile archive could re-tag
// `ubuntu:latest` or `alpine:3.21` on a contributor's machine and have their own later
// containers run attacker-controlled layers. The worker therefore inspects the tags first and
// refuses archives that touch anything outside its own namespace (see AllowedTags).
func InspectImageTar(path string) (ImageTar, error) {
	f, err := os.Open(path)
	if err != nil {
		return ImageTar{}, fmt.Errorf("open image archive: %w", err)
	}
	defer f.Close()

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return ImageTar{}, errors.New("image archive has no manifest.json")
		}
		if err != nil {
			return ImageTar{}, fmt.Errorf("read image archive: %w", err)
		}
		if hdr.Name != "manifest.json" && hdr.Name != "./manifest.json" {
			continue
		}
		if hdr.Size > maxManifestBytes {
			return ImageTar{}, fmt.Errorf("manifest.json is %d bytes (limit %d)", hdr.Size, maxManifestBytes)
		}
		raw, err := io.ReadAll(io.LimitReader(tr, maxManifestBytes))
		if err != nil {
			return ImageTar{}, fmt.Errorf("read manifest.json: %w", err)
		}
		var entries []struct {
			RepoTags []string `json:"RepoTags"`
		}
		if err := json.Unmarshal(raw, &entries); err != nil {
			return ImageTar{}, fmt.Errorf("parse manifest.json: %w", err)
		}
		var out ImageTar
		for _, e := range entries {
			out.RepoTags = append(out.RepoTags, e.RepoTags...)
		}
		return out, nil
	}
}

// AllowedTags returns an error unless every tag starts with one of the allowed prefixes
// (for example "computehive-job:") and wantRef is among the tags.
func (t ImageTar) AllowedTags(prefixes []string, wantRef string) error {
	if len(t.RepoTags) == 0 {
		return errors.New("image archive carries no tags; the job's image_ref must be tagged")
	}
	found := false
	for _, tag := range t.RepoTags {
		ok := false
		for _, p := range prefixes {
			if strings.HasPrefix(tag, p) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("image archive would create tag %q outside the allowed namespaces %v: refusing to load it", tag, prefixes)
		}
		if tag == wantRef {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("image archive does not contain the job's image_ref %q (it has %v)", wantRef, t.RepoTags)
	}
	return nil
}
