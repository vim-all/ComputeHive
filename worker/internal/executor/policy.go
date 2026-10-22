package executor

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

// Everything in a job spec is attacker-controlled. The coordinator validates it, but the worker
// protects a contributor's machine and must not depend on the coordinator being honest (or
// uncompromised), so it re-validates every field that reaches `docker run`.

var (
	imageRefPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/:@-]*$`)
	envNamePattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

const (
	maxImageRefLen = 255
	maxEnvEntries  = 256
	maxEnvValueLen = 8192
	maxCommandArgs = 256
	maxCommandArg  = 4096
	reservedPrefix = "COMPUTEHIVE_"
)

// ValidateImageRef rejects references that could be mistaken for a docker flag
// (leading '-') or that contain characters outside the image-reference alphabet.
func ValidateImageRef(ref string) error {
	switch {
	case ref == "":
		return fmt.Errorf("image reference is empty")
	case len(ref) > maxImageRefLen:
		return fmt.Errorf("image reference longer than %d bytes", maxImageRefLen)
	case strings.HasPrefix(ref, "-"):
		return fmt.Errorf("image reference %q starts with '-' and would be parsed as a docker flag", ref)
	case !imageRefPattern.MatchString(ref):
		return fmt.Errorf("image reference %q contains characters outside [a-zA-Z0-9._/:@-]", ref)
	case strings.Contains(ref, ".."):
		return fmt.Errorf("image reference %q contains '..'", ref)
	}
	return nil
}

// ValidateJobFields checks the environment and command a job asks to run with.
func ValidateJobFields(env map[string]string, command []string) error {
	if len(env) > maxEnvEntries {
		return fmt.Errorf("more than %d environment variables", maxEnvEntries)
	}
	for k, v := range env {
		if !envNamePattern.MatchString(k) {
			return fmt.Errorf("environment variable name %q is not valid", k)
		}
		if strings.HasPrefix(strings.ToUpper(k), reservedPrefix) {
			return fmt.Errorf("environment variable %q uses the reserved %s prefix", k, reservedPrefix)
		}
		if len(v) > maxEnvValueLen {
			return fmt.Errorf("environment variable %q is longer than %d bytes", k, maxEnvValueLen)
		}
		if strings.ContainsRune(v, 0) {
			return fmt.Errorf("environment variable %q contains a NUL byte", k)
		}
	}
	if len(command) > maxCommandArgs {
		return fmt.Errorf("command has more than %d arguments", maxCommandArgs)
	}
	for i, a := range command {
		if len(a) > maxCommandArg || strings.ContainsRune(a, 0) {
			return fmt.Errorf("command argument %d is too long or contains a NUL byte", i)
		}
	}
	return nil
}

// ImagePolicy is the contributor's allow/deny list for images that are NOT delivered as an
// artifact (those are confined to the computehive-job namespace; see Sandbox.LoadPrefixes).
// Patterns use path.Match globs against the whole reference; deny wins; an empty allow list
// allows nothing, so by default a worker only runs images it was sent.
type ImagePolicy struct {
	Allow []string
	Deny  []string
}

func matchAny(patterns []string, ref string) bool {
	for _, p := range patterns {
		if ok, err := path.Match(p, ref); err == nil && ok {
			return true
		}
	}
	return false
}

// Check reports whether ref may be run from the local image store.
func (p ImagePolicy) Check(ref string) error {
	if err := ValidateImageRef(ref); err != nil {
		return err
	}
	if matchAny(p.Deny, ref) {
		return fmt.Errorf("image %q is denied by the contributor's policy", ref)
	}
	if !matchAny(p.Allow, ref) {
		return fmt.Errorf("image %q is not on the contributor's allow list (the worker never pulls images; allowed: %v)", ref, p.Allow)
	}
	return nil
}
