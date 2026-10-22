package executor

import (
	"fmt"
	"strconv"
	"strings"
)

// auditRunArgs is the last line of defence before a job container starts. It parses the
// `docker run` flags with a strict whitelist and refuses anything that could weaken the sandbox,
// so a future bug in the argument builder (or a hostile job spec reaching it) cannot silently
// produce `--privileged`, a host mount, the Docker socket, host namespaces, extra capabilities,
// an unconfined seccomp/AppArmor profile, or a root user.
//
// Only flags are audited. Everything after the image name is the container's command line and
// cannot influence Docker.
func auditRunArgs(args []string, imageRef string, a auditExpect) error {
	if len(args) < 2 || args[0] != "run" {
		return fmt.Errorf("sandbox audit: not a docker run command")
	}

	valueFlags := map[string]bool{"--name": true, "--label": true, "--user": true, "--cap-drop": true, "--security-opt": true,
		"--tmpfs": true, "-v": true, "--ulimit": true, "--runtime": true, "--cpus": true, "--memory": true, "--memory-swap": true,
		"--pids-limit": true, "--network": true, "--gpus": true, "-e": true, "--pull": true}
	boolFlags := map[string]bool{"--read-only": true, "--init": true}

	var (
		capDropAll, noNewPrivs, readOnly, pullNever bool
		user, memory, memorySwap, pids, network     string
		mounts                                      []string
		runtimeFlag                                 string
		gpus                                        bool
	)
	i := 1
	for ; i < len(args); i++ {
		tok := args[i]
		if !strings.HasPrefix(tok, "-") {
			break // first non-flag token is the image
		}
		if boolFlags[tok] {
			if tok == "--read-only" {
				readOnly = true
			}
			continue
		}
		if !valueFlags[tok] {
			return fmt.Errorf("sandbox audit: flag %q is not on the whitelist", tok)
		}
		i++
		if i >= len(args) {
			return fmt.Errorf("sandbox audit: flag %q has no value", tok)
		}
		val := args[i]
		if strings.Contains(val, "docker.sock") {
			return fmt.Errorf("sandbox audit: %s %q references the Docker socket", tok, val)
		}
		switch tok {
		case "--cap-drop":
			if val != "ALL" {
				return fmt.Errorf("sandbox audit: --cap-drop must be ALL, got %q", val)
			}
			capDropAll = true
		case "--security-opt":
			if val != "no-new-privileges" {
				return fmt.Errorf("sandbox audit: --security-opt %q is not allowed (only no-new-privileges)", val)
			}
			noNewPrivs = true
		case "--user":
			user = val
		case "--memory":
			memory = val
		case "--memory-swap":
			memorySwap = val
		case "--pids-limit":
			pids = val
		case "--network":
			network = val
		case "--runtime":
			runtimeFlag = val
		case "--pull":
			pullNever = val == "never"
		case "--gpus":
			if val != "all" {
				return fmt.Errorf("sandbox audit: --gpus must be 'all', got %q", val)
			}
			gpus = true
		case "-v":
			mounts = append(mounts, val)
		case "--tmpfs":
			if !strings.Contains(val, "noexec") && strings.HasPrefix(val, "/tmp") {
				return fmt.Errorf("sandbox audit: /tmp scratch must be mounted noexec")
			}
		case "-e":
			k, _, ok := strings.Cut(val, "=")
			if !ok || !envNamePattern.MatchString(k) {
				return fmt.Errorf("sandbox audit: malformed environment entry %q", val)
			}
		}
	}
	if i >= len(args) {
		return fmt.Errorf("sandbox audit: no image in command")
	}
	if args[i] != imageRef {
		return fmt.Errorf("sandbox audit: image position holds %q, expected %q", args[i], imageRef)
	}

	switch {
	case !capDropAll:
		return fmt.Errorf("sandbox audit: --cap-drop ALL is missing")
	case !noNewPrivs:
		return fmt.Errorf("sandbox audit: --security-opt no-new-privileges is missing")
	case !readOnly:
		return fmt.Errorf("sandbox audit: --read-only is missing")
	case !pullNever:
		return fmt.Errorf("sandbox audit: --pull never is missing (the worker must never pull images with the contributor's credentials)")
	case user == "" || isRootUser(user):
		return fmt.Errorf("sandbox audit: container user %q is root or unset", user)
	case memory == "" || memory != memorySwap:
		return fmt.Errorf("sandbox audit: --memory (%q) must be set and equal to --memory-swap (%q)", memory, memorySwap)
	}
	if n, err := strconv.Atoi(pids); err != nil || n <= 0 {
		return fmt.Errorf("sandbox audit: --pids-limit must be a positive number, got %q", pids)
	}
	if !a.networkAllowed(network) {
		return fmt.Errorf("sandbox audit: network %q is not permitted for this job", network)
	}
	if runtimeFlag != a.runtime {
		return fmt.Errorf("sandbox audit: runtime %q does not match the worker's configured runtime %q", runtimeFlag, a.runtime)
	}
	if gpus && !a.allowGPU {
		return fmt.Errorf("sandbox audit: --gpus requested but not allowed")
	}
	if len(mounts) != 1 || mounts[0] != a.mount {
		return fmt.Errorf("sandbox audit: the only permitted bind mount is %q, got %v", a.mount, mounts)
	}
	return nil
}

// auditExpect is what the executor intended to build; the audit checks the arguments agree.
type auditExpect struct {
	mount          string // "<host output dir>:<container output dir>", the single permitted -v
	runtime        string // "" or "runsc"
	allowGPU       bool
	networkAllowed func(name string) bool
}

func isRootUser(u string) bool {
	name, _, _ := strings.Cut(u, ":")
	return name == "" || name == "0" || name == "root"
}
