package grpcserver

import (
	"regexp"

	pb "coordinator/pkg/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Upper bounds on submitted data: the coordinator stores these in Redis and hands
// them to arbitrary workers, so they must be bounded regardless of who submits.
const (
	maxImageLen    = 512
	maxCommandArgs = 256
	maxArgLen      = 4096
	maxEnvEntries  = 256
	maxEnvKeyLen   = 128
	maxEnvValueLen = 8192
	maxURLLen      = 2048
	maxUserLen     = 64
	maxRuntimeSecs = 7 * 24 * 3600
	maxCPUCores    = 1024
	maxMemoryMB    = 4 * 1024 * 1024
	maxGPUs        = 64
	maxJobAttempts = 20
	maxNetHosts    = 16
)

var (
	sha256Hex  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	envKeyName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	userName   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@-]*$`)
	hostName   = regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)
)

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

func validateSubmit(req *pb.SubmitJobRequest) error {
	if req.GetContainerImage() == "" {
		return invalid("container_image is required")
	}
	if len(req.GetContainerImage()) > maxImageLen {
		return invalid("container_image longer than %d bytes", maxImageLen)
	}
	if len(req.GetCommand()) == 0 && req.GetArtifactUrl() == "" {
		return invalid("command is required unless an artifact (whose image CMD is used) is attached")
	}
	if len(req.GetCommand()) > maxCommandArgs {
		return invalid("command has more than %d arguments", maxCommandArgs)
	}
	for i, a := range req.GetCommand() {
		if len(a) > maxArgLen {
			return invalid("command argument %d longer than %d bytes", i, maxArgLen)
		}
	}
	res := req.GetRequiredResources()
	if res == nil {
		return invalid("required_resources is required")
	}
	if res.GetCpuCores() < 0 || res.GetCpuCores() > maxCPUCores || res.GetMemoryMb() < 0 || res.GetMemoryMb() > maxMemoryMB ||
		res.GetGpuCount() < 0 || res.GetGpuCount() > maxGPUs {
		return invalid("required_resources out of range (cpu 0-%d, memory_mb 0-%d, gpu 0-%d)", maxCPUCores, maxMemoryMB, maxGPUs)
	}
	if len(req.GetEnvironment()) > maxEnvEntries {
		return invalid("environment has more than %d entries", maxEnvEntries)
	}
	for k, v := range req.GetEnvironment() {
		if len(k) > maxEnvKeyLen || !envKeyName.MatchString(k) {
			return invalid("environment key %q is not a valid variable name (max %d bytes)", k, maxEnvKeyLen)
		}
		if len(v) > maxEnvValueLen {
			return invalid("environment value for %q longer than %d bytes", k, maxEnvValueLen)
		}
	}
	if req.GetMaxRuntimeSeconds() < 0 || req.GetMaxRuntimeSeconds() > maxRuntimeSecs {
		return invalid("max_runtime_seconds must be 0-%d", maxRuntimeSecs)
	}
	if len(req.GetArtifactUrl()) > maxURLLen {
		return invalid("artifact_url longer than %d bytes", maxURLLen)
	}
	if (req.GetArtifactUrl() == "") != (req.GetArtifactSha256() == "") {
		return invalid("artifact_url and artifact_sha256 must be provided together")
	}
	if req.GetArtifactSha256() != "" && !sha256Hex.MatchString(req.GetArtifactSha256()) {
		return invalid("artifact_sha256 must be 64 lowercase hex characters")
	}
	if req.GetMaxAttempts() < 0 || req.GetMaxAttempts() > maxJobAttempts {
		return invalid("max_attempts must be 0-%d", maxJobAttempts)
	}
	switch req.GetRequiredIsolation() {
	case "", "hardened-runc", "gvisor":
	default:
		return invalid("required_isolation must be empty, hardened-runc or gvisor")
	}
	if len(req.GetNetworkHosts()) > maxNetHosts {
		return invalid("network_hosts has more than %d entries", maxNetHosts)
	}
	for _, h := range req.GetNetworkHosts() {
		if len(h) > 253 || !hostName.MatchString(h) {
			return invalid("network_hosts entry %q is not a valid host name (IP addresses and wildcards are not accepted)", h)
		}
	}
	if u := req.GetUser(); u != "" && (len(u) > maxUserLen || !userName.MatchString(u)) {
		return invalid("user must match %s and be at most %d bytes", userName, maxUserLen)
	}
	return nil
}
