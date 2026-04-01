package resources

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/vim-all/ComputeHive/worker/internal/domain"
)

type Reporter struct {
	dockerBinary string
}

func NewReporter(dockerBinary string) *Reporter {
	return &Reporter{dockerBinary: dockerBinary}
}

func (r *Reporter) Snapshot(ctx context.Context) domain.ResourceSnapshot {
	totalMemory, _ := detectMemory(ctx)
	dockerAvailable, _ := detectDocker(ctx, r.dockerBinary)
	gpuModels := detectNVIDIAGPUs(ctx)

	return domain.ResourceSnapshot{
		CPUCores:        runtime.NumCPU(),
		MemoryMB:        int64(totalMemory / (1024 * 1024)),
		GPU:             len(gpuModels) > 0,
		DockerAvailable: dockerAvailable,
		GPUSupported:    len(gpuModels) > 0,
	}
}

func detectDocker(ctx context.Context, binary string) (bool, string) {
	if _, err := exec.LookPath(binary); err != nil {
		return false, ""
	}

	output, err := commandOutput(ctx, binary, "--version")
	if err != nil {
		return true, ""
	}

	return true, strings.TrimSpace(output)
}

func detectNVIDIAGPUs(ctx context.Context) []string {
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		return nil
	}

	output, err := commandOutput(ctx, "nvidia-smi", "--query-gpu=name", "--format=csv,noheader")
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	models := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			models = append(models, line)
		}
	}

	return models
}

func detectMemory(ctx context.Context) (uint64, uint64) {
	switch runtime.GOOS {
	case "linux":
		return linuxMemory()
	case "darwin":
		return darwinMemory(ctx)
	default:
		return 0, 0
	}
}

func linuxMemory() (uint64, uint64) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer file.Close()

	var total uint64
	var available uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		switch fields[0] {
		case "MemTotal:":
			total = parseKB(fields[1])
		case "MemAvailable:":
			available = parseKB(fields[1])
		}
	}

	return total, available
}

func darwinMemory(ctx context.Context) (uint64, uint64) {
	totalOutput, err := commandOutput(ctx, "sysctl", "-n", "hw.memsize")
	if err != nil {
		return 0, 0
	}

	total, err := strconv.ParseUint(strings.TrimSpace(totalOutput), 10, 64)
	if err != nil {
		return 0, 0
	}

	vmStatOutput, err := commandOutput(ctx, "vm_stat")
	if err != nil {
		return total, 0
	}

	pageSize := parseDarwinPageSize(vmStatOutput)
	freePages := parseDarwinPageCount(vmStatOutput, "Pages free")
	inactivePages := parseDarwinPageCount(vmStatOutput, "Pages inactive")
	speculativePages := parseDarwinPageCount(vmStatOutput, "Pages speculative")
	available := uint64(pageSize) * (freePages + inactivePages + speculativePages)
	return total, available
}

func parseKB(value string) uint64 {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}

	return parsed * 1024
}

func parseDarwinPageSize(output string) uint64 {
	const marker = "page size of "
	index := strings.Index(output, marker)
	if index == -1 {
		return 4096
	}

	start := index + len(marker)
	end := strings.Index(output[start:], " bytes")
	if end == -1 {
		return 4096
	}

	size, err := strconv.ParseUint(strings.TrimSpace(output[start:start+end]), 10, 64)
	if err != nil {
		return 4096
	}

	return size
}

func parseDarwinPageCount(output, prefix string) uint64 {
	scanner := bufio.NewScanner(bytes.NewBufferString(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, prefix) {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			return 0
		}

		value := strings.TrimSpace(strings.TrimSuffix(parts[1], "."))
		count, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0
		}

		return count
	}

	return 0
}

func commandOutput(ctx context.Context, binary string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return string(output), nil
}
