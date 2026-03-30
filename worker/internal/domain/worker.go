package domain

const (
	WorkerStatusAvailable = "available"
	WorkerStatusBusy      = "busy"
	WorkerStatusOffline   = "offline"

	// Backward-compatibility alias.
	WorkerStatusIdle = WorkerStatusAvailable
)

type ResourceSnapshot struct {
	CPUCores        int   `json:"cpu_cores"`
	MemoryMB        int64 `json:"memory_mb"`
	GPU             bool  `json:"gpu"`
	DockerAvailable bool  `json:"docker_available"`
	GPUSupported    bool  `json:"gpu_supported"`
}

type WorkerHeartbeat struct {
	ID            string             `json:"id"`
	Status        string             `json:"status"`
	Resources     WorkerResources    `json:"resources"`
	CurrentLoad   WorkerCurrentLoad  `json:"current_load"`
	Capabilities  WorkerCapabilities `json:"capabilities"`
	LastHeartbeat int64              `json:"last_heartbeat"`
	Stats         WorkerStats        `json:"stats"`
}

type WorkerResources struct {
	CPUCores int   `json:"cpu_cores"`
	MemoryMB int64 `json:"memory_mb"`
	GPU      bool  `json:"gpu"`
}

type WorkerCurrentLoad struct {
	CPUUsed    int   `json:"cpu_used"`
	MemoryUsed int64 `json:"memory_used"`
}

type WorkerCapabilities struct {
	Docker       bool `json:"docker"`
	GPUSupported bool `json:"gpu_supported"`
}

type WorkerStats struct {
	JobsCompleted int64 `json:"jobs_completed"`
	JobsFailed    int64 `json:"jobs_failed"`
}

type Assignment struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	Status   string `json:"status"`
}

const (
	AssignmentStatusAssigned = "assigned"
	AssignmentStatusRunning  = "running"
)
