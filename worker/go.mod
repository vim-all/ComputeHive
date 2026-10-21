module github.com/vim-all/ComputeHive/worker

go 1.25.0

require (
	coordinator v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.41.5
	github.com/shirou/gopsutil/v4 v4.25.9
	google.golang.org/grpc v1.76.0
)

require (
	github.com/aws/smithy-go v1.24.2 // indirect
	github.com/ebitengine/purego v0.9.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace coordinator => ../coordinator
