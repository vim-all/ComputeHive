module github.com/vim-all/ComputeHive/worker

go 1.25.0

require (
	coordinator v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.41.5
	google.golang.org/grpc v1.76.0
)

require (
	github.com/aws/smithy-go v1.24.2 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace coordinator => ../coordinator
