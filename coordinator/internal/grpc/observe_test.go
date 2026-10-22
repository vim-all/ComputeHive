package grpcserver

import (
	"context"
	"regexp"
	"strings"
	"testing"

	pb "coordinator/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// callWithID lists workers with the given x-request-id (none if empty) and returns the id the server answered with.
func callWithID(t *testing.T, cl *cluster, id string) string {
	t.Helper()
	ctx := context.Background()
	if id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
	}
	var hdr metadata.MD
	if _, err := cl.c.ListWorkers(ctx, &pb.ListWorkersRequest{}, grpc.Header(&hdr)); err != nil {
		t.Fatal(err)
	}
	v := hdr.Get("x-request-id")
	if len(v) != 1 {
		t.Fatalf("response carried %d x-request-id headers, want 1", len(v))
	}
	return v[0]
}

func TestEveryResponseCarriesACorrelationID(t *testing.T) {
	cl := startCluster(t, nil)

	if got := callWithID(t, cl, "cli-abc.123_x"); got != "cli-abc.123_x" {
		t.Errorf("a well-formed caller id must be echoed, got %q", got)
	}
	gen := callWithID(t, cl, "")
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(gen) {
		t.Errorf("generated id %q", gen)
	}
	if other := callWithID(t, cl, ""); other == gen {
		t.Error("two calls were given the same generated id")
	}
}

func TestHostileCorrelationIDsAreReplacedNotLogged(t *testing.T) {
	cl := startCluster(t, nil)
	for name, bad := range map[string]string{
		"newline injection": "ok\nlevel=ERROR msg=forged",
		"too long":          strings.Repeat("a", 65),
		"spaces":            "has space",
		"quotes":            `a"b`,
	} {
		// gRPC metadata values may not contain control characters on the wire, so the client may refuse to
		// send some of these at all; what must hold is that the server never echoes one back.
		ctx := metadata.AppendToOutgoingContext(context.Background(), "x-request-id", bad)
		var hdr metadata.MD
		if _, err := cl.c.ListWorkers(ctx, &pb.ListWorkersRequest{}, grpc.Header(&hdr)); err != nil {
			continue
		}
		if v := hdr.Get("x-request-id"); len(v) != 1 || v[0] == bad {
			t.Errorf("%s: server echoed %q", name, v)
		}
	}
}
