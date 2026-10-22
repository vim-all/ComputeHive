package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	pb "coordinator/pkg/pb"
)

// sanitizeTerminal makes untrusted job output safe to print. A job controls its own stdout, and raw
// terminal control sequences can rewrite the screen, change the window title, move the cursor over earlier
// output or (on some terminals) read or write the clipboard. Keep printable text, newline and tab; drop
// everything else, including ESC sequences and the C1 control range. Invalid UTF-8 is dropped too.
func sanitizeTerminal(s string) string {
	const (
		text      = iota
		afterESC  // saw ESC, deciding which kind of sequence follows
		csi       // ESC [ ... final byte 0x40-0x7e
		str       // OSC / DCS / APC / PM / SOS: payload up to BEL or ST (ESC \)
		strAfterE // saw ESC inside a string: "\" ends it
	)
	var b strings.Builder
	b.Grow(len(s))
	state := text
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		s = s[size:]
		if r == utf8.RuneError && size <= 1 {
			continue // invalid UTF-8
		}
		switch state {
		case afterESC:
			switch r {
			case '[':
				state = csi
			case ']', 'P', '_', '^', 'X':
				state = str
			default:
				state = text // a two-character escape such as ESC c or ESC 7: both dropped
			}
			continue
		case csi:
			if r >= 0x40 && r <= 0x7e {
				state = text
			}
			continue
		case str:
			switch r {
			case 0x07:
				state = text
			case 0x1b:
				state = strAfterE
			}
			continue
		case strAfterE:
			if r == '\\' {
				state = text
			} else if r != 0x1b {
				state = str
			}
			continue
		}
		switch {
		case r == 0x1b:
			state = afterESC
		case r == 0x9b: // single-character CSI
			state = csi
		case r == 0x9d || r == 0x90 || r == 0x9e || r == 0x9f || r == 0x98: // single-character OSC / DCS / PM / APC / SOS
			state = str
		case r == '\n' || r == '\t':
			b.WriteRune(r)
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f):
			// other control characters (CR, BEL, BS, NUL, ...) and the rest of the C1 range are dropped
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// newSuffix returns the part of cur that has not been printed yet, given that prev (the previous window of a
// sliding tail) was already shown. The tail is the last few KB of output, so successive windows overlap.
func newSuffix(prev, cur string) string {
	if prev == "" {
		return cur
	}
	if cur == prev {
		return ""
	}
	if strings.HasPrefix(cur, prev) {
		return cur[len(prev):]
	}
	// The window slid: find the longest suffix of prev that is a prefix of cur.
	for k := min(len(prev), len(cur)); k > 0; k-- {
		if strings.HasSuffix(prev, cur[:k]) {
			return cur[k:]
		}
	}
	return "\n[... output skipped ...]\n" + cur
}

func terminalStatus(s pb.Status) bool {
	return s == pb.Status_STATUS_SUCCEEDED || s == pb.Status_STATUS_FAILED || s == pb.Status_STATUS_CANCELED || s == pb.Status_STATUS_DEAD
}

// cmdFollow streams a job's output as it runs (the worker reports the recent output with every heartbeat, so
// it lags by about two seconds), then prints the final result and fails if the job did not succeed.
func cmdFollow(ctx context.Context, g globals, c pb.ClientServiceClient, id string, timeout time.Duration, out io.Writer) error {
	deadline := time.Now().Add(timeout)
	var shown string
	announced := ""
	for {
		st, err := c.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: &pb.JobID{Value: id}})
		if err != nil {
			return err
		}
		if label := shortStatus(st.GetStatus()); label != announced && !terminalStatus(st.GetStatus()) {
			announced = label
			fmt.Fprintf(out, "[%s%s]\n", label, workerNote(st.GetAssignedWorkerId()))
		}
		if tail := st.GetLogTail(); tail != "" {
			fmt.Fprint(out, sanitizeTerminal(newSuffix(shown, tail)))
			shown = tail
		}
		if terminalStatus(st.GetStatus()) {
			if shown != "" && !strings.HasSuffix(shown, "\n") {
				fmt.Fprintln(out)
			}
			fmt.Fprintln(out, "--- final result ---")
			return cmdResult(ctx, g, c, []string{id, "--wait"}, out, false)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stopped following after %s (job %s is %s)", timeout, id, shortStatus(st.GetStatus()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func workerNote(worker string) string {
	if worker == "" {
		return ""
	}
	return " on " + sanitizeTerminal(worker)
}

// cmdLogs shows a finished job's captured output, or with --follow streams a running job's.
func cmdLogs(ctx context.Context, g globals, c pb.ClientServiceClient, args []string, out io.Writer) error {
	follow := false
	timeout := 30 * time.Minute
	var rest []string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "-f" || a == "--follow":
			follow = true
		case a == "--timeout" && i+1 < len(args):
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return fmt.Errorf("invalid --timeout %q: %w", args[i+1], err)
			}
			timeout = d
			i++
		default:
			rest = append(rest, a)
		}
	}
	if !follow {
		return cmdResult(ctx, g, c, rest, out, true)
	}
	id, err := jobIDArg(rest, "logs")
	if err != nil {
		return err
	}
	return cmdFollow(ctx, g, c, id, timeout, out)
}

// cmdJobs lists recent jobs, newest first.
func cmdJobs(ctx context.Context, c pb.ClientServiceClient, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("jobs", flag.ContinueOnError)
	limit := fs.Int("limit", 20, "how many jobs to show (at most 200)")
	statusName := fs.String("status", "", "only jobs in this state: queued, assigned, running, succeeded, failed, requeued, dead, canceled")
	if err := fs.Parse(args); err != nil {
		return err
	}
	req := &pb.ListJobsRequest{Limit: int32(*limit)}
	if *statusName != "" {
		v, ok := pb.Status_value["STATUS_"+strings.ToUpper(*statusName)]
		if !ok {
			return fmt.Errorf("unknown --status %q", *statusName)
		}
		req.Status = pb.Status(v)
	}
	resp, err := c.ListJobs(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.GetJobs()) == 0 {
		fmt.Fprintln(out, "no jobs")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tSTATUS\tATTEMPT\tWORKER\tAGE\tIMAGE\tUSER")
	for _, j := range resp.GetJobs() {
		age := "-"
		if j.GetCreatedAtUnix() > 0 {
			age = time.Since(time.Unix(j.GetCreatedAtUnix(), 0)).Round(time.Second).String()
		}
		fmt.Fprintf(tw, "%s\t%s\t%d/%d\t%s\t%s\t%s\t%s\n", j.GetJobId().GetValue(), shortStatus(j.GetStatus()), j.GetAttempt(), j.GetMaxAttempts(),
			orDash(j.GetAssignedWorkerId()), age, sanitizeTerminal(j.GetContainerImage()), orDash(sanitizeTerminal(j.GetUser())))
	}
	return tw.Flush()
}
