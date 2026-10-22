package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The contributor controls talk to the local worker over its private Unix socket
// (mode 0600): they work only for the user running the worker.

const controlUsage = `usage:
  computehive worker status                       state, why it is not taking jobs, resources, policy
  computehive worker pause [--now]                stop taking jobs (--now also hands back the running job)
  computehive worker resume
  computehive worker kill                         kill switch: stop everything at once and exit
  computehive worker history [--limit 20]         what ran on this machine
  computehive worker policy                       show the contributor policy
  computehive worker policy set KEY=VALUE ...     change it, for example:
        max_cpus=2 max_memory_mb=4096 max_job_disk_mb=512
        idle_only=true idle_cpu_percent=25 schedule=22:00-07:00
        allow_network=true allowed_hosts=pypi.org,*.githubusercontent.com
        image_allow=alpine:* image_deny=alpine:edge require_gvisor=true

common flag: --socket PATH   (default: the only ~/.local/state/computehive/*/control.sock)
`

func findSocket(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if env := os.Getenv("COMPUTEHIVE_SOCKET"); env != "" {
		return env, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	matches, _ := filepath.Glob(filepath.Join(home, ".local", "state", "computehive", "*", "control.sock"))
	switch len(matches) {
	case 0:
		return "", errors.New("no running worker found for this user (no control socket under ~/.local/state/computehive/): is `computehive worker join` running? Pass --socket to point at one")
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("several workers found; pick one with --socket: %s", strings.Join(matches, ", "))
	}
}

func controlClient(sock string) *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

func controlCall(c *http.Client, method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, "http://worker"+path, rdr)
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach the worker: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		if json.Unmarshal(b, &e) == nil && e.Error != "" {
			return nil, errors.New(e.Error)
		}
		return nil, fmt.Errorf("worker answered %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return b, nil
}

func cmdWorkerControl(args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, controlUsage)
		return errors.New("missing worker subcommand")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("worker "+sub, flag.ContinueOnError)
	fs.SetOutput(errOut)
	socket := fs.String("socket", "", "control socket path")
	now := fs.Bool("now", false, "pause: also hand back the running job")
	limit := fs.Int("limit", 20, "history: how many entries")

	// allow flags after positional arguments (policy set k=v --socket x)
	var pos, flags []string
	for i := 0; i < len(rest); i++ {
		if strings.HasPrefix(rest[i], "--") {
			flags = append(flags, rest[i])
			if !strings.Contains(rest[i], "=") && rest[i] != "--now" && i+1 < len(rest) {
				i++
				flags = append(flags, rest[i])
			}
		} else {
			pos = append(pos, rest[i])
		}
	}
	if err := fs.Parse(flags); err != nil {
		return err
	}
	sock, err := findSocket(*socket)
	if err != nil {
		return err
	}
	c := controlClient(sock)

	switch sub {
	case "status":
		b, err := controlCall(c, "GET", "/v1/status", nil)
		if err != nil {
			return err
		}
		return printStatus(out, b)
	case "pause":
		q := ""
		if *now {
			q = "?now=true"
		}
		return printOK(out, c, "POST", "/v1/pause"+q)
	case "resume":
		return printOK(out, c, "POST", "/v1/resume")
	case "kill":
		return printOK(out, c, "POST", "/v1/kill")
	case "history":
		b, err := controlCall(c, "GET", "/v1/history?limit="+strconv.Itoa(*limit), nil)
		if err != nil {
			return err
		}
		return printHistory(out, b)
	case "policy":
		if len(pos) == 0 {
			b, err := controlCall(c, "GET", "/v1/policy", nil)
			if err != nil {
				return err
			}
			var pretty bytes.Buffer
			_ = json.Indent(&pretty, b, "", "  ")
			fmt.Fprintln(out, strings.TrimSpace(pretty.String()))
			return nil
		}
		if pos[0] != "set" || len(pos) < 2 {
			fmt.Fprint(errOut, controlUsage)
			return errors.New("usage: computehive worker policy set KEY=VALUE [KEY=VALUE]")
		}
		return policySet(out, c, pos[1:])
	default:
		fmt.Fprint(errOut, controlUsage)
		return fmt.Errorf("unknown worker subcommand %q", sub)
	}
}

func printOK(out io.Writer, c *http.Client, method, path string) error {
	b, err := controlCall(c, method, path, nil)
	if err != nil {
		return err
	}
	var r struct {
		OK string `json:"ok"`
	}
	_ = json.Unmarshal(b, &r)
	fmt.Fprintln(out, r.OK)
	return nil
}

func printStatus(out io.Writer, raw []byte) error {
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return err
	}
	str := func(k string) string { s, _ := generic[k].(string); return s }
	fmt.Fprintf(out, "worker:     %s (%s)\nstate:      %s", str("worker_id"), str("version"), str("state"))
	if nt := str("not_taking"); nt != "" {
		fmt.Fprintf(out, "  (not taking new jobs: %s)", nt)
	}
	fmt.Fprintf(out, "\nisolation:  %s\nuptime:     %s\n", str("isolation"), str("uptime"))
	if j := str("current_job"); j != "" {
		fmt.Fprintf(out, "running:    job %s\n", j)
	}
	if m, ok := generic["metrics"].(map[string]any); ok {
		f := func(k string) float64 { v, _ := m[k].(float64); return v }
		fmt.Fprintf(out, "host:       RAM %.0f%%  CPU %.0f%%  disk %.0f%%\n", f("MemPercent"), f("CPUPercent"), max(f("WorkDiskUsedPercent"), f("DockerDiskUsedPercent")))
	}
	if p, ok := generic["policy"].(map[string]any); ok {
		keys := make([]string, 0, len(p))
		for k := range p {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintln(out, "policy:")
		for _, k := range keys {
			fmt.Fprintf(out, "  %-18s %v\n", k, p[k])
		}
	}
	return nil
}

func printHistory(out io.Writer, raw []byte) error {
	var entries []map[string]any
	if err := json.Unmarshal(raw, &entries); err != nil {
		return err
	}
	if len(entries) == 0 {
		fmt.Fprintln(out, "no jobs have run on this worker yet")
		return nil
	}
	for _, e := range entries {
		s := func(k string) string { v, _ := e[k].(string); return v }
		t, _ := time.Parse(time.RFC3339Nano, s("started_at"))
		dur := ""
		if f, err := time.Parse(time.RFC3339Nano, s("finished_at")); err == nil {
			dur = f.Sub(t).Round(time.Millisecond).String()
		}
		fmt.Fprintf(out, "%s  %-9s job %s  image %s  isolation %s  took %s\n", t.Local().Format("2006-01-02 15:04:05"), s("status"), s("job_id"), s("image"), s("isolation"), dur)
		fmt.Fprintf(out, "    ran: %s\n", s("command"))
		if env, ok := e["env_names"].([]any); ok && len(env) > 0 {
			names := make([]string, len(env))
			for i, v := range env {
				names[i] = fmt.Sprint(v)
			}
			fmt.Fprintf(out, "    env: %s (values are never recorded)\n", strings.Join(names, ", "))
		}
		if net, ok := e["network_hosts"].([]any); ok && len(net) > 0 {
			fmt.Fprintf(out, "    network: %v\n", net)
		}
		if r := s("failure_reason"); r != "" {
			fmt.Fprintf(out, "    outcome: %s %s\n", r, s("error"))
		}
	}
	return nil
}

// policySet applies KEY=VALUE pairs to the current policy, converting each value to the type the
// field already has, then sends the whole policy back (the worker validates and rejects bad values).
func policySet(out io.Writer, c *http.Client, pairs []string) error {
	b, err := controlCall(c, "GET", "/v1/policy", nil)
	if err != nil {
		return err
	}
	var p map[string]any
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	for _, kv := range pairs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("%q is not KEY=VALUE", kv)
		}
		cur, known := p[k]
		if !known {
			return fmt.Errorf("unknown policy field %q (see `computehive worker policy` for the fields)", k)
		}
		switch cur.(type) {
		case bool:
			bv, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("%s must be true or false", k)
			}
			p[k] = bv
		case float64:
			fv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("%s must be a number", k)
			}
			p[k] = fv
		case []any, nil:
			var list []string
			for _, item := range strings.Split(v, ",") {
				if item = strings.TrimSpace(item); item != "" {
					list = append(list, item)
				}
			}
			p[k] = list
		default:
			p[k] = v
		}
	}
	if _, err := controlCall(c, "PUT", "/v1/policy", p); err != nil {
		return err
	}
	fmt.Fprintln(out, "policy updated; it applies to the next job")
	return nil
}
