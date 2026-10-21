package artifact

import "strings"

// Scheme is the location-independent artifact reference produced by the CLI:
// computehive-artifact://<sha256>. Each worker resolves it against its own
// coordinator HTTP address, so the same job works inside a compose network,
// on a LAN, or across NAT without the submitter knowing the worker's view.
const Scheme = "computehive-artifact://"

// ResolveURL rewrites a computehive-artifact:// reference to
// <coordinatorHTTPURL>/artifacts/<sha256>. Other URLs (https S3/R2) pass through.
func ResolveURL(raw, coordinatorHTTPURL string) string {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, Scheme) {
		return raw
	}
	sha := strings.TrimPrefix(raw, Scheme)
	return strings.TrimRight(coordinatorHTTPURL, "/") + "/artifacts/" + sha
}
