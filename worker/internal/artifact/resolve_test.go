package artifact

import "testing"

func TestResolveURL(t *testing.T) {
	cases := []struct{ name, raw, base, want string }{
		{"scheme resolves", "computehive-artifact://abc123", "http://coordinator:8080", "http://coordinator:8080/artifacts/abc123"},
		{"trailing slash trimmed", "computehive-artifact://abc123", "http://c:8080/", "http://c:8080/artifacts/abc123"},
		{"https passes through", "https://bucket.example/x.tar", "http://c:8080", "https://bucket.example/x.tar"},
		{"empty stays empty", "  ", "http://c:8080", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveURL(tc.raw, tc.base); got != tc.want {
				t.Fatalf("ResolveURL(%q,%q) = %q, want %q", tc.raw, tc.base, got, tc.want)
			}
		})
	}
}
