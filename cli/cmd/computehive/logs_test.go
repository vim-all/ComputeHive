package main

import (
	"strings"
	"testing"
)

func TestSanitizeTerminalStripsEverythingThatCouldDriveATerminal(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"plain text and layout survive", "hello\n\tworld\n", "hello\n\tworld\n"},
		{"unicode survives", "héllo 日本語 🚀", "héllo 日本語 🚀"},
		{"colour codes", "\x1b[31mred\x1b[0m text", "red text"},
		{"clear screen and cursor movement", "before\x1b[2J\x1b[Hafter\x1b[10;5Hx", "beforeafterx"},
		{"set window title (OSC, BEL terminated)", "a\x1b]0;you have been hacked\x07b", "ab"},
		{"carriage return would overwrite earlier output", "real output\rfake output", "real outputfake output"},
		{"bell and nul and backspace", "a\x07b\x00c\x08d", "abcd"},
		{"C1 control (single-byte CSI)", "a\u009b31mb", "ab"},
		{"OSC terminated by ST (ESC backslash), payload skipped", "a\x1b]8;;http://evil\x1b\\b", "ab"},
		{"DCS string skipped", "a\x1bPq#0;2;0;0;0\x1b\\b", "ab"},
		{"two-character escape (reset terminal)", "a\x1bcb", "ab"},
		{"unterminated OSC swallows only to the end of the input", "a\x1b]0;title with no end", "a"},
		{"text after a complete sequence is kept", "\x1b[1;32mOK\x1b[0m done", "OK done"},
		{"delete", "a\x7fb", "ab"},
		{"invalid utf-8 is dropped", "ok\xff\xfeend", "okend"},
		{"escape at end of input", "text\x1b", "text"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeTerminal(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeTerminal(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if strings.ContainsAny(got, "\x1b\r\x07\x00") {
				t.Fatalf("control characters survived: %q", got)
			}
		})
	}
}

func TestNewSuffixPrintsOnlyWhatIsNew(t *testing.T) {
	cases := []struct{ name, prev, cur, want string }{
		{"first window", "", "line1\n", "line1\n"},
		{"nothing new", "line1\n", "line1\n", ""},
		{"appended", "line1\n", "line1\nline2\n", "line2\n"},
		{"window slid forward", "aaaa\nbbbb\ncccc\n", "bbbb\ncccc\ndddd\n", "dddd\n"},
		{"slid with only a partial overlap", "0123456789", "56789abc", "abc"},
		{"nothing in common: say so and print the rest", "old output", "completely different", "\n[... output skipped ...]\ncompletely different"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := newSuffix(tc.prev, tc.cur); got != tc.want {
				t.Fatalf("newSuffix(%q, %q) = %q, want %q", tc.prev, tc.cur, got, tc.want)
			}
		})
	}
}

func TestFollowingASlidingWindowReassemblesTheOutputExactly(t *testing.T) {
	// Simulate a long output observed through a 40-byte sliding window sampled irregularly.
	var full strings.Builder
	for i := 0; i < 30; i++ {
		full.WriteString("line-")
		full.WriteString(string(rune('A' + i%26)))
		full.WriteString("\n")
	}
	all := full.String()
	var shown, printed string
	for end := 7; end <= len(all)+6; end += 7 { // a sample every 7 bytes: windows overlap by more than they advance
		e := min(end, len(all))
		start := max(0, e-40)
		window := all[start:e]
		printed += newSuffix(shown, window)
		shown = window
	}
	if printed != all {
		t.Fatalf("reassembled output differs:\n got %q\nwant %q", printed, all)
	}
}
