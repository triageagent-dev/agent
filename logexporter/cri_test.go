package logexporter

import (
	"bytes"
	"testing"
)

func TestSplitCRI(t *testing.T) {
	cases := []struct {
		name        string
		line        string
		wantFlag    byte
		wantContent string
		wantOK      bool
	}{
		{
			name:        "full line",
			line:        "2026-08-22T10:15:23.123456789Z stdout F hello world",
			wantFlag:    'F',
			wantContent: "hello world",
			wantOK:      true,
		},
		{
			name:        "partial line",
			line:        "2026-08-22T10:15:23.123456789Z stderr P a very long line chunk",
			wantFlag:    'P',
			wantContent: "a very long line chunk",
			wantOK:      true,
		},
		{
			name:        "content itself has spaces",
			line:        "2026-08-22T10:15:23.123456789Z stdout F level=error msg=\"boom now\"",
			wantFlag:    'F',
			wantContent: `level=error msg="boom now"`,
			wantOK:      true,
		},
		{name: "too few fields", line: "not a cri line", wantOK: false},
		{name: "bad flag", line: "ts stdout X content", wantOK: false},
		{name: "empty", line: "", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flag, content, ok := splitCRI([]byte(tc.line))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if flag != tc.wantFlag {
				t.Errorf("flag = %q, want %q", flag, tc.wantFlag)
			}
			if !bytes.Equal(content, []byte(tc.wantContent)) {
				t.Errorf("content = %q, want %q", content, tc.wantContent)
			}
		})
	}
}

func TestTrimEOL(t *testing.T) {
	cases := map[string]string{
		"abc\n":   "abc",
		"abc\r\n": "abc",
		"abc":     "abc",
		"\n":      "",
		"":        "",
	}
	for in, want := range cases {
		if got := string(trimEOL([]byte(in))); got != want {
			t.Errorf("trimEOL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseLogFilename(t *testing.T) {
	cases := []struct {
		name          string
		wantPod       string
		wantNamespace string
		wantContainer string
		wantOK        bool
	}{
		{
			name:          "api-7d9f8b6c4-abcde_ops_api-3f2a1b9c8d7e6f5a4b3c2d1e0f9a8b7c.log",
			wantPod:       "api-7d9f8b6c4-abcde",
			wantNamespace: "ops",
			wantContainer: "api",
			wantOK:        true,
		},
		{
			name:          "hyphenated-container-name_teamns_my-sidecar-container-abc123.log",
			wantPod:       "hyphenated-container-name",
			wantNamespace: "teamns",
			wantContainer: "my-sidecar-container",
			wantOK:        true,
		},
		{name: "no_underscores_here.log", wantOK: false},
		{name: "missing-namespace_container-id.log", wantOK: false},
		{name: "not-a-log-file.txt", wantOK: false},
		{name: "", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod, ns, container, ok := parseLogFilename(tc.name)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (pod=%q ns=%q container=%q)", ok, tc.wantOK, pod, ns, container)
			}
			if !ok {
				return
			}
			if pod != tc.wantPod || ns != tc.wantNamespace || container != tc.wantContainer {
				t.Errorf("got pod=%q ns=%q container=%q, want pod=%q ns=%q container=%q",
					pod, ns, container, tc.wantPod, tc.wantNamespace, tc.wantContainer)
			}
		})
	}
}
