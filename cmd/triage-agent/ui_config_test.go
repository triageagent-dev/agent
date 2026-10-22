package main

import (
	"flag"
	"testing"
)

func TestUIConfigLeavesSecretsOut(t *testing.T) {
	old := flag.CommandLine
	defer func() { flag.CommandLine = old }()
	flag.CommandLine = flag.NewFlagSet("t", flag.ContinueOnError)
	flag.String("api-tokens-file", "/var/run/tok", "")
	flag.String("triage-endpoint", "wss://u:pw@core.example/events/ws", "")
	flag.String("cluster-name", "c1", "")
	flag.String("ui-listen", "", "")
	_ = flag.CommandLine.Parse([]string{"--cluster-name=c2"})

	got := map[string]string{}
	var first string
	for i, e := range uiConfig() {
		got[e.Name] = e.Value
		if i == 0 {
			first = e.Name
		}
	}
	if _, ok := got["api-tokens-file"]; ok {
		t.Error("a token flag is listed")
	}
	if got["triage-endpoint"] != "wss://core.example/events/ws" {
		t.Errorf("endpoint = %q, want the host and path only", got["triage-endpoint"])
	}
	if got["cluster-name"] != "c2" || first != "cluster-name" {
		t.Errorf("cluster-name = %q, first = %q; changed flags come first", got["cluster-name"], first)
	}
}
