package main

import (
	"flag"
	"net/url"
	"sort"
	"strings"

	"github.com/triageagent-dev/agent/agentui"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// secretFlagWords mark a flag whose value must not reach the UI: a name
// containing one of them is left out. Flags that only name a Secret
// (--sensor-tls-secret) hold no secret and are shown.
var secretFlagWords = []string{"token", "password", "passwd", "credential", "apikey", "api-key", "private"}

// visibleFlag is one flag of the running process that is safe to show.
type visibleFlag struct {
	name, value, def, usage string
	changed                 bool
}

// visibleFlags lists every flag of the running process with its value, minus
// the secret ones, changed flags first.
func visibleFlags() []visibleFlag {
	var out []visibleFlag
	flag.VisitAll(func(f *flag.Flag) {
		if f.Name == "ui-admin-key" {
			return // the key itself
		}
		for _, w := range secretFlagWords {
			if strings.Contains(strings.ToLower(f.Name), w) {
				return
			}
		}
		v := f.Value.String()
		if f.Name == "triage-endpoint" {
			v = hostOnly(v) // never a user:password in the URL
		}
		out = append(out, visibleFlag{name: f.Name, value: v, def: f.DefValue, usage: f.Usage,
			changed: f.Value.String() != f.DefValue})
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].changed != out[j].changed {
			return out[i].changed
		}
		return out[i].name < out[j].name
	})
	return out
}

// uiConfig lists the controller's flags for the TUI.
func uiConfig() []agentui.ConfigEntry {
	var out []agentui.ConfigEntry
	for _, f := range visibleFlags() {
		out = append(out, agentui.ConfigEntry{Name: f.name, Value: f.value, Default: f.def, Changed: f.changed, Usage: f.usage})
	}
	return out
}

// sensorSettings is the same list as the Hello's settings: what this sensor
// process runs, for the controller to show.
func sensorSettings() []*sensorpb.Setting {
	var out []*sensorpb.Setting
	for _, f := range visibleFlags() {
		out = append(out, &sensorpb.Setting{Name: f.name, Value: f.value, Default: f.def, Changed: f.changed})
	}
	return out
}

func hostOnly(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u.Scheme + "://" + u.Host + u.Path
	}
	return raw
}
