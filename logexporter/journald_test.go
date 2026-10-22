package logexporter

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func newTestHostLog(t *testing.T, o HostLogOptions) *HostLog {
	t.Helper()
	if o.Units == nil {
		o.Units = func() []string { return []string{"api"} }
	}
	o.Namespace, o.Node = "host", "box"
	h, err := NewHostLog(o, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func readFixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// sums returns metric name -> "svc|sample" -> value for the node's counters.
func sums(t *testing.T, h *HostLog) map[string]map[string]float64 {
	t.Helper()
	rms, err := h.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]float64{}
	for _, rm := range rms {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if out[m.Name] == nil {
					out[m.Name] = map[string]float64{}
				}
				for _, dp := range sumPoints(m) {
					key := ""
					for _, kv := range dp.Attributes {
						switch kv.Key {
						case LabelNamespace:
							key = kv.Value.GetStringValue() + "/" + key
						case LabelService:
							key += kv.Value.GetStringValue()
						case LabelSample:
							key += "|" + kv.Value.GetStringValue()
						}
					}
					out[m.Name][key] += dp.GetAsDouble()
				}
			}
		}
	}
	return out
}

func sumPoints(m *metricspb.Metric) []*metricspb.NumberDataPoint {
	if s := m.GetSum(); s != nil {
		return s.DataPoints
	}
	if g := m.GetGauge(); g != nil {
		return g.DataPoints
	}
	return nil
}

func TestHostLog_FixtureClassificationAndAttribution(t *testing.T) {
	h := newTestHostLog(t, HostLogOptions{})
	n := h.consume(strings.NewReader(readFixture(t, "journal_sample.jsonl")))
	if n != 9 {
		t.Fatalf("entries read = %d, want 9", n)
	}
	got := sums(t, h)

	// api: priority 3, regex error on priority 6, the byte-array message.
	var apiErrors float64
	var redacted bool
	for k, v := range got[MetricLogErrorsTotal] {
		if strings.HasPrefix(k, "host/api") {
			apiErrors += v
		}
		if strings.Contains(k, "hunter2") {
			t.Errorf("secret leaked into sample label: %q", k)
		}
		if strings.Contains(k, "password=***") {
			redacted = true
		}
	}
	if apiErrors != 3 {
		t.Errorf("api errors = %v, want 3 (%v)", apiErrors, got[MetricLogErrorsTotal])
	}
	if !redacted {
		t.Errorf("no redacted sample among %v", got[MetricLogErrorsTotal])
	}
	if got[MetricLogWarnsTotal]["host/api"] != 1 {
		t.Errorf("api warns = %v, want 1 (PRIORITY 4)", got[MetricLogWarnsTotal])
	}
	if got[MetricLogLinesTotal]["host/api"] != 5 {
		t.Errorf("api lines = %v, want 5", got[MetricLogLinesTotal]["host/api"])
	}
	// worker@1: regex warn fallback, unit name without .service.
	if got[MetricLogWarnsTotal]["host/worker@1"] != 1 {
		t.Errorf("worker warns = %v", got[MetricLogWarnsTotal])
	}
	// Non-service scope, entries without a unit and bad JSON are not attributed.
	for _, m := range got {
		for k := range m {
			if strings.Contains(k, "session") {
				t.Errorf("scope unit attributed: %q", k)
			}
		}
	}
	if d := got[MetricLinesDroppedTotal][""]; d != 1 {
		t.Errorf("dropped = %v, want 1 (the malformed line)", d)
	}
}

func TestRawString(t *testing.T) {
	cases := map[string]string{
		`"plain"`:     "plain",
		`[104,105]`:   "hi",
		`[72,255,73]`: "H�I",
		`["a","b"]`:   "a",
		`null`:        "null",
		``:            "",
		`[300]`:       "",
	}
	for in, want := range cases {
		if got := rawString([]byte(in)); got != want {
			t.Errorf("rawString(%s) = %q, want %q", in, got, want)
		}
	}
}

func TestPriorityOf(t *testing.T) {
	for in, want := range map[string]int{`"3"`: 3, `4`: 4, `"x"`: -1, ``: -1} {
		if got := priorityOf([]byte(in)); got != want {
			t.Errorf("priorityOf(%s) = %d, want %d", in, got, want)
		}
	}
}

func TestNormalizeUnitsAndArgs(t *testing.T) {
	got := normalizeUnits([]string{"b", "a.service", " ", "b.service"})
	if !slices.Equal(got, []string{"a.service", "b.service"}) {
		t.Fatalf("units = %v", got)
	}
	want := []string{"--follow", "--output=json", "--since=now", "--no-pager", "_SYSTEMD_UNIT=a.service", "_SYSTEMD_UNIT=b.service"}
	if a := journalArgs(got); !slices.Equal(a, want) {
		t.Fatalf("args = %v", a)
	}
}

// fakeJournal records every launch and hands out scripted outputs.
type fakeJournal struct {
	mu      sync.Mutex
	calls   [][]string
	outputs []string // per launch; beyond the list the stream stays open
}

func (f *fakeJournal) start(ctx context.Context, args []string) (io.ReadCloser, func() error, error) {
	f.mu.Lock()
	i := len(f.calls)
	f.calls = append(f.calls, args)
	var data string
	scripted := i < len(f.outputs)
	if scripted {
		data = f.outputs[i]
	}
	f.mu.Unlock()
	pr, pw := io.Pipe()
	go func() {
		_, _ = io.WriteString(pw, data)
		if scripted {
			_ = pw.Close() // EOF: the process exits
			return
		}
		<-ctx.Done()
		_ = pw.Close()
	}()
	return pr, func() error { return nil }, nil
}

func (f *fakeJournal) launches() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHostLog_RestartsOnEOFWithBackoff(t *testing.T) {
	line := `{"_SYSTEMD_UNIT":"api.service","PRIORITY":"3","MESSAGE":"boom"}` + "\n"
	f := &fakeJournal{outputs: []string{line, "", line}}
	h := newTestHostLog(t, HostLogOptions{
		Start: f.start, UnitPoll: time.Hour, BackoffMin: 20 * time.Millisecond, BackoffMax: 80 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	start := time.Now()
	go func() { _ = h.Run(ctx); close(done) }()
	eventually(t, "three launches", func() bool { return len(f.launches()) >= 3 })
	eventually(t, "two error lines", func() bool {
		var n float64
		for _, v := range sums(t, h)[MetricLogErrorsTotal] {
			n += v
		}
		return n == 2
	})
	if time.Since(start) < 30*time.Millisecond {
		t.Errorf("restarts were not delayed by backoff")
	}
	cancel()
	<-done
}

func TestHostLog_RebuildsMatchListOnUnitChange(t *testing.T) {
	var mu sync.Mutex
	units := []string{"api"}
	f := &fakeJournal{}
	h := newTestHostLog(t, HostLogOptions{
		Units: func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(units) },
		Start: f.start, UnitPoll: 10 * time.Millisecond, BackoffMin: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = h.Run(ctx); close(done) }()
	eventually(t, "first launch", func() bool { return len(f.launches()) == 1 })
	mu.Lock()
	units = []string{"web", "api"}
	mu.Unlock()
	// BackoffMin is an hour: a second launch can only be the unit change,
	// which must not wait for backoff.
	eventually(t, "second launch", func() bool { return len(f.launches()) == 2 })
	l := f.launches()
	if !slices.Contains(l[0], "_SYSTEMD_UNIT=api.service") || slices.Contains(l[0], "_SYSTEMD_UNIT=web.service") {
		t.Errorf("first args = %v", l[0])
	}
	if !slices.Contains(l[1], "_SYSTEMD_UNIT=api.service") || !slices.Contains(l[1], "_SYSTEMD_UNIT=web.service") {
		t.Errorf("second args = %v", l[1])
	}
	cancel()
	<-done
}

func TestHostLog_NoUnitsNoProcess(t *testing.T) {
	f := &fakeJournal{}
	h := newTestHostLog(t, HostLogOptions{
		Units: func() []string { return nil }, Start: f.start, UnitPoll: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = h.Run(ctx)
	if n := len(f.launches()); n != 0 {
		t.Fatalf("launched %d times with no units", n)
	}
}

func TestParseHostLogFiles(t *testing.T) {
	got, err := ParseHostLogFiles([]string{"nginx=/var/log/nginx/*.log, app.service=/opt/app/*.log", "db=/x/y"})
	if err != nil {
		t.Fatal(err)
	}
	want := []HostLogFile{{"nginx", "/var/log/nginx/*.log"}, {"app.service", "/opt/app/*.log"}, {"db", "/x/y"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v", got)
	}
	for _, bad := range []string{"nope", "=x", "a=", "a=[", "a=b,c"} {
		if _, err := ParseHostLogFiles([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// A glob match that is a symlink is not tailed (the agent reads as root), and
// the refusal is logged once per path, not on every rescan.
func TestHostLog_RefusesSymlinkedLogFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "secret.txt")
	if err := os.WriteFile(target, []byte("ERROR from a file the glob should not reach\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "app.log")); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var refusals int
	log := funcr.New(func(prefix, args string) {
		if strings.Contains(args, "not a regular file") {
			mu.Lock()
			refusals++
			mu.Unlock()
		}
	}, funcr.Options{})
	h, err := NewHostLog(HostLogOptions{
		Units:     func() []string { return nil },
		Namespace: "host", Node: "box",
		LogFiles: []HostLogFile{{Unit: "app.service", Glob: filepath.Join(dir, "app.log")}},
		UnitPoll: time.Hour, RescanInterval: 5 * time.Millisecond, PollInterval: 5 * time.Millisecond,
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = h.Run(ctx); close(done) }()
	eventually(t, "refusal logged", func() bool { mu.Lock(); defer mu.Unlock(); return refusals == 1 })
	time.Sleep(50 * time.Millisecond) // several rescans
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if refusals != 1 {
		t.Errorf("refusal logged %d times, want once", refusals)
	}
	if n := sums(t, h)[MetricFilesTracked][""]; n != 0 {
		t.Errorf("files tracked = %v, want 0", n)
	}
}

// The tailer itself refuses a symlink (O_NOFOLLOW) and a non-regular file
// when marked as a host log file, even if the path was swapped after the
// rescan's check.
func TestTailer_HostFileOpen(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real.log")
	if err := os.WriteFile(real, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.log")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if err := newTailerHost(link).openFresh(true); err == nil {
		t.Error("a symlink was opened as a host log file")
	}
	if err := newTailerHost(dir).openFresh(true); !errors.Is(err, errNotRegular) {
		t.Errorf("a directory: err = %v, want errNotRegular", err)
	}
	tl := newTailerHost(real)
	if err := tl.openFresh(true); err != nil {
		t.Errorf("a regular file: %v", err)
	}
	tl.closeFile()
	plain := newTailer(link, "ns", "pod", "container", logr.Discard())
	if err := plain.openFresh(true); err != nil {
		t.Errorf("a container log symlink must still open: %v", err)
	}
	plain.closeFile()
}

func newTailerHost(path string) *tailer {
	t := newTailer(path, "host", "", "", logr.Discard())
	t.hostFile = true
	return t
}

func TestHostLog_FileTailUnderFixedService(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "app.log")
	if err := os.WriteFile(p, []byte("old ERROR ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := newTestHostLog(t, HostLogOptions{
		Units:    func() []string { return nil },
		LogFiles: []HostLogFile{{Unit: "app.service", Glob: filepath.Join(dir, "*.log")}},
		UnitPoll: time.Hour, RescanInterval: 10 * time.Millisecond, PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = h.Run(ctx); close(done) }()
	eventually(t, "file tracked", func() bool { return sums(t, h)[MetricFilesTracked][""] == 1 })
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Tracked is counted when the tailer starts, but it opens the file at its
	// end on its first tick: plain lines go in until one is counted, so the
	// lines under test are written to an open tailer.
	eventually(t, "tailer open", func() bool {
		_, _ = f.WriteString("ready\n")
		return sums(t, h)[MetricLogLinesTotal]["host/app"] > 0
	})
	_, _ = f.WriteString("level=error msg=\"disk full\"\nfine\n")
	_ = f.Close()
	errs := func() (n float64) {
		for k, v := range sums(t, h)[MetricLogErrorsTotal] {
			if strings.HasPrefix(k, "host/app") {
				n += v
			}
		}
		return n
	}
	eventually(t, "error counted", func() bool { return errs() > 0 })
	if errs := errs(); errs != 1 {
		t.Errorf("errors = %v, want 1", errs)
	}
	cancel()
	<-done
}
