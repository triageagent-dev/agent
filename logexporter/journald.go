package logexporter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
)

// Host mode LOG role (docs/adr/0018-linux-host-mode.md, phase 4). A host has
// no /var/log/containers; its services log to journald. HostLog follows the
// journal of the selected systemd units with `journalctl --output=json` and
// counts lines into the same per-(namespace, service) series the file tailer
// produces, so the logmetrics collector sees identical metrics. Services that
// log to plain files are tailed with the same tailer as the cluster exporter.

const (
	serviceSuffix = ".service"
	// maxJournalLineBytes bounds one JSON entry; a longer one is dropped.
	maxJournalLineBytes = 1 << 20

	defaultUnitPoll       = 10 * time.Second
	defaultBackoffMin     = time.Second
	defaultBackoffMax     = 30 * time.Second
	defaultJournalCommand = "journalctl"
)

// JournalStart launches a journal follower with args and returns its JSON
// output, a wait function that reports how the process ended, and an error if
// it could not start. Cancelling ctx must end the process. It is the seam
// tests replace so that journalctl is never run.
type JournalStart func(ctx context.Context, args []string) (out io.ReadCloser, wait func() error, err error)

// ExecJournalStart runs the journalctl binary.
func ExecJournalStart(command string) JournalStart {
	if command == "" {
		command = defaultJournalCommand
	}
	return func(ctx context.Context, args []string) (io.ReadCloser, func() error, error) {
		cmd := exec.CommandContext(ctx, command, args...)
		out, err := cmd.StdoutPipe()
		if err != nil {
			return nil, nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, nil, err
		}
		return out, cmd.Wait, nil
	}
}

// HostLogFile names a plain-file log of one service: every file matching Glob
// is tailed and counted under Unit.
type HostLogFile struct {
	Unit string
	Glob string
}

// ParseHostLogFiles parses --host-log-files values, each `<unit>=<glob>`; an
// entry may itself hold several comma-separated specs.
func ParseHostLogFiles(values []string) ([]HostLogFile, error) {
	var out []HostLogFile
	for _, v := range values {
		for _, spec := range strings.Split(v, ",") {
			spec = strings.TrimSpace(spec)
			if spec == "" {
				continue
			}
			unit, glob, ok := strings.Cut(spec, "=")
			unit, glob = strings.TrimSpace(unit), strings.TrimSpace(glob)
			if !ok || unit == "" || glob == "" {
				return nil, fmt.Errorf("--host-log-files %q: want <unit>=<glob>", spec)
			}
			if _, err := filepath.Match(glob, ""); err != nil {
				return nil, fmt.Errorf("--host-log-files %q: %w", spec, err)
			}
			out = append(out, HostLogFile{Unit: unit, Glob: glob})
		}
	}
	return out, nil
}

// HostLogOptions configures HostLog.
type HostLogOptions struct {
	// Node names the emitted resource (the host's cluster name).
	Node string
	// Namespace is the synthetic namespace every unit reports under.
	Namespace string
	// Units returns the currently selected unit names, with or without the
	// ".service" suffix. It is polled every UnitPoll; a changed set restarts
	// the follower with a new match list. Required.
	Units func() []string
	// LogFiles are optional plain-file tails.
	LogFiles []HostLogFile
	// Start launches journalctl; default ExecJournalStart("journalctl").
	Start JournalStart

	// Classification and sampling: same meaning and defaults as Options.
	ErrorRegex           string
	WarnRegex            string
	SampleMaxLen         int
	MaxSamplesPerService int
	SampleJSONFields     []string

	UnitPoll       time.Duration // default 10s
	BackoffMin     time.Duration // default 1s
	BackoffMax     time.Duration // default 30s
	RescanInterval time.Duration // file glob rescan; default 10s
	PollInterval   time.Duration // file tail poll; default 250ms
}

// HostLog is the host LOG role: a metricset role source plus the run loop
// that feeds it.
type HostLog struct {
	opts HostLogOptions
	cls  *classifier
	reg  *seriesRegistry
	self *selfMetrics
	log  logr.Logger

	mu      sync.Mutex
	tracked map[string]context.CancelFunc // tailed file -> stop
	// refused holds the glob matches already logged as not regular files, so
	// each is reported once.
	refused map[string]bool
}

// NewHostLog builds the host LOG role. Nothing runs until Run.
func NewHostLog(opts HostLogOptions, log logr.Logger) (*HostLog, error) {
	if opts.Units == nil {
		return nil, errors.New("logexporter: HostLogOptions.Units is required")
	}
	eo := Options{
		ErrorRegex: opts.ErrorRegex, WarnRegex: opts.WarnRegex, SampleMaxLen: opts.SampleMaxLen,
		MaxSamplesPerService: opts.MaxSamplesPerService, SampleJSONFields: opts.SampleJSONFields,
		RescanInterval: opts.RescanInterval, PollInterval: opts.PollInterval,
	}
	eo.setDefaults()
	opts.ErrorRegex, opts.WarnRegex, opts.SampleMaxLen = eo.ErrorRegex, eo.WarnRegex, eo.SampleMaxLen
	opts.MaxSamplesPerService, opts.SampleJSONFields = eo.MaxSamplesPerService, eo.SampleJSONFields
	opts.RescanInterval, opts.PollInterval = eo.RescanInterval, eo.PollInterval
	if opts.Start == nil {
		opts.Start = ExecJournalStart("")
	}
	if opts.UnitPoll <= 0 {
		opts.UnitPoll = defaultUnitPoll
	}
	if opts.BackoffMin <= 0 {
		opts.BackoffMin = defaultBackoffMin
	}
	if opts.BackoffMax < opts.BackoffMin {
		opts.BackoffMax = max(defaultBackoffMax, opts.BackoffMin)
	}
	cls, err := newClassifier(opts.ErrorRegex, opts.WarnRegex, opts.SampleMaxLen, opts.MaxSamplesPerService, opts.SampleJSONFields)
	if err != nil {
		return nil, err
	}
	return &HostLog{
		opts: opts, cls: cls, reg: newSeriesRegistry(), self: &selfMetrics{},
		log: log.WithName("hostlog"), tracked: map[string]context.CancelFunc{},
	}, nil
}

// Collect returns the counters as cumulative OTLP (sensorstream.RoleSource).
func (h *HostLog) Collect(_ context.Context) ([]*metricspb.ResourceMetrics, error) {
	b := metricset.NewBuilder("k8s.node.name", h.opts.Node)
	buildMetrics(b, h.reg, h.self)
	return b.ResourceMetrics(), nil
}

// Run follows the journal and the plain files until ctx ends.
func (h *HostLog) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	if len(h.opts.LogFiles) > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); h.runFiles(ctx) }()
	}
	h.runJournal(ctx)
	wg.Wait()
	return nil
}

// normalizeUnits returns the sorted, de-duplicated unit names with the
// ".service" suffix.
func normalizeUnits(units []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, u := range units {
		if u = strings.TrimSpace(u); u == "" {
			continue
		}
		if !strings.HasSuffix(u, serviceSuffix) {
			u += serviceSuffix
		}
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	slices.Sort(out)
	return out
}

// journalArgs builds the journalctl command line: one `_SYSTEMD_UNIT=` match
// per unit (matches on one field are ORed).
func journalArgs(units []string) []string {
	args := []string{"--follow", "--output=json", "--since=now", "--no-pager"}
	for _, u := range units {
		args = append(args, "_SYSTEMD_UNIT="+u)
	}
	return args
}

// runJournal keeps one journalctl follower alive for the current unit set.
func (h *HostLog) runJournal(ctx context.Context) {
	backoff := h.opts.BackoffMin
	var units []string
	for ctx.Err() == nil {
		units = normalizeUnits(h.opts.Units())
		if len(units) == 0 {
			// Nothing selected yet: no process, look again shortly.
			if !sleepCtx(ctx, h.opts.UnitPoll) {
				return
			}
			continue
		}
		started := time.Now()
		changed, lines := h.followOnce(ctx, units)
		if ctx.Err() != nil {
			return
		}
		if changed {
			backoff = h.opts.BackoffMin
			h.log.Info("unit set changed, restarting journal follower", "units", len(units))
			continue
		}
		// An exit that read lines or ran for a while was healthy; start over
		// from the minimum so a daily journald rotation costs one second.
		if lines > 0 || time.Since(started) > h.opts.BackoffMax {
			backoff = h.opts.BackoffMin
		}
		h.log.Info("journal follower ended, restarting", "after", backoff)
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, h.opts.BackoffMax)
	}
}

// followOnce runs the follower until it exits, ctx ends, or the unit set
// changes. It reports whether the unit set changed and how many entries were
// read.
func (h *HostLog) followOnce(ctx context.Context, units []string) (changed bool, lines int) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	out, wait, err := h.opts.Start(cctx, journalArgs(units))
	if err != nil {
		h.log.Error(err, "start journal follower")
		return false, 0
	}
	done := make(chan int, 1)
	go func() { done <- h.consume(out) }()

	t := time.NewTicker(h.opts.UnitPoll)
	defer t.Stop()
	for {
		select {
		case lines = <-done:
			_ = out.Close()
			if err := wait(); err != nil && ctx.Err() == nil {
				h.log.Info("journal follower exited", "error", err.Error())
			}
			return false, lines
		case <-t.C:
			if !slices.Equal(units, normalizeUnits(h.opts.Units())) {
				cancel()
				_ = out.Close()
				lines = <-done
				_ = wait()
				return true, lines
			}
		case <-ctx.Done():
			_ = out.Close()
			lines = <-done
			_ = wait()
			return false, lines
		}
	}
}

// consume reads JSON entries until EOF and returns how many it read. An entry
// longer than maxJournalLineBytes is dropped and counted.
func (h *HostLog) consume(r io.Reader) int {
	br := bufio.NewReaderSize(r, 64*1024)
	n := 0
	var buf []byte
	oversize := false
	for {
		chunk, err := br.ReadSlice('\n')
		if !oversize {
			if len(buf)+len(chunk) > maxJournalLineBytes {
				oversize = true
				buf = buf[:0]
				h.self.linesDropped.Add(1)
			} else {
				buf = append(buf, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if !oversize && len(bytes.TrimSpace(buf)) > 0 {
			n++
			h.handleEntry(buf)
		}
		buf, oversize = buf[:0], false
		if err != nil {
			return n
		}
	}
}

// journalEntry holds the fields of a `journalctl --output=json` entry that
// matter here. Values are raw because the format is irregular: a field is a
// string, or an array of byte values when its content is not UTF-8, or an
// array of values when it occurs more than once.
type journalEntry struct {
	Unit     json.RawMessage `json:"_SYSTEMD_UNIT"`
	Priority json.RawMessage `json:"PRIORITY"`
	Message  json.RawMessage `json:"MESSAGE"`
}

// rawString decodes one journal JSON field value as text.
func rawString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	switch raw[0] {
	case '"':
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
	case '[':
		var nums []int
		if json.Unmarshal(raw, &nums) == nil {
			b := make([]byte, 0, len(nums))
			for _, n := range nums {
				if n < 0 || n > 255 {
					return ""
				}
				b = append(b, byte(n))
			}
			return strings.ToValidUTF8(string(b), "�")
		}
		var vals []json.RawMessage
		if json.Unmarshal(raw, &vals) == nil && len(vals) > 0 {
			return rawString(vals[0])
		}
	default:
		return string(raw)
	}
	return ""
}

// priorityOf parses PRIORITY; -1 when absent or unparsable.
func priorityOf(raw json.RawMessage) int {
	n, err := strconv.Atoi(strings.TrimSpace(rawString(raw)))
	if err != nil {
		return -1
	}
	return n
}

// unitService maps a _SYSTEMD_UNIT value to a service name: the unit name
// without ".service". Anything that is not a service unit is not attributed.
func unitService(unit string) (string, bool) {
	svc, ok := strings.CutSuffix(unit, serviceSuffix)
	return svc, ok && svc != ""
}

// handleEntry attributes and counts one journal entry.
func (h *HostLog) handleEntry(line []byte) {
	h.self.bytesProcessed.Add(uint64(len(line)))
	h.self.linesProcessed.Add(1)
	var e journalEntry
	if err := json.Unmarshal(line, &e); err != nil {
		h.self.linesDropped.Add(1)
		return
	}
	svc, ok := unitService(rawString(e.Unit))
	if !ok {
		return
	}
	cs := h.reg.getOrCreate(h.opts.Namespace, svc)
	msg := []byte(rawString(e.Message))
	cs.total.Add(1)
	var v verdict
	switch p := priorityOf(e.Priority); {
	case p >= 0 && p <= 3:
		v = verdictError
	case p == 4:
		v = verdictWarn
	default:
		v = h.cls.classify(msg)
	}
	record(cs, v, msg, h.cls)
}

// sleepCtx waits d or until ctx ends; it reports whether the full wait ran.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// runFiles tails the configured plain files: every file matching a glob is
// counted under the glob's unit, through the cluster exporter's own tailer.
func (h *HostLog) runFiles(ctx context.Context) {
	h.rescanFiles(ctx)
	t := time.NewTicker(h.opts.RescanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			for p, stop := range h.tracked {
				stop()
				delete(h.tracked, p)
			}
			h.mu.Unlock()
			return
		case <-t.C:
			h.rescanFiles(ctx)
		}
	}
}

func (h *HostLog) rescanFiles(ctx context.Context) {
	present := map[string]bool{}
	refusedNow := map[string]bool{}
	h.mu.Lock()
	for _, lf := range h.opts.LogFiles {
		svc, _ := strings.CutSuffix(lf.Unit, serviceSuffix)
		paths, _ := filepath.Glob(lf.Glob)
		for _, p := range paths {
			// The agent reads as root: a match that is a symlink (to any
			// file the root can read), a FIFO or a device is not tailed.
			fi, err := os.Lstat(p)
			if err != nil {
				continue // gone since the glob
			}
			if !fi.Mode().IsRegular() {
				refusedNow[p] = true
				if !h.refused[p] {
					h.log.Info("host log file refused: not a regular file", "path", p, "mode", fi.Mode().Type().String())
				}
				continue
			}
			key := svc + "\x00" + p
			present[key] = true
			if _, ok := h.tracked[key]; ok {
				continue
			}
			t := newTailer(p, h.opts.Namespace, "", "", h.log)
			t.hostFile = true
			t.counters.Store(h.reg.getOrCreate(h.opts.Namespace, svc))
			tctx, cancel := context.WithCancel(ctx)
			h.tracked[key] = cancel
			h.self.filesTracked.Add(1)
			go h.runTailer(tctx, t)
		}
	}
	for key, stop := range h.tracked {
		if !present[key] {
			stop()
			delete(h.tracked, key)
			h.self.filesTracked.Add(-1)
		}
	}
	h.refused = refusedNow
	h.mu.Unlock()
}

func (h *HostLog) runTailer(ctx context.Context, t *tailer) {
	defer t.closeFile()
	tk := time.NewTicker(h.opts.PollInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.tick(h.cls, h.self)
		}
	}
}
