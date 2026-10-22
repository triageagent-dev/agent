package logexporter

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/go-logr/logr"
)

const (
	// tailBufferSize is the bufio.Reader's internal buffer — large enough that a
	// normal log line is read in one shot (docs/adr/0014-log-exporter-in-agent-
	// ds.md "Capacity" section).
	tailBufferSize = 256 * 1024
	// maxPendingLineBytes bounds how much of an undelimited (no trailing "\n"
	// seen yet) line this tailer will accumulate before giving up on it —
	// "never buffer unboundedly and never OOM" (requirement 3): past this, the
	// partial is dropped (counted in selfMetrics.linesDropped) and the tailer
	// resyncs at the next newline it finds.
	maxPendingLineBytes = 1 << 20 // 1MiB
)

// tailer owns one container log file: its own goroutine (via Exporter.run),
// its own bufio.Reader, its own rotation/truncation state. Nothing here is
// shared with another tailer, and the only shared state touched from the
// per-line hot path — counters, a *counterSet resolved once at file-open/
// discovery time — is accessed exclusively through atomics. This is the
// isolation guarantee: a hot or stuck file can only stall ITS OWN goroutine's
// ticks, never another file's, and Collect's read (metrics.go's buildMetrics)
// never blocks on, or is blocked by, any tailer.
type tailer struct {
	path                      string
	namespace, pod, container string

	// counters is resolved once (discovery.go's resolveService, invoked at
	// file-open time, retried on later rescans if not yet resolvable) and read
	// on every processed line via Load — no map lookup, no lock, in the hot
	// path. nil means "not yet attributable" (pod not in the lister cache yet)
	// or "pod has no service label" (permanently unattributable for this file).
	counters atomic.Pointer[counterSet]

	file *os.File
	br   *bufio.Reader
	ino  uint64
	// opened mirrors "t.file != nil" as an atomic — the only tailer-internal
	// state safe to observe from another goroutine (e.g. a test polling
	// "has this file been opened yet"). Every other field above/below is
	// touched exclusively by this tailer's own goroutine.
	opened atomic.Bool
	// offset is this tailer's best-known logical read position in the current
	// file — used only to detect in-place truncation (fi.Size() < offset), not
	// for exactness.
	offset int64

	partial []byte // bytes of an in-progress (no trailing "\n" seen yet) line
	inSplit bool   // CRI P/F dedup state — see cri.go / processLine

	// hostFile marks a file named by --host-log-files, read as root: it is
	// opened without following a symlink and must be a regular file.
	hostFile bool

	log logr.Logger
}

func newTailer(path, namespace, pod, container string, log logr.Logger) *tailer {
	return &tailer{path: path, namespace: namespace, pod: pod, container: container, log: log}
}

// errNotRegular is returned for a host log file that is not a regular file.
var errNotRegular = errors.New("not a regular file")

// open opens the path. A host log file is opened with O_NOFOLLOW (a symlink
// fails with ELOOP) and O_NONBLOCK (a FIFO does not hang the open).
func (t *tailer) open() (*os.File, error) {
	if !t.hostFile {
		return os.Open(t.path)
	}
	return os.OpenFile(t.path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

// stat is the rotation check's stat: it does not follow a symlink for a
// host log file, so a path swapped for a link reads as rotated and the
// reopen refuses it.
func (t *tailer) stat() (os.FileInfo, error) {
	if t.hostFile {
		return os.Lstat(t.path)
	}
	return os.Stat(t.path)
}

// fileIno extracts the inode from an os.FileInfo via its platform Sys() value.
// syscall.Stat_t.Ino is present (as uint64) on every platform this module
// builds for (Linux in production, Darwin for local dev/test), so no build
// tags are needed.
func fileIno(fi os.FileInfo) (uint64, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Ino), true
}

// openFresh (re)opens t.path, replacing any existing file handle and resetting
// all per-file read state. atEnd=true means "first time this exporter has ever
// looked at this path" — start at the current end of file, per the "never
// re-read history" requirement. atEnd=false means a rotation created a
// brand-new, never-before-seen file at this same path — start at its
// beginning, since none of ITS content has been read yet.
func (t *tailer) openFresh(atEnd bool) error {
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
		t.br = nil
	}
	f, err := t.open()
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if t.hostFile && !fi.Mode().IsRegular() {
		_ = f.Close()
		return errNotRegular
	}
	var offset int64
	if atEnd {
		offset = fi.Size()
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
	}
	t.file = f
	t.br = bufio.NewReaderSize(f, tailBufferSize)
	t.ino, _ = fileIno(fi)
	t.offset = offset
	t.partial = t.partial[:0]
	t.inSplit = false
	t.opened.Store(true)
	return nil
}

func (t *tailer) closeFile() {
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
		t.br = nil
	}
	t.opened.Store(false)
}

// tick is one poll cycle for this file: check for rotation/truncation, then
// drain whatever new, complete lines are available. Runs entirely on this
// tailer's own goroutine.
func (t *tailer) tick(cls *classifier, self *selfMetrics) {
	if t.file == nil {
		if err := t.openFresh(true); err != nil {
			return // not there / not readable yet — retry next tick
		}
	} else if fi, statErr := t.stat(); statErr == nil {
		ino, hasIno := fileIno(fi)
		switch {
		case hasIno && ino != t.ino:
			// Rotated (rename-and-recreate): a genuinely new file now lives at
			// this path. Any undelimited partial we were holding for the OLD
			// file is unrecoverable — it will never see its trailing newline.
			if len(t.partial) > 0 {
				self.linesDropped.Add(1)
			}
			if err := t.openFresh(false); err != nil {
				return
			}
		case fi.Size() < t.offset:
			// Truncated in place (copytruncate-style rotation): same inode,
			// shrunk. Same file handle, seek back to the new (empty) start.
			if len(t.partial) > 0 {
				self.linesDropped.Add(1)
			}
			if _, err := t.file.Seek(0, io.SeekStart); err != nil {
				return
			}
			t.br.Reset(t.file)
			t.offset = 0
			t.partial = t.partial[:0]
			t.inSplit = false
		}
	}
	// A Stat error here (ENOENT mid-rescan-race, transient) is not fatal: skip
	// the rotation check this tick and still try to drain whatever is already
	// readable on the currently-open fd. The exporter's directory rescan is
	// what actually retires a removed file's tailer (see exporter.go).

	t.drain(cls, self)
	if pos, err := t.file.Seek(0, io.SeekCurrent); err == nil {
		t.offset = pos
	}
}

// drain reads and processes every complete ("\n"-terminated) line currently
// available, then returns — it never blocks waiting for more data (the caller's
// poll ticker handles that). This is the per-line hot path: ReadSlice returns a
// slice into bufio's own internal buffer with zero allocation for the common
// case (a line that fits within tailBufferSize and completes on this read), so
// the only allocation on the whole hot path is the rare cross-poll partial-line
// copy in appendPartial.
func (t *tailer) drain(cls *classifier, self *selfMetrics) {
	for {
		line, err := t.br.ReadSlice('\n')
		switch {
		case err == nil:
			full := line
			if len(t.partial) > 0 {
				t.partial = append(t.partial, line...)
				full = t.partial
			}
			t.processLine(full, cls, self)
			if len(t.partial) > 0 {
				t.partial = t.partial[:0]
			}
		case errors.Is(err, bufio.ErrBufferFull):
			// No delimiter within one full buffer's worth — this one line is
			// wider than tailBufferSize. More of it may already be sitting in
			// the file unread, so keep looping immediately rather than waiting
			// for the next poll tick.
			t.appendPartial(line, self)
		default:
			// io.EOF (nothing more available right now) or a genuine read
			// error. Save any trailing undelimited bytes and stop; the next
			// poll tick picks up where this left off.
			t.appendPartial(line, self)
			return
		}
	}
}

// appendPartial accumulates bytes of a line that hasn't seen its trailing "\n"
// yet, bounded by maxPendingLineBytes so a pathological single line can never
// grow this tailer's memory without limit (requirement 3: never OOM). Past the
// cap the accumulated partial is dropped and counted — the tailer resyncs at
// the next newline boundary it finds.
func (t *tailer) appendPartial(line []byte, self *selfMetrics) {
	if len(line) == 0 {
		return
	}
	if len(t.partial)+len(line) > maxPendingLineBytes {
		self.linesDropped.Add(1)
		t.partial = t.partial[:0]
		return
	}
	t.partial = append(t.partial, line...)
}

// processLine handles ONE complete ("\n"-terminated) raw line: CRI-prefix skip
// and the P/F split-sequence dedup, then hands the actual content to countLine.
// full is trimmed of its trailing EOL first (a cheap sub-slice, not a copy).
func (t *tailer) processLine(full []byte, cls *classifier, self *selfMetrics) {
	self.bytesProcessed.Add(uint64(len(full)))
	body := trimEOL(full)

	flag, content, ok := splitCRI(body)
	if !ok {
		// Not CRI-formatted (or a malformed/legacy line) — fall back to
		// counting the whole line as one complete entry rather than dropping
		// it silently.
		t.countLine(body, cls, self)
		return
	}

	switch flag {
	case 'F':
		if t.inSplit {
			// Final chunk of a split sequence: already counted at the first
			// 'P' chunk below. Just close out the sequence.
			t.inSplit = false
			return
		}
		t.countLine(content, cls, self)
	case 'P':
		if t.inSplit {
			// A later chunk of an already-counted split sequence: skip. This
			// is the documented limitation (docs/adr/0014-log-exporter-in-
			// agent-ds.md) — a marker appearing only in a LATER chunk of a
			// >16KB split line is not seen, same class of caveat Fluent Bit's
			// own multiline handling carries.
			return
		}
		t.inSplit = true
		t.countLine(content, cls, self)
	}
}

// countLine is the innermost hot-path step: bump the exporter-wide
// lines-processed self-metric, then — if this file resolved to an
// attributable (namespace, service) — bump total plus error/warn via the
// classifier. No map lookup, no allocation, one or two atomic adds — UNLESS
// the line classifies as an error, in which case structured-message
// extraction (jsonextract.go, logfmtextract.go) + fingerprint (the IDENTITY
// key — leading-noise-stripped on its already-masked form, unchanged since
// before round 5) + recordError run
// (docs/adr/0014-log-exporter-in-agent-ds.md "Samples" section): that cost is
// bounded to the ~5% of lines that are errors and never reached from the
// non-error (warn/plain) path. text (the raw, unmasked extracted text) is
// passed through to recordError so it can build the DISPLAY exemplar —
// round 5's identity/display split — but only actually does so on the rare
// new-fingerprint path (recordError's own doc comment); the steady-state
// existing-fingerprint path here costs exactly what it did before round 5.
func (t *tailer) countLine(content []byte, cls *classifier, self *selfMetrics) {
	self.linesProcessed.Add(1)
	cs := t.counters.Load()
	if cs == nil {
		// Unattributable (no service label resolvable, or not yet resolved) —
		// counted nowhere per-service, matching the Fluent Bit collector's own
		// skip of unattributable entries. Still contributes to the exporter's
		// own throughput self-metric above.
		return
	}
	cs.total.Add(1)
	record(cs, cls.classify(content), content, cls)
}

// record bumps cs for one line already classified as v. Shared by the file
// tailer and the journald reader so both feed identical series.
func record(cs *counterSet, v verdict, content []byte, cls *classifier) {
	switch v {
	case verdictError:
		text := extractErrorText(content, cls.sampleFields)
		fp := fingerprint(text, cls.sampleMaxLen)
		fp = stripLeadingNoise(fp)
		cs.recordError(fp, text, cls.sampleMaxLen, cls.maxSamplesPerService)
	case verdictWarn:
		cs.warns.Add(1)
	}
}
