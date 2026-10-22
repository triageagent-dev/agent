// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ebpfcommon // import "go.opentelemetry.io/obi/pkg/ebpf/common"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/net/http2"

	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/obi/pkg/appolly/app"
	"go.opentelemetry.io/obi/pkg/appolly/app/request"
	"go.opentelemetry.io/obi/pkg/ebpf/ringbuf"
	"go.opentelemetry.io/obi/pkg/internal/ebpf/bhpack"
	"go.opentelemetry.io/obi/pkg/internal/largebuf"
)

type BPFHTTP2Info BpfHttp2GrpcRequestT

type Protocol uint8

// The following consts need to coincide with some C identifiers:
// EVENT_HTTP_REQUEST, EVENT_GRPC_REQUEST, EVENT_HTTP_CLIENT, EVENT_GRPC_CLIENT, EVENT_SQL_CLIENT
const (
	HTTP2 Protocol = iota + 1
	GRPC
)

const initialHeaderTableSize = 4096

const (
	maxPendingH2Streams   = 128
	maxCompletedH2Streams = 128
)

const (
	h2HpackRequestUnreliable  = 0x1
	h2HpackResponseUnreliable = 0x2
	h2HpackNewConnection      = 0x4
)

var (
	// anchored to '/': a desynced HPACK dynamic table can resolve :path to another
	// field's value (e.g. a traceparent), which must degrade to "*", not a label
	validPath        = regexp.MustCompile(`^/[A-Za-z0-9\-/._~]*$`)
	validContentType = regexp.MustCompile(`^[A-Za-z\-/\+]+$`)
)

type h2Connection struct {
	hdec      *bhpack.Decoder
	hdecRet   *bhpack.Decoder
	protocol  Protocol
	streams   map[uint32]*h2StreamMeta
	completed map[uint32]struct{}
	respSlots h2ResponseSlots
}

// responseGap records that part of the response header stream was not seen:
// the decoder's table is not the server's, and slots learned so far no longer
// line up with the entries they named.
func (c *h2Connection) responseGap() {
	c.hdecRet.MarkUnreliable()
	c.respSlots = h2ResponseSlots{}
}

type h2ResponseMeta struct {
	status int
	grpc   bool
	ok     bool
	// statusUnread: the block ended the stream and named a dynamic table
	// entry the sensor never saw (it attached to a connection already open),
	// and no grpc-status could be read from it. If this is gRPC, the block
	// carried the status and it is not known: it may be an error or OK.
	statusUnread bool
}

// grpcStatusUnread is the span status of a gRPC response whose status could
// not be read and could not be inferred (see h2ResponseSlots). It is outside
// the gRPC code enum on purpose, so request.GRPCStatusCodeString returns ""
// and the status attribute is omitted: the call is counted, as neither OK nor
// an error. It must not be UNKNOWN (2), which is a server error: a successful
// trailers-only response on a connection older than the sensor is just as
// unreadable as a failed one.
const grpcStatusUnread = -1

const (
	// maxH2LearnedSlots bounds each set h2ResponseSlots keeps per connection.
	maxH2LearnedSlots = 64
	// h2OKTrailersConfirmed is how many trailers blocks in a row must name
	// the same entries before those entries are taken as "status OK".
	h2OKTrailersConfirmed = 2
	grpcStatusMax         = 16
	grpcStatusUnknown     = 2
)

// h2ResponseSlots is what the responses on a connection older than the sensor
// have taught about table entries the decoder cannot read. The decoder cannot
// say what such an entry holds, but it can say which entry a block named
// (bhpack.SlotEvent), and the shape of a gRPC response says the rest:
//
//   - trailers that follow DATA are how a call succeeds, so the entries they
//     name are grpc-status 0 (and an empty grpc-message);
//   - response headers that do not end the stream name content-type and the
//     like, never a status, and a header whose value changes with every
//     response (a proxy's timing header) names its own previous entry;
//   - a status sent as a literal is readable even when its name is not, and
//     the entry it adds is that status from then on.
//
// A trailers-only response that names every "OK" entry succeeded; one that
// names other entries, outside the header ones, failed. Nothing is inferred
// until successful trailers have been seen, so a connection that only ever
// answers trailers-only stays unread.
type h2ResponseSlots struct {
	headers map[int64]struct{}
	// ok is the sorted set of entries the last trailers after DATA named,
	// okSeen how many such blocks in a row named exactly that set.
	ok     []int64
	okSeen int
	// codes are entries known to be grpc-status with this code.
	codes map[int64]int
}

// h2BlockSlots is what one response header block did to the table.
type h2BlockSlots struct {
	refs []int64            // unresolved entries named as whole fields
	lits []bhpack.SlotEvent // literals with an unresolved name, or added
}

func grpcStatusValue(v string) (int, bool) {
	if v == "" || len(v) > 2 {
		return 0, false
	}
	code, err := strconv.Atoi(v)
	return code, err == nil && code >= 0 && code <= grpcStatusMax
}

func (s *h2ResponseSlots) setCode(slot int64, code int) {
	if s.codes == nil || len(s.codes) >= maxH2LearnedSlots {
		s.codes = map[int64]int{}
	}
	s.codes[slot] = code
}

func (s *h2ResponseSlots) setHeader(slot int64) {
	if s.headers == nil || len(s.headers) >= maxH2LearnedSlots {
		s.headers = map[int64]struct{}{}
	}
	s.headers[slot] = struct{}{}
}

func (s *h2ResponseSlots) isHeader(slot int64) bool {
	_, header := s.headers[slot]
	return header
}

func (s *h2ResponseSlots) okTrusted() bool {
	return s.okSeen >= h2OKTrailersConfirmed && len(s.ok) > 0
}

// observe learns from a block that decoded to its end. opensResponse: the
// block carried :status; ended: it ended the stream; afterData: the stream's
// span had already ended at a DATA frame, so the block is trailers of a
// response that carried a message.
func (s *h2ResponseSlots) observe(b *h2BlockSlots, opensResponse, ended, afterData bool) {
	trailers := !opensResponse && ended && afterData
	for _, ev := range b.lits {
		if !ev.Adds {
			continue
		}
		// a header block's literals are headers, and so is a literal named
		// after a header entry wherever it appears
		if opensResponse && (!ended || (ev.Unresolved && s.isHeader(ev.Slot))) {
			s.setHeader(ev.AddedSlot)
			continue
		}
		code, isCode := grpcStatusValue(ev.Value)
		if !isCode {
			continue
		}
		// readable by name, or an unnamed small integer in trailers
		if ev.Name == "grpc-status" || (ev.Unresolved && trailers) {
			s.setCode(ev.AddedSlot, code)
		}
	}
	switch {
	case opensResponse && !ended:
		for _, slot := range b.refs {
			s.setHeader(slot)
		}
	case trailers:
		if len(b.refs) == 0 {
			return
		}
		named := slices.Clone(b.refs)
		slices.Sort(named)
		named = slices.Compact(named)
		if slices.Equal(named, s.ok) {
			s.okSeen++
			return
		}
		s.ok, s.okSeen = named, 1
	}
}

// trailersOnlyStatus infers the gRPC status of a trailers-only block whose
// status the decoder could not read. ok is false when nothing can be said.
func (s *h2ResponseSlots) trailersOnlyStatus(b *h2BlockSlots) (int, bool) {
	for _, ev := range b.lits {
		if !ev.Unresolved || s.isHeader(ev.Slot) {
			continue
		}
		code, isCode := grpcStatusValue(ev.Value)
		if !isCode {
			continue
		}
		// A small integer under an unreadable name is the status when the
		// name is an entry known to be grpc-status, or when the headers of
		// this connection are known and it is not one of them.
		_, statusEntry := s.codes[ev.Slot]
		if statusEntry || s.okTrusted() {
			if ev.Adds {
				s.setCode(ev.AddedSlot, code)
			}
			return code, true
		}
	}
	for _, slot := range b.refs {
		if code, known := s.codes[slot]; known {
			return code, true
		}
	}
	if !s.okTrusted() {
		return 0, false
	}
	covered := true
	for _, slot := range s.ok {
		if !slices.Contains(b.refs, slot) {
			covered = false
			break
		}
	}
	if covered {
		return 0, true
	}
	for _, slot := range b.refs {
		if !s.isHeader(slot) {
			return grpcStatusUnknown, true
		}
	}
	return 0, false
}

// h2SpanStatus maps a gRPC response to the span status.
func h2SpanStatus(response h2ResponseMeta) int {
	if response.statusUnread {
		return grpcStatusUnread
	}
	return http2grpcStatus(response.status)
}

// assumeGRPC is the guess for a stream nothing identifies: cleartext HTTP/2
// is almost always gRPC. It is not made for a response whose status was
// unread, because that response says nothing readable beyond :status (a plain
// HTTP/2 304 looks the same), so it stays HTTP/2 unless something positive, a
// gRPC content-type or a readable grpc-* header on the stream or earlier on
// the connection, says it is gRPC.
func assumeGRPC(event *BPFHTTP2Info, response h2ResponseMeta) bool {
	return event.Ssl == 0 && !response.statusUnread
}

type pendingH2Event struct {
	event    BPFHTTP2Info
	response h2ResponseMeta
}

type h2StreamMeta struct {
	requestSeen  bool
	request      h2RequestMeta
	requestOK    bool
	responseSeen bool
	response     h2ResponseMeta
}

type h2RequestMeta struct {
	method   string
	path     string
	fullPath string
	grpc     bool
}

func byteFramer(data []uint8) *http2.Framer {
	fr := http2.NewFramer(
		// we never write. We can save some resources
		io.Discard,
		bytes.NewReader(data),
	)

	return fr
}

// not all requests for a given stream specify the protocol, but one must
// we remember if we see grpc mentioned and tag the rest of the streams for
// a given connection as grpc. default assumes plain HTTP2
// this is why we need the h2c cache
func getOrInitH2ConnWithTrust(activeGRPCConnections *lru.Cache[uint64, *h2Connection], connID uint64, trustedNew bool) *h2Connection {
	v, ok := activeGRPCConnections.Get(connID)

	dynamicTableSize := initialHeaderTableSize
	if connID == 0 {
		dynamicTableSize = 0
	}

	if !ok {
		v = &h2Connection{
			hdec:      bhpack.NewDecoder(uint32(dynamicTableSize), nil),
			hdecRet:   bhpack.NewDecoder(uint32(dynamicTableSize), nil),
			protocol:  HTTP2,
			streams:   map[uint32]*h2StreamMeta{},
			completed: map[uint32]struct{}{},
		}
		if connID != 0 && !trustedNew {
			v.hdec.MarkUnreliable()
			v.hdecRet.MarkUnreliable()
		}
		activeGRPCConnections.Add(connID, v)
	}

	return v
}

func getOrInitH2Conn(activeGRPCConnections *lru.Cache[uint64, *h2Connection], connID uint64) *h2Connection {
	return getOrInitH2ConnWithTrust(activeGRPCConnections, connID, true)
}

func protocolIsGRPC(activeGRPCConnections *lru.Cache[uint64, *h2Connection], connID uint64) {
	h2c := getOrInitH2Conn(activeGRPCConnections, connID)
	if h2c != nil {
		h2c.protocol = GRPC
	}
}

var commonHDec = bhpack.NewDecoder(0, nil)

func isHTTPOp(op string) bool {
	return op == "GET" || op == "POST" || op == "PATCH" || op == "DELETE" || op == "OPTIONS" || op == "HEAD"
}

func handleHeaderField(hf *bhpack.HeaderField) bool {
	switch hf.Name {
	case ":method":
		if isHTTPOp(hf.Value) {
			return true
		}
	case ":scheme":
		if hf.Value == "http" {
			return true
		}
	case "traceparent":
		return true
	case ":path":
		val := hf.Value
		if pos := strings.Index(val, "?"); pos >= 0 {
			val = val[:pos]
		}
		if validPath.MatchString(val) {
			return true
		}
	case "content-type":
		val := hf.Value
		if validContentType.MatchString(val) {
			return true
		}
	case "grpc-status":
		return true
	}

	return false
}

func knownFrameKeys(fr *http2.Framer, hf *http2.HeadersFrame) bool {
	knownCount := 0
	commonHDec.SetEmitFunc(func(hf bhpack.HeaderField) {
		if handleHeaderField(&hf) {
			knownCount++
		}
	})
	// Lose reference to MetaHeadersFrame:
	defer commonHDec.SetEmitFunc(func(_ bhpack.HeaderField) {})
	defer commonHDec.Close()

	frag := hf.HeaderBlockFragment()
	for {
		if _, err := commonHDec.Write(frag); err != nil {
			break
		}
		if hf.HeadersEnded() {
			break
		}
		hff, err := fr.ReadFrame()
		if err != nil {
			break
		}
		cf, ok := hff.(*http2.ContinuationFrame)
		if !ok {
			break
		}
		frag = cf.HeaderBlockFragment()
	}

	return knownCount > 1
}

// hpackFieldStart is the offset of the first header field in frag, past the dynamic table size
// updates a block may open with. Negative when the block holds no field.
func hpackFieldStart(frag []byte) int {
	const (
		sizeUpdate     = 0x20
		sizeUpdateMask = 0xe0
		intPrefix5     = 0x1f
		more           = 0x80
	)

	i := 0
	for i < len(frag) && frag[i]&sizeUpdateMask == sizeUpdate {
		if frag[i]&intPrefix5 != intPrefix5 {
			i++
			continue
		}
		i++
		for i < len(frag) && frag[i]&more != 0 {
			i++
		}
		i++
	}

	if i >= len(frag) {
		return -1
	}

	return i
}

// A block opening with :status is a response. Indexed forms carry the whole field; literal
// forms reference only the name, and every static entry 8-14 names :status.
func hpackOpensResponse(frag []byte) bool {
	const (
		statusFirst = 8
		statusLast  = 14
		indexed     = 0x80
		formMask    = 0x40 | 0x10
	)

	i := hpackFieldStart(frag)
	if i < 0 {
		return false
	}

	b := frag[i]
	if b&indexed != 0 {
		idx := b &^ byte(indexed)
		return idx >= statusFirst && idx <= statusLast
	}

	idx := b &^ byte(formMask)
	if idx < statusFirst || idx > statusLast {
		return false
	}

	// 0x50 is not a literal prefix, so the form must be checked, not just the index
	return b&formMask != formMask
}

func readMetaFrame(parseContext *EBPFParseContext, connID uint64, fr *http2.Framer, hf *http2.HeadersFrame) (string, string, string, bool, bool) {
	h2c := getOrInitH2Conn(parseContext.h2c, connID)

	ok := false
	isResponse := false
	method := ""
	path := ""
	contentType := ""

	if h2c == nil {
		return method, path, contentType, ok, isResponse
	}

	h2c.hdec.SetEmitFunc(func(hf bhpack.HeaderField) {
		switch hf.Name {
		case ":method":
			method = hf.Value
			ok = true
		case ":path":
			path = hf.Value
			ok = true
		case ":status":
			// only responses carry :status — this HEADERS was misread as a request start
			isResponse = true
		case "content-type":
			contentType = hf.Value
			if contentType == "application/grpc" {
				protocolIsGRPC(parseContext.h2c, connID)
			}
			ok = true
		}
	})
	// Lose reference to MetaHeadersFrame:
	defer h2c.hdec.SetEmitFunc(func(_ bhpack.HeaderField) {})
	defer h2c.hdec.Close()

	frag := hf.HeaderBlockFragment()

	// HPACK state is per direction. Decoding a response block with the request decoder
	// desyncs its dynamic table against the peer's, so every later index resolves wrong.
	if hpackOpensResponse(frag) {
		return method, path, contentType, ok, true
	}

	for {
		if _, err := h2c.hdec.Write(frag); err != nil {
			return method, path, contentType, ok, isResponse
		}
		if hf.HeadersEnded() {
			break
		}
		hff, err := fr.ReadFrame()
		if err != nil {
			break
		}
		cf, ok := hff.(*http2.ContinuationFrame)
		if !ok {
			break
		}
		frag = cf.HeaderBlockFragment()
	}

	return method, path, contentType, ok, isResponse
}

func http2grpcStatus(status int) int {
	if status < 100 {
		return status
	}
	if status < 400 {
		return 0
	}

	return 2 // Unknown
}

// readRetMetaFrame decodes one response header block. afterData: the stream's
// span already ended at a DATA frame, so the block is its trailers.
func readRetMetaFrame(parseContext *EBPFParseContext, connID uint64, fr *http2.Framer, hf *http2.HeadersFrame, afterData bool) h2ResponseMeta {
	h2c := getOrInitH2Conn(parseContext.h2c, connID)

	ok := false
	status := 0
	grpc := false
	badIndex := false
	opensResponse := false
	var block h2BlockSlots
	meta := func() h2ResponseMeta {
		return h2ResponseMeta{
			status: status, grpc: grpc, ok: ok,
			statusUnread: !grpc && badIndex && hf.StreamEnded(),
		}
	}

	if h2c == nil {
		return meta()
	}

	h2c.hdecRet.SetEmitFunc(func(hf bhpack.HeaderField) {
		// grpc requests may have :status and grpc-status. :status will be HTTP code.
		// we prefer the grpc one if it exists, it's always later since : tagged headers
		// end up first in the headers list.
		switch hf.Name {
		case ":status":
			if !grpc { // only set the HTTP status if we didn't find grpc status
				status, _ = strconv.Atoi(hf.Value)
			}
			ok = true
			opensResponse = true
		case "grpc-status":
			status, _ = strconv.Atoi(hf.Value)
			protocolIsGRPC(parseContext.h2c, connID)
			grpc = true
			ok = true
		case "grpc-message":
			if hf.Value != "" {
				if !grpc { // unset or we have the HTTP status
					status = 2
				}
			}
			protocolIsGRPC(parseContext.h2c, connID)
			grpc = true
			ok = true
		case "<BAD INDEX>":
			badIndex = true
		}
	})
	h2c.hdecRet.SetSlotFunc(func(ev bhpack.SlotEvent) {
		if ev.Literal {
			block.lits = append(block.lits, ev)
			return
		}
		block.refs = append(block.refs, ev.Slot)
	})
	// Lose reference to MetaHeadersFrame:
	defer h2c.hdecRet.SetEmitFunc(func(_ bhpack.HeaderField) {})
	defer h2c.hdecRet.SetSlotFunc(nil)
	defer h2c.hdecRet.Close()

	frag := hf.HeaderBlockFragment()
	for {
		if _, err := h2c.hdecRet.Write(frag); err != nil {
			return meta()
		}

		if hf.HeadersEnded() {
			break
		}
		frame, err := fr.ReadFrame()
		if err != nil {
			return meta()
		}
		continuation, ok := frame.(*http2.ContinuationFrame)
		if !ok {
			return meta()
		}
		frag = continuation.HeaderBlockFragment()
		if continuation.HeadersEnded() {
			if _, err := h2c.hdecRet.Write(frag); err != nil {
				return meta()
			}
			break
		}
	}

	h2c.respSlots.observe(&block, opensResponse, hf.StreamEnded(), afterData)
	response := meta()
	if response.statusUnread && opensResponse {
		if code, known := h2c.respSlots.trailersOnlyStatus(&block); known {
			response.status, response.grpc, response.statusUnread = code, true, false
		}
	}
	return response
}

func http2InfoToSpan(info *BPFHTTP2Info, method, path, fullPath, peer, host string, status int, protocol Protocol) request.Span {
	return request.Span{
		Type:              info.eventType(protocol),
		Method:            method,
		Path:              removeQuery(path),
		FullPath:          fullPath,
		Peer:              peer,
		PeerPort:          int(info.ConnInfo.S_port),
		Host:              host,
		HostPort:          int(info.ConnInfo.D_port),
		ContentLength:     int64(info.Len),
		RequestStart:      int64(info.StartMonotimeNs),
		Start:             int64(info.StartMonotimeNs),
		End:               int64(info.EndMonotimeNs),
		Status:            status,
		TraceID:           trace.TraceID(info.Tp.TraceId),
		SpanID:            trace.SpanID(info.Tp.SpanId),
		ParentSpanID:      trace.SpanID(info.Tp.ParentId),
		ParentConditional: info.ParentStatus == parentStatusConditional,
		TraceFlags:        info.Tp.Flags,
		Pid: request.PidInfo{
			HostPID:   app.PID(info.Pid.HostPid),
			UserPID:   app.PID(info.Pid.UserPid),
			Namespace: info.Pid.Ns,
		},
	}
}

// The eBPF kernel side gives us information only if the event type is server or client. We reuse what's
// done for HTTP 1.1. We figure out what the protocol is by looking at the response status, is it :grpc-status,
// or :status. Then we know what the protocol actually is.
func (event *BPFHTTP2Info) eventType(protocol Protocol) request.EventType {
	eventType := request.EventType(event.Type)

	switch protocol {
	case HTTP2:
		return eventType // just use HTTP as is, no special handling
	case GRPC:
		switch eventType {
		case request.EventTypeHTTP:
			return request.EventTypeGRPC
		case request.EventTypeHTTPClient:
			return request.EventTypeGRPCClient
		}
	}

	return 0
}

func readFrameHeader(buf []byte) (http2.FrameHeader, error) {
	if len(buf) < frameHeaderLen {
		return http2.FrameHeader{}, errors.New("EOF")
	}
	return http2.FrameHeader{
		Length:   (uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])),
		Type:     http2.FrameType(buf[3]),
		Flags:    http2.Flags(buf[4]),
		StreamID: binary.BigEndian.Uint32(buf[5:]) & (1<<31 - 1),
	}, nil
}

func capturedHeaderBlockComplete(buf []byte) bool {
	framer := byteFramer(buf)
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return false
		}
		headers, ok := frame.(*http2.HeadersFrame)
		if !ok {
			continue
		}
		if headers.HeadersEnded() {
			return true
		}
		for {
			frame, err = framer.ReadFrame()
			if err != nil {
				return false
			}
			continuation, ok := frame.(*http2.ContinuationFrame)
			if !ok {
				return false
			}
			if continuation.HeadersEnded() {
				return true
			}
		}
	}
}

func truncateCapturedFrame(buf []byte) []byte {
	if len(buf) <= frameHeaderLen {
		return buf
	}
	fh, err := readFrameHeader(buf)
	if err != nil || fh.Length <= uint32(len(buf)-frameHeaderLen) {
		return buf
	}
	truncated := append([]byte(nil), buf...)
	newLen := len(truncated) - frameHeaderLen
	truncated[0] = uint8(newLen >> 16)
	truncated[1] = uint8(newLen >> 8)
	truncated[2] = uint8(newLen)
	return truncated
}

func readResponseMeta(parseContext *EBPFParseContext, connID uint64, event *BPFHTTP2Info) h2ResponseMeta {
	bLen := len(event.RetData)
	if event.Flags == EventTypeKHTTP2ResponseHeaders && event.Len >= 0 && event.Len < int32(bLen) {
		bLen = int(event.Len)
	}
	return readResponseBlock(parseContext, connID, event.RetData[:bLen], false)
}

// readResponseBlock decodes the first HEADERS block in buf with the
// connection's response decoder. afterData: see readRetMetaFrame.
func readResponseBlock(parseContext *EBPFParseContext, connID uint64, buf []byte, afterData bool) h2ResponseMeta {
	retFramer := byteFramer(truncateCapturedFrame(buf))
	for {
		frame, err := retFramer.ReadFrame()
		if err != nil {
			return h2ResponseMeta{}
		}

		if headers, ok := frame.(*http2.HeadersFrame); ok {
			return readRetMetaFrame(parseContext, connID, retFramer, headers, afterData)
		}
	}
}

//nolint:cyclop
func http2EventToSpan(parseContext *EBPFParseContext, pending *pendingH2Event) (request.Span, bool, error) {
	event := &pending.event
	bLen := len(event.Data)
	if event.Len < int32(bLen) {
		bLen = int(event.Len)
	}

	framer := byteFramer(event.Data[:bLen])

	// We don't set the framer.ReadMetaHeaders function to hpack.NewDecoder because
	// the http2.MetaHeadersFrame code wants a full grpc buffer with all the fields,
	// and if it sees our partially captured eBPF buffers, it will not parse the frame
	// while returning a (nil, error) tuple. We read the meta frame ourselves as long as
	// we can and terminate without an error when things fail to decode because of
	// partial buffers.

	status := pending.response.status
	eventType := HTTP2
	connID := event.NewConnId

	for {
		f, err := framer.ReadFrame()
		if err != nil {
			fail := true
			// We could have read incomplete buffer from eBPF, if the grpc request was
			// too large. In this case the frame will be with size bigger than our buffer.
			// We don't care about what's all in this request, we want to see if we can
			// find the method and path, so we attempt to adjust the frame size and re-read.
			if strings.Contains(err.Error(), "unexpected EOF") && bLen > frameHeaderLen {
				truncated := truncateCapturedFrame(event.Data[:bLen])
				if len(truncated) == bLen && !bytes.Equal(truncated, event.Data[:bLen]) {
					framer = byteFramer(truncated)

					f, err = framer.ReadFrame()
					if err == nil {
						fail = false
					}
				}
			}
			if fail {
				break
			}
		}

		if ff, ok := f.(*http2.HeadersFrame); ok {
			method, path, contentType, ok, isResponse := readMetaFrame(parseContext, connID, framer, ff)
			if isResponse {
				return request.Span{}, true, nil // response HEADERS misread as a request start
			}
			fullPath := path
			if pos := strings.Index(path, "?"); pos >= 0 {
				path = path[:pos]
			}
			if path == "" || !validPath.MatchString(path) {
				path = "*"
			}

			// We read nothing of value
			if !ok && !pending.response.ok {
				return request.Span{}, true, nil
			}

			// if we don't have protocol, assume gRPC if it's not ssl. HTTP2 is almost always SSL.
			if eventType != GRPC && (pending.response.grpc || contentType == "application/grpc" || (contentType == "" && assumeGRPC(event, pending.response))) {
				eventType = GRPC
				status = h2SpanStatus(pending.response)
			}

			peer := ""
			host := ""
			if event.ConnInfo.S_port != 0 || event.ConnInfo.D_port != 0 {
				source, target := (*BPFConnInfo)(unsafe.Pointer(&event.ConnInfo)).reqHostInfo()
				host = target
				peer = source
			}

			return http2InfoToSpan(event, method, path, fullPath, peer, host, status, eventType), false, nil
		}
	}

	return request.Span{}, true, nil // ignore if we couldn't parse it
}

func markH2DecodersUnreliable(h2c *h2Connection, flags uint8) {
	if flags&h2HpackRequestUnreliable != 0 {
		h2c.hdec.MarkUnreliable()
	}
	if flags&h2HpackResponseUnreliable != 0 {
		h2c.hdecRet.MarkUnreliable()
	}
}

func getOrInitH2Stream(h2c *h2Connection, streamID uint32) *h2StreamMeta {
	stream := h2c.streams[streamID]
	if stream == nil {
		if len(h2c.streams) >= maxPendingH2Streams {
			clear(h2c.streams)
			h2c.hdec.MarkUnreliable()
			h2c.responseGap()
		}
		stream = &h2StreamMeta{}
		h2c.streams[streamID] = stream
	}
	return stream
}

func h2HeaderCapture(event *BPFHTTP2Info) []byte {
	var buf []byte
	switch event.Flags {
	case EventTypeKHTTP2RequestHeaders:
		buf = event.Data[:]
	case EventTypeKHTTP2ResponseHeaders:
		buf = event.RetData[:]
	}
	if event.Len >= 0 && event.Len < int32(len(buf)) {
		buf = buf[:event.Len]
	}
	return buf
}

func readHTTP2HeaderEvent(parseContext *EBPFParseContext, event *BPFHTTP2Info) error {
	if event.Len < 0 {
		return errors.New("invalid HTTP/2 record length")
	}

	connID := event.NewConnId
	if connID == 0 {
		return nil
	}
	h2c := getOrInitH2ConnWithTrust(
		parseContext.h2c, connID, event.HpackFlags&h2HpackNewConnection != 0,
	)
	if h2c == nil {
		return nil
	}
	markH2DecodersUnreliable(h2c, event.HpackFlags)
	capture := h2HeaderCapture(event)
	if event.Flags == EventTypeKHTTP2RequestHeaders && h2BlockIsResponseSide(h2c, event.StreamId, capture) {
		// The kernel labels a HEADERS block a request when it has no
		// request in flight on the stream: a server's trailers after the
		// span ended at the first DATA frame, or a response it saw no
		// request for. The block was encoded with the response table, so
		// only the response decoder may read it; feeding it to the request
		// decoder desyncs both, and a later error's grpc-status, named
		// through an entry these trailers added, decodes as OK.
		if !capturedHeaderBlockComplete(capture) {
			h2c.responseGap()
		}
		_, completed := h2c.completed[event.StreamId]
		readResponseBlock(parseContext, connID, capture, completed && event.StreamId != 0)
		return nil
	}
	if !capturedHeaderBlockComplete(capture) {
		switch event.Flags {
		case EventTypeKHTTP2RequestHeaders:
			h2c.hdec.MarkUnreliable()
		case EventTypeKHTTP2ResponseHeaders:
			h2c.responseGap()
		}
	}
	stream := getOrInitH2Stream(h2c, event.StreamId)

	switch event.Flags {
	case EventTypeKHTTP2RequestHeaders:
		stream.requestSeen = true
		span, ignore, err := http2EventToSpan(parseContext, &pendingH2Event{event: *event})
		if err != nil {
			return err
		}
		if !ignore && !stream.requestOK {
			stream.request = h2RequestMeta{
				method:   span.Method,
				path:     span.Path,
				fullPath: span.FullPath,
				grpc:     h2SpanIsGRPC(span),
			}
			stream.requestOK = true
		}
	case EventTypeKHTTP2ResponseHeaders:
		stream.responseSeen = true
		response := readResponseMeta(parseContext, connID, event)
		if response.ok {
			stream.response = response
		}
	}
	return nil
}

// h2BlockIsResponseSide reports whether a block the kernel labelled a
// request belongs to the response direction: its stream already completed
// (stream IDs are never reused, so only the peer's trailers can follow), or
// the block opens with :status, which only responses carry.
func h2BlockIsResponseSide(h2c *h2Connection, streamID uint32, capture []byte) bool {
	if _, completed := h2c.completed[streamID]; completed && streamID != 0 {
		return true
	}
	frame, err := byteFramer(truncateCapturedFrame(capture)).ReadFrame()
	if err != nil {
		return false
	}
	headers, ok := frame.(*http2.HeadersFrame)
	return ok && hpackOpensResponse(headers.HeaderBlockFragment())
}

func h2SpanIsGRPC(span request.Span) bool {
	return span.Type == request.EventTypeGRPC || span.Type == request.EventTypeGRPCClient
}

func http2FromBuffers(parseContext *EBPFParseContext, event *BPFHTTP2Info) (request.Span, bool, error) {
	if event.Len < 0 {
		return request.Span{}, true, errors.New("invalid HTTP/2 record length")
	}

	connID := event.NewConnId
	h2c := getOrInitH2ConnWithTrust(parseContext.h2c, connID, event.StreamId == 0)
	if h2c == nil {
		return request.Span{}, true, nil
	}
	markH2DecodersUnreliable(h2c, event.HpackFlags)
	if event.StreamId == 0 {
		return http2EventToSpan(parseContext, &pendingH2Event{
			event:    *event,
			response: readResponseMeta(parseContext, connID, event),
		})
	}
	stream := h2c.streams[event.StreamId]
	if _, completed := h2c.completed[event.StreamId]; completed {
		return request.Span{}, true, nil
	}
	if len(h2c.completed) >= maxCompletedH2Streams {
		clear(h2c.completed)
	}
	h2c.completed[event.StreamId] = struct{}{}
	if stream != nil {
		delete(h2c.streams, event.StreamId)
	}

	var response h2ResponseMeta
	if stream != nil && stream.responseSeen {
		response = stream.response
	} else {
		h2c.responseGap()
		response = readResponseMeta(parseContext, connID, event)
	}

	if stream == nil || !stream.requestSeen {
		h2c.hdec.MarkUnreliable()
		return http2EventToSpan(parseContext, &pendingH2Event{event: *event, response: response})
	}
	if !stream.requestOK {
		stream.request = h2RequestMeta{
			path: "*",
			grpc: response.grpc || h2c.protocol == GRPC || assumeGRPC(event, response),
		}
	}

	cached := stream.request
	protocol := HTTP2
	status := response.status
	// cached.grpc may be the cleartext guess made when the request headers
	// were read, before the response was known; it does not count for a
	// response whose status was unread (see assumeGRPC). A request that said
	// gRPC in a readable content-type also set h2c.protocol.
	if (cached.grpc && !response.statusUnread) || response.grpc || h2c.protocol == GRPC {
		protocol = GRPC
		status = h2SpanStatus(response)
	}

	peer := ""
	host := ""
	if event.ConnInfo.S_port != 0 || event.ConnInfo.D_port != 0 {
		source, target := (*BPFConnInfo)(unsafe.Pointer(&event.ConnInfo)).reqHostInfo()
		host = target
		peer = source
	}
	span := http2InfoToSpan(
		event, cached.method, cached.path, cached.fullPath, peer, host, status, protocol,
	)
	return span, false, nil
}

func ReadHTTP2HeaderEvent(parseContext *EBPFParseContext, record *ringbuf.Record, filter ServiceFilter) error {
	event, err := ReinterpretCast[BPFHTTP2Info](record.RawSample)
	if err != nil {
		return err
	}
	if !filter.ValidPID(app.PID(event.Pid.UserPid), event.Pid.Ns, PIDTypeKProbes) {
		return nil
	}
	return readHTTP2HeaderEvent(parseContext, event)
}

func ReadHTTP2InfoIntoSpan(parseContext *EBPFParseContext, record *ringbuf.Record, filter ServiceFilter) (request.Span, bool, error) {
	event, err := ReinterpretCast[BPFHTTP2Info](record.RawSample)
	if err != nil {
		return request.Span{}, true, err
	}

	if !filter.ValidPID(app.PID(event.Pid.UserPid), event.Pid.Ns, PIDTypeKProbes) {
		return request.Span{}, true, nil
	}

	return http2FromBuffers(parseContext, event)
}

type http2FrameType uint8

type frameHeader struct {
	Length   uint32
	Type     http2FrameType
	Flags    uint8
	Ignore   uint8
	StreamID uint32
}

const (
	FrameData         http2FrameType = 0x0
	FrameHeaders      http2FrameType = 0x1
	FramePriority     http2FrameType = 0x2
	FrameRSTStream    http2FrameType = 0x3
	FrameSettings     http2FrameType = 0x4
	FramePushPromise  http2FrameType = 0x5
	FramePing         http2FrameType = 0x6
	FrameGoAway       http2FrameType = 0x7
	FrameWindowUpdate http2FrameType = 0x8
	FrameContinuation http2FrameType = 0x9
)

const frameHeaderLen = 9

// maxPlausibleHTTP2FrameLen is a deliberately loose upper bound on HTTP/2
// frame payload length, chosen well above any realistic SETTINGS_MAX_FRAME_SIZE
// negotiation. Per RFC 7540 6.5.2 the spec default is 2^14 and the absolute
// maximum is 2^24 - 1; we pick 4 MiB so that eBPF captures starting after a
// peer has bumped the limit (e.g. gRPC streaming workloads) still pass the
// prefilter, while ~75% of random 24-bit length values are still rejected.
// Bump this if real traffic is dropped - false negatives are worse than
// false positives here, since the downstream parser rejects garbage anyway.
const maxPlausibleHTTP2FrameLen = 1 << 22

func readHTTP2Frame(buf []uint8, length int) (*frameHeader, bool) {
	if length < frameHeaderLen {
		return nil, false
	}

	// RFC 7540 4.1: the high bit of the stream-identifier word is reserved
	// and MUST be 0 when sent. Real HTTP/2 implementations set it to 0;
	// rejecting it filters ~50% of random byte sequences with one bit test.
	if buf[5]&0x80 != 0 {
		return nil, false
	}

	frame := frameHeader{
		Length:   (uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])),
		Type:     http2FrameType(buf[3]),
		Flags:    buf[4],
		StreamID: binary.BigEndian.Uint32(buf[5:]) & (1<<31 - 1),
	}

	if frame.Length == 0 || frame.Type > FrameContinuation {
		return nil, false
	}

	return &frame, true
}

func isHeadersFrame(frame *frameHeader) bool {
	return frame.Type == FrameHeaders && frame.StreamID != 0
}

func isInvalidFrame(frame *frameHeader) bool {
	return frame.Length == 0 && frame.Type == FrameData
}

// http2FlagsMask returns the bitmask of flag bits that have spec-defined
// semantics for the given frame type. RFC 7540 4.1 requires senders to leave
// all other flag bits zero (receivers MUST ignore them), so a non-zero bit
// outside this mask is a strong signal that the bytes did not come from a
// real HTTP/2 sender.
func http2FlagsMask(t http2FrameType) uint8 {
	switch t {
	case FrameData:
		return 0x09 // END_STREAM | PADDED
	case FrameHeaders:
		return 0x2D // END_STREAM | END_HEADERS | PADDED | PRIORITY
	case FrameSettings, FramePing:
		return 0x01 // ACK
	case FramePushPromise:
		return 0x0C // END_HEADERS | PADDED
	case FrameContinuation:
		return 0x04 // END_HEADERS
	case FramePriority, FrameRSTStream, FrameGoAway, FrameWindowUpdate:
		return 0x00 // no defined flags
	}
	return 0x00
}

// isPlausibleHTTP2Frame applies the per-frame-type stream-ID, payload-length
// and flag constraints from RFC 7540 6 + 4.1. Real HTTP/2 implementations
// cannot send frames that violate these rules, so we can safely abort the
// prefilter walk when a candidate frame fails them - almost no random byte
// sequence satisfies the fixed-length / stream-zero / known-flag rules.
func isPlausibleHTTP2Frame(fr *frameHeader) bool {
	// Reserved flag bits must be zero (4.1).
	if fr.Flags & ^http2FlagsMask(fr.Type) != 0 {
		return false
	}

	switch fr.Type {
	case FrameData, FrameHeaders, FramePushPromise, FrameContinuation:
		// 6.1 / 6.2 / 6.6 / 6.10: MUST be associated with a stream. Length
		// is capped by SETTINGS_MAX_FRAME_SIZE.
		return fr.StreamID != 0 && fr.Length <= maxPlausibleHTTP2FrameLen
	case FramePriority:
		// 6.3: stream-associated, length MUST be 5.
		return fr.StreamID != 0 && fr.Length == 5
	case FrameRSTStream:
		// 6.4: stream-associated, length MUST be 4.
		return fr.StreamID != 0 && fr.Length == 4
	case FrameSettings:
		// 6.5: MUST be on stream 0, length MUST be a multiple of 6 and
		// bounded by SETTINGS_MAX_FRAME_SIZE.
		return fr.StreamID == 0 && fr.Length%6 == 0 && fr.Length <= maxPlausibleHTTP2FrameLen
	case FramePing:
		// 6.7: MUST be on stream 0, length MUST be 8.
		return fr.StreamID == 0 && fr.Length == 8
	case FrameGoAway:
		// 6.8: MUST be on stream 0, length MUST be at least 8 and bounded
		// by SETTINGS_MAX_FRAME_SIZE.
		return fr.StreamID == 0 && fr.Length >= 8 && fr.Length <= maxPlausibleHTTP2FrameLen
	case FrameWindowUpdate:
		// 6.9: length MUST be 4; allowed on any stream.
		return fr.Length == 4
	}
	return false
}

func isLikelyHTTP2(data []uint8, eventLen int) bool {
	pos := 0
	l := min(eventLen, len(data))
	for range 8 {
		if pos > l-frameHeaderLen {
			break
		}

		fr, ok := readHTTP2Frame(data[pos:], l)
		if !ok {
			break
		}

		// A frame that violates the per-type spec rules cannot have come
		// from a real HTTP/2 sender; bail out of the walk so random bytes
		// can't accidentally land on a HEADERS frame later in the buffer.
		if !isPlausibleHTTP2Frame(fr) {
			break
		}

		if isHeadersFrame(fr) {
			return true
		}

		if isInvalidFrame(fr) {
			break
		}

		if pos < (l - int(fr.Length+frameHeaderLen)) {
			pos += int(fr.Length + frameHeaderLen)
			continue
		}

		break
	}

	return false
}

func isHTTP2(data *largebuf.LargeBuffer, eventLen int) bool {
	// Parsing HTTP2 frames with the Go HTTP2/gRPC parser is very expensive.
	// Therefore, we replicate some of our HTTP2 frame reader from eBPF here to
	// check if this payload even remotely looks like HTTP2/gRPC, e.g. we must
	// find a resonably looking HTTP "headers" frame.
	if !isLikelyHTTP2(data.UnsafeView(), eventLen) {
		return false
	}

	dataReader := data.NewReader()

	framer := http2.NewFramer(io.Discard, &dataReader)

	for {
		f, err := framer.ReadFrame()
		if err != nil {
			break
		}

		if ff, ok := f.(*http2.HeadersFrame); ok {
			return knownFrameKeys(framer, ff)
		}
	}

	return false
}
