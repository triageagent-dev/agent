// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// ClientOptions configures a sensor's stream to the controller.
type ClientOptions struct {
	// Addr is the controller's sensor Service, host:port.
	Addr string
	// CAFile is the controller certificate the sensor pins (the mounted
	// ConfigMap key ca.crt).
	CAFile string
	// TokenFile is the projected ServiceAccount token for TokenAudience.
	// Empty when the process authenticates with ClientCertFile instead.
	TokenFile string
	// ClientCertFile and ClientKeyFile are the certificate the otlp role
	// presents instead of a token: the controller's serving certificate,
	// which the controller pins (ServerTLS).
	ClientCertFile, ClientKeyFile string
	Node                          string
	Pod                           string
	Version                       string
	// Roles maps each role this process runs to its metrics.
	Roles map[string]RoleSource
	// Feed, when set, makes this the otlp role (ADR-0022): the Hello names
	// RoleOTLP, and each tick sends the sources the feed holds as Snapshots
	// that name their source. A process with a Feed runs no other role.
	Feed SourceFeed
	// Settings are this process's flags, secrets left out, sent in the Hello
	// so the controller can show what each sensor runs.
	Settings []*sensorpb.Setting
	Tick     time.Duration
	Log      logr.Logger
}

// RoleSource produces one role's metrics as cumulative OTLP.
type RoleSource interface {
	Collect(ctx context.Context) ([]*metricspb.ResourceMetrics, error)
}

// RoleStatus is implemented by a RoleSource that can fail in a way a
// successful Collect does not show, such as probes that did not attach.
// A non-empty StatusError becomes the role's LastError in SensorState.
type RoleStatus interface {
	StatusError() string
}

// RunClient keeps a stream to the controller open until ctx ends,
// reconnecting with backoff.
func RunClient(ctx context.Context, o ClientOptions) error {
	if o.Tick <= 0 {
		o.Tick = 15 * time.Second
	}
	c := &client{opts: o, log: o.Log.WithName("sensorstream"), roleStats: map[string]*sensorpb.RoleState{}}
	backoff := time.Second
	for {
		started := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		c.log.Info("sensor stream closed; reconnecting", "reason", errString(err), "in", backoff.String())
		c.mu.Lock()
		c.reconnects++
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

type client struct {
	opts ClientOptions
	log  logr.Logger

	mu            sync.Mutex
	roleStats     map[string]*sensorpb.RoleState
	reconnects    uint64
	sendErrors    uint64
	sendMicros    int64
	tickBytes     uint64
	ackRTTMicros  int64
	connectMicros int64
}

func (c *client) session(ctx context.Context) error {
	pool, err := loadPinnedCA(c.opts.CAFile)
	if err != nil {
		return err
	}
	var token []byte
	if c.opts.TokenFile != "" {
		token, err = os.ReadFile(c.opts.TokenFile)
		if err != nil {
			return fmt.Errorf("read the sensor token: %w", err)
		}
	}
	tc := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	if c.opts.ClientCertFile != "" {
		pair, err := tls.LoadX509KeyPair(c.opts.ClientCertFile, c.opts.ClientKeyFile)
		if err != nil {
			return fmt.Errorf("load the client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{pair}
	} else if len(token) == 0 {
		return errors.New("the sensor has neither a token nor a client certificate")
	}
	host, _, err := net.SplitHostPort(c.opts.Addr)
	if err != nil {
		return fmt.Errorf("sensor stream address %q: %w", c.opts.Addr, err)
	}
	conn, err := grpc.NewClient(c.opts.Addr,
		grpc.WithTransportCredentials(credentials.NewTLS(withServerName(tc, host))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageBytes)),
	)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	sctx, cancel := context.WithCancel(ctx)
	if len(token) > 0 {
		sctx = metadata.AppendToOutgoingContext(sctx, "authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	defer cancel()
	opened := time.Now()
	stream, err := sensorpb.NewSensorServiceClient(conn).Stream(sctx)
	if err != nil {
		return err
	}

	var seq uint64
	send := func(m *sensorpb.StreamRequest) error {
		seq++
		m.Seq, m.Time = seq, timestamppb.Now()
		return stream.Send(m)
	}
	roles := make([]string, 0, len(c.opts.Roles))
	for r := range c.opts.Roles {
		roles = append(roles, r)
	}
	sort.Strings(roles)
	helloRoles := roles
	if c.opts.Feed != nil {
		helloRoles = append(slices.Clone(roles), RoleOTLP)
		sort.Strings(helloRoles)
	}
	if err := send(&sensorpb.StreamRequest{Body: &sensorpb.StreamRequest_Hello{Hello: &sensorpb.Hello{
		Node: c.opts.Node, Pod: c.opts.Pod, Version: c.opts.Version, Roles: helloRoles, Settings: c.opts.Settings,
	}}}); err != nil {
		return err
	}

	// Acks: the round trip of each state message, and the first one timing
	// the connect.
	var pendMu sync.Mutex
	pending := map[uint64]time.Time{}
	recvErr := make(chan error, 1)
	go func() {
		first := true
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			ack := resp.GetAck()
			if ack == nil {
				continue
			}
			now := time.Now()
			pendMu.Lock()
			sent, ok := pending[ack.GetSeq()]
			delete(pending, ack.GetSeq())
			pendMu.Unlock()
			c.mu.Lock()
			if ok {
				c.ackRTTMicros = now.Sub(sent).Microseconds()
			}
			if first {
				c.connectMicros = now.Sub(opened).Microseconds()
				first = false
			}
			c.mu.Unlock()
		}
	}()

	ticker := time.NewTicker(c.opts.Tick)
	defer ticker.Stop()
	for first := true; ; first = false {
		start := time.Now()
		var bytesSent uint64
		if c.opts.Feed != nil {
			n, err := c.sendSources(send, first)
			bytesSent += n
			if err != nil {
				c.countSendError()
				return err
			}
		}
		for _, role := range roles {
			rms, err := c.collect(ctx, role)
			if err != nil {
				continue
			}
			m := &sensorpb.StreamRequest{Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{Role: role, ResourceMetrics: rms}}}
			if err := send(m); err != nil {
				c.countSendError()
				return err
			}
			bytesSent += uint64(proto.Size(m))
		}
		state := c.state()
		m := &sensorpb.StreamRequest{Body: &sensorpb.StreamRequest_State{State: state}}
		if err := send(m); err != nil {
			c.countSendError()
			return err
		}
		pendMu.Lock()
		pending[m.Seq] = time.Now()
		pendMu.Unlock()
		bytesSent += uint64(proto.Size(m))
		c.mu.Lock()
		c.sendMicros, c.tickBytes = time.Since(start).Microseconds(), bytesSent
		c.mu.Unlock()

		// Between ticks, what the otlp role's feed queues goes out as it
		// arrives: the feed holds nothing longer than this takes.
		var ready <-chan struct{}
		if c.opts.Feed != nil {
			ready = c.opts.Feed.Ready()
		}
	wait:
		for {
			select {
			case <-ctx.Done():
				_ = stream.CloseSend()
				return nil
			case err := <-recvErr:
				return err
			case <-ready:
				if _, err := c.sendSources(send, false); err != nil {
					c.countSendError()
					return err
				}
			case <-ticker.C:
				break wait
			}
		}
	}
}

// sendSources sends the otlp role's changed sources, or all of them on the
// first tick of a stream, and records what the role carried.
func (c *client) sendSources(send func(*sensorpb.StreamRequest) error, full bool) (uint64, error) {
	start := time.Now()
	var bytes uint64
	var metrics, points uint32
	for _, s := range c.opts.Feed.Pending(full) {
		m := &sensorpb.StreamRequest{Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
			Role: s.Role, Source: s.Key, ResourceMetrics: s.ResourceMetrics,
		}}}
		if err := send(m); err != nil {
			return bytes, err
		}
		bytes += uint64(proto.Size(m))
		for _, rm := range s.ResourceMetrics {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mm := range sm.GetMetrics() {
					metrics++
					points += dataPoints(mm)
				}
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.roleStats[RoleOTLP]
	if st == nil {
		st = &sensorpb.RoleState{Role: RoleOTLP}
		c.roleStats[RoleOTLP] = st
	}
	st.GatherMicros = time.Since(start).Microseconds()
	st.Metrics, st.DataPoints = metrics, points
	return bytes, nil
}

// withServerName returns tc with the name the controller's certificate is
// verified against.
func withServerName(tc *tls.Config, name string) *tls.Config {
	tc.ServerName = name
	return tc
}

func (c *client) countSendError() {
	c.mu.Lock()
	c.sendErrors++
	c.mu.Unlock()
}

// collect runs the role's source and records how it went.
func (c *client) collect(ctx context.Context, role string) ([]*metricspb.ResourceMetrics, error) {
	start := time.Now()
	rms, err := c.opts.Roles[role].Collect(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.roleStats[role]
	if st == nil {
		st = &sensorpb.RoleState{Role: role}
		c.roleStats[role] = st
	}
	st.GatherMicros = time.Since(start).Microseconds()
	if err != nil {
		st.GatherErrors++
		st.LastError = err.Error()
		return nil, err
	}
	var metrics, points uint32
	for _, rm := range rms {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				metrics++
				points += dataPoints(m)
			}
		}
	}
	st.Metrics, st.DataPoints = metrics, points
	if rs, ok := c.opts.Roles[role].(RoleStatus); ok {
		if msg := rs.StatusError(); msg != "" {
			st.LastError = msg
		}
	}
	return rms, nil
}

func (c *client) state() *sensorpb.SensorState {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &sensorpb.SensorState{
		Goroutines:    uint32(runtime.NumGoroutine()),
		HeapBytes:     ms.HeapAlloc,
		SysBytes:      ms.Sys,
		CpuSeconds:    cpuSeconds(),
		Reconnects:    c.reconnects,
		SendErrors:    c.sendErrors,
		SendMicros:    c.sendMicros,
		AckRttMicros:  c.ackRTTMicros,
		TickBytes:     c.tickBytes,
		ConnectMicros: c.connectMicros,
	}
	for _, r := range sortedKeys(c.roleStats) {
		s.Roles = append(s.Roles, proto.Clone(c.roleStats[r]).(*sensorpb.RoleState))
	}
	return s
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

func errString(err error) string {
	if err == nil {
		return "closed"
	}
	return err.Error()
}

func dataPoints(m *metricspb.Metric) uint32 {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		return uint32(len(d.Sum.GetDataPoints()))
	case *metricspb.Metric_Gauge:
		return uint32(len(d.Gauge.GetDataPoints()))
	case *metricspb.Metric_Histogram:
		return uint32(len(d.Histogram.GetDataPoints()))
	case *metricspb.Metric_ExponentialHistogram:
		return uint32(len(d.ExponentialHistogram.GetDataPoints()))
	case *metricspb.Metric_Summary:
		return uint32(len(d.Summary.GetDataPoints()))
	}
	return 0
}
