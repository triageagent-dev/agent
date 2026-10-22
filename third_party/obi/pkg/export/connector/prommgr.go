// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package connector provides tools for sharing the connection of diverse exporters
// (Prometheus, OTEL...) from different nodes
package connector // import "go.opentelemetry.io/obi/pkg/export/connector"

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.opentelemetry.io/obi/pkg/internal/helpers/maps"
)

func log() *slog.Logger {
	return slog.With("component", "connector.PrometheusManager")
}

// EmbedHTTP, set by the embedding process before OBI starts: LOCAL PATCH
// (triage-agent). When non-nil, StartHTTP hands the completed mux for a
// newly-serving port to EmbedHTTP instead of calling listenAndServe, and
// this package opens no socket for that port. nil (the default) keeps this
// module serving its own listener unchanged, so the vendored tree stays
// usable standalone. Set once, before the embedded instrumenter starts —
// StartHTTP reads it fresh on every call, so a restart of the caller's own
// server in response to a later mux (an instrumentation reload building a
// new PrometheusManager) is the caller's responsibility, not this
// package's.
var EmbedHTTP func(port int, mux *http.ServeMux)

// PrometheusManager allows exporting metrics from different sources (instrumented metrics, internal metrics...)
// sharing the same port and path, or using different ones, depending on the configuration provided by the registrars.
type PrometheusManager struct {
	mt sync.Mutex
	// mux of each port that already has a listener, so a path registered later can be
	// added to the running mux instead of needing a second listener
	muxes map[int]*http.ServeMux
	// (port, path) pairs already wired into a mux. http.ServeMux panics on a duplicate
	// pattern, so a repeated StartHTTP must skip them.
	handled maps.Map2[int, string, struct{}]
	// key 1: port. Key 2: path
	registries maps.Map2[int, string, *prometheus.Registry]

	metrics internalIntrumenter
}

type internalIntrumenter interface {
	PrometheusRequest(port, path string)
}

func (pm *PrometheusManager) InstrumentWith(ii internalIntrumenter) {
	pm.mt.Lock()
	defer pm.mt.Unlock()
	pm.metrics = ii
}

// Register a set of prometheus metrics to be accessible through an HTTP port/path.
// This method is not thread-safe
func (pm *PrometheusManager) Register(port int, path string, collectors ...prometheus.Collector) {
	pm.mt.Lock()
	defer pm.mt.Unlock()
	log().Debug("registering Prometheus metrics collectors",
		"len", len(collectors), "port", port, "path", path)

	if pm.registries == nil {
		pm.registries = maps.Map2[int, string, *prometheus.Registry]{}
	}
	reg, ok := pm.registries.Get(port, path)
	if !ok {
		reg = prometheus.NewRegistry()
		pm.registries.Put(port, path, reg)
	}
	reg.MustRegister(collectors...)
}

// StartHTTP serves metrics in background. Safe to call repeatedly: each call serves what is
// registered but not served yet, opening a new port or adding a path to a running mux.
func (pm *PrometheusManager) StartHTTP(ctx context.Context) {
	pm.mt.Lock()
	defer pm.mt.Unlock()

	if pm.muxes == nil {
		pm.muxes = map[int]*http.ServeMux{}
	}
	if pm.handled == nil {
		pm.handled = maps.Map2[int, string, struct{}]{}
	}

	log := log()
	for port, paths := range pm.registries {
		mux, listening := pm.muxes[port]
		if !listening {
			mux = http.NewServeMux()
		}
		for path, registry := range paths {
			if _, ok := pm.handled.Get(port, path); ok {
				continue
			}
			log.With("port", port, "path", path).Info("opening prometheus scrape endpoint")
			promHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
				Registry:          registry,
				EnableOpenMetrics: true,
			})
			promHandler = wrapDebugHandler(log, promHandler)
			promHandler = wrapInstrumentedHandler(pm.metrics, port, path, promHandler)
			// ServeMux takes its own write lock, so this is safe while it serves
			mux.Handle(path, promHandler)
			pm.handled.Put(port, path, struct{}{})
		}
		if !listening {
			pm.muxes[port] = mux
			// LOCAL PATCH (triage-agent): hand the mux to the embedder
			// instead of opening a listener when one is registered —
			// see EmbedHTTP's doc comment.
			if EmbedHTTP != nil {
				EmbedHTTP(port, mux)
			} else {
				listenAndServe(ctx, port, mux)
			}
		}
	}
}

func wrapInstrumentedHandler(metrics internalIntrumenter, port int, path string, promHandler http.Handler) http.HandlerFunc {
	// we don't wrap anything if the reporter is nil
	if metrics == nil {
		return promHandler.ServeHTTP
	}
	portStr := strconv.Itoa(port)
	return func(rw http.ResponseWriter, req *http.Request) {
		promHandler.ServeHTTP(rw, req)
		metrics.PrometheusRequest(portStr, path)
	}
}

func wrapDebugHandler(log *slog.Logger, promHandler http.Handler) http.HandlerFunc {
	// we don't wrap anything for if the log level is not debug or lower
	if !log.Enabled(context.TODO(), slog.LevelDebug) {
		return promHandler.ServeHTTP
	}
	return func(rw http.ResponseWriter, req *http.Request) {
		log.Debug("received metrics request", "uri", req.RequestURI, "remoteAddr", req.RemoteAddr)
		promHandler.ServeHTTP(rw, req)
	}
}

func listenAndServe(ctx context.Context, port int, handler http.Handler) {
	// TODO: support TLS configuration
	// LOCAL PATCH (triage-agent): bind loopback only. Reached only when
	// EmbedHTTP is nil, i.e. the vendored tree running standalone rather
	// than embedded by the triage-agent module's own code — that code sets
	// EmbedHTTP before starting OBI, so this listener never actually opens
	// in that binary; see EmbedHTTP's doc comment above. A Kubernetes pod
	// network is flat by default and a namespace is an API-server boundary
	// rather than a network one, so a standalone run's wildcard bind here
	// would be readable by any pod in the cluster. A bump that silently
	// drops this one-line patch restores that wildcard bind, so keep it
	// minimal and easy to diff on upgrade.
	server := http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", port), Handler: handler}
	log := log().With("port", port)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			log.Debug("Prometheus endpoint server was closed", "error", err)
		} else {
			log.Error("Prometheus endpoint service ended unexpectedly", "error", err)
			err = syscall.Kill(os.Getpid(), syscall.SIGINT) // interrupt for graceful shutdown, instead of os.Exit
			if err != nil {
				log.Error("unable to terminate", "error", err)
			}
		}
	}()
	go func() {
		<-ctx.Done()
		if err := server.Close(); err != nil {
			log.Warn("error closing HTTP server", "err", err.Error())
		}
	}()
}
