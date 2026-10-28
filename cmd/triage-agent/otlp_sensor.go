// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/triageagent-dev/agent/otlpreceiver"
)

// otlpSourceRetain is how long the otlp role keeps a source it has heard
// nothing from for a reconnect to replay. The receiver drops a silent
// resource after its own 10 minutes; this adds a margin.
const otlpSourceRetain = 15 * time.Minute

// otlpSensorFlags are the flags the otlp role (docs/adr/0022-otlp-sensor-role.md)
// reads: the controller's receiver flags, plus where its serving certificate
// is mounted.
type otlpSensorFlags struct {
	listen, tlsCert, tlsKey, clientCA string
	allowed, forwarders, tenants      []string
	// insecure serves plain gRPC and authenticates nobody, for Collectors in
	// the cluster that have no certificate or token for the agent. The chart
	// fences the port with a NetworkPolicy.
	insecure                 bool
	maxResources, maxPoints  int
	maxPerSender, maxRequest int
	maxSeries                int
	// flush is how often changed sources are handed on; zero takes the
	// receiver's default. Tests shorten it.
	flush time.Duration
}

// newOTLPSensorReceiver builds the OTLP receiver an otlp-role pod serves and
// hands every flushed source to sink. The pod serves the controller's
// sensor-stream certificate, so senders keep pinning the one in the
// <fullname>-sensor-ca ConfigMap. Tenants are the joined tenants a client
// certificate may name; the workspace scope is not known here and is applied
// by the controller to what the pod streams (otlpreceiver.ScopeFilter).
func newOTLPSensorReceiver(f otlpSensorFlags, sink otlpreceiver.Sink) (*otlpreceiver.Receiver, error) {
	if f.listen == "" {
		return nil, errors.New("--mode=otlp needs --otlp-listen")
	}
	if f.insecure {
		if f.tlsCert != "" || f.tlsKey != "" || f.clientCA != "" || len(f.allowed) > 0 || len(f.forwarders) > 0 || len(f.tenants) > 0 {
			return nil, errors.New("--otlp-insecure serves plain gRPC to anonymous senders: it excludes the certificate, client CA, token and tenant flags")
		}
		return otlpreceiver.New(otlpreceiver.Options{
			Addr: f.listen, Insecure: true, Sink: sink, Log: ctrl.Log, Flush: f.flush, Wanted: otlpreceiver.ReadByCollectors,
			MaxResources: f.maxResources, MaxPointsPerResource: f.maxPoints,
			MaxResourcesPerSender: f.maxPerSender, MaxRequestBytes: f.maxRequest, MaxSeries: f.maxSeries,
		}), nil
	}
	if f.tlsCert == "" || f.tlsKey == "" {
		return nil, errors.New("--mode=otlp needs --otlp-tls-cert-file and --otlp-tls-key-file")
	}
	if len(f.allowed) == 0 && f.clientCA == "" {
		return nil, errors.New("--mode=otlp needs --otlp-allowed-service-accounts or --otlp-client-ca-file: nobody could send")
	}
	cert, err := tls.LoadX509KeyPair(f.tlsCert, f.tlsKey)
	if err != nil {
		return nil, fmt.Errorf("otlp serving certificate: %w", err)
	}
	var pool *x509.CertPool
	if f.clientCA != "" {
		pem, err := os.ReadFile(f.clientCA)
		if err != nil {
			return nil, fmt.Errorf("otlp client CA: %w", err)
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificate in the client CA file %s", f.clientCA)
		}
	}
	var auth otlpreceiver.Authenticator
	if len(f.allowed) > 0 {
		client, err := kubernetes.NewForConfig(ctrl.GetConfigOrDie())
		if err != nil {
			return nil, fmt.Errorf("build clientset for TokenReview: %w", err)
		}
		auth = &otlpreceiver.TokenReviewAuth{Client: client, Allowed: f.allowed, Forwarders: f.forwarders}
	}
	return otlpreceiver.New(otlpreceiver.Options{
		Addr:          f.listen,
		Flush:         f.flush,
		TLS:           credentials.NewTLS(otlpreceiver.ServerTLS(cert, pool, len(f.allowed) > 0)),
		Auth:          auth,
		Sink:          sink,
		Log:           ctrl.Log,
		Wanted:        otlpreceiver.ReadByCollectors,
		TenantAllowed: func(tenant string) bool { return slices.Contains(f.tenants, tenant) },

		MaxResources: f.maxResources, MaxPointsPerResource: f.maxPoints,
		MaxResourcesPerSender: f.maxPerSender, MaxRequestBytes: f.maxRequest, MaxSeries: f.maxSeries,
	}), nil
}
