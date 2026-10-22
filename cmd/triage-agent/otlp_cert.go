// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/triageagent-dev/agent/otlpreceiver"
	"github.com/triageagent-dev/agent/sensorstream"
)

// otlpCertCommand is `triage-agent otlp-cert`: it issues a client certificate
// for a remote Collector from the agent's own client CA
// (docs/adr/0017-otlp-receiver.md). Run it in the controller pod:
//
//	kubectl -n <ns> exec deploy/<release> -- triage-agent otlp-cert --tenant a,b --cn collector-1
//
// It reads the CA from the Secret named by --client-ca-secret with the pod's
// ServiceAccount and writes the result to stdout. It returns the process exit code.
func otlpCertCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("otlp-cert", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		tenants, cn, domain, secretName, caConfigMap, format, outName, outNS string
		days                                                                 int
	)
	fs.StringVar(&tenants, "tenant", "", "comma-separated tenants the certificate may publish for (required)")
	fs.StringVar(&cn, "cn", "", "common name: the sender's name (required)")
	fs.StringVar(&domain, "domain", otlpreceiver.DefaultDomain, "SPIFFE trust domain of the tenant URI SANs")
	fs.StringVar(&secretName, "client-ca-secret", "", "Secret holding the agent's client CA (required; the chart prints it in NOTES)")
	fs.StringVar(&caConfigMap, "serving-ca-configmap", "", "ConfigMap with the agent's serving certificate (ca.crt), included in the output so the sender can pin it")
	fs.IntVar(&days, "days", int(otlpreceiver.DefaultCertValidity/(24*time.Hour)), "validity in days")
	fs.StringVar(&format, "format", "secret", "secret: a Kubernetes Secret manifest for the sender's cluster; pem: the three PEM blocks")
	fs.StringVar(&outName, "secret-name", "triage-otlp-client", "format secret: the Secret's name")
	fs.StringVar(&outNS, "secret-namespace", "", "format secret: the Secret's namespace (omitted when empty)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if tenants == "" || cn == "" || secretName == "" {
		fmt.Fprintln(stderr, "otlp-cert: --tenant, --cn and --client-ca-secret are required")
		return 2
	}
	if format != "secret" && format != "pem" {
		fmt.Fprintln(stderr, "otlp-cert: --format is secret or pem")
		return 2
	}
	ns := podNamespace()
	if ns == "" {
		fmt.Fprintln(stderr, "otlp-cert: run it inside the controller pod (no pod namespace found)")
		return 1
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		fmt.Fprintln(stderr, "otlp-cert: run it inside the controller pod:", err)
		return 1
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintln(stderr, "otlp-cert:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	caCert, caKey, err := otlpreceiver.LoadClientCA(ctx, client, ns, secretName)
	if err != nil {
		fmt.Fprintln(stderr, "otlp-cert:", err)
		return 1
	}
	var list []string
	for _, t := range strings.Split(tenants, ",") {
		if t = strings.TrimSpace(t); t != "" {
			list = append(list, t)
		}
	}
	cert, key, err := otlpreceiver.IssueClientCert(caCert, caKey, domain, cn, list, time.Duration(days)*24*time.Hour)
	if err != nil {
		fmt.Fprintln(stderr, "otlp-cert:", err)
		return 1
	}
	var serving []byte
	if caConfigMap != "" {
		cm, err := client.CoreV1().ConfigMaps(ns).Get(ctx, caConfigMap, metav1.GetOptions{})
		if err != nil {
			fmt.Fprintln(stderr, "otlp-cert: serving CA:", err)
			return 1
		}
		serving = []byte(cm.Data[sensorstream.CAKey])
	}
	if format == "pem" {
		fmt.Fprintf(stdout, "# tls.crt\n%s# tls.key\n%s", cert, key)
		if len(serving) > 0 {
			fmt.Fprintf(stdout, "# ca.crt (the agent's serving certificate)\n%s", serving)
		}
		return 0
	}
	b64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	fmt.Fprintf(stdout, "apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s\n", outName)
	if outNS != "" {
		fmt.Fprintf(stdout, "  namespace: %s\n", outNS)
	}
	fmt.Fprintf(stdout, "type: Opaque\ndata:\n  tls.crt: %s\n  tls.key: %s\n", b64(cert), b64(key))
	if len(serving) > 0 {
		fmt.Fprintf(stdout, "  ca.crt: %s\n", b64(serving))
	}
	return 0
}

func runOTLPCert() { os.Exit(otlpCertCommand(os.Args[2:], os.Stdout, os.Stderr)) }
