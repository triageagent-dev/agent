package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// maxBodyBytes bounds one webhook body; Alertmanager groups are small.
const maxBodyBytes = 4 << 20

type server struct {
	m      mapper
	client interface {
		send(ctx context.Context, ev triageEvent) error
	}
	// auth is the webhook's bearer token; nil serves it unauthenticated
	// (WEBHOOK_INSECURE).
	auth *webhookAuth
	// sent remembers the alerts already delivered; nil sends every alert.
	sent *sentAlerts
}

// webhookAuth is the bearer token Alertmanager must present. The bridge posts
// into every mapped tenant with its own token, so without this any pod that
// can reach its Service could inject events into any of them.
type webhookAuth struct {
	tokenFile string // re-read on every request, so a rotated Secret applies
}

func (a *webhookAuth) expected() (string, error) {
	b, err := os.ReadFile(a.tokenFile)
	if err != nil {
		return "", fmt.Errorf("read webhook token: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// allowed reports whether the request carries the token. Both sides are
// hashed first, so the comparison takes the same time whatever the presented
// token's length or content.
func (a *webhookAuth) allowed(r *http.Request) bool {
	want, err := a.expected()
	if err != nil || want == "" {
		if err != nil {
			log.Printf("webhook auth: %v", err)
		}
		return false
	}
	scheme, got, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	w, g := sha256.Sum256([]byte(want)), sha256.Sum256([]byte(strings.TrimSpace(got)))
	return subtle.ConstantTimeCompare(w[:], g[:]) == 1
}

// sentAlerts remembers, for a short window, the alerts already delivered, by
// (fingerprint, status, startsAt). One failed alert makes the webhook answer
// 5xx, and Alertmanager then sends the whole group again: without this every
// alert that had already gone through would be sent a second time. The
// window must stay under Alertmanager's repeat_interval, or the repeats of a
// still-firing alert, which carry the same key, would be swallowed too.
type sentAlerts struct {
	window time.Duration
	now    func() time.Time
	mu     sync.Mutex
	at     map[string]time.Time
}

func newSentAlerts(window time.Duration) *sentAlerts {
	if window <= 0 {
		return nil
	}
	return &sentAlerts{window: window, now: time.Now, at: map[string]time.Time{}}
}

// alertKey identifies one alert state. An alert with no fingerprint (not
// every sender sets one) is identified by its labels.
func alertKey(a webhookAlert) string {
	id := a.Fingerprint
	if id == "" {
		pairs := make([]string, 0, len(a.Labels))
		for k, v := range a.Labels {
			pairs = append(pairs, k+"="+v)
		}
		sort.Strings(pairs)
		id = strings.Join(pairs, ",")
	}
	return id + "\x00" + a.Status + "\x00" + a.StartsAt.UTC().Format(time.RFC3339Nano)
}

func (s *sentAlerts) seen(key string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.at[key]
	return ok && s.now().Sub(at) < s.window
}

func (s *sentAlerts) mark(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for k, at := range s.at {
		if now.Sub(at) >= s.window {
			delete(s.at, k)
		}
	}
	s.at[key] = now
}

// handleWebhook answers 200 once every usable alert in the body reached
// triage (or was rejected as permanently bad), 401 without the webhook token,
// 400 for a body it cannot read, and 502 when triage could not be reached or
// is busy, so Alertmanager retries. On that retry only the alerts that did
// not go through are sent again (sentAlerts).
func (s *server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.auth != nil && !s.auth.allowed(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	var body webhookBody
	if err := json.Unmarshal(b, &body); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	sent, skipped, repeated := 0, 0, 0
	for _, a := range body.Alerts {
		ev, ok := s.m.event(a)
		if !ok {
			skipped++
			continue
		}
		key := alertKey(a)
		if s.sent.seen(key) {
			repeated++
			continue
		}
		if err := s.client.send(r.Context(), ev); err != nil {
			log.Printf("triage unreachable or busy, asking for a retry (%d of %d alerts sent): %v", sent, len(body.Alerts), err)
			http.Error(w, "triage unreachable", http.StatusBadGateway)
			return
		}
		s.sent.mark(key)
		sent++
	}
	log.Printf("webhook: %d alerts, %d sent, %d skipped (no workload or tenant), %d already sent", len(body.Alerts), sent, skipped, repeated)
	w.WriteHeader(http.StatusOK)
}

// webhookAuthFromEnv reads the webhook token's file path. The token itself is
// never taken from the environment: a mounted file can be rotated and does
// not show in the pod spec or in `kubectl describe`. With no file configured
// it is an error unless WEBHOOK_INSECURE=true says the open webhook is
// intended.
func webhookAuthFromEnv(getenv func(string) string) (*webhookAuth, error) {
	if a := (&webhookAuth{tokenFile: getenv("WEBHOOK_TOKEN_FILE")}); a.tokenFile != "" {
		if _, err := a.expected(); err != nil {
			return nil, err
		}
		return a, nil
	}
	if strings.EqualFold(getenv("WEBHOOK_INSECURE"), "true") {
		return nil, nil
	}
	return nil, fmt.Errorf("set WEBHOOK_TOKEN_FILE (a file holding the bearer token Alertmanager sends), or WEBHOOK_INSECURE=true to serve /alertmanager without authentication")
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	base := strings.TrimSuffix(env("TRIAGE_EVENTS_URL", ""), "/")
	if base == "" {
		log.Fatal("TRIAGE_EVENTS_URL is not set")
	}
	tenants, err := parseTenants(env("NAMESPACE_TENANTS", ""))
	if err != nil {
		log.Fatal(err)
	}
	fallback := env("TRIAGE_TENANT", "")
	if len(tenants) == 0 && fallback == "" {
		log.Fatal("set NAMESPACE_TENANTS (ns=tenant,...) or TRIAGE_TENANT")
	}
	labels := defaultWorkloadLabels
	if v := env("WORKLOAD_LABELS", ""); v != "" {
		labels = nil
		for _, l := range strings.Split(v, ",") {
			if l = strings.TrimSpace(l); l != "" {
				labels = append(labels, l)
			}
		}
	}
	client := &triageClient{
		baseURL:   base,
		provider:  env("PROVIDER", "prometheus"),
		tokenFile: env("TRIAGE_TOKEN_FILE", ""),
		http:      &http.Client{Timeout: sendTimeout},
	}
	if client.tokenFile == "" {
		log.Fatal("set TRIAGE_TOKEN_FILE (the projected ServiceAccount token)")
	}
	auth, err := webhookAuthFromEnv(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	if auth == nil {
		log.Print("WARNING: WEBHOOK_INSECURE=true, /alertmanager accepts alerts from anyone who can reach it")
	}
	dedup, err := time.ParseDuration(env("DEDUP_WINDOW", "2m"))
	if err != nil {
		log.Fatalf("DEDUP_WINDOW: %v", err)
	}
	s := &server{
		m:      mapper{tenants: tenants, fallbackTenant: fallback, workloadLabels: labels},
		client: client,
		auth:   auth,
		sent:   newSentAlerts(dedup),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/alertmanager", s.handleWebhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	addr := env("LISTEN", ":8080")
	log.Printf("promevents listening on %s, sending to %s/events/%s/", addr, base, client.provider)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
