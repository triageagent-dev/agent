package agentui

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"time"
)

type scopeKey struct{}

type scope struct {
	name   string
	tenant string // "" is unrestricted
}

// scopeOf returns the tenant a request is restricted to; restricted is false
// for the TUI and for an API token that is not tied to a tenant.
func scopeOf(r *http.Request) (tenant string, restricted bool) {
	if sc, ok := r.Context().Value(scopeKey{}).(scope); ok && sc.tenant != "" {
		return sc.tenant, true
	}
	return "", false
}

// APIOptions configure the integration listener (ADR-0020).
type APIOptions struct {
	// Addr is host:port. Unlike the TUI it may be any address, since
	// Prometheus and Grafana reach it over the network; every request needs a
	// bearer token.
	Addr string
	// TokensFile holds the tokens (see tokenSet). Required.
	TokensFile string
	// CertFile and KeyFile serve TLS; both empty serves plain HTTP.
	CertFile, KeyFile string
	// MCP serves the Model Context Protocol endpoint (POST /mcp); Docs serves
	// the OpenAPI spec and the Swagger page. Both are off unless set.
	MCP, Docs bool
	// SA, when set, also accepts ServiceAccount tokens checked with
	// TokenReview and SubjectAccessReview (cluster mode). TokensFile is then
	// optional.
	SA *SAAuth
}

// APIServer is the listener monitoring tools read from: Prometheus metrics, a
// discovery document and the JSON API, all behind a bearer token whose tenant
// scope the server enforces.
type APIServer struct {
	s    *Server
	o    APIOptions
	toks credentials
}

// NewAPI validates o and returns an APIServer over s. It refuses to run
// without tokens: there is no unauthenticated mode.
func NewAPI(s *Server, o APIOptions) (*APIServer, error) {
	if o.Addr == "" {
		return nil, errors.New("agentui: --api-listen is empty")
	}
	if o.TokensFile == "" && o.SA == nil {
		return nil, errors.New("agentui: --api-listen needs --api-tokens-file or --api-sa-auth: the API has no unauthenticated mode")
	}
	if (o.CertFile == "") != (o.KeyFile == "") {
		return nil, errors.New("agentui: --api-tls-cert-file and --api-tls-key-file go together")
	}
	var chain credentialChain
	if o.TokensFile != "" {
		ts, err := newTokenSet(o.TokensFile)
		if err != nil {
			return nil, err
		}
		chain = append(chain, ts)
	}
	if o.SA != nil {
		chain = append(chain, o.SA)
	}
	return &APIServer{s: s, o: o, toks: chain}, nil
}

// NeedLeaderElection lets every replica serve; only the leader exports signals.
func (a *APIServer) NeedLeaderElection() bool { return false }

// Start serves until ctx ends. It implements manager.Runnable.
func (a *APIServer) Start(ctx context.Context) error {
	srv := &http.Server{Addr: a.o.Addr, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	a.s.o.Log.Info("agent API listening", "addr", a.o.Addr, "tls", a.o.CertFile != "")
	var err error
	if a.o.CertFile != "" {
		err = srv.ListenAndServeTLS(a.o.CertFile, a.o.KeyFile)
	} else {
		a.s.o.Log.Info("agent API serves plain HTTP: bearer tokens cross the network in the clear; set api.tls to serve TLS")
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Handler returns the routes behind the token check.
func (a *APIServer) Handler() http.Handler {
	s := a.s
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/triage-agent", a.wellKnown)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/api/v1/status", s.status)
	mux.HandleFunc("/api/v1/tenants", s.tenants)
	mux.HandleFunc("/api/v1/apps", s.apps)
	mux.HandleFunc("/api/v1/services", s.services)
	mux.HandleFunc("/api/v1/graph", s.graph)
	mux.HandleFunc("/api/v1/sources", s.sources)
	mux.HandleFunc("/api/v1/events", s.events)
	mux.HandleFunc("/api/v1/stream", s.stream)
	top := http.NewServeMux()
	if a.o.Docs {
		// The spec and its Swagger page are static and carry no data, so a
		// browser can load them; the calls they make still need a token.
		top.Handle("/api/v1/openapi.json", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			staticHeaders(w)
			writeJSON(w, openAPIDoc(a.o.MCP))
		}))
		top.HandleFunc("/api/v1/docs", swaggerUI)
		top.HandleFunc("/api/v1/docs.js", swaggerJS)
	}
	if a.o.MCP {
		top.Handle("/mcp", a.bearer(mcp(mux, s.o.Version)))
	}
	top.Handle("/", a.bearer(mux))
	return top
}

func (a *APIServer) bearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && !(a.o.MCP && r.Method == http.MethodPost && r.URL.Path == "/mcp") {
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		name, tenant, valid := a.toks.lookup(strings.TrimSpace(tok))
		if !ok || !valid {
			w.Header().Set("WWW-Authenticate", `Bearer realm="triage-agent"`)
			http.Error(w, "a valid bearer token is required", http.StatusUnauthorized)
			return
		}
		if tenant != "" {
			q := r.URL.Query()
			if t := q.Get("tenant"); t != "" && t != tenant {
				http.Error(w, "this token is limited to its own tenant", http.StatusForbidden)
				return
			}
			q.Set("tenant", tenant)
			r.URL.RawQuery = q.Encode()
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), scopeKey{}, scope{name: name, tenant: tenant})))
	})
}

// wellKnown is the discovery document: what this agent is and where its
// endpoints are, as seen by the caller's scope.
func (a *APIServer) wellKnown(w http.ResponseWriter, r *http.Request) {
	s := a.s
	role := ""
	if rr, ok := s.o.Link.(interface{ Role() string }); ok {
		role = rr.Role()
	}
	tenant, restricted := scopeOf(r)
	eps := map[string]any{
		"metrics":  map[string]any{"path": "/metrics", "format": "prometheus", "leaderOnly": true},
		"api":      map[string]any{"path": "/api/v1", "format": "json", "tenantQuery": "tenant"},
		"stream":   map[string]any{"path": "/api/v1/stream", "format": "server-sent-events"},
		"discover": map[string]any{"path": "/.well-known/triage-agent"},
	}
	if a.o.Docs {
		eps["openapi"] = map[string]any{"path": "/api/v1/openapi.json", "docs": "/api/v1/docs"}
	}
	if a.o.MCP {
		eps["mcp"] = map[string]any{"path": "/mcp", "transport": "streamable-http"}
	}
	writeJSON(w, map[string]any{
		"name": "triage-agent", "version": s.o.Version, "cluster": s.o.Cluster, "mode": s.o.Mode, "role": role,
		"scope":     map[string]any{"restricted": restricted, "tenant": tenant},
		"tenants":   s.visibleTenants(r),
		"auth":      map[string]any{"type": "bearer"},
		"endpoints": eps,
	})
}

// credentials resolves a bearer token to a caller name and tenant scope
// ("" is unrestricted).
type credentials interface {
	lookup(tok string) (name, tenant string, ok bool)
}

// credentialChain tries each source in order: the token file first, a cheap
// local compare, then the Kubernetes reviews.
type credentialChain []credentials

func (c credentialChain) lookup(tok string) (name, tenant string, ok bool) {
	for _, s := range c {
		if name, tenant, ok = s.lookup(tok); ok {
			return name, tenant, true
		}
	}
	return "", "", false
}
