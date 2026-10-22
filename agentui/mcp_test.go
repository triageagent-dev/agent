package agentui

import (
	"encoding/json"
	"fmt"
	"github.com/triageagent-dev/agent/internal/ledger"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func rpc(h http.Handler, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "http://agent.example/mcp", strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestMCPNeedsToken(t *testing.T) {
	_, _, h := mcpFixture(t)
	if w := rpc(h, "", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`); w.Code != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", w.Code)
	}
	// POST is allowed on /mcp only.
	r := httptest.NewRequest(http.MethodPost, "http://agent.example/api/v1/apps", nil)
	r.Header.Set("Authorization", "Bearer "+tokAll)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/v1/apps: %d, want 405", w.Code)
	}
}

func TestMCPToolsAndTenantScope(t *testing.T) {
	_, _, h := mcpFixture(t)
	w := rpc(h, tokAll, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	var list struct {
		Result struct{ Tools []struct{ Name string } }
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Result.Tools) != len(mcpTools) {
		t.Fatalf("tools/list: %v %s", err, w.Body.String())
	}
	call := func(tok, tenant string) string {
		body := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_apps","arguments":{"tenant":"` + tenant + `"}}}`
		return rpc(h, tok, body).Body.String()
	}
	if got := call(tokAll, "t1"); !strings.Contains(got, "checkout") || strings.Contains(got, "billing") {
		t.Errorf("unrestricted token asking t1: %s", got)
	}
	// A t1 token that asks for t2 still gets t1 only.
	if got := call(tokT1, "t2"); !strings.Contains(got, "checkout") || strings.Contains(got, "billing") {
		t.Errorf("t1 token asking t2: %s", got)
	}
	if w := rpc(h, tokAll, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"nope"}}`); !strings.Contains(w.Body.String(), "unknown tool") {
		t.Errorf("unknown tool: %s", w.Body.String())
	}
	if w := rpc(h, tokAll, `{"jsonrpc":"2.0","method":"notifications/initialized"}`); w.Code != http.StatusAccepted {
		t.Errorf("notification: %d, want 202", w.Code)
	}
}

func TestOpenAPIServedWithoutToken(t *testing.T) {
	_, _, h := mcpFixture(t)
	w := call(h, "/api/v1/openapi.json", "")
	var doc struct {
		OpenAPI string
		Paths   map[string]any
	}
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil || doc.OpenAPI == "" {
		t.Fatalf("openapi: %v %s", err, w.Body.String())
	}
	for _, p := range []string{"/api/v1/apps", "/api/v1/stream", "/mcp", "/metrics"} {
		if _, ok := doc.Paths[p]; !ok {
			t.Errorf("spec lacks %s", p)
		}
	}
	if w := call(h, "/api/v1/docs", ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "swagger-ui-bundle") {
		t.Errorf("docs page: %d", w.Code)
	}
	if w := call(h, "/api/v1/apps", ""); w.Code != http.StatusUnauthorized {
		t.Errorf("data still needs a token: %d", w.Code)
	}
}

func TestMCPRefusals(t *testing.T) {
	_, _, h := mcpFixture(t)
	r := httptest.NewRequest(http.MethodPost, "http://agent.example/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	r.Header.Set("Authorization", "Bearer "+tokAll)
	r.Header.Set("Origin", "http://evil.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Errorf("cross-origin: %d, want 403", w.Code)
	}
	if w := rpc(h, tokAll, `[{"jsonrpc":"2.0","id":1,"method":"ping"}]`); !strings.Contains(w.Body.String(), "-32700") {
		t.Errorf("batch: %s", w.Body.String())
	}
	if w := rpc(h, tokAll, `{"jsonrpc":"1.0","id":1,"method":"ping"}`); !strings.Contains(w.Body.String(), "-32600") {
		t.Errorf("wrong version: %s", w.Body.String())
	}
	if w := rpc(h, tokAll, strings.Repeat("x", mcpMaxBody+1)); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize: %d, want 413", w.Code)
	}
}

func TestDocsHeaders(t *testing.T) {
	_, _, h := mcpFixture(t)
	w := call(h, "/api/v1/docs", "")
	if !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") || w.Header().Get("X-Frame-Options") != "DENY" {
		t.Errorf("docs headers: %v", w.Header())
	}
	if !strings.Contains(w.Body.String(), "integrity=") {
		t.Error("docs page lacks SRI")
	}
	if w := call(h, "/api/v1/docs.js", ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "SwaggerUIBundle") {
		t.Errorf("docs.js: %d", w.Code)
	}
}

// mcpFixture is apiFixture with MCP and the docs switched on, which are off
// by default.
func mcpFixture(t *testing.T) (*APIServer, *Store, http.Handler) {
	a, st, _ := apiFixture(t)
	a.o.MCP, a.o.Docs = true, true
	return a, st, a.Handler()
}

func TestMCPAndDocsOffByDefault(t *testing.T) {
	_, _, h := apiFixture(t)
	if w := rpc(h, tokAll, `{"jsonrpc":"2.0","id":1,"method":"ping"}`); w.Code == http.StatusOK {
		t.Errorf("/mcp answered with the switch off: %d", w.Code)
	}
	for _, p := range []string{"/api/v1/openapi.json", "/api/v1/docs", "/api/v1/docs.js"} {
		if w := call(h, p, tokAll); w.Code != http.StatusNotFound {
			t.Errorf("%s with docs off: %d, want 404", p, w.Code)
		}
	}
	if w := call(h, "/.well-known/triage-agent", tokAll); strings.Contains(w.Body.String(), "mcp") || strings.Contains(w.Body.String(), "openapi") {
		t.Errorf("discovery lists switched-off endpoints: %s", w.Body.String())
	}
}

// A tenant that raises many events does not push a quiet tenant's out of the
// buffer.
func TestEventsAreKeptPerTenant(t *testing.T) {
	st := NewStoreSized(MinMaxEvents)
	st.add(Event{Tenant: "quiet", Family: FamilyLog, Reason: "log"})
	for i := 0; i < 5*MinMaxEvents; i++ {
		st.add(Event{Tenant: "noisy", Family: FamilyRed, Reason: "red"})
	}
	if n := len(st.Events("quiet", nil, 0)); n != 1 {
		t.Errorf("the quiet tenant kept %d events, want 1", n)
	}
	if n := len(st.Events("noisy", nil, 0)); n != MinMaxEvents {
		t.Errorf("the noisy tenant kept %d events, want %d", n, MinMaxEvents)
	}
}

func TestSourcesCarryTheLedger(t *testing.T) {
	_, _, h := apiFixture(t)
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	ledger.Default.Add("ebpf", "n1", ledger.Ticks, 3)
	ledger.Default.Add("ebpf", ledger.All, ledger.Gated, 2)
	var out struct {
		Sources []struct {
			Kind, Node string
			Ledger     map[string]int64
			RoleLedger map[string]int64
		}
	}
	if err := json.Unmarshal(call(h, "/api/v1/sources", tokAll).Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	for _, s := range out.Sources {
		if s.Kind == "ebpf" && s.Node == "n1" {
			if s.Ledger["ticks"] != 3 || s.RoleLedger["gated"] != 2 {
				t.Errorf("ebpf n1 ledger = %v / %v", s.Ledger, s.RoleLedger)
			}
			return
		}
	}
	t.Error("no ebpf n1 source")
}

// A tenant-bound token sees no node-wide sensor rows, so no ledger of one.
func TestLedgerIsHiddenFromTenantTokens(t *testing.T) {
	_, _, h := apiFixture(t)
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	ledger.Default.Add("ebpf", "n1", ledger.Ticks, 3)
	ledger.Default.Add("ebpf", ledger.All, ledger.Sent, 2)
	body := call(h, "/api/v1/sources", tokT1).Body.String()
	if strings.Contains(body, `"ledger"`) || strings.Contains(body, `"roleLedger"`) {
		t.Errorf("a tenant token saw the ledger: %s", body)
	}
	if m := call(h, "/metrics", tokT1).Body.String(); strings.Contains(m, "signal_path") {
		t.Error("a tenant token saw the signal-path series")
	}
}

func TestSnapshotIsBoundedOverTenants(t *testing.T) {
	st := NewStoreSized(MaxMaxEvents)
	for i := 0; i < 20; i++ {
		for j := 0; j < 150; j++ {
			st.add(Event{Tenant: fmt.Sprintf("t%d", i), Family: FamilyLog, Reason: "x"})
		}
	}
	if n := len(st.Export().Events); n > maxSnapshotTotal {
		t.Errorf("snapshot holds %d events, want at most %d", n, maxSnapshotTotal)
	}
	st2 := NewStoreSized(MinMaxEvents)
	st2.Import(st.Export())
	for i := 0; i < 20; i++ {
		if n := len(st2.Events(fmt.Sprintf("t%d", i), nil, 0)); n > MinMaxEvents {
			t.Errorf("tenant t%d kept %d after import, want at most %d", i, n, MinMaxEvents)
		}
	}
}
