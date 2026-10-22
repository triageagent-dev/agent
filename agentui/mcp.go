package agentui

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// mcpMaxBody caps a JSON-RPC request; the tools take a few short arguments.
const mcpMaxBody = 64 << 10

// mcpTool is one read-only tool: a name, what it returns, the API path it
// reads, and the query arguments it takes.
type mcpTool struct {
	name, desc, path string
	args             []mcpArg
}

type mcpArg struct {
	name, typ, desc string
}

var tenantArg = mcpArg{"tenant", "string", "Limit the answer to one tenant. Ignored (forced) for a tenant-bound token."}

// mcpTools mirrors the JSON API one to one, so a tool answers exactly what
// the same GET answers, under the same bearer token and tenant scope.
var mcpTools = []mcpTool{
	{"agent_status", "Agent health: link to the core, leader, queue and limits.", "/api/v1/status", []mcpArg{tenantArg}},
	{"list_tenants", "Tenants this token can see.", "/api/v1/tenants", nil},
	{"list_apps", "Virtual apps with their workloads and traffic.", "/api/v1/apps", []mcpArg{tenantArg}},
	{"list_services", "Services with their golden signals (rate, errors, p99).", "/api/v1/services", []mcpArg{tenantArg}},
	{"get_graph", "Observed service graph: nodes and edges with call counts.", "/api/v1/graph", []mcpArg{tenantArg}},
	{"list_sources", "Signal sources (sensors, collectors) and their state.", "/api/v1/sources", []mcpArg{tenantArg}},
	{"get_events", "Recent events the agent sent to the core.", "/api/v1/events", []mcpArg{tenantArg,
		{"family", "string", "Comma-separated families, for example log,request,resource."},
		{"limit", "integer", "Maximum number of events."}}},
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func toolSchema(t mcpTool) map[string]any {
	props := map[string]any{}
	for _, a := range t.args {
		props[a.name] = map[string]any{"type": a.typ, "description": a.desc}
	}
	return map[string]any{"type": "object", "properties": props, "additionalProperties": false}
}

// mcp serves the Model Context Protocol over streamable HTTP: JSON-RPC on
// POST, answered as one JSON body. data is the API router without the bearer
// check; the request already passed it, so scope rides on r's context.
func mcp(data http.Handler, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "POST JSON-RPC to this endpoint", http.StatusMethodNotAllowed)
			return
		}
		if o := r.Header.Get("Origin"); o != "" {
			if u, err := url.Parse(o); err != nil || u.Host != r.Host { // DNS rebinding
				http.Error(w, "cross-origin request refused", http.StatusForbidden)
				return
			}
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, mcpMaxBody))
		if err != nil {
			http.Error(w, "request too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		var req rpcRequest
		null := json.RawMessage("null")
		if json.Unmarshal(body, &req) != nil {
			writeJSON(w, rpcResponse{JSONRPC: "2.0", ID: null, Error: &rpcError{-32700, "parse error (batches are not supported)"}})
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" || string(req.ID) == "null" {
			writeJSON(w, rpcResponse{JSONRPC: "2.0", ID: null, Error: &rpcError{-32600, "invalid request"}})
			return
		}
		if len(req.ID) == 0 { // a notification has no answer
			w.WriteHeader(http.StatusAccepted)
			return
		}
		resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "initialize":
			resp.Result = map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "triage-agent", "version": version},
			}
		case "ping":
			resp.Result = map[string]any{}
		case "tools/list":
			tools := make([]map[string]any, 0, len(mcpTools))
			for _, t := range mcpTools {
				tools = append(tools, map[string]any{"name": t.name, "description": t.desc, "inputSchema": toolSchema(t),
					"annotations": map[string]any{"readOnlyHint": true}})
			}
			resp.Result = map[string]any{"tools": tools}
		case "tools/call":
			resp.Result, resp.Error = callTool(data, r, req.Params)
		default:
			resp.Error = &rpcError{-32601, "method not found"}
		}
		writeJSON(w, resp)
	}
}

// callTool runs the tool's GET against the API router and returns its body as
// the tool result; an API error is a tool error, not a protocol error.
func callTool(data http.Handler, r *http.Request, raw json.RawMessage) (any, *rpcError) {
	var p struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return nil, &rpcError{-32602, "invalid params"}
	}
	var tool *mcpTool
	for i := range mcpTools {
		if mcpTools[i].name == p.Name {
			tool = &mcpTools[i]
		}
	}
	if tool == nil {
		return nil, &rpcError{-32602, "unknown tool " + strconv.Quote(p.Name)}
	}
	q := url.Values{}
	for _, a := range tool.args {
		if v, ok := p.Arguments[a.name]; ok {
			switch x := v.(type) {
			case string:
				q.Set(a.name, x)
			case float64:
				if x >= 0 && x <= 1e6 {
					q.Set(a.name, strconv.Itoa(int(x)))
				}
			}
		}
	}
	if t, restricted := scopeOf(r); restricted {
		q.Set("tenant", t) // the token's tenant wins over any argument
	}
	sub, err := http.NewRequestWithContext(r.Context(), http.MethodGet, tool.path+"?"+q.Encode(), nil)
	if err != nil {
		return nil, &rpcError{-32603, "internal error"}
	}
	sub.Host = r.Host
	rec := &capWriter{h: http.Header{}, code: http.StatusOK}
	data.ServeHTTP(rec, sub)
	text, isErr := rec.buf.String(), rec.code >= 400
	if rec.over {
		text, isErr = "the answer is larger than the tool limit; narrow it with the tenant argument", true
	}
	return map[string]any{
		"isError": isErr,
		"content": []map[string]any{{"type": "text", "text": text}},
	}, nil
}

// mcpMaxResult caps one tool answer.
const mcpMaxResult = 2 << 20

// capWriter is the in-memory ResponseWriter a tool call reads its answer from;
// it keeps at most mcpMaxResult bytes and notes when more arrived.
type capWriter struct {
	h    http.Header
	code int
	buf  bytes.Buffer
	over bool
}

func (c *capWriter) Header() http.Header  { return c.h }
func (c *capWriter) WriteHeader(code int) { c.code = code }
func (c *capWriter) Write(p []byte) (int, error) {
	if room := mcpMaxResult - c.buf.Len(); len(p) > room {
		c.buf.Write(p[:room])
		c.over = true
	} else {
		c.buf.Write(p)
	}
	return len(p), nil
}
