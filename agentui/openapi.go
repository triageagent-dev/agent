package agentui

import "net/http"

// openAPIDoc describes the JSON API for Swagger UI and client generators. The
// MCP tools in mcp.go read the same paths.
func openAPIDoc(mcpOn bool) map[string]any {
	tenant := map[string]any{"name": "tenant", "in": "query", "schema": map[string]any{"type": "string"},
		"description": "Limit the answer to one tenant. A tenant-bound token is forced to its own."}
	get := func(summary string, params ...any) map[string]any {
		return map[string]any{"get": map[string]any{
			"summary": summary, "parameters": params, "security": []any{map[string]any{"bearer": []any{}}},
			"responses": map[string]any{"200": map[string]any{"description": "OK",
				"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": "object"}}}}},
		}}
	}
	paths := map[string]any{}
	for _, t := range mcpTools {
		params := []any{}
		for _, a := range t.args {
			if a.name == "tenant" {
				params = append(params, tenant)
				continue
			}
			params = append(params, map[string]any{"name": a.name, "in": "query", "description": a.desc,
				"schema": map[string]any{"type": a.typ}})
		}
		paths[t.path] = get(t.desc, params...)
	}
	paths["/api/v1/stream"] = map[string]any{"get": map[string]any{
		"summary": "Server-sent events: each new event as it arrives.", "security": []any{map[string]any{"bearer": []any{}}},
		"parameters": []any{tenant, map[string]any{"name": "family", "in": "query", "schema": map[string]any{"type": "string"}}},
		"responses":  map[string]any{"200": map[string]any{"description": "text/event-stream"}},
	}}
	paths["/metrics"] = get("Prometheus metrics (leader only).")
	paths["/.well-known/triage-agent"] = get("Discovery document: version, scope, endpoints.")
	if mcpOn {
		paths["/mcp"] = map[string]any{"post": map[string]any{
			"summary":     "Model Context Protocol over streamable HTTP (JSON-RPC 2.0): the read-only tools above.",
			"security":    []any{map[string]any{"bearer": []any{}}},
			"requestBody": map[string]any{"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": "object"}}}},
			"responses":   map[string]any{"200": map[string]any{"description": "JSON-RPC response"}},
		}}
	}
	return map[string]any{
		"openapi": "3.0.3",
		"info":    map[string]any{"title": "Triage agent API", "version": "1", "description": "Read-only monitoring API of the per-cluster triage agent."},
		"paths":   paths,
		"components": map[string]any{"securitySchemes": map[string]any{
			"bearer": map[string]any{"type": "http", "scheme": "bearer"}}},
	}
}

const swaggerPage = `<!doctype html><html><head><meta charset="utf-8"><title>Triage agent API</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.17.14/swagger-ui.min.css" integrity="sha384-+R8fx41ke61Yq4+j30C8Uh8fbNUEGy8Le9lzVmeMfiUNd+H3q8gN7qW9LM3rbesj" crossorigin="anonymous"></head>
<body><div id="ui"></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.17.14/swagger-ui-bundle.min.js" integrity="sha384-5zikP7B6JemcHcJalIiFMnUirmSVBr3lZ85ClVPXO7mamWuqUixgyCMUPcKKfZLk" crossorigin="anonymous"></script>
<script src="/api/v1/docs.js"></script></body></html>`

const swaggerInit = `SwaggerUIBundle({url:"/api/v1/openapi.json",dom_id:"#ui",validatorUrl:null})`

// staticHeaders pin the unauthenticated pages: scripts only from this origin
// and the pinned CDN files, no framing.
func staticHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self' https://cdnjs.cloudflare.com; "+
		"style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	h.Set("X-Frame-Options", "DENY")
	h.Set("X-Content-Type-Options", "nosniff")
}

func swaggerUI(w http.ResponseWriter, _ *http.Request) {
	staticHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerPage))
}

func swaggerJS(w http.ResponseWriter, _ *http.Request) {
	staticHeaders(w)
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	_, _ = w.Write([]byte(swaggerInit))
}
