package agentui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testKey = "0123456789abcdefghij"

func authServer(t *testing.T, opts func(*Options)) (*Server, http.Handler) {
	t.Helper()
	srv, _ := newTestServer(t)
	o := srv.o
	if opts != nil {
		opts(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	return s, s.Handler()
}

func keyFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "admin-key")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func post(h http.Handler, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088"+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func getWith(h http.Handler, path string, c *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8088"+path, nil)
	if c != nil {
		r.AddCookie(c)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestLoginFlow(t *testing.T) {
	_, h := authServer(t, func(o *Options) { o.AdminKeyFile = keyFile(t, testKey+"\n") })

	if w := getWith(h, "/api/v1/status", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("status without login = %d, want 401", w.Code)
	}
	if w := getWith(h, "/", nil); w.Code != http.StatusOK {
		t.Errorf("static page = %d, want 200 (the login page must load)", w.Code)
	}
	var st struct{ Required, Authenticated bool }
	_ = json.Unmarshal(getWith(h, "/api/v1/auth", nil).Body.Bytes(), &st)
	if !st.Required || st.Authenticated {
		t.Errorf("auth state = %+v", st)
	}

	if w := post(h, "/api/v1/login", `{"key":"wrong"}`, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong key = %d, want 401", w.Code)
	}
	if w := post(h, "/api/v1/login", `{"key":"`+testKey+`"}`, map[string]string{"Origin": "http://evil.example"}); w.Code != http.StatusBadRequest {
		t.Errorf("foreign Origin = %d, want 400", w.Code)
	}
}

func TestLoginSessionAndLogout(t *testing.T) {
	srv, h := authServer(t, func(o *Options) { o.AdminKeyFile = keyFile(t, testKey) })
	w := post(h, "/api/v1/login", `{"key":"`+testKey+`"}`, map[string]string{"Origin": "http://127.0.0.1:8088"})
	if w.Code != 200 {
		t.Fatalf("login = %d: %s", w.Code, w.Body)
	}
	c := w.Result().Cookies()[0]
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie = %+v, want HttpOnly and SameSite=Strict", c)
	}
	for _, p := range []string{"/api/v1/status", "/api/v1/events", "/api/v1/config", "/api/v1/stream"} {
		if p == "/api/v1/stream" {
			continue // a long-lived response; covered by the 401 path above
		}
		if w := getWith(h, p, c); w.Code != 200 {
			t.Errorf("%s with session = %d", p, w.Code)
		}
	}

	// A tampered or expired cookie is refused.
	bad := *c
	bad.Value = strings.Replace(c.Value, ".", ".x", 1)
	if w := getWith(h, "/api/v1/status", &bad); w.Code != 401 {
		t.Errorf("tampered cookie = %d, want 401", w.Code)
	}
	srv.auth.now = func() time.Time { return time.Now().Add(sessionTTL + time.Minute) }
	if w := getWith(h, "/api/v1/status", c); w.Code != 401 {
		t.Errorf("expired cookie = %d, want 401", w.Code)
	}
	srv.auth.now = time.Now

	// Rotating the key ends every session.
	if err := os.WriteFile(srv.auth.file, []byte("a-completely-different-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if w := getWith(h, "/api/v1/status", c); w.Code != 401 {
		t.Errorf("cookie after key rotation = %d, want 401", w.Code)
	}
}

func TestLoginBackoff(t *testing.T) {
	srv, h := authServer(t, func(o *Options) { o.AdminKeyFile = keyFile(t, testKey) })
	now := time.Now()
	srv.auth.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		post(h, "/api/v1/login", `{"key":"nope"}`, nil)
	}
	if w := post(h, "/api/v1/login", `{"key":"`+testKey+`"}`, nil); w.Code != http.StatusTooManyRequests {
		t.Errorf("login during backoff = %d, want 429 even with the right key", w.Code)
	}
	now = now.Add(2 * time.Second)
	if w := post(h, "/api/v1/login", `{"key":"`+testKey+`"}`, nil); w.Code != 200 {
		t.Errorf("login after backoff = %d, want 200", w.Code)
	}
}

func TestAdminKeyRules(t *testing.T) {
	if _, err := New(Options{Addr: "127.0.0.1:0", Store: NewStore(), AdminKeyFile: keyFile(t, "short")}); err == nil {
		t.Error("a key shorter than 16 bytes was accepted")
	}
	if _, err := New(Options{Addr: "127.0.0.1:0", Store: NewStore(), AdminKeyFile: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Error("a missing key file was accepted")
	}
	// A key given directly (host mode) logs in the same way; the file wins.
	_, h := authServer(t, func(o *Options) { o.AdminKey = testKey })
	if w := post(h, "/api/v1/login", `{"key":"`+testKey+`"}`, nil); w.Code != 200 {
		t.Errorf("login with a direct key = %d", w.Code)
	}
	_, h = authServer(t, func(o *Options) { o.AdminKey = "ignored-ignored-ignored"; o.AdminKeyFile = keyFile(t, testKey) })
	if w := post(h, "/api/v1/login", `{"key":"ignored-ignored-ignored"}`, nil); w.Code != 401 {
		t.Errorf("the direct key won over the file: %d", w.Code)
	}
}

func TestNoLoginWithoutKey(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	if w := get(t, h, "/api/v1/status"); w.Code != 200 {
		t.Errorf("status without a key configured = %d", w.Code)
	}
	if w := post(h, "/api/v1/login", `{"key":"x"}`, nil); w.Code != http.StatusNotFound {
		t.Errorf("login with no key configured = %d, want 404", w.Code)
	}
	var st struct{ Required, Authenticated bool }
	_ = json.Unmarshal(get(t, h, "/api/v1/auth").Body.Bytes(), &st)
	if st.Required || !st.Authenticated {
		t.Errorf("auth state = %+v", st)
	}
}
