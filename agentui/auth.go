package agentui

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cookieName = "triage_agent_ui"
	sessionTTL = 12 * time.Hour
	// minKeyLen is the shortest admin key the server accepts.
	minKeyLen = 16
	// maxLoginBody bounds the login request body.
	maxLoginBody = 4 << 10
	maxBackoff   = time.Minute
)

// auth is the login: one admin key, read from a file the Secret is mounted
// as. A session is a cookie signed with a key derived from the admin key, so
// every replica accepts it and rotating the key ends every session. The file
// is read on each use, so a rotated Secret takes effect without a restart.
type auth struct {
	file   string
	static []byte // a key given directly (host mode); wins over file
	now    func() time.Time

	mu       sync.Mutex
	failures int
	until    time.Time // logins are refused until then
}

func newAuth(file, static string) (*auth, error) {
	a := &auth{file: file, static: []byte(strings.TrimSpace(static)), now: time.Now}
	if _, err := a.key(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *auth) key() ([]byte, error) {
	if len(a.static) > 0 {
		if len(a.static) < minKeyLen {
			return nil, fmt.Errorf("agentui: the admin key is shorter than %d bytes", minKeyLen)
		}
		return a.static, nil
	}
	b, err := os.ReadFile(a.file)
	if err != nil {
		return nil, fmt.Errorf("agentui: admin key file: %w", err)
	}
	k := []byte(strings.TrimSpace(string(b)))
	if len(k) < minKeyLen {
		return nil, fmt.Errorf("agentui: the admin key in %s is shorter than %d bytes", a.file, minKeyLen)
	}
	return k, nil
}

func sessionKey(adminKey []byte) []byte {
	m := hmac.New(sha256.New, adminKey)
	m.Write([]byte("triage-agent-ui-session-v1"))
	return m.Sum(nil)
}

func sign(key []byte, exp int64) string {
	m := hmac.New(sha256.New, key)
	fmt.Fprintf(m, "%d", exp)
	return strconv.FormatInt(exp, 10) + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// authenticated reports whether r carries a live session cookie.
func (a *auth) authenticated(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	expStr, _, ok := strings.Cut(c.Value, ".")
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if !ok || err != nil || a.now().Unix() >= exp {
		return false
	}
	k, err := a.key()
	if err != nil {
		return false
	}
	return hmac.Equal([]byte(c.Value), []byte(sign(sessionKey(k), exp)))
}

// sameOrigin allows a request with no Origin or one naming this Host.
func sameOrigin(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	u, err := url.Parse(o)
	return err == nil && u.Host == r.Host
}

// backoff is how long logins are refused after n consecutive failures.
func backoff(n int) time.Duration {
	if n < 3 {
		return 0
	}
	d := time.Second << (n - 3)
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}

func (a *auth) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !sameOrigin(r) || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	wait := a.until.Sub(a.now())
	a.mu.Unlock()
	if wait > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		http.Error(w, "too many attempts", http.StatusTooManyRequests)
		return
	}
	var body struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxLoginBody)).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	k, err := a.key()
	if err != nil {
		http.Error(w, "login unavailable", http.StatusServiceUnavailable)
		return
	}
	// Compare hashes so the length of the key does not leak.
	got, want := sha256.Sum256([]byte(body.Key)), sha256.Sum256(k)
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		a.mu.Lock()
		a.failures++
		a.until = a.now().Add(backoff(a.failures))
		a.mu.Unlock()
		http.Error(w, "wrong key", http.StatusUnauthorized)
		return
	}
	a.mu.Lock()
	a.failures, a.until = 0, time.Time{}
	a.mu.Unlock()
	exp := a.now().Add(sessionTTL)
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: sign(sessionKey(k), exp.Unix()), Path: "/",
		Expires: exp, MaxAge: int(sessionTTL.Seconds()), HttpOnly: true, SameSite: http.SameSiteStrictMode})
	writeJSON(w, map[string]any{"ok": true})
}

func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !sameOrigin(r) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
	writeJSON(w, map[string]any{"ok": true})
}

// errNoAuth is returned for a request that needs a session and has none.
var errNoAuth = errors.New("login required")
