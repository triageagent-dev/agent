package agentui

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// minTokenLen is the shortest API token the server accepts: a token is a
// bearer credential on a network listener, so it must be generated, not chosen.
const minTokenLen = 24

// AllTenants in a token's tenant column means the token is not restricted.
const AllTenants = "*"

type tokenEntry struct {
	name   string
	tenant string // "" for AllTenants
	hash   [sha256.Size]byte
}

// tokenSet is the API's credentials, read from a file the Secret is mounted
// as: one token per line, `name tenant token`, `#` comments. A token whose
// tenant is not `*` can read that tenant only. The file is reloaded when it
// changes, so a rotated Secret applies without a restart; a reload that fails
// keeps the tokens already loaded.
type tokenSet struct {
	file string

	mu      sync.Mutex
	mtime   time.Time
	entries []tokenEntry
}

func newTokenSet(file string) (*tokenSet, error) {
	t := &tokenSet{file: file}
	if err := t.load(); err != nil {
		return nil, err
	}
	return t, nil
}

func parseTokens(content string) ([]tokenEntry, error) {
	var out []tokenEntry
	names := map[string]bool{}
	sc := bufio.NewScanner(strings.NewReader(content))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 3 {
			return nil, fmt.Errorf("line %d: want `name tenant token`", n)
		}
		name, tenant, tok := f[0], f[1], f[2]
		if len(tok) < minTokenLen {
			return nil, fmt.Errorf("line %d (%s): the token is shorter than %d bytes", n, name, minTokenLen)
		}
		if names[name] {
			return nil, fmt.Errorf("line %d: the name %q is used twice", n, name)
		}
		names[name] = true
		if tenant == AllTenants {
			tenant = ""
		}
		out = append(out, tokenEntry{name: name, tenant: tenant, hash: sha256.Sum256([]byte(tok))})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no tokens")
	}
	return out, nil
}

func (t *tokenSet) load() error {
	st, err := os.Stat(t.file)
	if err != nil {
		return fmt.Errorf("agentui: API tokens file: %w", err)
	}
	b, err := os.ReadFile(t.file)
	if err != nil {
		return fmt.Errorf("agentui: API tokens file: %w", err)
	}
	e, err := parseTokens(string(b))
	if err != nil {
		return fmt.Errorf("agentui: API tokens file %s: %w", t.file, err)
	}
	t.mu.Lock()
	t.entries, t.mtime = e, st.ModTime()
	t.mu.Unlock()
	return nil
}

// lookup returns the token's name and tenant scope ("" is unrestricted).
func (t *tokenSet) lookup(tok string) (name, tenant string, ok bool) {
	if st, err := os.Stat(t.file); err == nil {
		t.mu.Lock()
		changed := !st.ModTime().Equal(t.mtime)
		t.mu.Unlock()
		if changed {
			_ = t.load()
		}
	}
	h := sha256.Sum256([]byte(tok))
	t.mu.Lock()
	defer t.mu.Unlock()
	// Compare against every entry, so the time does not say which matched.
	for _, e := range t.entries {
		if subtle.ConstantTimeCompare(h[:], e.hash[:]) == 1 {
			name, tenant, ok = e.name, e.tenant, true
		}
	}
	return name, tenant, ok
}
