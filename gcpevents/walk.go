package gcpevents

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The two Pub/Sub payloads this function decodes are documented loosely and are
// not stable enough to bind to a struct: the cost anomaly root-cause tree is a
// recursive shape whose field names appear as both camelCase and snake_case in
// Google's own documentation, and the Personalized Service Health EventLog
// reaches us wrapped in whatever envelope the log sink produced.
//
// So both decoders work over a decoded map[string]any and pull out what they need
// by pattern rather than by position: a billing service id looks like
// "services/XXXX-XXXX-XXXX" wherever it appears, and an EventLog is the object
// carrying an "@type" of ...EventLog. A field rename upstream costs us nothing;
// only a change to those two shapes would.

var (
	// serviceIDRe matches a Cloud Billing service id, e.g. services/24E6-581D-38E5.
	serviceIDRe = regexp.MustCompile(`^services/[0-9A-Za-z]{4}-[0-9A-Za-z]{4}-[0-9A-Za-z]{4}$`)
	// projectRefRe matches a project reference, e.g. projects/my-project or
	// projects/123456789 (anomaly attributes use the latter).
	projectRefRe = regexp.MustCompile(`^projects/[A-Za-z0-9][A-Za-z0-9\-_.:]{2,}$`)
)

// walkStrings visits every string value in a decoded JSON document in a
// deterministic order — map keys sorted, slices in order — so "the first service
// id in the payload" is a stable choice across runs.
func walkStrings(v any, fn func(s string)) {
	switch t := v.(type) {
	case string:
		fn(t)
	case []any:
		for _, e := range t {
			walkStrings(e, fn)
		}
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			walkStrings(t[k], fn)
		}
	}
}

// findMatching collects every string in the document matching re, de-duplicated,
// first-seen order preserved.
func findMatching(v any, re *regexp.Regexp) []string {
	var out []string
	seen := make(map[string]bool)
	walkStrings(v, func(s string) {
		s = strings.TrimSpace(s)
		if !re.MatchString(s) || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	})
	return out
}

// str returns the first non-empty string value among the given key spellings.
// Accepts camelCase and snake_case siblings without the caller branching.
func str(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				if s = strings.TrimSpace(s); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

// obj returns the first map value among the given key spellings.
func obj(m map[string]any, keys ...string) map[string]any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if o, ok := v.(map[string]any); ok {
				return o
			}
		}
	}
	return nil
}

// num returns the first numeric value among the given key spellings.
//
// A spend value reaches us as a JSON number, a decimal string, or a
// google.type.Money object ({currencyCode, units, nanos}) depending on which
// field it is — Google's own notification-format table mixes all three. num
// accepts any of them and also reports the currency when the value carried one.
func num(m map[string]any, keys ...string) (value float64, currency string, ok bool) {
	for _, k := range keys {
		v, present := m[k]
		if !present {
			continue
		}
		if f, cur, ok := toNumber(v); ok {
			return f, cur, true
		}
	}
	return 0, "", false
}

func toNumber(v any) (value float64, currency string, ok bool) {
	switch t := v.(type) {
	case float64:
		return finite(t, "")
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0, "", false
		}
		return finite(f, "")
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return 0, "", false
		}
		return finite(f, "")
	case map[string]any:
		// google.type.Money: units is an int64-as-string, nanos an int32.
		cur := str(t, "currencyCode", "currency_code", "currency")
		units, _, hasUnits := num(t, "units")
		nanos, _, hasNanos := num(t, "nanos")
		if !hasUnits && !hasNanos {
			// Some payloads nest the value one level deeper.
			if f, c, ok := num(t, "amount", "value"); ok {
				if c == "" {
					c = cur
				}
				return f, c, true
			}
			return 0, "", false
		}
		return finite(units+nanos/1e9, cur)
	}
	return 0, "", false
}

// finite rejects a value JSON cannot carry.
//
// protojson renders a non-finite double as the string "Infinity", "-Infinity" or
// "NaN", and strconv.ParseFloat accepts all three — so an anomaly whose expected
// spend was zero arrives with deviationPercentage:"Infinity" and used to reach the
// event body as +Inf, where json.Marshal rejected the whole event. Reporting the
// field as absent instead drops that one number and keeps the event: the original
// spelling still travels in raw.anomaly, and severity falls back to the absolute
// deviation, which is the meaningful trigger when expected spend was zero anyway.
func finite(f float64, currency string) (float64, string, bool) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, "", false
	}
	return f, currency, true
}

// formatAmount renders a spend value for the event message.
func formatAmount(value float64, currency string) string {
	s := strconv.FormatFloat(value, 'f', 2, 64)
	if currency == "" {
		return s
	}
	return s + " " + currency
}

// findEventLog locates the Personalized Service Health EventLog inside whatever
// envelope delivered it — a raw log sink message has it at .jsonPayload, but the
// same object may arrive nested one level deeper.
//
// Identified by its "@type" ending in EventLog, or, failing that, by the field
// combination no other payload in this function has.
func findEventLog(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		if list, ok := v.([]any); ok {
			for _, e := range list {
				if found := findEventLog(e); found != nil {
					return found
				}
			}
		}
		return nil
	}

	if t := str(m, "@type"); strings.HasSuffix(t, "EventLog") {
		return m
	}
	_, hasState := m["state"]
	_, hasProducts := m["impactedProducts"]
	if hasState && hasProducts {
		return m
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if found := findEventLog(m[k]); found != nil {
			return found
		}
	}
	return nil
}

// stringList reads a field that may be a JSON array of strings, an array of
// objects carrying a name, a single string, or a Python-style rendering of a
// list — Personalized Service Health log entries show `"['us-east1', 'eu-west1']"`
// for impactedLocations in Google's own example.
func stringList(m map[string]any, keys ...string) []string {
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		if out := coerceStringList(v); len(out) > 0 {
			return out
		}
	}
	return nil
}

func coerceStringList(v any) []string {
	switch t := v.(type) {
	case []any:
		var out []string
		for _, e := range t {
			switch ev := e.(type) {
			case string:
				if s := strings.TrimSpace(ev); s != "" {
					out = append(out, s)
				}
			case map[string]any:
				if s := str(ev, "productName", "product_name", "displayName", "display_name", "name", "title", "id"); s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	case string:
		return parseBracketedList(t)
	}
	return nil
}

// parseBracketedList turns "['a', 'b']" or "a, b" into []string{"a","b"}.
func parseBracketedList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `'"`)
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// slugForKey resolves any known spelling of a GCP service — billing service id,
// billing display name, Personalized Service Health product name — to the
// canonical node name in the triage `gcp` tenant's service graph. Reports false
// when the catalog has no entry, so the caller can pick a catch-all.
func slugForKey(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	// Candidates, cheapest and most exact first. normalizeKey runs on the ORIGINAL
	// casing — splitting "CloudSQL" into "cloud sql" needs the capital S.
	candidates := []string{strings.ToLower(raw), normalizeKey(raw)}
	for _, c := range candidates {
		if slug, ok := serviceSlugByKey[c]; ok {
			return slug, true
		}
		// Product names often arrive with a "Google " or "Google Cloud " courtesy
		// prefix that the billing catalog spells without.
		for _, prefix := range []string{"google cloud ", "google "} {
			if trimmed := strings.TrimPrefix(c, prefix); trimmed != c {
				if slug, ok := serviceSlugByKey[trimmed]; ok {
					return slug, true
				}
			}
		}
	}
	return "", false
}

// normalizeKey reduces a product name to a comparable form: separators become
// single spaces and a run-together CamelCase word is split ("CloudSQL" →
// "cloud sql"), which is how service health names some products.
func normalizeKey(s string) string {
	var b strings.Builder
	var prev rune
	for i, r := range s {
		switch {
		case r == '-' || r == '_' || r == '/' || r == '.' || r == '(' || r == ')':
			b.WriteRune(' ')
		default:
			if i > 0 && isUpper(r) && !isUpper(prev) && prev != ' ' {
				b.WriteRune(' ')
			}
			b.WriteRune(r)
		}
		prev = r
	}
	return strings.Join(strings.Fields(strings.ToLower(b.String())), " ")
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }

// cloudGCP is the key space every identifier this bridge emits belongs to. It
// rides on the event as `raw.cloud` so a consumer never has to infer the cloud
// from a source path or from the shape of an id.
const cloudGCP = "gcp"

// projectID reduces a "projects/<id>" reference to the bare canonical id used as
// a join key. Lowercased because the ownership matrices this feeds are keyed
// case-insensitively and a mixed-case id must not read as a different project.
// Returns "" when ref carries no id.
func projectID(ref string) string {
	return strings.ToLower(strings.TrimSpace(strings.TrimPrefix(ref, "projects/")))
}

// projectLabel renders a project reference for humans: the display name the
// notification carried, otherwise the bare id. Project names are not resolved from
// a table here — a richer name needs a runtime Cloud Resource Manager lookup.
func projectLabel(ref, displayName string) string {
	id := strings.TrimPrefix(ref, "projects/")
	if displayName != "" && displayName != id {
		return fmt.Sprintf("%s (%s)", displayName, id)
	}
	if id == "" {
		return ""
	}
	return id
}
