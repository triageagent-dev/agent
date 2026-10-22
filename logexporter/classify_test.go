package logexporter

import "testing"

// nodeOtelCases are real-world Node.js / OpenTelemetry / pino shapes; shared by
// the table test and the prefilter-vs-regex-only agreement test.
var nodeOtelCases = []struct {
	name string
	line string
	want verdict
}{
	{"node grpc error", "Error: 14 UNAVAILABLE: No connection established. Last error: Error: connect ETIMEDOUT 10.0.211.7:8080. Resolution note: ", verdictError},
	{"node internal error", "Error: 13 INTERNAL: shipping quote failure: failed POST to email service: expected 200, got 400", verdictError},
	{"indented TypeError", "  TypeError: cannot read properties of undefined", verdictError},
	{"error continuation line not counted again", "  details: 'No connection established. Last error: Error: connect ETIMEDOUT 10.0.211.7:8080. Resolution note: ',", verdictNone},
	{"prose error colon mid-line", "request finished with an Error: see above", verdictNone},
	{"otel dump severity error", "  severityText: 'error',", verdictError},
	{"otel dump severity ERROR", "  severityText: 'ERROR',", verdictError},
	{"otel dump severity fatal", "  severityText: 'fatal',", verdictError},
	{"otel json severity ERROR", `{"severityText":"ERROR","body":"x"}`, verdictError},
	{"otel dump severity info", "  severityText: 'info',", verdictNone},
	{"otel dump body line", "  body: 'Payment request failed. Invalid token. demo.user_context.loyalty_level=gold',", verdictNone},
	{"otel dump type Error not counted", "      type: 'Error',", verdictNone},
	{"otel dump message line", "      message: 'Payment request failed. Invalid token. demo.user_context.loyalty_level=gold',", verdictNone},
	{"otel dump open brace", "{", verdictNone},
	{"otel dump resource", "  resource: {", verdictNone},
	{"otel dump severityNumber", "  severityNumber: 9,", verdictNone},
	{"pino error", `{"level":50,"time":1,"msg":"boom"}`, verdictError},
	{"pino fatal", `{"level":60,"time":1,"msg":"boom"}`, verdictError},
	{"pino 500 not error", `{"level":500,"msg":"x"}`, verdictNone},
	{"pino 5 not error", `{"level":5,"msg":"x"}`, verdictNone},
	{"pino info", `{"level":30,"msg":"x"}`, verdictNone},
	{"pino warn", `{"level":40,"msg":"x"}`, verdictWarn},
	{"pino 400 not warn", `{"level":400,"msg":"x"}`, verdictNone},
	{"otel dump severity warn", "  severityText: 'warn',", verdictWarn},
	{"otel json severity WARNING", `{"severityText":"WARNING"}`, verdictWarn},
}

func TestClassify_NodeOtelPino(t *testing.T) {
	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	// Same regexes, prefilter forced off: the regex-only path.
	plain := *cls
	plain.prefilter = false
	for _, tc := range nodeOtelCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cls.classify([]byte(tc.line)); got != tc.want {
				t.Errorf("prefilter path: classify(%q) = %v, want %v", tc.line, got, tc.want)
			}
			if got := plain.classify([]byte(tc.line)); got != tc.want {
				t.Errorf("regex-only path: classify(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestClassify_DefaultRegexes(t *testing.T) {
	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	if !cls.prefilter {
		t.Fatal("expected prefilter enabled for default regexes")
	}

	cases := []struct {
		name string
		line string
		want verdict
	}{
		{"plain info", "starting up, listening on :8080", verdictNone},
		{"json info", `{"level":"info","msg":"ready"}`, verdictNone},
		{"level= error", "time=now level=error msg=boom", verdictError},
		{"level= fatal", "level=fatal something broke", verdictError},
		{"json level error", `{"level":"error","msg":"boom"}`, verdictError},
		{"json level fatal", `{"level":"fatal","msg":"boom"}`, verdictError},
		{"bare ERROR", "ERROR: connection refused", verdictError},
		{"bare FATAL", "FATAL: out of memory", verdictError},
		{"panic prefix", "panic: runtime error: index out of range", verdictError},
		{"level= warn", "level=warn disk almost full", verdictWarn},
		{"level= warning", "level=warning retrying", verdictWarn},
		{"json level warn", `{"level":"warn","msg":"retry"}`, verdictWarn},
		{"json level warning", `{"level":"warning","msg":"retry"}`, verdictWarn},
		{"bare WARN", "WARN: slow response", verdictWarn},
		{"zerolog console WRN", "\x1b[90m2026-10-10 21:38:09.886\x1b[0m WRN \x1b[1mfile.go:323\x1b[0m > decision model failed error=\"decisions: HTTP 404\"", verdictWarn},
		{"zerolog console ERR", "2026-10-10 21:38:09.886 ERR file.go:1 > boom", verdictError},
		{"ERR_ code is not a level", "connect failed: ERR_CONNECTION_REFUSED", verdictNone},
		{"bare WARNING", "WARNING: slow response", verdictWarn},
		{"error wins over warn when both present", "level=error level=warn both here", verdictError},
		{"lowercase error word alone is not a match", "this is not an error condition really", verdictNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cls.classify([]byte(tc.line))
			if got != tc.want {
				t.Errorf("classify(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestClassify_LiteralPrefilterNeverRejectsARealMatch(t *testing.T) {
	errorLines := []string{
		"level=error x", "level=fatal x", `"level":"error"`, `"level":"fatal"`,
		"xERRORx", "xFATALx", "panic: x", " ERR x", " FTL x", " PNC x",
	}
	for _, line := range errorLines {
		if !containsAny([]byte(line), errorLiterals) {
			t.Errorf("errorLiterals rejected %q, but DefaultErrorRegex would match it", line)
		}
	}
	warnLines := []string{
		"level=warn x", "level=warning x", `"level":"warn"`, `"level":"warning"`,
		"xWARNx", "xWARNINGx", " WRN x",
	}
	for _, line := range warnLines {
		if !containsAny([]byte(line), warnLiterals) {
			t.Errorf("warnLiterals rejected %q, but DefaultWarnRegex would match it", line)
		}
	}
}

func TestClassify_CustomRegexDisablesPrefilterButStaysCorrect(t *testing.T) {
	cls, err := newClassifier(`CUSTOM_ERR`, `CUSTOM_WARN`, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	if cls.prefilter {
		t.Fatal("expected prefilter disabled for a non-default regex pair")
	}
	if got := cls.classify([]byte("this line has CUSTOM_ERR in it")); got != verdictError {
		t.Errorf("got %v, want verdictError", got)
	}
	if got := cls.classify([]byte("this line has CUSTOM_WARN in it")); got != verdictWarn {
		t.Errorf("got %v, want verdictWarn", got)
	}
	if got := cls.classify([]byte("plain line")); got != verdictNone {
		t.Errorf("got %v, want verdictNone", got)
	}
}

func TestClassify_InvalidRegexErrors(t *testing.T) {
	if _, err := newClassifier(`(unclosed`, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields); err == nil {
		t.Fatal("expected error for invalid error regex")
	}
	if _, err := newClassifier(DefaultErrorRegex, `(unclosed`, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields); err == nil {
		t.Fatal("expected error for invalid warn regex")
	}
}
