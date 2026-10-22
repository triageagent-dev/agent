package logexporter

import "testing"

// Well-known logging libraries and what their lines look like, one sample per
// output format, with the verdict each must get.
var libraryLines = []struct {
	name string
	line string
	want verdict
}{
	{"zerolog console warn as written, level coloured", "\x1b[90m2026-10-10 22:23:39.902\x1b[0m \x1b[33mWRN\x1b[0m \x1b[1mgithub.com/x/y.go:323\x1b[0m\x1b[36m >\x1b[0m \x1b[1mprediction rank: decision model failed\x1b[0m\x1b[36m error=\x1b[0m\"decisions: HTTP 404\"", verdictWarn},
	{"zerolog console error as written, level coloured", "\x1b[90m2026-10-10 22:23:39.902\x1b[0m \x1b[31mERR\x1b[0m \x1b[1mmain.go:1\x1b[0m > boom", verdictError},
	{"coloured info stays quiet", "\x1b[90m2026-10-10 22:23:39.902\x1b[0m \x1b[32mINF\x1b[0m main.go:1 > fine", verdictNone},
	{"zerolog console error", "2026-10-10 21:38:09.886 ERR main.go:12 > boom", verdictError},
	{"zerolog console warn", "\x1b[90m2026-10-10 21:38:09.886\x1b[0m WRN main.go:12 > slow", verdictWarn},
	{"zerolog console fatal", "2026-10-10 21:38:09.886 FTL main.go:12 > dead", verdictError},
	{"zerolog console panic", "2026-10-10 21:38:09.886 PNC main.go:12 > dead", verdictError},
	{"zerolog console info", "2026-10-10 21:38:09.886 INF main.go:12 > fine", verdictNone},
	{"zerolog json", `{"level":"error","time":"x","message":"boom"}`, verdictError},
	{"zap console error", "2026-10-10T21:38:09.886Z\tERROR\tmain.go:12\tboom", verdictError},
	{"zap console dpanic", "2026-10-10T21:38:09.886Z\tDPANIC\tmain.go:12\tboom", verdictError},
	{"zap console warn", "2026-10-10T21:38:09.886Z\tWARN\tmain.go:12\tslow", verdictWarn},
	{"zap json upper", `{"level":"ERROR","ts":1,"msg":"boom"}`, verdictError},
	{"zap json warn", `{"level":"warn","ts":1,"msg":"slow"}`, verdictWarn},
	{"json with spaces", `{"level": "error", "msg": "boom"}`, verdictError},
	{"json warning title", `{"level": "Warning", "msg": "slow"}`, verdictWarn},
	{"json critical", `{"severity":"CRITICAL","message":"x"}`, verdictError},
	{"stackdriver warning", `{"severity":"WARNING","message":"x"}`, verdictWarn},
	{"stackdriver info", `{"severity":"INFO","message":"x"}`, verdictNone},
	{"python json logger", `{"levelname": "ERROR", "message": "x"}`, verdictError},
	{"ecs log.level", `{"log.level":"warn","message":"x"}`, verdictWarn},
	{"pino error", `{"level":50,"msg":"x"}`, verdictError},
	{"pino spaced fatal", `{"level": 60,"msg":"x"}`, verdictError},
	{"pino warn", `{"level":40,"msg":"x"}`, verdictWarn},
	{"pino info", `{"level":30,"msg":"x"}`, verdictNone},
	{"logrus text", `time="2026-10-10T21:38:09Z" level=error msg="boom"`, verdictError},
	{"logrus text warning", `time="2026-10-10T21:38:09Z" level=warning msg="slow"`, verdictWarn},
	{"logrus panic", `time="2026-10-10T21:38:09Z" level=panic msg="boom"`, verdictError},
	{"slog text", `time=2026-10-10T21:38:09.886Z level=ERROR msg=boom`, verdictError},
	{"slog text warn", `time=2026-10-10T21:38:09.886Z level=WARN msg=slow`, verdictWarn},
	{"go-kit eror", `ts=2026-10-10T21:38:09Z lvl=eror msg=boom`, verdictError},
	{"go-kit crit", `lvl=crit msg=boom`, verdictError},
	{"go-kit info", `lvl=info msg=fine`, verdictNone},
	{"klog error", `E1010 21:38:09.886123       1 main.go:12] boom`, verdictError},
	{"klog fatal", `F1010 21:38:09.886123       1 main.go:12] boom`, verdictError},
	{"klog warning", `W1010 21:38:09.886123       1 main.go:12] slow`, verdictWarn},
	{"klog info", `I1010 21:38:09.886123       1 main.go:12] fine`, verdictNone},
	{"serilog error", `[21:38:09 ERR] boom`, verdictError},
	{"serilog warn", `[21:38:09 WRN] slow`, verdictWarn},
	{"dotnet fail", "fail: Microsoft.Hosting.Lifetime[0]", verdictError},
	{"dotnet warn", "warn: Microsoft.Hosting.Lifetime[0]", verdictWarn},
	{"dotnet info", "info: Microsoft.Hosting.Lifetime[0]", verdictNone},
	{"nginx error", `2026/10/10 21:38:09 [error] 29#29: *1 connect() failed`, verdictError},
	{"nginx crit", `2026/10/10 21:38:09 [crit] 29#29: *1 SSL_do_handshake() failed`, verdictError},
	{"nginx warn", `2026/10/10 21:38:09 [warn] 29#29: *1 an upstream response is buffered`, verdictWarn},
	{"envoy warning", `[2026-10-10 21:38:09.886][1][warning][config] gRPC config stream closed`, verdictWarn},
	{"envoy error", `[2026-10-10 21:38:09.886][1][error][main] boom`, verdictError},
	{"python logging", "2026-10-10 21:38:09,886 CRITICAL app: boom", verdictError},
	{"python warning", "2026-10-10 21:38:09,886 WARNING app: slow", verdictWarn},
	{"java util logging", "SEVERE: boom", verdictError},
	{"log4j", "2026-10-10 21:38:09,886 ERROR [main] App - boom", verdictError},
	{"error code not a level", "connect failed: ERR_CONNECTION_REFUSED", verdictNone},
	{"prose", "the errors were few and the warnings fewer", verdictNone},
}

func TestClassify_WellKnownLibraries(t *testing.T) {
	cls, err := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range libraryLines {
		t.Run(tc.name, func(t *testing.T) {
			if got := cls.classify([]byte(tc.line)); got != tc.want {
				t.Errorf("classify(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

// The prefilter is a speedup only: with it off, every line must get the same
// verdict, or a spelling the regex accepts is being rejected before it runs.
func TestClassify_PrefilterAgreesWithRegexOnlyForLibraries(t *testing.T) {
	fast, _ := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	slow, _ := newClassifier(DefaultErrorRegex, DefaultWarnRegex, DefaultSampleMaxLen, DefaultMaxSamplesPerService, DefaultSampleJSONFields)
	slow.prefilter = false
	for _, tc := range libraryLines {
		l := []byte(tc.line)
		if f, s := fast.classify(l), slow.classify(l); f != s && !(s == verdictNone && klogLevel(l) != verdictNone) {
			t.Errorf("%q: prefilter says %v, regex alone says %v", tc.line, f, s)
		}
	}
}
