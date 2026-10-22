package gcpevents

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"
)

func fakeRead(data string, err error, got *string) secretReader {
	return func(_ context.Context, name string) ([]byte, error) {
		if got != nil {
			*got = name
		}
		return []byte(data), err
	}
}

func TestLoadTokenFromSecretManager(t *testing.T) {
	const name = "projects/p/secrets/s/versions/latest"
	t.Setenv("TRIAGE_TOKEN_SECRET", name)
	t.Setenv("TRIAGE_TOKEN_FILE", "")
	t.Setenv("TRIAGE_TOKEN", "plain-env-must-be-ignored")
	var got string
	tok, err := loadToken(context.Background(), fakeRead("abc123\n \r\n", nil, &got))
	if err != nil || tok != "abc123" || got != name {
		t.Fatalf("tok=%q name=%q err=%v", tok, got, err)
	}
}

func TestLoadTokenSecretError(t *testing.T) {
	t.Setenv("TRIAGE_TOKEN_SECRET", "projects/p/secrets/s/versions/1")
	if _, err := loadToken(context.Background(), fakeRead("", errors.New("denied"), nil)); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadTokenFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("filetok \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRIAGE_TOKEN_SECRET", "")
	t.Setenv("TRIAGE_TOKEN_FILE", path)
	tok, err := loadToken(context.Background(), fakeRead("unused", nil, nil))
	if err != nil || tok != "filetok" {
		t.Fatalf("tok=%q err=%v", tok, err)
	}
}

func TestLoadTokenRefusesPlainEnv(t *testing.T) {
	t.Setenv("TRIAGE_TOKEN_SECRET", "")
	t.Setenv("TRIAGE_TOKEN_FILE", "")
	t.Setenv("TRIAGE_TOKEN", "plain")
	if _, err := loadToken(context.Background(), fakeRead("x", nil, nil)); err == nil {
		t.Fatal("expected refusal when neither source is set")
	}
}

func TestLoadTokenEmpty(t *testing.T) {
	t.Setenv("TRIAGE_TOKEN_SECRET", "projects/p/secrets/s/versions/1")
	if _, err := loadToken(context.Background(), fakeRead(" \n", nil, nil)); err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestDecodeFailureDoesNotLogPayload(t *testing.T) {
	const secret = "SENSITIVE-BILLING-PAYLOAD"
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	for _, data := range []string{secret, `{"` + secret + `":1}`} {
		buf.Reset()
		e := event.New()
		e.SetSource("//pubsub.googleapis.com/projects/p/topics/cost-anomalies-alerts")
		e.SetType("google.cloud.pubsub.topic.v1.messagePublished")
		e.SetID("1")
		body := `{"message":{"messageId":"m-42","data":"` + base64.StdEncoding.EncodeToString([]byte(data)) + `"}}`
		if err := e.SetData("application/json", []byte(body)); err != nil {
			t.Fatal(err)
		}
		if err := HandleGCPEvent(context.Background(), e); err != nil {
			t.Fatalf("decode failure must ack, got %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "m-42") {
			t.Fatalf("log lacks message id: %q", out)
		}
		if strings.Contains(out, secret) {
			t.Fatalf("payload leaked into log: %q", out)
		}
	}
}
