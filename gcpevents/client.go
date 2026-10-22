package gcpevents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

// triageClient posts events to the triage agent's inbound events server
// (internal/events, :8086 behind the /events route). Auth is a Bearer token from
// the same triageagent-api-tokens Secret the rest of the estate uses; X-Tenant
// selects the service graph the event is classified against.
type triageClient struct {
	baseURL string
	token   string
	tenant  string
	http    *http.Client
}

const (
	sendTimeout = 15 * time.Second
	maxAttempts = 3
)

func newTriageClient() (*triageClient, error) {
	base := strings.TrimSuffix(env("TRIAGE_EVENTS_URL", ""), "/")
	if base == "" {
		return nil, fmt.Errorf("TRIAGE_EVENTS_URL is not set")
	}
	token, err := loadToken(context.Background(), accessSecretVersion)
	if err != nil {
		return nil, err
	}
	return &triageClient{
		baseURL: base,
		token:   token,
		tenant:  env("TRIAGE_TENANT", "gcp"),
		http:    &http.Client{Timeout: sendTimeout},
	}, nil
}

// secretReader reads one Secret Manager version by full resource name. It is the
// only call to GCP the token load makes, and tests replace it.
type secretReader func(ctx context.Context, name string) ([]byte, error)

// loadToken returns the core's Bearer token. It comes from Secret Manager when
// TRIAGE_TOKEN_SECRET (projects/<p>/secrets/<name>/versions/<v>) is set, read
// with the function's own service account, or else from the file named by
// TRIAGE_TOKEN_FILE (a mounted secret volume). A token in a plain environment
// variable is not supported: environment variables show up in the function's
// configuration and in process listings. Trailing whitespace is trimmed.
func loadToken(ctx context.Context, read secretReader) (string, error) {
	var raw []byte
	switch {
	case env("TRIAGE_TOKEN_SECRET", "") != "":
		name := env("TRIAGE_TOKEN_SECRET", "")
		b, err := read(ctx, name)
		if err != nil {
			return "", fmt.Errorf("read secret %s: %w", name, err)
		}
		raw = b
	case env("TRIAGE_TOKEN_FILE", "") != "":
		path := env("TRIAGE_TOKEN_FILE", "")
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read token file %s: %w", path, err)
		}
		raw = b
	default:
		return "", fmt.Errorf("neither TRIAGE_TOKEN_SECRET nor TRIAGE_TOKEN_FILE is set")
	}
	token := strings.TrimRight(string(raw), " \t\r\n")
	if token == "" {
		return "", fmt.Errorf("triage token is empty")
	}
	return token, nil
}

// accessSecretVersion is the production secretReader: the Secret Manager API
// with application default credentials, which on Cloud Run is the function's
// own service account.
func accessSecretVersion(ctx context.Context, name string) ([]byte, error) {
	c, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	resp, err := c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return resp.GetPayload().GetData(), nil
}

// send delivers one event, retrying transient failures.
//
// X-Tenant is the event's own tenant when it carries one — a per-caller alert
// is routed by the tenant its service account names, not by the function's
// configuration — and TRIAGE_TENANT otherwise.
//
// A 4xx is permanent — a bad token or a malformed body will fail identically on
// every redelivery — so it is logged and swallowed rather than nacking the
// message into a retry loop. 5xx and transport errors are returned so the caller
// can nack. An event we cannot even encode is permanent for the same reason, and
// is dropped on the same grounds: nacking it returns a 500 that has Pub/Sub
// redeliver a message no attempt can ever get past this line.
func (c *triageClient) send(ctx context.Context, ev triageEvent) error {
	tenant := c.tenant
	if ev.tenant != "" {
		tenant = ev.tenant
	}
	body, err := json.Marshal(ev)
	if err != nil {
		log.Printf("drop: cannot marshal event source=%s type=%s: %v", ev.Source, ev.Type, err)
		return nil
	}
	url := fmt.Sprintf("%s/events/%s/", c.baseURL, ev.provider)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			delay := time.Duration(attempt-1) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		status, respBody, err := c.post(ctx, url, body, tenant)
		switch {
		case err != nil:
			lastErr = err
		case status >= 200 && status < 300:
			return nil
		case status >= 400 && status < 500:
			log.Printf("drop: triage rejected event source=%s status=%d body=%s",
				ev.Source, status, truncate(respBody, 300))
			return nil
		default:
			lastErr = fmt.Errorf("triage returned %d: %s", status, truncate(respBody, 300))
		}
		log.Printf("retry %d/%d: source=%s: %v", attempt, maxAttempts, ev.Source, lastErr)
	}
	return lastErr
}

func (c *triageClient) post(ctx context.Context, url string, body []byte, tenant string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Tenant", tenant)

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, string(respBody), nil
}

// statusSnapshot is the body for POST /events/status/ — the readiness ingress for
// tenants whose services are not Kubernetes Applications.
//
// It is a snapshot, not a delta: "these services are degraded right now", and
// triage marks everything else in the tenant ready. That is what lets this
// function stay stateless — recovery needs no explicit signal, the service simply
// stops appearing.
type statusSnapshot struct {
	Reporter string            `json:"reporter"`
	Degraded []degradedService `json:"degraded"`
}

type degradedService struct {
	Service string `json:"service"`
	Status  string `json:"status,omitempty"`
}

// sendStatus posts a readiness snapshot. Failures are the caller's to log: a
// missed snapshot self-corrects on the next poll, so it must never abort a sweep
// that has already delivered its events.
func (c *triageClient) sendStatus(ctx context.Context, snap statusSnapshot) error {
	body, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal status snapshot: %w", err)
	}
	status, respBody, err := c.post(ctx, c.baseURL+"/events/status/", body, c.tenant)
	switch {
	case err != nil:
		return err
	case status >= 200 && status < 300:
		return nil
	default:
		return fmt.Errorf("triage returned %d: %s", status, truncate(respBody, 300))
	}
}
