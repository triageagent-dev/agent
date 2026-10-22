package main

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
)

// triageClient posts events to the triage events server. Auth is a Bearer
// token read from a file on every send: the pod's projected ServiceAccount
// token (audience triage-core), which the kubelet rotates. The token is never
// taken from the environment.
type triageClient struct {
	baseURL   string
	provider  string
	tokenFile string
	http      *http.Client
}

const (
	sendTimeout = 15 * time.Second
	maxAttempts = 3
)

// retryBackoff is the wait before the second attempt; the third waits twice
// as long. A variable so the tests do not sleep.
var retryBackoff = time.Second

func (c *triageClient) bearer() (string, error) {
	b, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return "", fmt.Errorf("read token: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// send delivers one event, retrying transient failures. A 4xx is permanent
// (a bad token or body fails the same way on every retry), so it is logged
// and swallowed, except 429 and 408: the core answers 429 when its queue is
// full, which passes. Those, 5xx and transport errors are retried and then
// returned, so the webhook answers 5xx and Alertmanager retries.
func (c *triageClient) send(ctx context.Context, ev triageEvent) error {
	body, err := json.Marshal(ev)
	if err != nil {
		log.Printf("drop: cannot marshal event source=%s type=%s: %v", ev.Source, ev.Type, err)
		return nil
	}
	url := fmt.Sprintf("%s/events/%s/", c.baseURL, c.provider)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt-1) * retryBackoff):
			}
		}
		status, respBody, err := c.post(ctx, url, body, ev.tenant)
		switch {
		case err != nil:
			lastErr = err
		case status >= 200 && status < 300:
			return nil
		case status == http.StatusTooManyRequests || status == http.StatusRequestTimeout:
			lastErr = fmt.Errorf("triage is busy (%d): %s", status, truncate(respBody, 300))
		case status >= 400 && status < 500:
			log.Printf("drop: triage rejected event source=%s type=%s status=%d body=%s", ev.Source, ev.Type, status, truncate(respBody, 300))
			return nil
		default:
			lastErr = fmt.Errorf("triage returned %d: %s", status, truncate(respBody, 300))
		}
		log.Printf("retry %d/%d: source=%s type=%s: %v", attempt, maxAttempts, ev.Source, ev.Type, lastErr)
	}
	return lastErr
}

func (c *triageClient) post(ctx context.Context, url string, body []byte, tenant string) (int, string, error) {
	token, err := c.bearer()
	if err != nil {
		return 0, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Tenant", tenant)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, string(b), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
