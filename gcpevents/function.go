// Package gcpevents is a Cloud Run function that bridges Google Cloud provider
// signals into the triage agent.
//
// Three Pub/Sub topics feed one deployed function:
//
//	cost-anomalies-alerts       Cloud Billing cost anomaly detection notifications
//	gcp-service-health-alerts   Personalized Service Health events, routed by a
//	                            Logging sink on servicehealth.googleapis.com/activity
//	monitoring-alerts           Cloud Monitoring alert policy incidents, delivered
//	                            by a Pub/Sub notification channel
//
// Each message is decoded, resolved against the GCP service catalog (see
// catalog_gen.go — generated from the same source as the triage `gcp` tenant's
// service graph, so both sides agree on what services/24E6-581D-38E5 means), and
// POSTed to the triage events server as an InfraEvent:
//
//	POST $TRIAGE_EVENTS_URL/events/gcp-cost/     {type,message,severity,source,raw}
//	Authorization: Bearer <token from Secret Manager>
//	X-Tenant: gcp
//
// `source` carries the resolved catalog slug, which the triage classifier turns
// into a service-graph node deterministically: events/gcp-cost/bigquery →
// normalizeEvents drops the provider segment → "bigquery" → graph lookup. Getting
// the slug right here is the whole job; nothing downstream re-guesses it.
//
// Ack semantics: a message we deliberately skip (unparseable, nothing to report)
// is ACKed — redelivering it would not help. Only a transient failure to reach
// triage returns an error, which nacks the message for redelivery when the trigger
// has "Retry on failure" enabled.
package gcpevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
)

func init() {
	functions.CloudEvent("HandleGCPEvent", HandleGCPEvent)
}

// pubsubMessagePublished is the CloudEvent payload Eventarc delivers for
// google.cloud.pubsub.topic.v1.messagePublished.
type pubsubMessagePublished struct {
	Message struct {
		Data        []byte            `json:"data"` // base64 in the wire JSON; []byte decodes it
		Attributes  map[string]string `json:"attributes"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// triageEvent is the events-server request body (internal/events.eventRequest).
type triageEvent struct {
	Type     string         `json:"type"`
	Message  string         `json:"message"`
	Severity string         `json:"severity,omitempty"`
	Source   string         `json:"source,omitempty"`
	Raw      map[string]any `json:"raw,omitempty"`

	// provider selects the events-server path segment; not part of the body.
	provider string `json:"-"`

	// tenant overrides the client's configured X-Tenant for this one event —
	// a per-caller alert is routed by the tenant its service account names
	// (see caller.go). Empty means "use TRIAGE_TENANT". Not part of the body.
	tenant string `json:"-"`
}

// Severity values the events server accepts. Anything else is a 400.
const (
	severityInfo     = "info"
	severityWarning  = "warning"
	severityCritical = "critical"
)

var (
	clientOnce sync.Once
	client     *triageClient
	clientErr  error
)

// HandleGCPEvent is the function entry point (--entry-point HandleGCPEvent).
func HandleGCPEvent(ctx context.Context, e event.Event) error {
	var msg pubsubMessagePublished
	if err := e.DataAs(&msg); err != nil {
		// Not a Pub/Sub CloudEvent, or a shape we cannot read. Redelivery will
		// produce the same bytes, so ack.
		log.Printf("skip: decode CloudEvent data: %v", err)
		return nil
	}

	topic := topicFromSource(e.Source())
	events, err := decode(topic, msg.Message.Data, msg.Message.Attributes)
	if err != nil {
		// Never log the payload, or any part of it: it can carry billing and
		// account data. The message id is enough to find it in Pub/Sub.
		log.Printf("skip: messageId=%s: decode: %v", msg.Message.MessageID, err)
		return nil
	}
	if len(events) == 0 {
		log.Printf("skip: topic=%q messageId=%s: nothing to report", topic, msg.Message.MessageID)
		return nil
	}

	clientOnce.Do(func() { client, clientErr = newTriageClient() })
	if clientErr != nil {
		return fmt.Errorf("triage client: %w", clientErr)
	}

	for _, ev := range events {
		if err := client.send(ctx, ev); err != nil {
			// Transient — nack so Pub/Sub redelivers (needs "Retry on failure").
			return fmt.Errorf("send %s/%s: %w", ev.provider, ev.Source, err)
		}
		log.Printf("sent: provider=%s source=%s type=%s severity=%s messageId=%s",
			ev.provider, ev.Source, ev.Type, ev.Severity, msg.Message.MessageID)
	}
	return nil
}

// decode routes a raw Pub/Sub payload to the right decoder.
//
// Routing prefers the topic name and falls back to sniffing the payload, so a
// topic renamed in the console does not silently stop producing events.
func decode(topic string, data []byte, attrs map[string]string) ([]triageEvent, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		// The decoder's own message can quote the offending byte of the payload,
		// so report only where it stopped.
		var syn *json.SyntaxError
		if errors.As(err, &syn) {
			return nil, fmt.Errorf("payload is not JSON (syntax error at offset %d)", syn.Offset)
		}
		return nil, errors.New("payload is not a JSON object")
	}

	switch {
	case matchesTopic(topic, monitoringTopicHints()):
		return decodeMonitoringAlert(payload)
	case obj(payload, "incident") != nil:
		// Shape wins over a topic name here: an "incident" object is unique to a
		// Cloud Monitoring notification, and only that decoder can parse one, so
		// a monitoring alert delivered to a topic that merely looks like the
		// cost topic (or an unrecognized one) still lands correctly rather than
		// failing in the cost decoder or, worse, in the default fallback below.
		return decodeMonitoringAlert(payload)
	case matchesTopic(topic, costTopicHints()):
		return decodeCostAnomaly(payload, attrs)
	case matchesTopic(topic, healthTopicHints()):
		return decodeServiceHealth(payload)
	case findEventLog(payload) != nil:
		return decodeServiceHealth(payload)
	default:
		// Cost anomalies are the other trigger; attempt them last so a genuinely
		// unrecognized payload surfaces as the cost decoder's error, which names
		// the fields it looked for.
		return decodeCostAnomaly(payload, attrs)
	}
}

// topicFromSource extracts the topic name from a Pub/Sub CloudEvent source
// ("//pubsub.googleapis.com/projects/PROJECT/topics/TOPIC").
func topicFromSource(source string) string {
	if i := strings.LastIndex(source, "/topics/"); i >= 0 {
		return source[i+len("/topics/"):]
	}
	if i := strings.LastIndex(source, "/"); i >= 0 {
		return source[i+1:]
	}
	return source
}

func matchesTopic(topic string, hints []string) bool {
	t := strings.ToLower(topic)
	for _, h := range hints {
		if h != "" && strings.Contains(t, h) {
			return true
		}
	}
	return false
}

// costTopicHints / healthTopicHints let a deployment with differently-named
// topics override routing without a code change.
func costTopicHints() []string {
	if v := os.Getenv("COST_TOPIC"); v != "" {
		return []string{strings.ToLower(v)}
	}
	return []string{"cost-anomal", "cost_anomal"}
}

func healthTopicHints() []string {
	if v := os.Getenv("HEALTH_TOPIC"); v != "" {
		return []string{strings.ToLower(v)}
	}
	return []string{"service-health", "service_health", "servicehealth"}
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
