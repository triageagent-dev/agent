package gcpevents

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"golang.org/x/oauth2/google"
)

// Personalized Service Health is polled, not subscribed to.
//
// The Pub/Sub route (a log sink on servicehealth.googleapis.com/activity) is an
// EDGE stream: it says something changed. Answering "is Cloud Storage healthy
// right now" from edges means replaying history and hoping none were missed, and
// it fans out one log entry per affected project — hundreds of near-identical
// deliveries for a single Google incident across an org this size.
//
// The API is a LEVEL source: one call returns the complete set of currently
// active events, already deduplicated. A missed poll costs nothing because the
// next one carries the whole truth again. That is the same reasoning the triage
// side uses for incident state (ADR-032): level beats edges for anything that is
// really a question about the present.
//
// Triage itself never calls Google. It stays provider-agnostic and only receives
// events; everything GCP-shaped lives here.
//
// Each tick sends two things: one event per affected service (the narrative, which
// absorbs into incidents), and one readiness SNAPSHOT naming every currently
// degraded service (the level, which drives the graph's health). The snapshot is
// why recovery needs no explicit signal — a service that stops appearing is marked
// ready by triage — and why this function holds no state and scales to zero
// between ticks.

func init() {
	functions.HTTP("PollServiceHealth", PollServiceHealth)
}

const (
	serviceHealthHost  = "servicehealth.googleapis.com"
	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
	pollTimeout        = 60 * time.Second
	// maxRawLocations caps the impacted-location sample kept in the event's raw
	// payload; the true count is always reported alongside it.
	maxRawLocations = 12
)

// serviceHealthEvent is one event from the Service Health API.
//
// Note this is NOT the same shape as the Cloud Logging EventLog the Pub/Sub path
// carries: the API nests products under eventImpacts[].product.productName, where
// the log entry has a flat (and sometimes Python-rendered) impactedProducts list.
// decodeServiceHealth handles the log form; this handles the API form.
type serviceHealthEvent struct {
	Name          string `json:"name"`
	Title         string `json:"title"`
	Description   string `json:"description"`
	Category      string `json:"category"`
	State         string `json:"state"`
	DetailedState string `json:"detailedState"`
	Relevance     string `json:"relevance"`
	StartTime     string `json:"startTime"`
	UpdateTime    string `json:"updateTime"`
	EventImpacts  []struct {
		Product struct {
			ProductName string `json:"productName"`
			ID          string `json:"id"`
		} `json:"product"`
		Location struct {
			LocationName string `json:"locationName"`
		} `json:"location"`
	} `json:"eventImpacts"`
}

type serviceHealthList struct {
	Events []serviceHealthEvent `json:"events"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// PollServiceHealth is the scheduled entry point (--entry-point PollServiceHealth,
// --trigger-http, driven by Cloud Scheduler with an OIDC token). It lists every
// ACTIVE service health event in scope and posts one triage event per affected
// service.
func PollServiceHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), pollTimeout)
	defer cancel()

	events, err := listActiveServiceHealthEvents(ctx)
	if err != nil {
		log.Printf("poll: list service health events: %v", err)
		http.Error(w, "list service health events failed", http.StatusBadGateway)
		return
	}

	clientOnce.Do(func() { client, clientErr = newTriageClient() })
	if clientErr != nil {
		log.Printf("poll: triage client: %v", clientErr)
		http.Error(w, "triage client unavailable", http.StatusInternalServerError)
		return
	}

	var sent, failed int
	for _, ev := range events {
		for _, te := range eventsFromServiceHealthAPI(ev) {
			if err := client.send(ctx, te); err != nil {
				// One failure must not abort the sweep: the remaining services would
				// silently age out of degradation as if they had recovered.
				log.Printf("poll: send %s/%s: %v", te.provider, te.Source, err)
				failed++
				continue
			}
			sent++
		}
	}

	// The readiness snapshot is what turns the graph's dots green: it names every
	// currently degraded service, and triage marks the rest of the tenant ready.
	// Sent even when nothing is degraded — an empty snapshot is how a recovered
	// estate is declared healthy.
	snap := statusSnapshot{Reporter: env("STATUS_REPORTER", "gcp-servicehealth")}
	for _, ev := range events {
		for _, te := range eventsFromServiceHealthAPI(ev) {
			snap.Degraded = append(snap.Degraded, degradedService{
				Service: te.Source,
				Status:  statusReason(ev),
			})
		}
	}
	if err := client.sendStatus(ctx, snap); err != nil {
		// Never fatal: the events are already delivered, and the next poll carries
		// the whole truth again.
		log.Printf("poll: status snapshot: %v", err)
	}

	log.Printf("poll: %d active event(s), %d sent, %d failed, %d degraded service(s)", len(events), sent, failed, len(snap.Degraded))
	if failed > 0 {
		// Report the partial failure so Cloud Scheduler retries the tick.
		http.Error(w, fmt.Sprintf("%d of %d sends failed", failed, sent+failed), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, "ok: %d active event(s), %d event(s) sent\n", len(events), sent)
}

// listActiveServiceHealthEvents returns every currently ACTIVE event for the
// configured scope. SERVICE_HEALTH_ORG lists organization-wide (needs the
// organization to be enrolled); otherwise it lists for SERVICE_HEALTH_PROJECT,
// falling back to the project the function runs in.
func listActiveServiceHealthEvents(ctx context.Context) ([]serviceHealthEvent, error) {
	httpClient, err := google.DefaultClient(ctx, cloudPlatformScope)
	if err != nil {
		return nil, fmt.Errorf("default credentials: %w", err)
	}

	parent := env("SERVICE_HEALTH_PARENT", "")
	if parent == "" {
		if org := env("SERVICE_HEALTH_ORG", ""); org != "" {
			parent = "organizations/" + org
		} else {
			project := env("SERVICE_HEALTH_PROJECT", env("GCP_PROJECT", ""))
			if project == "" {
				return nil, fmt.Errorf("no scope configured: set SERVICE_HEALTH_PROJECT or SERVICE_HEALTH_ORG")
			}
			parent = "projects/" + project
		}
	}

	collection := "events"
	if strings.HasPrefix(parent, "organizations/") {
		collection = "organizationEvents"
	}

	var out []serviceHealthEvent
	pageToken := ""
	for {
		endpoint := fmt.Sprintf("https://%s/v1/%s/locations/global/%s", serviceHealthHost, parent, collection)
		q := url.Values{}
		q.Set("filter", `state="ACTIVE"`)
		q.Set("pageSize", "100")
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("service health API returned %d: %s", resp.StatusCode, truncate(string(body), 300))
		}

		var page struct {
			serviceHealthList
			NextPageToken string `json:"nextPageToken"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode service health response: %w", err)
		}
		out = append(out, page.Events...)
		if page.NextPageToken == "" {
			break
		}
		pageToken = page.NextPageToken
	}
	return out, nil
}

// eventsFromServiceHealthAPI turns one API event into one triage event per
// affected service, mirroring the Pub/Sub decoder's output so both paths produce
// identical events for the same incident.
func eventsFromServiceHealthAPI(ev serviceHealthEvent) []triageEvent {
	products := map[string]bool{}
	locations := map[string]bool{}
	for _, impact := range ev.EventImpacts {
		if name := strings.TrimSpace(impact.Product.ProductName); name != "" {
			products[name] = true
		}
		if loc := strings.TrimSpace(impact.Location.LocationName); loc != "" {
			locations[loc] = true
		}
	}

	productNames := sortedKeys(products)
	locationNames := sortedKeys(locations)

	title := firstNonEmpty(ev.Title, ev.Description, "Google Cloud service health event")
	relevance := strings.ToUpper(ev.Relevance)
	severity := healthSeverity(relevance, false) // only ACTIVE events are polled

	var b strings.Builder
	fmt.Fprintf(&b, "Google Cloud %s: %s", strings.ToLower(firstNonEmpty(ev.Category, "event")), title)
	if len(productNames) > 0 {
		fmt.Fprintf(&b, " — affects %s", strings.Join(productNames, ", "))
	}
	if n := len(locationNames); n > 0 {
		// A global incident can list hundreds of locations; name a few and count.
		shown := locationNames
		if n > 4 {
			shown = locationNames[:4]
		}
		fmt.Fprintf(&b, " in %s", strings.Join(shown, ", "))
		if n > len(shown) {
			fmt.Fprintf(&b, " +%d more", n-len(shown))
		}
	}
	if ev.DetailedState != "" {
		fmt.Fprintf(&b, " [%s]", ev.DetailedState)
	}
	if relevance != "" {
		fmt.Fprintf(&b, " (relevance: %s)", relevance)
	}
	message := b.String()

	raw := map[string]any{
		"provider":      "gcp",
		"signal":        "service_health",
		"discoveredVia": "api-poll",
		"eventName":     ev.Name,
		"state":         ev.State,
		"detailedState": ev.DetailedState,
		"category":      ev.Category,
		"relevance":     relevance,
		"startTime":     ev.StartTime,
		"updateTime":    ev.UpdateTime,
	}
	if len(productNames) > 0 {
		raw["impactedProducts"] = productNames
	}
	if len(locationNames) > 0 {
		// A platform-wide event lists every zone Google runs — the live Cloud
		// Logging incident carried 184. The full list re-lands on every poll and
		// ends up persisted in the event's memory cell, so keep a readable sample
		// and the true count rather than the wall.
		raw["impactedLocations"] = locationNames[:min(len(locationNames), maxRawLocations)]
		raw["impactedLocationCount"] = len(locationNames)
	}

	provider := env("HEALTH_PROVIDER", "gcp-status")
	out := make([]triageEvent, 0, len(productNames))
	for _, slug := range resolveHealthServices(productNames) {
		evRaw := make(map[string]any, len(raw)+2)
		for k, v := range raw {
			evRaw[k] = v
		}
		evRaw["service"] = slug
		if display := serviceDisplayBySlug[slug]; display != "" {
			evRaw["serviceDisplayName"] = display
		}
		out = append(out, triageEvent{
			provider: provider,
			Type:     "ProviderIncident",
			Severity: severity,
			Source:   slug,
			Message:  message,
			Raw:      evRaw,
		})
	}
	return out
}

// statusReason is the short line shown where an Application's componentsReady
// string would be, so the graph view says why a service is down.
func statusReason(ev serviceHealthEvent) string {
	title := firstNonEmpty(ev.Title, ev.Description, "Google Cloud incident")
	if len(title) > 90 {
		title = title[:90] + "…"
	}
	if ev.DetailedState != "" {
		return fmt.Sprintf("%s [%s]", title, ev.DetailedState)
	}
	return title
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
