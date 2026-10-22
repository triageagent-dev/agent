package gcpevents

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

// The fixture is a verbatim capture of the live Service Health API (project
// example-project-0000, 2026-07-26), trimmed only in the number of impacted
// locations per event. It pins the API's actual shape — eventImpacts[].product
// .productName — which differs from the Cloud Logging EventLog the Pub/Sub path
// carries, and it exercises the exact product names Google emits.
func loadAPIEvents(t *testing.T) []serviceHealthEvent {
	t.Helper()
	raw, err := os.ReadFile("testdata/servicehealth_api.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var list serviceHealthList
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(list.Events) == 0 {
		t.Fatal("fixture has no events")
	}
	return list.Events
}

func TestEventsFromServiceHealthAPI(t *testing.T) {
	var sources []string
	bySource := map[string]triageEvent{}
	for _, ev := range loadAPIEvents(t) {
		for _, te := range eventsFromServiceHealthAPI(ev) {
			sources = append(sources, te.Source)
			bySource[te.Source] = te
		}
	}
	sort.Strings(sources)

	// Every product in the live feed must resolve to a real catalog node. The
	// networking one is the regression that motivated the alias work: Google
	// reports "Virtual Private Cloud (VPC)" where billing says "Networking", and
	// before the aliases landed it fell through to the platform catch-all.
	want := []string{"cloud-logging", "cloud-storage", "networking"}
	if len(sources) != len(want) {
		t.Fatalf("sources = %v, want %v", sources, want)
	}
	for i := range want {
		if sources[i] != want[i] {
			t.Errorf("sources = %v, want %v", sources, want)
			break
		}
	}

	for src, te := range bySource {
		if te.Type != "ProviderIncident" {
			t.Errorf("%s: type = %q, want ProviderIncident", src, te.Type)
		}
		if te.provider != "gcp-status" {
			t.Errorf("%s: provider = %q, want gcp-status", src, te.provider)
		}
		if te.Raw["discoveredVia"] != "api-poll" {
			t.Errorf("%s: raw.discoveredVia = %v, want api-poll", src, te.Raw["discoveredVia"])
		}
		if te.Raw["service"] != src {
			t.Errorf("%s: raw.service = %v", src, te.Raw["service"])
		}
		// Only ACTIVE events are polled, so nothing here may look resolved.
		if te.Raw["state"] != "ACTIVE" {
			t.Errorf("%s: raw.state = %v, want ACTIVE", src, te.Raw["state"])
		}
		if te.Message == "" || !strings.HasPrefix(te.Message, "Google Cloud ") {
			t.Errorf("%s: message = %q", src, te.Message)
		}
	}

	// Relevance drives severity: this feed carries one RELATED (Cloud Storage) and
	// two PARTIALLY_RELATED. Confirmed impact would be critical, which is what
	// promotes an event to an incident on the triage side.
	if got := bySource["cloud-storage"].Severity; got != severityWarning {
		t.Errorf("cloud-storage severity = %q, want warning (RELATED)", got)
	}
	for _, src := range []string{"cloud-logging", "networking"} {
		if got := bySource[src].Severity; got != severityInfo {
			t.Errorf("%s severity = %q, want info (PARTIALLY_RELATED)", src, got)
		}
	}
}

// A global incident can list hundreds of impacted locations. The message names a
// few and counts the rest rather than rendering an unreadable wall.
func TestEventsFromServiceHealthAPI_TruncatesLocations(t *testing.T) {
	ev := serviceHealthEvent{
		Title: "Widespread disruption", Category: "INCIDENT", State: "ACTIVE",
		Relevance: "IMPACTED", DetailedState: "CONFIRMED",
	}
	for _, loc := range []string{"a1", "b2", "c3", "d4", "e5", "f6", "g7"} {
		impact := struct {
			Product struct {
				ProductName string `json:"productName"`
				ID          string `json:"id"`
			} `json:"product"`
			Location struct {
				LocationName string `json:"locationName"`
			} `json:"location"`
		}{}
		impact.Product.ProductName = "BigQuery"
		impact.Location.LocationName = loc
		ev.EventImpacts = append(ev.EventImpacts, impact)
	}

	out := eventsFromServiceHealthAPI(ev)
	if len(out) != 1 || out[0].Source != "bigquery" {
		t.Fatalf("got %+v, want one bigquery event", out)
	}
	if !strings.Contains(out[0].Message, "+3 more") {
		t.Errorf("message did not truncate locations: %q", out[0].Message)
	}
	// IMPACTED means Google confirmed impact on our resources — that is the one
	// relevance that should open an incident downstream.
	if out[0].Severity != severityCritical {
		t.Errorf("severity = %q, want critical for IMPACTED", out[0].Severity)
	}
	if got := out[0].Raw["impactedLocationCount"]; got != 7 {
		t.Errorf("raw.impactedLocationCount = %v, want 7", got)
	}

	// A platform-wide event lists every zone Google runs; raw keeps a bounded
	// sample so the persisted memory cell does not carry the whole wall.
	if locs, _ := out[0].Raw["impactedLocations"].([]string); len(locs) > maxRawLocations {
		t.Errorf("raw.impactedLocations kept %d entries, want at most %d", len(locs), maxRawLocations)
	}
}

// An event with no product attributed must still surface, on the platform
// catch-all, rather than vanishing.
func TestEventsFromServiceHealthAPI_NoProducts(t *testing.T) {
	out := eventsFromServiceHealthAPI(serviceHealthEvent{
		Title: "Something broad", State: "ACTIVE", Relevance: "RELATED",
	})
	if len(out) != 1 || out[0].Source != catchAllHealthSlug {
		t.Fatalf("got %+v, want a single %s event", out, catchAllHealthSlug)
	}
}
