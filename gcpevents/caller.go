package gcpevents

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/oauth2/google"
)

// Per-caller alert policies group a service's own API traffic by the identity
// that made the call: metric serviceruntime.googleapis.com/api/request_count on
// monitored resource type "consumed_api", grouped by the resource label
// credential_id. That label is the only thing in the incident that names the
// caller, and it names it by id rather than by anything a human — or a service
// graph — would recognize:
//
//	serviceaccount:116448827429795893791            a service account's unique id
//	oauth2:32555940559.apps.googleusercontent.com   a human or tooling OAuth client
//
// A per-caller alert is not really about the API that was called — every tenant
// calls Vertex AI — but about who called it, so the alert is routed by the
// caller. The unique id is resolved through the IAM API to the service account,
// and the account's own description says where its events belong:
//
//	Per-tenant Vertex AI caller tenant=triage vapp=triageagent
//
// Routing from the account rather than from a service-graph alias keeps the
// decision with whoever provisioned the service account: a new tenant needs a
// description, not a code change or a graph edit here. An account that says
// nothing (or a caller we cannot resolve at all) is not a routing failure —
// the event simply falls back to the shape every other monitoring alert has,
// sourced at the API under the configured tenant, with the caller preserved in
// the message and in raw.
//
// See https://cloud.google.com/monitoring/api/resources#tag_consumed_api

const (
	// consumedAPIResourceType is the monitored resource type of a per-caller
	// alert; anything else keeps the ordinary API-derived source.
	consumedAPIResourceType = "consumed_api"

	callerKindServiceAccount = "serviceaccount"
	callerKindOAuth2         = "oauth2"
	callerKindUnknown        = "unknown"

	// oauth2CallerName is the name every OAuth client shares. A client id is
	// not a service, and nothing on it says where it belongs, so they all
	// collapse to one name rather than minting one per developer.
	oauth2CallerName = "oauth2-client"

	// tenantToken and vappToken are the routing keys read from a service
	// account's description. sourceToken is accepted as a synonym of vappToken,
	// because "source" is what the events path calls the same thing.
	tenantToken = "tenant"
	vappToken   = "vapp"
	sourceToken = "source"

	iamHost = "iam.googleapis.com"

	// serviceAccountLookupTimeout bounds one IAM lookup. The decode path has no
	// deadline of its own, and an unresolved caller is recoverable, so a slow
	// IAM must never hold up the event.
	serviceAccountLookupTimeout = 5 * time.Second
)

// alertCaller is the identity a per-caller alert fired for.
type alertCaller struct {
	name   string // the SA email local part, oauth2-client, or the raw id
	kind   string // serviceaccount | oauth2 | unknown
	raw    string // the credential_id label verbatim
	email  string // resolved service account email, empty when there is none
	tenant string // tenant= from the account's description, empty when absent
	vapp   string // vapp= (or source=) from the same, empty when absent
}

// serviceAccount is the part of the IAM API's ServiceAccount resource this
// package reads.
type serviceAccount struct {
	Email       string `json:"email"`
	DisplayName string `json:"displayName"`
	Description string `json:"description"`
	UniqueID    string `json:"uniqueId"`
}

// resolveCaller extracts the caller from a monitoring alert's monitored
// resource, reporting false when the alert is not a per-caller one.
//
// It is deliberately total: every failure below — an unreadable credential_id,
// an IAM lookup that errors, an account that carries no routing tokens —
// degrades to a caller with no tenant, which the decoder then treats as an
// ordinary alert on the API. A dropped alert would be the real regression.
func resolveCaller(resource map[string]any) (alertCaller, bool) {
	if str(resource, "type") != consumedAPIResourceType {
		return alertCaller{}, false
	}
	credentialID := str(obj(resource, "labels"), "credential_id", "credentialId")
	if credentialID == "" {
		return alertCaller{}, false
	}

	kind, id, found := strings.Cut(credentialID, ":")
	kind, id = strings.ToLower(strings.TrimSpace(kind)), strings.TrimSpace(id)
	if !found || id == "" {
		// A spelling we have not seen. Carry it through as its own name rather
		// than guessing at its shape.
		return alertCaller{name: credentialID, kind: callerKindUnknown, raw: credentialID}, true
	}

	caller := alertCaller{kind: kind, raw: credentialID}
	switch kind {
	case callerKindServiceAccount:
		caller.name = id
		account, err := resolveServiceAccount(context.Background(), id)
		if err != nil {
			log.Printf("caller: IAM lookup for service account %s: %v; routing the alert by its API instead", id, err)
			return caller, true
		}
		caller.email = account.Email
		if name := serviceAccountName(account.Email); name != "" {
			caller.name = name
		}
		tokens := callerTokens(account.Description, account.DisplayName)
		caller.tenant = tokens[tenantToken]
		caller.vapp = firstNonEmpty(tokens[vappToken], tokens[sourceToken])
	case callerKindOAuth2:
		caller.name = oauth2CallerName
	default:
		caller.name = id
		caller.kind = callerKindUnknown
	}
	return caller, true
}

// callerTokens pulls whitespace- or comma-separated key=value pairs out of the
// given fields, earlier fields winning: a service account's description is the
// intended place for them, its display name the fallback for accounts whose
// description is managed by something else.
//
// Everything that is not a key=value pair is prose and is ignored, so
// "Per-tenant Vertex AI caller tenant=triage vapp=triageagent" yields exactly
// {tenant: triage, vapp: triageagent}.
func callerTokens(fields ...string) map[string]string {
	tokens := map[string]string{}
	separator := func(r rune) bool { return unicode.IsSpace(r) || r == ',' || r == ';' }
	for _, field := range fields {
		for _, token := range strings.FieldsFunc(field, separator) {
			key, value, ok := strings.Cut(token, "=")
			if !ok {
				continue
			}
			key = strings.ToLower(strings.TrimSpace(key))
			value = strings.Trim(strings.TrimSpace(value), `"'`)
			if key == "" || value == "" {
				continue
			}
			if _, seen := tokens[key]; !seen {
				tokens[key] = value
			}
		}
	}
	return tokens
}

// serviceAccountName is the readable half of a service account email:
// triage-vertexai@project.iam.gserviceaccount.com -> triage-vertexai.
func serviceAccountName(email string) string {
	if local, _, found := strings.Cut(email, "@"); found && local != "" {
		return local
	}
	return email
}

// resolveServiceAccount reads a service account by its numeric unique id. A
// variable so tests can stub the lookup without credentials or a network.
var resolveServiceAccount = lookupServiceAccount

var (
	saByUniqueIDMu sync.Mutex
	saByUniqueID   = map[string]serviceAccount{}
)

// lookupServiceAccount reads the service account from the IAM API (projects/-
// searches every project the caller can see) using the function's own
// Application Default Credentials. The runtime service account needs
// roles/iam.serviceAccountViewer.
//
// Results are memoized for the life of the process: callers repeat across the
// alerts of a single policy, function instances are long-lived, and neither an
// account's email nor its description changes often enough to be worth a
// request per alert.
func lookupServiceAccount(ctx context.Context, uniqueID string) (serviceAccount, error) {
	saByUniqueIDMu.Lock()
	cached, ok := saByUniqueID[uniqueID]
	saByUniqueIDMu.Unlock()
	if ok {
		return cached, nil
	}

	ctx, cancel := context.WithTimeout(ctx, serviceAccountLookupTimeout)
	defer cancel()

	httpClient, err := google.DefaultClient(ctx, cloudPlatformScope)
	if err != nil {
		return serviceAccount{}, fmt.Errorf("default credentials: %w", err)
	}
	endpoint := fmt.Sprintf("https://%s/v1/projects/-/serviceAccounts/%s", iamHost, url.PathEscape(uniqueID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return serviceAccount{}, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return serviceAccount{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return serviceAccount{}, fmt.Errorf("iam returned %d: %s", resp.StatusCode, truncate(string(body), 300))
	}
	var account serviceAccount
	if err := json.Unmarshal(body, &account); err != nil {
		return serviceAccount{}, fmt.Errorf("decode IAM response: %w", err)
	}
	if account.Email == "" {
		return serviceAccount{}, fmt.Errorf("iam returned no email for unique id %s", uniqueID)
	}

	saByUniqueIDMu.Lock()
	saByUniqueID[uniqueID] = account
	saByUniqueIDMu.Unlock()
	return account, nil
}
