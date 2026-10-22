// Virtual apps (docs/todo/virtual-apps.md): label-derived Application synthesis.
//
// A CR-less cluster has no app-level readiness — onboarding a tenant today means
// authoring Application manifests. The push agent already watches every workload;
// this file lets it synthesize Application objects itself, in memory only, from
// Deployment/StatefulSet/DaemonSet state grouped by label — no manifests, no CRs.
//
// Synthesis never writes to the cluster. It only ever produces *VirtualApp values
// held in this process and (mode "on") pushed over the existing WebSocket — the
// wire format is the one push/protocol.go carries in both SnapshotPayload and
// AppDeltaPayload.
package push

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// Virtual-app modes for --virtual-apps. "off" (default) does nothing at all —
// no extra informers, no CPU spent grouping. "on" synthesizes every group and
// merges the set into the snapshot, emitting app_delta as groups change.
const (
	VirtualAppsOff = "off"
	VirtualAppsOn  = "on"
)

// ValidateVirtualAppsMode rejects anything other than the two defined modes (or
// empty, which New treats as VirtualAppsOff).
func ValidateVirtualAppsMode(mode string) error {
	switch mode {
	case "", VirtualAppsOff, VirtualAppsOn:
		return nil
	default:
		return fmt.Errorf("--virtual-apps must be one of off|on, got %q", mode)
	}
}

// Label keys grouping/version-resolution read, and the marker label stamped on
// every synthesized Application (docs/todo/virtual-apps.md).
const (
	labelPartOf    = "app.kubernetes.io/part-of"
	labelInstance  = "app.kubernetes.io/instance"
	labelName      = "app.kubernetes.io/name"
	labelVersion   = "app.kubernetes.io/version"
	labelManagedBy = "app.kubernetes.io/managed-by"

	managedByVirtualApps = "virtual-apps"
)

// Per-workload annotation keys (docs/todo/virtual-apps.md annotations
// addendum), read from the WORKLOAD OBJECT's own metadata.annotations,
// deliberately not the pod template's, since these control how the workload
// object itself participates in grouping/synthesis, not anything about the
// pods it manages. Each key's name is declared once, in
// internal/annotationkeys; every reader and writer uses that constant
// instead of a literal of its own.
//
// annotationkeys.VirtualApp, when exactly "false", excludes the workload
// from grouping/synthesis entirely. Any other value (including "true") or
// absence keeps it in: this is an opt-out, not a gate.
//
// annotationkeys.VirtualAppGroup overrides the workload's GROUP MEMBERSHIP,
// not its display name: it outranks part-of/instance/name, the highest
// grouping priority, so an owner can split a release-aggregate (e.g.
// instance=prod-ecosystem) into per-service vapps without touching labels.
// Same value means same vapp (namespace-scoped): two otherwise-unrelated
// workloads sharing one annotation value join ONE group. This is also how a
// shared resource joins several apps' vapps: a neo4j Deployment used by
// several charts in one namespace, each annotated virtual-app-group: neo4j,
// folds into one shared neo4j vapp instead of each chart's own
// release-scoped group claiming it. GroupNameFor's callers outside this
// package (e.g. the USE-metrics collector resolving an owning workload's
// identity) resolve this same annotation off the workload object they
// fetch, keyed by annotationkeys.VirtualAppGroup directly, so the key has
// exactly one defined location.
//
// annotationkeys.Domain, annotationkeys.Description, and annotationkeys.Tier
// are the three metadata annotations Phase 2 adds (docs/todo/virtual-apps.md
// annotations addendum): service domain, human description, and a
// criticality tier. Read from the same place as the two control annotations
// above (the workload object's own metadata.annotations, never the pod
// template) and carried onto the synthesized Application: the
// metadata.annotations copies are the wire contract the triage hub reads.
//
// annotationkeys.Tier's value is an integer 0-9 as a string, parsed
// strictly (strconv.Atoi + range check). Lower is more critical. Missing or
// unparsable is treated identically as "not declared", the difference only
// matters for the once-at-V(1) log line a caller emits on the unparsable
// case.
//
// annotationkeys.Weight is the operator's blast-radius coefficient
// (docs/todo/service-weight.md, triageagent-dev/core repo), a float multiplier in
// [0.1, 10] as a string, parsed strictly (strconv.ParseFloat + finite +
// range check). Unlike tier, an annotated weight OVERRIDES a catalog value
// on the hub side rather than only filling a gap: that precedence lives
// entirely in the hub's graph enrichment, not here. This side just carries
// the declared value onto the synthesized Application, same as
// domain/description/tier.
const (
	// tierMin and tierMax bound a valid tier value.
	tierMin = 0
	tierMax = 9

	// weightMin and weightMax bound a valid weight value
	// (docs/todo/service-weight.md §1's decided range).
	weightMin = 0.1
	weightMax = 10
)

// virtualAppsDebounce coalesces workload churn (a rollout's pod-template update
// touches the Deployment once but ready-replica counts tick several times) before
// recomputing groups and emitting deltas. Compiled, not a flag/chart value —
// docs/todo/virtual-apps.md specifies "~2s coalesce" as a fixed behavior.
const virtualAppsDebounce = 2 * time.Second

// workloadKind holds a workload's discovered Kind (resolveWorkloadResources)
// — any namespaced kind named by --workload-resources, not a fixed enum.
type workloadKind string

// workloadInfo is the generic per-workload shape grouping/synthesis operates on,
// extracted once from a Deployment/StatefulSet/DaemonSet so nothing downstream of
// this ever branches on concrete type again.
type workloadInfo struct {
	Kind      workloadKind
	Namespace string
	Name      string
	Labels    map[string]string
	Ready     bool
	// Replicas and ReadyReplicas are the workload's desired and ready pod
	// counts, both 0 when the workload carries no replica count (a kind
	// judged by its conditions alone). They travel on the component so the
	// core can tell one replica of several down from all of them.
	Replicas      int64
	ReadyReplicas int64
	// ImageTag is the first container's image tag in the workload's pod template
	// ("" if there are no containers or the image carries no ":tag").
	ImageTag string
	// Excluded mirrors the triageagent.dev/virtual-app annotation being exactly "false" —
	// the workload never participates in grouping/synthesis when true
	// (docs/todo/virtual-apps.md annotations addendum).
	Excluded bool
	// ScaledToZero is true when the workload's desired replica count is 0.
	// workloadFromUnstructured also sets Excluded for it; applyWorkload clears
	// that again for a workload this process has seen running (see
	// virtualAppsState.seenRunning), so it stays in its app as not ready.
	ScaledToZero bool
	// annotationExcluded is Excluded as set by the annotation alone, before the
	// scaled-to-zero exclusion, so applyWorkload can tell the two apart.
	annotationExcluded bool
	// TriageExcluded mirrors annotationkeys.Triage being exactly "false" on
	// the workload's own metadata.annotations or on its pod template
	// (exclusion.go). It implies Excluded, and additionally drops every
	// signal about the workload, not just its virtual app.
	TriageExcluded bool
	// GroupOverride mirrors the triageagent.dev/virtual-app-group annotation, when set.
	// It is the highest-priority GROUP MEMBERSHIP key — see groupNameFor — not
	// a display name: same value on multiple workloads means one shared vapp.
	GroupOverride string
	// Domain mirrors the triageagent.dev/domain annotation ("" when absent).
	Domain string
	// Description mirrors the triageagent.dev/description annotation ("" when absent).
	Description string
	// Tier mirrors the triageagent.dev/tier annotation's parsed value. Only meaningful
	// when HasTier is true — 0 is itself a valid (the most critical) tier
	// value, so a plain zero can't stand in for "not declared".
	Tier int
	// HasTier is true when the workload declared a strictly-parsed, in-range
	// (0-9) triageagent.dev/tier value.
	HasTier bool
	// TierInvalid is true when triageagent.dev/tier was present but failed strict
	// parsing (non-integer, or outside [0,9]). The workload is otherwise
	// treated exactly as if the annotation were absent (HasTier stays
	// false) — this field exists only so the informer callback can log the
	// bad value once at V(1) (docs/todo/virtual-apps.md annotations Phase 2
	// addendum).
	TierInvalid bool
	// Weight mirrors the triageagent.dev/weight annotation's parsed value
	// (docs/todo/service-weight.md). Only meaningful when HasWeight is
	// true.
	Weight float64
	// HasWeight is true when the workload declared a strictly-parsed,
	// finite, in-range ([0.1, 10]) triageagent.dev/weight value.
	HasWeight bool
	// WeightInvalid is true when triageagent.dev/weight was present but failed
	// strict parsing (unparsable, non-finite, or outside [0.1, 10]). The
	// workload is otherwise treated exactly as if the annotation were
	// absent (HasWeight stays false) — this field exists only so the
	// informer callback can log the bad value once at V(1), mirroring
	// TierInvalid.
	WeightInvalid bool
	// PodLabels is the pod TEMPLATE's own labels
	// (spec.template.metadata.labels), distinct from Labels (the workload
	// OBJECT's own labels, above): a Service's selector is matched against a
	// workload's pod template, never the workload object itself, so the
	// Service-to-workload join (WorkloadForSelector, below) reads this field
	// while grouping/synthesis keeps reading Labels — even though the two
	// label sets are identical on most charts.
	PodLabels map[string]string
	// IdentityMode mirrors the owning NAMESPACE's triageagent.dev/virtual-app-identity
	// annotation (IdentityModeLabels/IdentityModeWorkload, above) — there is
	// no per-workload counterpart, so this is always the zero value (which
	// GroupNameFor resolves identically to IdentityModeWorkload, the
	// default) until applyNamespaceDefaultsToWorkload overlays the
	// namespace's value (namespace_defaults.go).
	IdentityMode string
}

// workloadAnnotations extracts the virtual-apps control + metadata annotations
// from a workload object's own metadata.annotations (never the pod template's).
func workloadAnnotations(annotations map[string]string) (excluded bool, groupOverride, domain, description string, tier int, hasTier, tierInvalid bool, weight float64, hasWeight, weightInvalid bool) {
	// annotationkeys.Triage "false" takes the workload out of triage
	// entirely (exclusion.go), which includes virtual-app synthesis.
	excluded = annotations[annotationkeys.VirtualApp] == "false" || TriageExcluded(annotations)
	groupOverride = annotations[annotationkeys.VirtualAppGroup]
	domain = annotations[annotationkeys.Domain]
	description = annotations[annotationkeys.Description]
	if raw, present := annotations[annotationkeys.Tier]; present {
		if v, err := strconv.Atoi(raw); err == nil && v >= tierMin && v <= tierMax {
			tier, hasTier = v, true
		} else {
			tierInvalid = true
		}
	}
	if raw, present := annotations[annotationkeys.Weight]; present {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) && v >= weightMin && v <= weightMax {
			weight, hasWeight = v, true
		} else {
			weightInvalid = true
		}
	}
	return excluded, groupOverride, domain, description, tier, hasTier, tierInvalid, weight, hasWeight, weightInvalid
}

// workloadKey uniquely identifies one workload within the flat set virtualAppsState
// tracks across all namespaces.
func workloadKey(kind workloadKind, namespace, name string) string {
	return namespace + "/" + string(kind) + "/" + name
}

// groupKey identifies a synthesized (or real) Application by namespace+name.
type groupKey struct {
	Namespace string
	Name      string
}

func (k groupKey) String() string { return k.Namespace + "/" + k.Name }

// IdentityMode values for WorkloadIdentity.Mode / namespaceDefaults.IdentityMode
// / workloadInfo.IdentityMode (triageagent.dev/virtual-app-identity, a NAMESPACE-only
// annotation — docs/todo/virtual-apps.md virtual-app-identity addendum).
// IdentityModeWorkload names a workload's virtual app after the workload's
// own name, ignoring labels entirely — this is the default: the zero value
// ("", an unannotated namespace) resolves identically to the explicit
// "workload" string, since GroupNameFor's only mode check is against
// IdentityModeLabels. IdentityModeLabels opts a namespace INTO the
// part-of/instance/name chain instead. Either way, GroupOverride
// (triageagent.dev/virtual-app-group) still outranks both — it is a higher-priority,
// explicit per-workload instruction, never a mode.
const (
	IdentityModeWorkload = "workload"
	IdentityModeLabels   = "labels"
)

// WorkloadIdentity is the minimal input GroupNameFor needs to resolve a
// workload's identity: its own labels, its annotationkeys.VirtualAppGroup
// annotation value when set ("" otherwise), its own name, and the identity
// mode its NAMESPACE declares (Mode, never a per-workload value). Every
// other workloadInfo field (Ready, ImageTag, Domain, ...) is synthesis-only
// and irrelevant to identity, so it stays out of this exported shape.
type WorkloadIdentity struct {
	Labels        map[string]string
	GroupOverride string
	Name          string
	// Mode selects IdentityModeWorkload (the zero value and the default) or
	// IdentityModeLabels.
	Mode string
}

// GroupNameFor resolves the identity a workload belongs to — the SAME chain
// that names every synthesized Application, and therefore every node in the
// core's service graph. The triageagent.dev/virtual-app-group annotation, when set,
// wins outright — highest priority, above the mode and every label — and is
// a GROUP MEMBERSHIP key, not a display name: same value on multiple
// workloads in a namespace means one shared vapp (e.g. a neo4j Deployment
// several charts depend on, each annotated triageagent.dev/virtual-app-group: neo4j,
// folds into one vapp). Absent that: the default (Mode is "" or explicitly
// IdentityModeWorkload) resolves straight to the workload's own name,
// ignoring labels entirely; only IdentityModeLabels opts into label
// priority: part-of → instance → name, falling back to the workload's own
// name when none of those are set (docs/todo/virtual-apps.md).
//
// Exported so every pipeline that must agree with virtual-app synthesis on a
// workload's identity — e.g. the USE-metrics collector resolving a pod's
// owning workload — calls this exact function instead of re-deriving
// the precedence: there must be exactly one implementation of this chain.
func GroupNameFor(w WorkloadIdentity) string {
	if v := strings.TrimSpace(w.GroupOverride); v != "" {
		return v
	}
	if w.Mode == IdentityModeLabels {
		for _, key := range []string{labelPartOf, labelInstance, labelName} {
			if v := strings.TrimSpace(w.Labels[key]); v != "" {
				return v
			}
		}
	}
	return w.Name
}

// groupNameFor adapts a full workloadInfo down to GroupNameFor's minimal
// input — the internal synthesis path's only caller of the chain above.
func groupNameFor(w workloadInfo) string {
	return GroupNameFor(WorkloadIdentity{Labels: w.Labels, GroupOverride: w.GroupOverride, Name: w.Name, Mode: w.IdentityMode})
}

// groupWorkloads groups workloads per namespace by groupNameFor, with a
// deterministic (Kind, Name) member order within each group. A workload with
// triageagent.dev/virtual-app: "false" is dropped here — excluded from grouping and
// synthesis entirely, so it can never count toward any group's members or
// readiness (docs/todo/virtual-apps.md annotations addendum).
func groupWorkloads(workloads []workloadInfo) map[groupKey][]workloadInfo {
	groups := make(map[groupKey][]workloadInfo)
	for _, w := range workloads {
		if w.Excluded {
			continue
		}
		key := groupKey{Namespace: w.Namespace, Name: groupNameFor(w)}
		groups[key] = append(groups[key], w)
	}
	for k := range groups {
		members := groups[k]
		sort.Slice(members, func(i, j int) bool {
			if members[i].Kind != members[j].Kind {
				return members[i].Kind < members[j].Kind
			}
			return members[i].Name < members[j].Name
		})
	}
	return groups
}

// --- Readiness (docs/todo/virtual-apps.md) ---
//
// A synthesized Application tracks no stabilization window, so readiness uses
// the plain "fully rolled out" definition: every member's desired count met
// by its ready count. This is also generic across kinds, so it cannot key off
// observedGeneration/generation (not every configured kind sets them
// meaningfully) — readiness is judged from replica/condition fields alone.

// workloadDesiredReady reads a workload's desired and ready replica counts
// generically, from whichever fields it actually carries:
//
//	desired: spec.replicas, else status.desiredNumberScheduled (DaemonSet's
//	  own field name — kept as a fallback rather than a kind check, so any
//	  other kind using the same convention is covered too), else 1 (a kind
//	  with no scaling concept at all).
//	ready: status.readyReplicas, else status.numberReady, else
//	  status.availableReplicas, else 1 if status.conditions[] carries a
//	  Ready or Available condition with status True, else 0.
//
// counted is true when ready came from a replica count field rather than a
// condition or the default: only then are the two numbers pod counts the
// core can read as replicas (a partial loss, 1 of 2 ready, against a full
// one, 0 of 2).
func workloadDesiredReady(obj map[string]any) (desired, ready int64, counted bool) {
	desired = 1
	if v, ok, _ := unstructured.NestedInt64(obj, "spec", "replicas"); ok {
		desired = v
	} else if v, ok, _ := unstructured.NestedInt64(obj, "status", "desiredNumberScheduled"); ok {
		desired = v
	}

	switch {
	case setInt64(&ready, obj, "status", "readyReplicas"),
		setInt64(&ready, obj, "status", "numberReady"),
		setInt64(&ready, obj, "status", "availableReplicas"):
		counted = true
	case workloadConditionTrue(obj, "Ready"), workloadConditionTrue(obj, "Available"):
		ready = 1
	case hasReplicaCounts(obj):
		// A scaling kind whose status omits a zero readyReplicas (Argo
		// Rollouts, Deployments before their first ready pod) still
		// counts: none of its replicas is ready.
		counted = true
	}
	return desired, ready, counted
}

// hasReplicaCounts reports whether obj's status carries any replica count
// at all (status.replicas or status.currentNumberScheduled), so an absent
// ready count reads as 0 ready rather than unknown.
func hasReplicaCounts(obj map[string]any) bool {
	var v int64
	return setInt64(&v, obj, "status", "replicas") || setInt64(&v, obj, "status", "currentNumberScheduled")
}

// setInt64 reads obj's nested int64 field into *dst and reports whether it
// was present — a small adapter so workloadDesiredReady's ready-field
// priority chain reads as a switch instead of a nested if/else ladder.
func setInt64(dst *int64, obj map[string]any, fields ...string) bool {
	v, ok, _ := unstructured.NestedInt64(obj, fields...)
	if ok {
		*dst = v
	}
	return ok
}

// workloadConditionTrue reports whether obj's status.conditions[] carries an
// entry of the given type with status "True".
func workloadConditionTrue(obj map[string]any, condType string) bool {
	conds, found, _ := unstructured.NestedSlice(obj, "status", "conditions")
	if !found {
		return false
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if t, _, _ := unstructured.NestedString(m, "type"); t != condType {
			continue
		}
		if s, _, _ := unstructured.NestedString(m, "status"); s == "True" {
			return true
		}
	}
	return false
}

// imageTag returns the tag portion of an image reference. A digest suffix
// ("@sha256:...") is dropped first — a bare digest reference carries no tag at
// all — then any registry host (which may itself contain ':' for a port) is
// stripped before looking for the tag separator, so "" comes back for a
// digest-pinned or bare-name image with no ":tag".
func imageTag(image string) string {
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	tagPart := image
	if i := strings.LastIndex(image, "/"); i >= 0 {
		tagPart = image[i+1:]
	}
	if i := strings.LastIndex(tagPart, ":"); i >= 0 {
		return tagPart[i+1:]
	}
	return ""
}

// firstContainerImageTagUnstructured returns imageTag() of
// spec.template.spec.containers[0].image, "" when there is no such
// container or field.
func firstContainerImageTagUnstructured(obj map[string]any) string {
	containers, found, err := unstructured.NestedSlice(obj, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		return ""
	}
	c, ok := containers[0].(map[string]any)
	if !ok {
		return ""
	}
	image, _, _ := unstructured.NestedString(c, "image")
	return imageTag(image)
}

// podTemplateLabels returns spec.template.metadata.labels, nil when absent —
// the PodLabels field every configured kind is expected to carry the same
// way (Deployment/StatefulSet/DaemonSet/Rollout all nest a pod template at
// this exact path).
func podTemplateLabels(obj map[string]any) map[string]string {
	labels, found, err := unstructured.NestedStringMap(obj, "spec", "template", "metadata", "labels")
	if err != nil || !found {
		return nil
	}
	return labels
}

// workloadFromUnstructured builds a workloadInfo from any configured,
// discovery-confirmed workload resource's object — the ONE adapter every
// discovery path (manager-cache informers, per-namespace runtime informers,
// backfill listers) converts through, in place of one adapter per kind. kind
// is the resource's discovered Kind (resolveWorkloadResources), passed by
// the caller rather than read off obj — a List response's items are not
// guaranteed to carry TypeMeta.
//
// A workload whose desired count resolves to 0 (scaled to zero) is marked
// Excluded — the same mechanism triageagent.dev/virtual-app: "false" uses — so it
// drops out of grouping/synthesis entirely instead of surfacing as a
// spuriously Ready 1/1 vapp. applyWorkload lifts that exclusion for a workload
// it has already seen running, so a real scale-down reads as down, not removed.
func workloadFromUnstructured(obj *unstructured.Unstructured, kind string) workloadInfo {
	excluded, groupOverride, domain, description, tier, hasTier, tierInvalid, weight, hasWeight, weightInvalid := workloadAnnotations(obj.GetAnnotations())
	triageExcluded := objectTriageExcluded(obj)
	excluded = excluded || triageExcluded
	desired, ready, counted := workloadDesiredReady(obj.Object)
	annotationExcluded := excluded
	// Replica counts go on the wire only when they are pod counts (counted)
	// and something is desired. Ready is capped at desired so a surge
	// (3 ready of 2 during a rollout) is not a change the core sees.
	var replicas, readyReplicas int64
	if counted && desired > 0 {
		replicas, readyReplicas = desired, min(ready, desired)
	}
	if desired == 0 {
		excluded = true
	}
	return workloadInfo{
		Kind:               workloadKind(kind),
		Namespace:          obj.GetNamespace(),
		Name:               obj.GetName(),
		Labels:             obj.GetLabels(),
		PodLabels:          podTemplateLabels(obj.Object),
		Ready:              desired > 0 && ready >= desired,
		Replicas:           replicas,
		ReadyReplicas:      readyReplicas,
		ImageTag:           firstContainerImageTagUnstructured(obj.Object),
		Excluded:           excluded,
		ScaledToZero:       desired == 0,
		annotationExcluded: annotationExcluded,
		TriageExcluded:     triageExcluded,
		GroupOverride:      groupOverride,
		Domain:             domain,
		Description:        description,
		Tier:               tier,
		HasTier:            hasTier,
		TierInvalid:        tierInvalid,
		Weight:             weight,
		HasWeight:          hasWeight,
		WeightInvalid:      weightInvalid,
	}
}

// --- Version resolution (docs/todo/virtual-apps.md) ---

// uniformLabelValue returns a label's value when every member carries it and every
// member agrees on the value.
func uniformLabelValue(members []workloadInfo, key string) (string, bool) {
	if len(members) == 0 {
		return "", false
	}
	val, ok := members[0].Labels[key]
	if !ok || val == "" {
		return "", false
	}
	for _, m := range members[1:] {
		v, ok := m.Labels[key]
		if !ok || v != val {
			return "", false
		}
	}
	return val, true
}

// uniformImageTag returns the first container's image tag when every member
// resolves the same non-empty tag.
func uniformImageTag(members []workloadInfo) (string, bool) {
	if len(members) == 0 || members[0].ImageTag == "" {
		return "", false
	}
	tag := members[0].ImageTag
	for _, m := range members[1:] {
		if m.ImageTag != tag {
			return "", false
		}
	}
	return tag, true
}

// resolveGroupVersion: uniform app.kubernetes.io/version label, else uniform first-
// container image tag, else "".
func resolveGroupVersion(members []workloadInfo) string {
	if v, ok := uniformLabelValue(members, labelVersion); ok {
		return v
	}
	if v, ok := uniformImageTag(members); ok {
		return v
	}
	return ""
}

// --- Metadata annotation aggregation (docs/todo/virtual-apps.md annotations
// Phase 2 addendum) ---

// uniformWorkloadString mirrors uniformLabelValue's uniform-or-absent rule,
// generalized to any per-workload string getter rather than a label lookup —
// domain/description come off workloadInfo fields (annotations), not labels.
func uniformWorkloadString(members []workloadInfo, get func(workloadInfo) string) (string, bool) {
	if len(members) == 0 {
		return "", false
	}
	val := get(members[0])
	if val == "" {
		return "", false
	}
	for _, m := range members[1:] {
		if get(m) != val {
			return "", false
		}
	}
	return val, true
}

// resolveGroupDomain: uniform triageagent.dev/domain across every member, else "".
func resolveGroupDomain(members []workloadInfo) string {
	v, _ := uniformWorkloadString(members, func(w workloadInfo) string { return w.Domain })
	return v
}

// resolveGroupDescription: uniform triageagent.dev/description across every member,
// else "".
func resolveGroupDescription(members []workloadInfo) string {
	v, _ := uniformWorkloadString(members, func(w workloadInfo) string { return w.Description })
	return v
}

// resolveGroupTier returns the MINIMUM triageagent.dev/tier declared by any member
// that declares one — lower is more critical, so the most critical member
// speaks for the whole group. ok is false when no member declares a tier at
// all (absent, not a fabricated 0).
func resolveGroupTier(members []workloadInfo) (tier int, ok bool) {
	for _, m := range members {
		if !m.HasTier {
			continue
		}
		if !ok || m.Tier < tier {
			tier, ok = m.Tier, true
		}
	}
	return tier, ok
}

// resolveGroupWeight returns the MAXIMUM triageagent.dev/weight declared by any
// member that declares one (docs/todo/service-weight.md §4's reducer,
// mirrored at the group level) — the most critical member's correction
// speaks for the whole group, the same "most critical wins" rule
// resolveGroupTier applies at the opposite end of its scale. ok is false
// when no member declares a weight at all.
func resolveGroupWeight(members []workloadInfo) (weight float64, ok bool) {
	for _, m := range members {
		if !m.HasWeight {
			continue
		}
		if !ok || m.Weight > weight {
			weight, ok = m.Weight, true
		}
	}
	return weight, ok
}

// --- Synthesis ---

// synthesizeApplication builds the in-memory VirtualApp for one group. Never
// persisted — this value only ever lives in this process and (mode "on") on the
// wire.
func synthesizeApplication(key groupKey, members []workloadInfo) *VirtualApp {
	readyCount := 0
	objects := make([]ObjectStatus, 0, len(members))
	var zeroMsgs []string
	for _, m := range members {
		status := "InProgress"
		if m.Ready {
			status = "Ready"
			readyCount++
		} else if m.ScaledToZero {
			zeroMsgs = append(zeroMsgs, fmt.Sprintf("%s/%s: scaled to zero (0 desired replicas)", m.Kind, m.Name))
		}
		objects = append(objects, ObjectStatus{
			Kind:          string(m.Kind),
			Name:          m.Name,
			Status:        status,
			Replicas:      m.Replicas,
			ReadyReplicas: m.ReadyReplicas,
		})
	}
	condStatus := corev1.ConditionFalse
	if len(members) > 0 && readyCount == len(members) {
		condStatus = corev1.ConditionTrue
	}
	cond := Condition{Type: conditionReady, Status: condStatus}
	if len(zeroMsgs) > 0 {
		cond.Reason = "ScaledToZero"
		cond.Message = strings.Join(zeroMsgs, "; ")
	}

	// Metadata annotations (docs/todo/virtual-apps.md annotations Phase 2
	// addendum), added to the map only when the group actually resolves a
	// value, so a group with no annotationkeys metadata at all produces none
	// on the synthesized Application (absent, not "").
	annotations := map[string]string{}
	description := resolveGroupDescription(members)
	if domain := resolveGroupDomain(members); domain != "" {
		annotations[annotationkeys.Domain] = domain
	}
	if description != "" {
		annotations[annotationkeys.Description] = description
	}
	if tier, ok := resolveGroupTier(members); ok {
		annotations[annotationkeys.Tier] = strconv.Itoa(tier)
	}
	if weight, ok := resolveGroupWeight(members); ok {
		annotations[annotationkeys.Weight] = strconv.FormatFloat(weight, 'f', -1, 64)
	}
	if len(annotations) == 0 {
		annotations = nil
	}

	return &VirtualApp{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
			Labels: map[string]string{
				labelName:      key.Name,
				labelManagedBy: managedByVirtualApps,
			},
			Annotations: annotations,
		},
		Spec: VirtualAppSpec{
			Descriptor: Descriptor{
				Version:     resolveGroupVersion(members),
				Description: description,
			},
		},
		Status: VirtualAppStatus{
			ComponentsReady: fmt.Sprintf("%d/%d", readyCount, len(members)),
			Conditions:      []Condition{cond},
			Objects:         objects,
		},
	}
}

// buildSynthesized synthesizes every group into a VirtualApp.
func buildSynthesized(groups map[groupKey][]workloadInfo) map[groupKey]*VirtualApp {
	synthesized := make(map[groupKey]*VirtualApp, len(groups))
	for key, members := range groups {
		synthesized[key] = synthesizeApplication(key, members)
	}
	return synthesized
}

// --- Engine wiring (informers, debounce, on-mode push) ---

// virtualAppsState owns the flat workload set + the last-computed synthesized
// Application set, and drives grouping/synthesis/push off the workload
// informers. One instance per Pusher; nil when Options.VirtualApps is off
// (the zero value, so every call site can be a plain nil check).
type virtualAppsState struct {
	mode       string
	cluster    string
	namespaces []string
	log        logr.Logger

	// enqueue hands a frame to the Pusher's send channel (mode "on" only); a
	// no-op when disconnected, mirroring every other Send* method on Pusher.
	enqueue func(*Frame)
	// nsDefaultsFn looks up a namespace's triageagent.dev/* defaults + vapp-synthesis
	// opt-out (docs/todo/virtual-apps.md namespace-inheritance addendum).
	// nil (the zero value — no constructor param, set directly like
	// listAppsFn/skipHandlers elsewhere) means "no namespace store wired",
	// so workloadsSnapshot's overlay is a plain no-op and every existing
	// caller/test is unaffected. Bound by Pusher.New to nsDefaults.get.
	nsDefaultsFn func(namespace string) (namespaceDefaults, bool)
	// dynamicScope is the live, triageagent.dev/tenant-annotation-derived scope set
	// consulted by inScope alongside the static namespaces list
	// (namespace_scope.go). nil (no constructor param, set directly like
	// nsDefaultsFn) means "no dynamic scope wired" — inScope falls back to
	// static-list-only behavior, unchanged from before this feature. Shared
	// with the owning Pusher's own dynamicScope (same pointer, wired in
	// Pusher.New), never a separate instance.
	dynamicScope *dynamicScopeSet

	// cacheNamespaces mirrors Options.CacheNamespaces (namespace_scope.go's
	// cacheCovers) — the namespaces the manager cache actually covers. nil/
	// empty means a cluster-wide cache, so cacheCovers always reports true and
	// dynInformers below is never consulted; this is what keeps every existing
	// cluster-wide deployment byte-for-byte unaffected by this field's
	// existence. Set directly by Pusher.New, like nsDefaultsFn/dynamicScope
	// above.
	cacheNamespaces []string
	// dynInformers is the fallback for a namespace that entered DYNAMIC scope
	// (namespace_scope.go) but is NOT covered by cacheNamespaces: a runtime,
	// per-namespace client-go informer factory (dynamic_informers.go), since
	// controller-runtime cannot add a namespace to an already namespace-scoped
	// manager cache at runtime. OWNED by the Pusher (Pusher.dynInformers) and
	// shared onto this field by Pusher.New. Always non-nil once wired by
	// Pusher.New (even with a nil clientset — see dynamicInformerManager's doc
	// comment); nil only for a bare struct literal built directly by a test
	// that predates this feature, in which every cacheCovers-gated call site
	// behaves exactly as it did before (manager-cache-only).
	dynInformers *dynamicInformerManager

	// workloadResources is the configured, discovery-confirmed list of
	// namespaced GVRs generic workload discovery watches (docs/todo/
	// virtual-apps.md "Workload resources") — resolved once, at construction,
	// by resolveWorkloadResources, and shared with dynInformers so both the
	// manager-cache path (registerInformers) and the out-of-cache runtime
	// factory path (dynamic_informers.go) watch exactly the same set. Empty
	// means nothing is watched — the pre-vapps-existing "vapps entirely off"
	// shape, and also what a bare struct literal in an older test gets.
	workloadResources []workloadResource

	// listWorkloadsFn sources every configured workload resource already
	// resident in the informer cache for ONE namespace — used only by
	// backfillNamespace (namespace-scope-by-annotation addendum).
	// Overridable in tests to avoid a real cache; nil defaults to listing off
	// the `cache` field below (wired by registerInformers) or dynInformers,
	// exactly like listCachedWorkloads.
	listWorkloadsFn func(ctx context.Context, namespace string) ([]rawWorkload, error)

	mu        sync.Mutex
	workloads map[string]workloadInfo // workloadKey() -> info, flat across all namespaces
	// seenRunning holds the workloadKey() of every workload this process has
	// observed with desired replicas > 0. A seen workload that later reaches 0
	// stays in its app (not ready) instead of being excluded; one first seen at
	// 0 stays excluded. Guarded by mu; entries are dropped on delete.
	seenRunning map[string]bool
	// clusterMode mirrors Pusher.clusterMode() — no primary tenant, so an
	// unannotated namespace is dropped rather than defaulted and need not be
	// watched. Set by Pusher.New; false in the pre-existing tests, which is the
	// legacy watch-all they were written against.
	clusterMode bool
	// allNamespaces puts every namespace in scope: a NoKubernetes Pusher with
	// an app source and no static list (docs/adr/0021-otlp-only-no-kubernetes-
	// watch.md), where there is no annotation to build a scope from.
	allNamespaces bool

	current map[groupKey]*VirtualApp // last computed synthesized set
	timer   *time.Timer
	ctx     context.Context // set once by registerInformers; used by the debounced recompute
	cache   cache.Cache     // set once by registerInformers; backs the default list*Fn implementations

	// identityByOwner and workloadsByNamespace back IdentityFor/
	// WorkloadForSelector (docs/todo/service-graph-from-obi.md) — rebuilt
	// once per recompute() from the SAME workload snapshot grouping already
	// reads, never a separate cache list. identityByOwner is keyed
	// "namespace/name" (a workload's own name, independent of Kind); a
	// namespace/name pair shared by two DIFFERENT kinds (e.g. a Deployment
	// and a StatefulSet with the same name) keeps whichever was observed
	// first in that recompute's snapshot and logs the collision once via
	// loggedIdentityCollision.
	identityByOwner      map[string]string
	workloadsByNamespace map[string][]workloadInfo
	// excludedNames is the per-namespace set of service names that belong
	// only to annotationkeys.Triage-excluded workloads (exclusion.go's
	// triageExcludedNames), rebuilt with the identity index.
	excludedNames map[string]map[string]bool
	// loggedSelectorAmbiguity/loggedIdentityCollision dedupe their
	// respective once-per-key log lines across recomputes (a live cluster's
	// selector/name collisions don't change tick to tick, so logging on
	// every recompute would repeat indefinitely).
	loggedSelectorAmbiguity map[string]bool
	loggedIdentityCollision map[string]bool
}

func newVirtualAppsState(mode, cluster string, namespaces []string, log logr.Logger,
	enqueue func(*Frame)) *virtualAppsState {
	return &virtualAppsState{
		mode:        mode,
		cluster:     cluster,
		namespaces:  namespaces,
		log:         log,
		enqueue:     enqueue,
		workloads:   make(map[string]workloadInfo),
		seenRunning: make(map[string]bool),
		current:     make(map[groupKey]*VirtualApp),
	}
}

// inScope applies the same scope-union rule as Pusher.inScope
// (namespace_scope.go): v.namespaces (static) unioned with v.dynamicScope
// (live), where an empty static list means watch-all with a primary tenant and
// the annotated set alone in cluster mode.
func (v *virtualAppsState) inScope(ns string) bool {
	if v.allNamespaces {
		return true
	}
	return effectiveInScopeMode(ns, v.namespaces, v.dynamicScope, v.clusterMode)
}

// cacheCovers reports whether the manager cache covers ns, per
// namespace_scope.go's cacheCovers. Used to route backfillNamespace and the
// default listCached* implementations to the manager cache (covered) or the
// runtime per-namespace informer factory (not covered, dynInformers).
func (v *virtualAppsState) cacheCovers(ns string) bool {
	return cacheCovers(ns, v.cacheNamespaces)
}

// snapshotApps returns the currently-synthesized Applications, for merging
// into the snapshot in mode "on".
func (v *virtualAppsState) snapshotApps() []*VirtualApp {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*VirtualApp, 0, len(v.current))
	for _, a := range v.current {
		out = append(out, a)
	}
	return out
}

// workloadsSnapshot returns every known workload with namespace-level
// triageagent.dev/* defaults already folded in (docs/todo/virtual-apps.md
// namespace-inheritance addendum, bullet 2) — applied here, once, so
// recompute's grouping/synthesis sees already-inherited values with no
// separate overlay step of its own.
func (v *virtualAppsState) workloadsSnapshot() []workloadInfo {
	v.mu.Lock()
	nsFn := v.nsDefaultsFn
	out := make([]workloadInfo, 0, len(v.workloads))
	for _, w := range v.workloads {
		out = append(out, w)
	}
	v.mu.Unlock()
	if nsFn == nil {
		return out
	}
	for i := range out {
		if ns, ok := nsFn(out[i].Namespace); ok {
			out[i] = applyNamespaceDefaultsToWorkload(out[i], ns)
		}
	}
	return out
}

// applyWorkload upserts or removes one workload from the flat set and schedules a
// debounced recompute. Exported to the package (not just registerInformers) so
// tests can drive it directly without a real cache/informer.
func (v *virtualAppsState) applyWorkload(key string, info *workloadInfo, deleted bool) {
	v.mu.Lock()
	if deleted || info == nil {
		delete(v.workloads, key)
		delete(v.seenRunning, key)
	} else {
		w := *info
		if v.seenRunning == nil {
			v.seenRunning = make(map[string]bool)
		}
		if !w.ScaledToZero {
			v.seenRunning[key] = true
		} else if v.seenRunning[key] && !w.annotationExcluded {
			w.Excluded = false
		}
		v.workloads[key] = w
	}
	v.scheduleRecomputeLocked()
	v.mu.Unlock()
}

// scheduleRecomputeLocked (re)arms the debounce timer that fires recompute
// after virtualAppsDebounce. Caller must hold v.mu.
func (v *virtualAppsState) scheduleRecomputeLocked() {
	if v.timer != nil {
		v.timer.Stop()
	}
	v.timer = time.AfterFunc(virtualAppsDebounce, func() { v.recompute() })
}

// scheduleRecompute is the external trigger a namespace annotation change
// uses (Pusher.onNamespace, docs/todo/virtual-apps.md namespace-inheritance
// addendum bullet 4): every currently-known workload may need its inherited
// Domain/Description/Tier/Excluded re-evaluated, so this re-arms the SAME
// debounce timer applyWorkload uses — a burst of workload churn and
// namespace annotation changes together still coalesces into one recompute.
func (v *virtualAppsState) scheduleRecompute() {
	v.mu.Lock()
	v.scheduleRecomputeLocked()
	v.mu.Unlock()
}

// recomputeNow cancels any pending debounced recompute (armed by applyWorkload)
// and runs recompute synchronously instead of waiting for it to fire.
//
// Without this, a workload that was already steady when the process started (no
// further informer events ever arrive for it) only ever gets synthesized once the
// initial virtualAppsDebounce timer fires — but Pusher.sendSnapshot runs well
// before that on first connect (and on every reconnect), so it read v.current
// while still empty and sent "apps=0". Once the debounce did fire, emitDeltas'
// OpAdd frame was silently dropped by Pusher.enqueue if the connection wasn't
// fully up yet, and since v.current had already latched, a workload that never
// changes again would never re-announce. Pusher.sendSnapshot calls this
// immediately before snapshotApps() so the snapshot it sends is always
// up to date, on first connect and every reconnect alike.
//
// Emitting deltas here (mode "on") is harmless even though a snapshot follows
// right after: the hub applies a snapshot then deltas idempotently, so a
// redundant OpAdd for a group the snapshot already carries is a no-op on the
// receiving end.
func (v *virtualAppsState) recomputeNow(ctx context.Context) {
	v.mu.Lock()
	if v.timer != nil {
		v.timer.Stop()
		v.timer = nil
	}
	v.mu.Unlock()
	v.recompute()
}

// recompute regroups every known workload, synthesizes every group, and
// (mode "on") diffs against the previous set to emit app_delta frames.
func (v *virtualAppsState) recompute() {
	workloads := v.workloadsSnapshot()
	v.rebuildIdentityIndex(workloads)
	groups := groupWorkloads(workloads)
	synthesized := buildSynthesized(groups)

	v.mu.Lock()
	old := v.current
	v.current = synthesized
	v.mu.Unlock()

	if v.mode == VirtualAppsOn {
		v.emitDeltas(old, synthesized)
	}
}

// emitDeltas diffs the previous and new synthesized sets and enqueues app_delta
// add/update/delete frames for every group that appeared, changed, or vanished.
// Each enqueued delta is also logged at V(1)/debug — an open follow-up
// (docs/todo/virtual-apps.md) so an operator can grep cluster-a logs for exactly
// what left this process on the wire.
func (v *virtualAppsState) emitDeltas(old, updated map[groupKey]*VirtualApp) {
	for key, app := range updated {
		if oldApp, ok := old[key]; !ok {
			v.log.V(1).Info("virtual-apps delta", "op", OpAdd, "namespace", key.Namespace, "name", key.Name)
			v.enqueue(newAppDelta(v.cluster, OpAdd, app))
		} else if !reflect.DeepEqual(oldApp, app) {
			v.log.V(1).Info("virtual-apps delta", "op", OpUpdate, "namespace", key.Namespace, "name", key.Name)
			v.enqueue(newAppDelta(v.cluster, OpUpdate, app))
		}
	}
	for key, app := range old {
		if _, ok := updated[key]; !ok {
			v.log.V(1).Info("virtual-apps delta", "op", OpDelete, "namespace", key.Namespace, "name", key.Name)
			v.enqueue(newAppDelta(v.cluster, OpDelete, app))
		}
	}
}

// --- Identity index (docs/todo/service-graph-from-obi.md) ---
//
// IdentityFor/WorkloadForSelector let redsource's k8sResolver name a
// topology edge's endpoints by virtual-app identity instead of by raw k8s
// object name — the whole point of building the service graph from OBI
// rather than from a hub-side declared graph. Both read an index rebuilt
// once per recompute(), never a live cache lookup: a resolver call happens
// on the redsource collector's own collection goroutine, which must never block
// on (or race) the informer-driven workload cache.

// rebuildIdentityIndex recomputes identityByOwner/workloadsByNamespace from
// the SAME workload snapshot recompute() already read this pass. Called
// with v.mu unlocked (workloadsSnapshot has already released it); takes the
// lock itself only to publish the new indexes.
func (v *virtualAppsState) rebuildIdentityIndex(workloads []workloadInfo) {
	byOwner := make(map[string]string, len(workloads))
	byNamespace := make(map[string][]workloadInfo, len(workloads))
	seen := make(map[string]bool, len(workloads))
	for _, w := range workloads {
		key := w.Namespace + "/" + w.Name
		identity := groupNameFor(w)
		if seen[key] {
			if byOwner[key] != identity {
				v.warnIdentityCollisionOnce(w.Namespace, w.Name)
			}
		} else {
			byOwner[key] = identity
			seen[key] = true
		}
		byNamespace[w.Namespace] = append(byNamespace[w.Namespace], w)
	}
	excludedNames := triageExcludedNames(workloads)
	v.mu.Lock()
	v.identityByOwner = byOwner
	v.workloadsByNamespace = byNamespace
	v.excludedNames = excludedNames
	v.mu.Unlock()
}

// identityFor resolves a known workload's owner name (a Deployment/
// StatefulSet/DaemonSet's own name, independent of kind) to the virtual-app
// identity it groups into — the SAME chain (GroupNameFor) that names its
// synthesized Application. ok is false when the owner is not a currently
// tracked workload (including: virtual-apps has not completed its first
// recompute yet).
func (v *virtualAppsState) identityFor(namespace, ownerName string) (string, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	identity, ok := v.identityByOwner[namespace+"/"+ownerName]
	return identity, ok
}

// workloadForSelector returns the workload in namespace whose pod-template
// labels (PodLabels) contain every (key, value) pair in selector — the
// Service-to-workload join a Service object's selector alone cannot answer
// (the Service names its selector, not which Deployment/StatefulSet/
// DaemonSet owns the matching pods). Deterministic when several workloads
// match: a workload whose PodLabels are EXACTLY selector (no extra labels)
// wins outright; otherwise the lexically smallest name wins. Logs once per
// (namespace, selector) the first time more than one workload matches, not
// on every subsequent lookup.
func (v *virtualAppsState) workloadForSelector(namespace string, selector map[string]string) (string, bool) {
	if len(selector) == 0 {
		return "", false
	}
	v.mu.Lock()
	candidates := v.workloadsByNamespace[namespace]
	v.mu.Unlock()

	var matches []workloadInfo
	for _, w := range candidates {
		if labelsContainAll(w.PodLabels, selector) {
			matches = append(matches, w)
		}
	}
	if len(matches) == 0 {
		return "", false
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Name < matches[j].Name })
	for _, m := range matches {
		if len(m.PodLabels) == len(selector) {
			if len(matches) > 1 {
				v.warnSelectorAmbiguityOnce(namespace, selector, matches)
			}
			return m.Name, true
		}
	}
	if len(matches) > 1 {
		v.warnSelectorAmbiguityOnce(namespace, selector, matches)
	}
	return matches[0].Name, true
}

// labelsContainAll reports whether labels carries every (key, value) pair in
// selector. A nil/empty selector never matches — an empty Service selector
// (a headless/manual-endpoints Service) has no pods to join to.
func labelsContainAll(labels, selector map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, val := range selector {
		if labels[k] != val {
			return false
		}
	}
	return true
}

// selectorLogKey renders namespace + selector as a stable, sorted string —
// the dedupe key for warnSelectorAmbiguityOnce.
func selectorLogKey(namespace string, selector map[string]string) string {
	keys := make([]string, 0, len(selector))
	for k := range selector {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(namespace)
	for _, k := range keys {
		b.WriteByte('/')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(selector[k])
	}
	return b.String()
}

func (v *virtualAppsState) warnSelectorAmbiguityOnce(namespace string, selector map[string]string, matches []workloadInfo) {
	key := selectorLogKey(namespace, selector)
	v.mu.Lock()
	if v.loggedSelectorAmbiguity == nil {
		v.loggedSelectorAmbiguity = make(map[string]bool)
	}
	already := v.loggedSelectorAmbiguity[key]
	v.loggedSelectorAmbiguity[key] = true
	v.mu.Unlock()
	if already {
		return
	}
	names := make([]string, len(matches))
	for i, m := range matches {
		names[i] = m.Name
	}
	v.log.Info("virtual-apps identity index: several workloads match one Service selector; picking the exact-label-set match or the lexically first",
		"namespace", namespace, "selector", selector, "candidates", names)
}

func (v *virtualAppsState) warnIdentityCollisionOnce(namespace, name string) {
	key := namespace + "/" + name
	v.mu.Lock()
	if v.loggedIdentityCollision == nil {
		v.loggedIdentityCollision = make(map[string]bool)
	}
	already := v.loggedIdentityCollision[key]
	v.loggedIdentityCollision[key] = true
	v.mu.Unlock()
	if already {
		return
	}
	v.log.Info("virtual-apps identity index: two workloads of different kinds share a namespace/name; keeping the first observed this recompute",
		"namespace", namespace, "name", name)
}

// --- Namespace-scope-by-annotation: backfill on add, evict on remove ---
// (docs/todo/virtual-apps.md namespace-scope-by-annotation addendum)

// rawWorkload pairs a listed workload object with the resource Kind it was
// listed under. listWorkloadsFn/listCachedWorkloads return these RAW (not
// yet adapted through workloadFromUnstructured) so backfillNamespace can log
// an unparsed triageagent.dev/tier or triageagent.dev/weight annotation's raw value exactly as
// onWorkload does from a live informer event.
type rawWorkload struct {
	Obj  *unstructured.Unstructured
	Kind string
}

// backfillNamespace lists every configured, discovery-confirmed workload
// resource already resident in the informer cache for a namespace that has
// just entered DYNAMIC scope (a matching triageagent.dev/tenant Namespace annotation
// — Pusher.addDynamicScope) and feeds each one through applyWorkload
// directly, bypassing the normal inScope gate in onWorkload — the caller has
// already established the namespace is in scope.
//
// This is the crux of the feature: onWorkload checks inScope only at
// INFORMER EVENT time. A workload that was already steady (no further add/
// update/delete event) before its namespace entered scope sits in the cache
// but fires no new event — without this explicit backfill it would never
// enter v.workloads, exactly the "steady workload" trap recomputeNow's own
// doc comment describes for the sendSnapshot path, just one level up (a
// whole namespace cold-starting into scope, rather than a single workload
// cold-starting into an already-scoped one).
//
// Ends with an immediate recomputeNow (not the debounced scheduleRecompute)
// so the newly-backfilled workloads' vapp — and, mode "on", its OpAdd
// delta — appear without waiting out virtualAppsDebounce, matching this
// feature's "scope changes take effect live" requirement.
func (v *virtualAppsState) backfillNamespace(namespace string) {
	v.mu.Lock()
	ctx := v.ctx
	v.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	// Scoped-cache fallback: namespace entered dynamic scope but the manager
	// cache was never told to watch it (controller-runtime cannot add a
	// namespace to an already namespace-scoped cache at runtime). Start —
	// idempotently — a runtime per-namespace informer factory and wait for
	// its initial sync BEFORE listing below, so listCachedWorkloads'
	// dynInformers branch has something synced to read. A failure here (most
	// commonly: no dynamic client configured) is logged and left to the
	// existing "list returns nil/err" handling right below — never a crash,
	// and the namespace simply stays unobserved until the process is
	// reconfigured with a working dynamic client.
	if v.dynInformers != nil && !v.cacheCovers(namespace) {
		if err := v.dynInformers.ensure(ctx, namespace); err != nil {
			v.log.Error(err, "namespace scope backfill: failed to start runtime informer factory for out-of-cache namespace; its workloads will not be observed", "namespace", namespace)
		}
	}

	wlFn := v.listWorkloadsFn
	if wlFn == nil {
		wlFn = v.listCachedWorkloads
	}
	if raws, err := wlFn(ctx, namespace); err != nil {
		v.log.Error(err, "namespace scope backfill: failed to list workloads", "namespace", namespace)
	} else {
		for _, rw := range raws {
			info := workloadFromUnstructured(rw.Obj, rw.Kind)
			if info.TierInvalid {
				v.logTierInvalid(info.Kind, info.Namespace, info.Name, rw.Obj.GetAnnotations(), false)
			}
			if info.WeightInvalid {
				v.logWeightInvalid(info.Kind, info.Namespace, info.Name, rw.Obj.GetAnnotations(), false)
			}
			v.applyWorkload(workloadKey(info.Kind, info.Namespace, info.Name), &info, false)
		}
	}

	v.recomputeNow(ctx)
}

// listCachedWorkloads lists namespace's configured workload resources from
// whichever source actually covers it: the manager cache when
// cacheCovers(namespace) is true (the pre-existing, unaffected path), or the
// runtime per-namespace informer factory's own listers otherwise
// (dynInformers — dynamic_informers.go). The manager-cache List call is
// exactly what fails with "unknown namespace for the cache" for a namespace
// outside the cache's DefaultNamespaces set, which is why this dispatch has
// to happen before ever calling it, not as a fallback after an error.
func (v *virtualAppsState) listCachedWorkloads(ctx context.Context, namespace string) ([]rawWorkload, error) {
	if v.dynInformers != nil && !v.cacheCovers(namespace) {
		return v.dynInformers.listWorkloads(namespace)
	}
	v.mu.Lock()
	c := v.cache
	resources := v.workloadResources
	v.mu.Unlock()
	if c == nil {
		return nil, nil
	}
	var out []rawWorkload
	for _, res := range resources {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: res.GVR.Group, Version: res.GVR.Version, Kind: res.Kind})
		if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
			return nil, fmt.Errorf("list %s: %w", res.Kind, err)
		}
		for i := range list.Items {
			out = append(out, rawWorkload{Obj: &list.Items[i], Kind: res.Kind})
		}
	}
	return out, nil
}

// evictNamespace drops every known workload belonging to namespace from the
// flat set. A namespace leaving DYNAMIC scope must have its previously-
// ingested workloads forgotten too, or the group they formed never leaves
// v.current — onDeployment/onStatefulSet/onDaemonSet only check inScope at
// EVENT time, so a workload already resident in v.workloads receives no
// further delete event just because scope narrowed later. Reports whether
// anything was actually removed.
func (v *virtualAppsState) evictNamespace(namespace string) bool {
	v.mu.Lock()
	changed := false
	for key, w := range v.workloads {
		if w.Namespace == namespace {
			delete(v.workloads, key)
			changed = true
		}
	}
	v.mu.Unlock()
	return changed
}

// evictNamespaceAndRecompute stops namespace's runtime informer factory (if
// any — dynInformers.stop is a no-op when namespace was always cache-covered
// or never dynamically scoped), evicts its workloads (evictNamespace), and,
// only when something was actually removed, forces an immediate recompute:
// the namespace's group vanishes from v.current and (mode "on") emitDeltas
// emits its OpDelete exactly like any other group disappearing. The factory
// is stopped unconditionally, before the evictNamespace no-op check, so a
// namespace whose annotation flaps off before any workload ever settled
// still has its informer goroutines torn down instead of leaking. A no-op
// recompute is skipped when nothing changed, so an annotation flip on a
// namespace that never contributed a workload does no redundant work.
func (v *virtualAppsState) evictNamespaceAndRecompute(namespace string) {
	if v.dynInformers != nil {
		v.dynInformers.stop(namespace)
	}
	if !v.evictNamespace(namespace) {
		return
	}
	v.mu.Lock()
	ctx := v.ctx
	v.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	v.recomputeNow(ctx)
}

// registerInformers wires one informer per configured, discovery-confirmed
// workload resource (v.workloadResources) that feeds applyWorkload, via an
// unstructured object carrying just the resource's GVK — the manager cache
// resolves ANY GVK generically this way (a dynamic client under the hood,
// keyed by its RESTMapper), so a CRD-owned kind needs no scheme registration
// beyond having survived resolveWorkloadResources' discovery gate.
// Registered once for the Pusher's whole lifetime (unlike the per-connection
// Event informer in pusher.go) — grouping state must survive a WebSocket
// reconnect, not reset with it.
func (v *virtualAppsState) registerInformers(ctx context.Context, c cache.Cache) error {
	v.mu.Lock()
	v.ctx = ctx
	v.cache = c
	resources := v.workloadResources
	v.mu.Unlock()

	kinds := make([]string, 0, len(resources))
	for _, res := range resources {
		kind := res.Kind
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(schema.GroupVersionKind{Group: res.GVR.Group, Version: res.GVR.Version, Kind: kind})
		inf, err := c.GetInformer(ctx, u)
		if err != nil {
			return fmt.Errorf("get %s informer: %w", kind, err)
		}
		if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { v.onWorkload(obj, kind, false) },
			UpdateFunc: func(_, obj any) { v.onWorkload(obj, kind, false) },
			DeleteFunc: func(obj any) { v.onWorkload(obj, kind, true) },
		}); err != nil {
			return fmt.Errorf("watch %s: %w", kind, err)
		}
		kinds = append(kinds, kind)
	}

	v.log.Info("registered workload informers", "kinds", kinds)
	return nil
}

// logTierInvalid emits the once-per-occurrence V(1) line for an unparsable
// annotationkeys.Tier value (docs/todo/virtual-apps.md annotations Phase 2
// addendum). Skipped on delete: the workload is leaving the flat set anyway,
// and there is nothing actionable left to log about it.
func (v *virtualAppsState) logTierInvalid(kind workloadKind, namespace, name string, annotations map[string]string, deleted bool) {
	if deleted {
		return
	}
	v.log.V(1).Info("annotation unparsable; treating as absent",
		"key", annotationkeys.Tier, "kind", string(kind), "namespace", namespace, "name", name, "value", annotations[annotationkeys.Tier])
}

// logWeightInvalid emits the once-per-occurrence V(1) line for an unparsable
// annotationkeys.Weight value (docs/todo/service-weight.md §8), mirroring
// logTierInvalid. Skipped on delete for the same reason: the workload is
// leaving the flat set anyway.
func (v *virtualAppsState) logWeightInvalid(kind workloadKind, namespace, name string, annotations map[string]string, deleted bool) {
	if deleted {
		return
	}
	v.log.V(1).Info("annotation unparsable; treating as absent",
		"key", annotationkeys.Weight, "kind", string(kind), "namespace", namespace, "name", name, "value", annotations[annotationkeys.Weight])
}

// onWorkload is the ONE informer-event callback for every configured,
// discovery-confirmed workload resource (Deployment, StatefulSet, DaemonSet,
// Rollout, or any other namespaced kind named by --workload-resources) —
// wired identically onto the manager-cache-backed informers
// (registerInformers) and the out-of-cache runtime factory
// (dynamic_informers.go), so a workload discovered either way feeds
// synthesis through the identical inScope-gated path. kind is the
// resource's discovered Kind, passed by the caller rather than read off obj.
func (v *virtualAppsState) onWorkload(obj any, kind string, deleted bool) {
	u := unstructuredFromAny(obj)
	if u == nil || !v.inScope(u.GetNamespace()) {
		return
	}
	info := workloadFromUnstructured(u, kind)
	if info.TierInvalid {
		v.logTierInvalid(info.Kind, u.GetNamespace(), u.GetName(), u.GetAnnotations(), deleted)
	}
	if info.WeightInvalid {
		v.logWeightInvalid(info.Kind, u.GetNamespace(), u.GetName(), u.GetAnnotations(), deleted)
	}
	v.applyWorkload(workloadKey(info.Kind, u.GetNamespace(), u.GetName()), &info, deleted)
}
