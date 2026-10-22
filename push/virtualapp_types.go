package push

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VirtualApp is the wire shape a synthesized virtual app travels in (snapshot
// and app_delta frames, protocol.go). It carries exactly the fields the core
// reads off an app.k8s.io Application today, with the same JSON tags, so the
// frames stay byte-identical to the ones this package used to build around
// that upstream type (push/virtualapp_golden_test.go pins the bytes).
// metav1.ObjectMeta is embedded from k8s.io/apimachinery rather than
// reimplemented, so its marshaling behavior (including a zero
// CreationTimestamp) is whatever that package does, unchanged.
type VirtualApp struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualAppSpec   `json:"spec,omitempty"`
	Status VirtualAppStatus `json:"status,omitempty"`
}

// VirtualAppSpec carries the one Spec field synthesis ever sets.
type VirtualAppSpec struct {
	Descriptor Descriptor `json:"descriptor,omitempty"`
}

// Descriptor carries the two Spec.Descriptor fields synthesis ever sets.
type Descriptor struct {
	Version     string `json:"version,omitempty"`
	Description string `json:"description,omitempty"`
}

// VirtualAppStatus carries a synthesized app's readiness: its Ready
// condition, its component list, and the componentsReady summary string.
type VirtualAppStatus struct {
	Conditions []Condition `json:"conditions,omitempty"`
	// Objects carries the JSON tag "components" — the upstream Application
	// type embeds this list inline from a ComponentList wrapper, which is why
	// the wire key is "components" rather than "objects".
	Objects         []ObjectStatus `json:"components,omitempty"`
	ComponentsReady string         `json:"componentsReady,omitempty"`
}

// ConditionType is a VirtualApp condition's type. Synthesis only ever
// declares one: conditionReady.
type ConditionType string

// conditionReady is the one condition type synthesis ever declares.
const conditionReady ConditionType = "Ready"

// Condition describes the state of a synthesized app at a point in time.
// Type and Status carry no omitempty tag, matching the upstream type: they
// are always meant to be present, never a zero value standing in for
// absence. LastUpdateTime/LastTransitionTime carry omitempty but not
// omitzero, so a zero metav1.Time still marshals as an explicit null rather
// than being dropped — synthesis never sets either field, so every
// synthesized condition carries both as null.
type Condition struct {
	Type               ConditionType          `json:"type"`
	Status             corev1.ConditionStatus `json:"status"`
	Reason             string                 `json:"reason,omitempty"`
	Message            string                 `json:"message,omitempty"`
	LastUpdateTime     metav1.Time            `json:"lastUpdateTime,omitempty"`
	LastTransitionTime metav1.Time            `json:"lastTransitionTime,omitempty"`
}

// ObjectStatus is one component's status inside a synthesized app.
//
// Replicas and ReadyReplicas are the workload's desired and ready pod counts
// (ready capped at desired). Both are absent for a workload judged by its
// conditions alone, so a core reads absent Replicas as "counts unknown" and
// a present Replicas with absent ReadyReplicas as none ready.
type ObjectStatus struct {
	Name          string `json:"name,omitempty"`
	Kind          string `json:"kind,omitempty"`
	Status        string `json:"status,omitempty"`
	Replicas      int64  `json:"replicas,omitempty"`
	ReadyReplicas int64  `json:"readyReplicas,omitempty"`
}
