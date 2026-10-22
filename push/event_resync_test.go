package push

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A manager-cache resync hands the Event informer every cached object as an
// update with old == new. Only a changed revision is a new occurrence.
func TestEventUnchanged_ResyncIsNotAnUpdate(t *testing.T) {
	ev := func(rv string, count int32) *corev1.Event {
		return &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "checkout-1.abc", Namespace: "shop", ResourceVersion: rv}, Type: corev1.EventTypeWarning, Reason: "BackOff", Count: count}
	}
	if !eventUnchanged(ev("7", 7), ev("7", 7)) {
		t.Fatal("same resourceVersion must read as unchanged")
	}
	if eventUnchanged(ev("7", 7), ev("8", 8)) {
		t.Fatal("a new resourceVersion is a new occurrence")
	}
	if eventUnchanged(ev("", 1), ev("", 1)) {
		t.Fatal("an object without a resourceVersion is never assumed unchanged")
	}
	if eventUnchanged("not an event", ev("7", 7)) {
		t.Fatal("a non-Event old object must not suppress the update")
	}
}
