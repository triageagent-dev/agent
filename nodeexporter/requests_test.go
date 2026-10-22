package nodeexporter

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestRegularContainerMemRequests_ExcludesInitContainers proves the init
// container's request (which would already have terminated by the time a pod
// is Running) is never summed in — same reasoning as
// regularContainerMemLimits.
func TestRegularContainerMemRequests_ExcludesInitContainers(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name: "init",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
					},
				},
				{Name: "sidecar"}, // no request declared
			},
		},
	}
	got := regularContainerMemRequests(pod)
	want := []int64{256 * 1024 * 1024, 0}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("regularContainerMemRequests = %v, want %v (init container excluded)", got, want)
	}
}

func TestRegularContainerCPURequestsMilli_ExcludesInitContainers(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name: "init",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					},
				},
			},
		},
	}
	got := regularContainerCPURequestsMilli(pod)
	want := []int64{500}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("regularContainerCPURequestsMilli = %v, want %v (init container excluded)", got, want)
	}
}

func TestSumRegularRequests(t *testing.T) {
	sum, ok := sumRegularRequests([]int64{100, 200, 0})
	if !ok || sum != 300 {
		t.Errorf("sumRegularRequests = (%d, %v), want (300, true)", sum, ok)
	}
}

// TestSumRegularRequests_OmitsWhenNothingDeclared proves the family is omitted
// (ok=false) rather than reported as a meaningless 0 when no regular container
// declared any request at all.
func TestSumRegularRequests_OmitsWhenNothingDeclared(t *testing.T) {
	sum, ok := sumRegularRequests([]int64{0, 0, 0})
	if ok {
		t.Errorf("ok = true with nothing declared (sum=%d), want false", sum)
	}
}

func TestSumRegularRequests_EmptySlice(t *testing.T) {
	if _, ok := sumRegularRequests(nil); ok {
		t.Error("ok = true for an empty slice, want false")
	}
}

// TestSumRegularRequests_NotAllOrNothing proves this differs from
// memLimitBytes' fallback rule: ONE container declaring a request is enough to
// produce a usable sum, even when siblings declare none — unlike a limit
// fallback, where one unbounded container makes the whole pod's denominator
// undefined.
func TestSumRegularRequests_NotAllOrNothing(t *testing.T) {
	sum, ok := sumRegularRequests([]int64{512, 0, 0})
	if !ok || sum != 512 {
		t.Errorf("sumRegularRequests = (%d, %v), want (512, true) — one declared request is enough", sum, ok)
	}
}
