package highnetworkcost

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestScoredPodsAreRankedByCostImprovement(t *testing.T) {
	scored := []scoredPod{
		{pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "highest-current-cost"}}, cost: 10000, targetCost: 9000, improvement: 1000},
		{pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "greatest-improvement"}}, cost: 8000, targetCost: 3000, improvement: 5000},
	}

	sortScoredPods(scored)

	if scored[0].pod.Name != "greatest-improvement" {
		t.Fatalf("winner = %q, want greatest-improvement", scored[0].pod.Name)
	}
}

func TestSelectBestAlternative(t *testing.T) {
	nodes := []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "current"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "cheapest-unfit"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "better-fit"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "worse-fit"}},
	}
	costs := map[string]float64{
		"current":        100,
		"cheapest-unfit": 10,
		"better-fit":     60,
		"worse-fit":      120,
	}

	name, cost, found := selectBestAlternative("current", 100, 0, nodes, func(node *v1.Node) bool {
		return node.Name != "cheapest-unfit"
	}, func(node *v1.Node) float64 {
		return costs[node.Name]
	})
	if !found || name != "better-fit" || cost != 60 {
		t.Fatalf("alternative = (%q, %v, %v), want (better-fit, 60, true)", name, cost, found)
	}
}

func TestSelectBestAlternativeRequiresConfiguredImprovement(t *testing.T) {
	nodes := []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "current"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "slightly-better"}},
	}

	_, _, found := selectBestAlternative("current", 100, 10, nodes, func(*v1.Node) bool {
		return true
	}, func(node *v1.Node) float64 {
		if node.Name == "slightly-better" {
			return 95
		}
		return 100
	})
	if found {
		t.Fatal("expected no alternative below the minimum cost improvement")
	}
}

func TestCommunicationCostMirrorsSchedulerModel(t *testing.T) {
	nodes := map[string]*v1.Node{
		"a": {ObjectMeta: metav1.ObjectMeta{Name: "a", Annotations: map[string]string{"network-latency.a": "0", "network-latency.b": "100"}}},
		"b": {ObjectMeta: metav1.ObjectMeta{Name: "b"}},
	}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "default", UID: types.UID("1"), Labels: map[string]string{"group": "shop", "app": "frontend"}}, Spec: v1.PodSpec{NodeName: "a"}}
	peers := []*v1.Pod{
		pod,
		{ObjectMeta: metav1.ObjectMeta{Name: "cart-1", Namespace: "default", UID: types.UID("2"), Labels: map[string]string{"group": "shop", "app": "cart"}}, Spec: v1.PodSpec{NodeName: "b"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "cart-2", Namespace: "default", UID: types.UID("3"), Labels: map[string]string{"group": "shop", "app": "cart"}}, Spec: v1.PodSpec{NodeName: "b"}},
	}
	got := communicationCost(pod, nodes["a"], peers, nodes, map[string]string{"traffic.cart": "25"})
	if got != 5000 {
		t.Fatalf("cost = %v, want 5000", got)
	}
}
