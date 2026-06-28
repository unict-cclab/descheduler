package highnetworkcost

import (
	"math"
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

func TestSelectScoredPodsHighestImprovementSelectsOnlyGreatestImprovement(t *testing.T) {
	scored := []scoredPod{
		{pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "smaller-improvement"}}, cost: 100, targetCost: 80, improvement: 20},
		{pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "greater-improvement"}}, cost: 100, targetCost: 20, improvement: 80},
	}

	selected := selectScoredPods(scored, SelectionPolicyHighestImprovement, func() float64 { return 0.99 })

	if len(selected) != 1 || selected[0].pod.Name != "greater-improvement" {
		t.Fatalf("selected = %v, want only greater-improvement", selectedPodNames(selected))
	}
}

func TestSelectScoredPodsWeightedRandomUsesImprovementWeightsWithoutReplacement(t *testing.T) {
	scored := []scoredPod{
		{pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "smaller-improvement"}}, cost: 100, targetCost: 80, improvement: 20},
		{pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "greater-improvement"}}, cost: 100, targetCost: 20, improvement: 80},
	}

	selected := selectScoredPods(scored, SelectionPolicyWeightedRandom, func() float64 { return 0.85 })

	if selected[0].pod.Name != "smaller-improvement" || selected[1].pod.Name != "greater-improvement" {
		t.Fatalf("selected = [%q, %q], want [smaller-improvement, greater-improvement]", selected[0].pod.Name, selected[1].pod.Name)
	}
}

func selectedPodNames(scored []scoredPod) []string {
	names := make([]string, 0, len(scored))
	for _, pod := range scored {
		names = append(names, pod.pod.Name)
	}
	return names
}

func TestSelectBestAlternative(t *testing.T) {
	nodes := []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "current"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "better-fit"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "worse-fit"}},
	}
	costs := map[string]float64{
		"current":    100,
		"better-fit": 60,
		"worse-fit":  120,
	}

	name, cost, found := selectBestAlternative("current", 100, 0, nodes, func(node *v1.Node) float64 {
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

	_, _, found := selectBestAlternative("current", 100, 10, nodes, func(node *v1.Node) float64 {
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
	model := newNetworkCostModel(pod, []*v1.Node{nodes["a"]}, peers, nodes, map[string]string{"traffic.cart": "25"})
	got := model.communicationCost("a")
	if got != 2 {
		t.Fatalf("cost = %v, want 2", got)
	}
}

func TestNetworkCostUsesTrafficTimesNetworkCost(t *testing.T) {
	metrics := nodeNetworkMetrics{
		latency:    100,
		bandwidth:  250,
		packetLoss: 2,
	}
	maxMetrics := nodeNetworkMetrics{
		latency:    200,
		bandwidth:  1000,
		packetLoss: 10,
	}

	got := networkCost(metrics, maxMetrics, 50, 100)
	want := 0.725 // 0.5 traffic * (0.5 latency + 0.75 bandwidth cost + 0.2 packet loss)
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("cost = %v, want %v", got, want)
	}
}

func TestNetworkCostCapsRatios(t *testing.T) {
	metrics := nodeNetworkMetrics{
		latency:    400,
		bandwidth:  0,
		packetLoss: 20,
	}
	maxMetrics := nodeNetworkMetrics{
		latency:    200,
		bandwidth:  1000,
		packetLoss: 10,
	}

	got := networkCost(metrics, maxMetrics, 200, 100)
	want := 3.0
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("cost = %v, want %v", got, want)
	}
}
