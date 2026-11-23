package networkaware

import (
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSortScoredPodsOrdersWithinIndexByImprovement(t *testing.T) {
	scored := []scoredPod{
		{pod: pod("smaller", "default", "1", "shop", "smaller", "2", "a"), index: 2, cost: 100, targetCost: 80, improvement: 20},
		{pod: pod("greater", "default", "2", "shop", "greater", "2", "a"), index: 2, cost: 100, targetCost: 20, improvement: 80},
	}

	sortScoredPods(scored)

	if scored[0].pod.Name != "greater" {
		t.Fatalf("first pod = %q, want greater", scored[0].pod.Name)
	}
}

func TestReachedMaxPodsToEvictIsUnboundedWhenUnset(t *testing.T) {
	d := &NetworkAware{args: &NetworkAwareArgs{}}

	if d.reachedMaxPodsToEvict(^uint(0)) {
		t.Fatal("expected unset maxPodsToEvict to be unbounded")
	}
}

func TestReachedMaxPodsToEvictUsesConfiguredLimit(t *testing.T) {
	limit := uint(2)
	d := &NetworkAware{args: &NetworkAwareArgs{MaxPodsToEvict: &limit}}

	if d.reachedMaxPodsToEvict(1) {
		t.Fatal("expected one eviction to be below limit")
	}
	if !d.reachedMaxPodsToEvict(2) {
		t.Fatal("expected two evictions to reach limit")
	}
}

func TestCrossZoneAlternativeNodesExcludesCurrentAndSameZoneNodes(t *testing.T) {
	nodes := []*v1.Node{
		node("current", "zone-a"),
		node("same-zone", "zone-a"),
		node("other-zone", "zone-b"),
	}
	p := pod("frontend", "default", "1", "shop", "frontend", "1", "current")

	alternatives := crossZoneAlternativeNodes(p, nodes)

	if len(alternatives) != 1 || alternatives[0].Name != "other-zone" {
		t.Fatalf("alternatives = %v, want only other-zone", nodeNames(alternatives))
	}
}

func TestCrossZoneAlternativeNodesKeepsNodesWithUnknownZone(t *testing.T) {
	nodes := []*v1.Node{
		node("current", "zone-a"),
		node("unknown-zone", ""),
	}
	p := pod("frontend", "default", "1", "shop", "frontend", "1", "current")

	alternatives := crossZoneAlternativeNodes(p, nodes)

	if len(alternatives) != 1 || alternatives[0].Name != "unknown-zone" {
		t.Fatalf("alternatives = %v, want unknown-zone", nodeNames(alternatives))
	}
}

func TestCommunicationCostMirrorsSchedulerModel(t *testing.T) {
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

	got := communicationCost(metrics, maxMetrics, 50, 100, [3]float64{1, 1, 1})
	want := 0.725
	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("cost = %v, want %v", got, want)
	}
}

func TestPreScoreStateIgnoresSameZoneWhenEnabled(t *testing.T) {
	candidate := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "candidate",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone-a",
			},
			Annotations: map[string]string{
				"network-latency.peer":   "100",
				"network-bandwidth.peer": "250",
				"packet-loss.peer":       "2",
			},
		},
	}
	peer := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "peer",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone-a",
			},
		},
	}
	p := pod("frontend", "default", "1", "shop", "frontend", "1", "candidate")

	preScore := newPreScoreState(
		p,
		[]*v1.Node{candidate},
		[]*v1.Pod{pod("peer", "default", "2", "shop", "peer", "0", "peer")},
		map[string]*v1.Node{"candidate": candidate, "peer": peer},
		map[string]string{"traffic.peer": "10"},
		true, [3]float64{1, 1, 1},
	)
	metrics := preScore.metricsByNode["candidate"][0]

	if !metrics.zeroCost {
		t.Fatalf("metrics = %#v, want zero-cost marker", metrics)
	}
	if cost := communicationCost(metrics, nodeNetworkMetrics{bandwidth: 1000}, 50, 100, [3]float64{1, 1, 1}); cost != 0 {
		t.Fatalf("cost = %v, want 0", cost)
	}
}

func TestPreScoreStateKeepsCrossZoneMetrics(t *testing.T) {
	candidate := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "candidate",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone-a",
			},
			Annotations: map[string]string{
				"network-latency.peer":   "100",
				"network-bandwidth.peer": "250",
				"packet-loss.peer":       "2",
			},
		},
	}
	peer := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "peer",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone-b",
			},
		},
	}
	p := pod("frontend", "default", "1", "shop", "frontend", "1", "candidate")

	preScore := newPreScoreState(
		p,
		[]*v1.Node{candidate},
		[]*v1.Pod{pod("peer", "default", "2", "shop", "peer", "0", "peer")},
		map[string]*v1.Node{"candidate": candidate, "peer": peer},
		map[string]string{"traffic.peer": "10"},
		true, [3]float64{1, 1, 1},
	)
	metrics := preScore.metricsByNode["candidate"][0]

	if metrics.latency != 100 || metrics.bandwidth != 250 || metrics.packetLoss != 2 {
		t.Fatalf("metrics = %#v, want annotated cross-zone metrics", metrics)
	}
}

func pod(name, namespace, uid, group, app, index, node string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
			Labels: map[string]string{
				"group": group,
				"app":   app,
				"index": index,
			},
		},
		Spec: v1.PodSpec{NodeName: node},
	}
}

func node(name, zone string) *v1.Node {
	labels := map[string]string{}
	if zone != "" {
		labels[v1.LabelTopologyZone] = zone
	}
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func nodeNames(nodes []*v1.Node) []string {
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, node.Name)
	}
	return names
}

func TestNormalizationExcludesZeroCostLinks(t *testing.T) {
	local := node("local", "a")
	local.Annotations = map[string]string{"network-latency.peer": "1000", "network-bandwidth.peer": "1000", "packet-loss.peer": "10"}
	remote := node("remote", "b")
	remote.Annotations = map[string]string{"network-latency.peer": "100", "network-bandwidth.peer": "50", "packet-loss.peer": "1"}
	other := node("other", "c")
	other.Annotations = map[string]string{"network-latency.peer": "200", "network-bandwidth.peer": "25", "packet-loss.peer": "2"}
	peer := node("peer", "a")
	for _, tt := range []struct {
		name   string
		ignore bool
		nodes  []*v1.Node
		want   nodeNetworkMetrics
	}{
		{"exclude same zone", true, []*v1.Node{local, remote, other}, nodeNetworkMetrics{latency: 200, bandwidth: 50, packetLoss: 2}},
		{"include same zone when disabled", false, []*v1.Node{local, remote, other}, nodeNetworkMetrics{latency: 1000, bandwidth: 1000, packetLoss: 10}},
		{"all links free", true, []*v1.Node{local}, nodeNetworkMetrics{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := newPreScoreState(pod("frontend", "default", "1", "shop", "frontend", "1", "local"), tt.nodes,
				[]*v1.Pod{pod("peer", "default", "2", "shop", "peer", "0", "peer")},
				map[string]*v1.Node{"local": local, "remote": remote, "other": other, "peer": peer}, map[string]string{"traffic.peer": "10"}, tt.ignore, [3]float64{1, 1, 1})
			if state.maxMetrics != tt.want {
				t.Fatalf("max metrics = %+v, want %+v", state.maxMetrics, tt.want)
			}
			metrics := state.metricsByNode["local"][0]
			if metrics.latency != 1000 || metrics.bandwidth != 1000 || metrics.packetLoss != 10 || metrics.zeroCost != tt.ignore {
				t.Fatalf("local metrics not preserved: %+v", metrics)
			}
			if tt.ignore && state.communicationCost("local") != 0 {
				t.Fatal("same-zone communication must remain free")
			}
			if tt.ignore && len(tt.nodes) > 1 {
				if cost := state.communicationCost("remote"); cost != 1 {
					t.Fatalf("remote cost = %v, want 1", cost)
				}
				if cost := state.communicationCost("other"); cost != 2.5 {
					t.Fatalf("other cost = %v, want 2.5", cost)
				}
			}
		})
	}
}
