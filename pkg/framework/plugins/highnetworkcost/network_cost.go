package highnetworkcost

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
)

type networkCostModel struct {
	peers         []networkPeer
	metricsByNode map[string][]nodeNetworkMetrics
	maxTraffic    float64
	maxMetrics    nodeNetworkMetrics
}

type networkPeer struct {
	traffic float64
}

type nodeNetworkMetrics struct {
	latency    float64
	bandwidth  float64
	packetLoss float64
}

type costAnalysisPods struct {
	nodeByName map[string]*v1.Node
	all        []*v1.Pod
	candidates []*v1.Pod
}

func (d *HighNetworkCost) collectPodsForCostAnalysis(nodes []*v1.Node) (costAnalysisPods, error) {
	pods := costAnalysisPods{
		nodeByName: make(map[string]*v1.Node, len(nodes)),
	}

	for _, node := range nodes {
		pods.nodeByName[node.Name] = node
		nodePods, err := podutil.ListPodsOnANode(node.Name, d.handle.GetPodsAssignedToNodeFunc(), nil)
		if err != nil {
			return costAnalysisPods{}, err
		}
		pods.all = append(pods.all, nodePods...)
		for _, pod := range nodePods {
			if !d.podFilter(pod) {
				d.logger.V(4).Info("pod rejected by eviction filters", "pod", klog.KObj(pod), "node", node.Name)
				continue
			}
			if !d.oldEnough(pod) {
				d.logger.V(3).Info("pod is younger than the configured minimum age", "pod", klog.KObj(pod), "node", node.Name, "minPodAgeSeconds", *d.args.MinPodAgeSeconds)
				continue
			}
			d.logger.V(3).Info("pod accepted as a cost-analysis candidate", "pod", klog.KObj(pod), "node", node.Name)
			pods.candidates = append(pods.candidates, pod)
		}
	}

	d.logger.V(2).Info("collected pods for cost analysis", "pods", len(pods.all), "candidates", len(pods.candidates))
	return pods, nil
}

func (d *HighNetworkCost) scoreCandidates(ctx context.Context, nodes []*v1.Node, pods costAnalysisPods) []scoredPod {
	scored := make([]scoredPod, 0, len(pods.candidates))
	for _, pod := range pods.candidates {
		traffic, err := d.trafficAnnotations(ctx, pod)
		if err != nil {
			d.logger.Info("skipping pod without deployment traffic annotations", "pod", klog.KObj(pod), "error", err)
			continue
		}
		feasibleNodes := d.feasibleAlternativeNodes(ctx, pod, nodes)
		modelNodes := append([]*v1.Node{pods.nodeByName[pod.Spec.NodeName]}, feasibleNodes...)
		costModel := newNetworkCostModel(pod, modelNodes, pods.all, pods.nodeByName, traffic)
		cost := costModel.communicationCost(pod.Spec.NodeName)
		d.logger.V(2).Info("calculated current pod communication cost", "pod", klog.KObj(pod), "node", pod.Spec.NodeName, "cost", cost)
		if cost <= d.args.MinCommunicationCost {
			d.logger.V(2).Info("pod cost does not exceed the minimum communication cost", "pod", klog.KObj(pod), "cost", cost, "minimum", d.args.MinCommunicationCost)
			continue
		}
		targetNode, targetCost, found := d.bestAlternative(pod, cost, feasibleNodes, costModel)
		if !found {
			d.logger.V(2).Info("pod has no feasible alternative with sufficient cost improvement", "pod", klog.KObj(pod), "currentNode", pod.Spec.NodeName, "currentCost", cost, "bestNode", targetNode, "bestCost", targetCost, "improvement", cost-targetCost, "minCostImprovement", d.args.MinCostImprovement)
			continue
		}
		improvement := cost - targetCost
		d.logger.V(2).Info("pod is eligible for cost-improving eviction", "pod", klog.KObj(pod), "currentNode", pod.Spec.NodeName, "currentCost", cost, "bestNode", targetNode, "bestCost", targetCost, "improvement", improvement)
		scored = append(scored, scoredPod{
			pod:         pod,
			cost:        cost,
			targetNode:  targetNode,
			targetCost:  targetCost,
			improvement: improvement,
		})
	}
	return scored
}

func (d *HighNetworkCost) feasibleAlternativeNodes(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) []*v1.Node {
	feasible := make([]*v1.Node, 0, len(nodes))
	for _, candidate := range nodes {
		if candidate.Name == pod.Spec.NodeName {
			continue
		}
		if err := nodeutil.NodeFit(ctx, d.handle.GetPodsAssignedToNodeFunc(), pod, candidate); err != nil {
			d.logger.V(3).Info("alternative node is not feasible for pod", "pod", klog.KObj(pod), "node", candidate.Name, "reason", err)
			continue
		}
		d.logger.V(3).Info("alternative node is feasible for pod", "pod", klog.KObj(pod), "node", candidate.Name)
		feasible = append(feasible, candidate)
	}
	return feasible
}

func (d *HighNetworkCost) bestAlternative(pod *v1.Pod, currentCost float64, nodes []*v1.Node, costModel networkCostModel) (string, float64, bool) {
	bestNode, bestCost, found := selectBestAlternative(
		pod.Spec.NodeName,
		currentCost,
		d.args.MinCostImprovement,
		nodes,
		func(candidate *v1.Node) float64 {
			cost := costModel.communicationCost(candidate.Name)
			d.logger.V(3).Info("calculated projected pod communication cost", "pod", klog.KObj(pod), "currentNode", pod.Spec.NodeName, "candidateNode", candidate.Name, "currentCost", currentCost, "candidateCost", cost, "improvement", currentCost-cost)
			return cost
		},
	)
	return bestNode, bestCost, found
}

func selectBestAlternative(currentNode string, currentCost, minImprovement float64, nodes []*v1.Node, cost func(*v1.Node) float64) (string, float64, bool) {
	bestNode, bestCost := "", currentCost
	for _, candidate := range nodes {
		if candidate.Name == currentNode {
			continue
		}
		candidateCost := cost(candidate)
		if candidateCost < bestCost {
			bestNode, bestCost = candidate.Name, candidateCost
		}
	}
	return bestNode, bestCost, bestNode != "" && currentCost-bestCost > minImprovement
}

func sortScoredPods(scored []scoredPod) {
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].improvement != scored[j].improvement {
			return scored[i].improvement > scored[j].improvement
		}
		if scored[i].cost != scored[j].cost {
			return scored[i].cost > scored[j].cost
		}
		return scored[i].pod.Namespace+"/"+scored[i].pod.Name < scored[j].pod.Namespace+"/"+scored[j].pod.Name
	})
}

func selectScoredPods(scored []scoredPod, selectionPolicy string, randomFloat func() float64) []scoredPod {
	sortScoredPods(scored)
	if selectionPolicy != SelectionPolicyWeightedRandom {
		if len(scored) == 0 {
			return nil
		}
		return scored[:1]
	}

	remaining := append([]scoredPod(nil), scored...)
	selected := make([]scoredPod, 0, len(scored))
	for len(remaining) > 0 {
		var totalImprovement float64
		for _, candidate := range remaining {
			totalImprovement += candidate.improvement
		}
		if totalImprovement <= 0 {
			selected = append(selected, remaining...)
			return selected
		}

		threshold := randomFloat() * totalImprovement
		var cumulative float64
		selectedIndex := len(remaining) - 1
		for i, candidate := range remaining {
			cumulative += candidate.improvement
			if threshold < cumulative {
				selectedIndex = i
				break
			}
		}
		selected = append(selected, remaining[selectedIndex])
		remaining = append(remaining[:selectedIndex], remaining[selectedIndex+1:]...)
	}
	return selected
}

func defaultRandomFloat() float64 {
	return rand.Float64()
}

func (d *HighNetworkCost) oldEnough(pod *v1.Pod) bool {
	if d.args.MinPodAgeSeconds == nil {
		return true
	}
	return time.Since(pod.CreationTimestamp.Time) >= time.Duration(*d.args.MinPodAgeSeconds)*time.Second
}

func (d *HighNetworkCost) trafficAnnotations(ctx context.Context, pod *v1.Pod) (map[string]string, error) {
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.Kind != "ReplicaSet" {
		return nil, fmt.Errorf("pod is not controlled by ReplicaSet")
	}
	rs, err := d.handle.ClientSet().AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	deploymentOwner := metav1.GetControllerOf(rs)
	if deploymentOwner == nil || deploymentOwner.Kind != "Deployment" {
		return nil, fmt.Errorf("ReplicaSet is not controlled by Deployment")
	}
	deployment, err := d.handle.ClientSet().AppsV1().Deployments(pod.Namespace).Get(ctx, deploymentOwner.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return deployment.Annotations, nil
}

func newNetworkCostModel(pod *v1.Pod, candidateNodes []*v1.Node, peers []*v1.Pod, nodes map[string]*v1.Node, traffic map[string]string) networkCostModel {
	group := pod.Labels["group"]
	model := networkCostModel{
		metricsByNode: make(map[string][]nodeNetworkMetrics, len(candidateNodes)),
	}

	for _, peer := range peers {
		if peer.UID == pod.UID || peer.Namespace != pod.Namespace || group == "" || peer.Labels["group"] != group {
			continue
		}
		if !hasLowerOrEqualIndex(pod, peer) {
			continue
		}
		peerNode := nodes[peer.Spec.NodeName]
		peerApp := peer.Labels["app"]
		if peerNode == nil || peerApp == "" {
			continue
		}
		volume, _ := strconv.ParseFloat(traffic["traffic."+peerApp], 64)
		if volume <= 0 {
			continue
		}
		if volume > model.maxTraffic {
			model.maxTraffic = volume
		}
		model.peers = append(model.peers, networkPeer{traffic: volume})

		for _, candidate := range candidateNodes {
			metrics := nodeNetworkMetrics{
				latency:    parseNodeNetworkAnnotation(candidate, "network-latency."+peerNode.Name),
				bandwidth:  parseNodeNetworkAnnotation(candidate, "network-bandwidth."+peerNode.Name),
				packetLoss: parseNodeNetworkAnnotation(candidate, "packet-loss."+peerNode.Name),
			}
			if metrics.latency > model.maxMetrics.latency {
				model.maxMetrics.latency = metrics.latency
			}
			if metrics.bandwidth > model.maxMetrics.bandwidth {
				model.maxMetrics.bandwidth = metrics.bandwidth
			}
			if metrics.packetLoss > model.maxMetrics.packetLoss {
				model.maxMetrics.packetLoss = metrics.packetLoss
			}
			model.metricsByNode[candidate.Name] = append(model.metricsByNode[candidate.Name], metrics)
		}
	}

	return model
}

func podIndex(pod *v1.Pod) (int, bool) {
	value, ok := pod.Labels["index"]
	if !ok {
		return 0, false
	}
	index, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	return index, true
}

func hasLowerOrEqualIndex(pod *v1.Pod, peer *v1.Pod) bool {
	index, ok := podIndex(pod)
	if !ok {
		return true
	}
	peerIndex, ok := podIndex(peer)
	return ok && peerIndex <= index
}

func (m networkCostModel) communicationCost(nodeName string) float64 {
	metrics := m.metricsByNode[nodeName]
	if len(metrics) == 0 {
		return 0
	}
	var total float64
	for i, peer := range m.peers {
		total += networkCost(metrics[i], m.maxMetrics, peer.traffic, m.maxTraffic)
	}
	return total
}

func parseNodeNetworkAnnotation(node *v1.Node, key string) float64 {
	value, _ := strconv.ParseFloat(node.Annotations[key], 64)
	return value
}

func networkCost(metrics, maxMetrics nodeNetworkMetrics, traffic, maxTraffic float64) float64 {
	if traffic <= 0 || maxTraffic <= 0 {
		return 0
	}

	trafficRatio := traffic / maxTraffic
	if trafficRatio > 1 {
		trafficRatio = 1
	}

	latencyRatio := 0.0
	if metrics.latency > 0 && maxMetrics.latency > 0 {
		latencyRatio = metrics.latency / maxMetrics.latency
	}
	if latencyRatio > 1 {
		latencyRatio = 1
	}

	bandwidthRatio := 0.0
	if maxMetrics.bandwidth > 0 {
		if metrics.bandwidth <= 0 {
			bandwidthRatio = 1
		} else {
			bandwidthRatio = 1 - metrics.bandwidth/maxMetrics.bandwidth
			if bandwidthRatio < 0 {
				bandwidthRatio = 0
			}
		}
	}

	packetLossRatio := 0.0
	if metrics.packetLoss > 0 && maxMetrics.packetLoss > 0 {
		packetLossRatio = metrics.packetLoss / maxMetrics.packetLoss
	}
	if packetLossRatio > 1 {
		packetLossRatio = 1
	}

	return trafficRatio * (latencyRatio + bandwidthRatio + packetLossRatio)
}
