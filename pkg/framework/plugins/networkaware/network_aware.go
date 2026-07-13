package networkaware

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
)

const PluginName = "NetworkAware"

var _ frameworktypes.DeschedulePlugin = &NetworkAware{}

type NetworkAware struct {
	logger    klog.Logger
	handle    frameworktypes.Handle
	args      *NetworkAwareArgs
	podFilter podutil.FilterFunc
}

type scoredPod struct {
	pod         *v1.Pod
	index       int
	cost        float64
	targetNode  string
	targetCost  float64
	improvement float64
}

type preScoreState struct {
	peers         []peerPlacement
	metricsByNode map[string][]nodeNetworkMetrics
	maxTraffic    float64
	maxMetrics    nodeNetworkMetrics
}

type peerPlacement struct {
	node    *v1.Node
	traffic float64
}

type nodeNetworkMetrics struct {
	latency    float64
	bandwidth  float64
	packetLoss float64
	zeroCost   bool
}

type costAnalysisPods struct {
	nodeByName map[string]*v1.Node
	all        []*v1.Pod
	byIndex    map[int][]*v1.Pod
	indexes    []int
}

func New(ctx context.Context, obj runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	args, ok := obj.(*NetworkAwareArgs)
	if !ok {
		return nil, fmt.Errorf("want args of type NetworkAwareArgs, got %T", obj)
	}
	var included, excluded sets.Set[string]
	if args.Namespaces != nil {
		included = sets.New(args.Namespaces.Include...)
		excluded = sets.New(args.Namespaces.Exclude...)
	}
	options := podutil.NewOptions().WithNamespaces(included).WithoutNamespaces(excluded)
	if args.LabelSelector != nil {
		options = options.WithLabelSelector(args.LabelSelector)
	}
	filter, err := options.WithFilter(podutil.WrapFilterFuncs(handle.Evictor().Filter, handle.Evictor().PreEvictionFilter)).BuildFilterFunc()
	if err != nil {
		return nil, err
	}
	return &NetworkAware{logger: klog.FromContext(ctx).WithValues("plugin", PluginName), handle: handle, args: args, podFilter: filter}, nil
}

func (d *NetworkAware) Name() string { return PluginName }

func (d *NetworkAware) Deschedule(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	d.logger.V(1).Info("starting network-aware descheduling cycle", "nodes", len(nodes), "minPodIndex", d.minPodIndex(), "minCommunicationCost", d.args.MinCommunicationCost, "minCostImprovement", d.args.MinCostImprovement, "maxPodsToEvict", d.args.MaxPodsToEvict, "ignoreSameZoneNetworkCost", d.args.IgnoreSameZoneNetworkCost)

	pods, err := d.collectPodsForCostAnalysis(nodes)
	if err != nil {
		return &frameworktypes.Status{Err: err}
	}

	scored := d.scoreLowestImprovingIndex(ctx, nodes, pods)
	if len(scored) == 0 {
		d.logger.V(1).Info("completed cycle without eviction because no index layer has a sufficiently better feasible placement")
		return nil
	}

	sortScoredPods(scored)
	evicted := uint(0)
	for _, candidate := range scored {
		if d.reachedMaxPodsToEvict(evicted) {
			break
		}
		d.logger.V(1).Info("selected pod for index-layered network-aware eviction", "pod", klog.KObj(candidate.pod), "index", candidate.index, "eligibleCandidates", len(scored), "currentNode", candidate.pod.Spec.NodeName, "currentCost", candidate.cost, "bestNode", candidate.targetNode, "bestCost", candidate.targetCost, "improvement", candidate.improvement, "maxPodsToEvict", d.args.MaxPodsToEvict)
		if !d.handle.Evictor().PreEvictionFilter(candidate.pod) {
			d.logger.Info("selected pod rejected by the final pre-eviction filter", "pod", klog.KObj(candidate.pod), "index", candidate.index)
			continue
		}
		d.logger.Info("evicting pod with feasible communication-cost improvement", "pod", klog.KObj(candidate.pod), "index", candidate.index, "cost", candidate.cost, "bestNode", candidate.targetNode, "bestCost", candidate.targetCost, "improvement", candidate.improvement)
		if err := d.handle.Evictor().Evict(ctx, candidate.pod, evictions.EvictOptions{StrategyName: PluginName}); err != nil {
			switch err.(type) {
			case *evictions.EvictionTotalLimitError:
				d.logger.V(1).Info("stopping network-aware evictions because the total eviction limit was reached", "evicted", evicted)
				return nil
			case *evictions.EvictionNodeLimitError, *evictions.EvictionNamespaceLimitError:
				d.logger.V(2).Info("skipping selected pod because an eviction limit was reached", "pod", klog.KObj(candidate.pod), "error", err)
				continue
			default:
				d.logger.V(2).Info("skipping selected pod because eviction was rejected", "pod", klog.KObj(candidate.pod), "error", err)
				continue
			}
		}
		evicted++
	}
	d.logger.V(1).Info("completed network-aware eviction cycle", "index", scored[0].index, "eligibleCandidates", len(scored), "evicted", evicted)
	return nil
}

func (d *NetworkAware) collectPodsForCostAnalysis(nodes []*v1.Node) (costAnalysisPods, error) {
	pods := costAnalysisPods{
		nodeByName: make(map[string]*v1.Node, len(nodes)),
		byIndex:    make(map[int][]*v1.Pod),
	}
	minIndex := d.minPodIndex()

	for _, node := range nodes {
		pods.nodeByName[node.Name] = node
		nodePods, err := podutil.ListPodsOnANode(node.Name, d.handle.GetPodsAssignedToNodeFunc(), nil)
		if err != nil {
			return costAnalysisPods{}, err
		}
		pods.all = append(pods.all, nodePods...)
		for _, pod := range nodePods {
			index, ok := podIndex(pod)
			if !ok || index < minIndex {
				continue
			}
			if !d.podFilter(pod) {
				d.logger.V(4).Info("pod rejected by eviction filters", "pod", klog.KObj(pod), "node", node.Name, "index", index)
				continue
			}
			if !d.oldEnough(pod) {
				d.logger.V(3).Info("pod is younger than the configured minimum age", "pod", klog.KObj(pod), "node", node.Name, "index", index, "minPodAgeSeconds", *d.args.MinPodAgeSeconds)
				continue
			}
			if len(pods.byIndex[index]) == 0 {
				pods.indexes = append(pods.indexes, index)
			}
			pods.byIndex[index] = append(pods.byIndex[index], pod)
			d.logger.V(3).Info("pod accepted as a cost-analysis candidate", "pod", klog.KObj(pod), "node", node.Name, "index", index)
		}
	}
	sort.Ints(pods.indexes)

	d.logger.V(2).Info("collected pods for cost analysis", "pods", len(pods.all), "candidateIndexes", len(pods.indexes))
	return pods, nil
}

func (d *NetworkAware) scoreLowestImprovingIndex(ctx context.Context, nodes []*v1.Node, pods costAnalysisPods) []scoredPod {
	return firstImprovingIndex(pods.indexes, func(index int) []scoredPod {
		scored := d.scoreCandidatesAtIndex(ctx, nodes, pods, index)
		if len(scored) > 0 {
			d.logger.V(1).Info("found improving network-aware index layer", "index", index, "candidates", len(scored))
			return scored
		}
		d.logger.V(2).Info("index layer has no improving candidates", "index", index)
		return nil
	})
}

func firstImprovingIndex(indexes []int, score func(int) []scoredPod) []scoredPod {
	for _, index := range indexes {
		if scored := score(index); len(scored) > 0 {
			return scored
		}
	}
	return nil
}

func (d *NetworkAware) scoreCandidatesAtIndex(ctx context.Context, nodes []*v1.Node, pods costAnalysisPods, index int) []scoredPod {
	candidates := pods.byIndex[index]
	scored := make([]scoredPod, 0, len(candidates))
	for _, pod := range candidates {
		traffic, err := d.trafficAnnotations(ctx, pod)
		if err != nil {
			d.logger.Info("skipping pod without deployment traffic annotations", "pod", klog.KObj(pod), "index", index, "error", err)
			continue
		}
		feasibleNodes := d.feasibleAlternativeNodes(ctx, pod, nodes)
		modelNodes := append([]*v1.Node{pods.nodeByName[pod.Spec.NodeName]}, feasibleNodes...)
		preScore := newPreScoreState(pod, modelNodes, pods.all, pods.nodeByName, traffic, d.args.IgnoreSameZoneNetworkCost)
		cost := preScore.communicationCost(pod.Spec.NodeName)
		d.logger.V(2).Info("calculated current pod communication cost", "pod", klog.KObj(pod), "node", pod.Spec.NodeName, "index", index, "cost", cost)
		if cost <= d.args.MinCommunicationCost {
			d.logger.V(2).Info("pod cost does not exceed the minimum communication cost", "pod", klog.KObj(pod), "index", index, "cost", cost, "minimum", d.args.MinCommunicationCost)
			continue
		}
		targetNode, targetCost, found := d.bestAlternative(pod, cost, feasibleNodes, preScore)
		if !found {
			d.logger.V(2).Info("pod has no feasible alternative with sufficient cost improvement", "pod", klog.KObj(pod), "index", index, "currentNode", pod.Spec.NodeName, "currentCost", cost, "bestNode", targetNode, "bestCost", targetCost, "improvement", cost-targetCost, "minCostImprovement", d.args.MinCostImprovement)
			continue
		}
		improvement := cost - targetCost
		scored = append(scored, scoredPod{
			pod:         pod,
			index:       index,
			cost:        cost,
			targetNode:  targetNode,
			targetCost:  targetCost,
			improvement: improvement,
		})
	}
	return scored
}

func (d *NetworkAware) feasibleAlternativeNodes(ctx context.Context, pod *v1.Pod, nodes []*v1.Node) []*v1.Node {
	feasible := make([]*v1.Node, 0, len(nodes))
	for _, candidate := range crossZoneAlternativeNodes(pod, nodes) {
		if err := nodeutil.NodeFit(ctx, d.handle.GetPodsAssignedToNodeFunc(), pod, candidate); err != nil {
			d.logger.V(3).Info("alternative node is not feasible for pod", "pod", klog.KObj(pod), "node", candidate.Name, "reason", err)
			continue
		}
		d.logger.V(3).Info("alternative node is feasible for pod", "pod", klog.KObj(pod), "node", candidate.Name)
		feasible = append(feasible, candidate)
	}
	return feasible
}

func crossZoneAlternativeNodes(pod *v1.Pod, nodes []*v1.Node) []*v1.Node {
	var currentNode *v1.Node
	for _, node := range nodes {
		if node.Name == pod.Spec.NodeName {
			currentNode = node
			break
		}
	}

	alternatives := make([]*v1.Node, 0, len(nodes))
	for _, candidate := range nodes {
		if candidate.Name == pod.Spec.NodeName || sameZone(currentNode, candidate) {
			continue
		}
		alternatives = append(alternatives, candidate)
	}
	return alternatives
}

func (d *NetworkAware) bestAlternative(pod *v1.Pod, currentCost float64, nodes []*v1.Node, preScore preScoreState) (string, float64, bool) {
	bestNode, bestCost := "", currentCost
	for _, candidate := range nodes {
		candidateCost := preScore.communicationCost(candidate.Name)
		d.logger.V(3).Info("calculated projected pod communication cost", "pod", klog.KObj(pod), "currentNode", pod.Spec.NodeName, "candidateNode", candidate.Name, "currentCost", currentCost, "candidateCost", candidateCost, "improvement", currentCost-candidateCost)
		if candidateCost < bestCost {
			bestNode, bestCost = candidate.Name, candidateCost
		}
	}
	return bestNode, bestCost, bestNode != "" && currentCost-bestCost > d.args.MinCostImprovement
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

func (d *NetworkAware) minPodIndex() int {
	if d.args.MinPodIndex == nil {
		return 0
	}
	return *d.args.MinPodIndex
}

func (d *NetworkAware) reachedMaxPodsToEvict(evicted uint) bool {
	return d.args.MaxPodsToEvict != nil && evicted >= *d.args.MaxPodsToEvict
}

func (d *NetworkAware) oldEnough(pod *v1.Pod) bool {
	if d.args.MinPodAgeSeconds == nil {
		return true
	}
	return time.Since(pod.CreationTimestamp.Time) >= time.Duration(*d.args.MinPodAgeSeconds)*time.Second
}

func (d *NetworkAware) trafficAnnotations(ctx context.Context, pod *v1.Pod) (map[string]string, error) {
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

func newPreScoreState(pod *v1.Pod, candidateNodes []*v1.Node, peerPods []*v1.Pod, nodeByName map[string]*v1.Node, traffic map[string]string, ignoreSameZoneNetworkCost bool) preScoreState {
	group := pod.Labels["group"]

	peers := make([]peerPlacement, 0, len(peerPods))
	maxTraffic := 0.0
	for _, peerPod := range peerPods {
		if peerPod.UID == pod.UID || peerPod.Namespace != pod.Namespace || group == "" || peerPod.Labels["group"] != group {
			continue
		}
		if !hasLowerOrEqualIndex(pod, peerPod) {
			continue
		}
		peerNode := nodeByName[peerPod.Spec.NodeName]
		peerApp := peerPod.Labels["app"]
		if peerNode == nil || peerApp == "" {
			continue
		}
		volume, _ := strconv.ParseFloat(traffic["traffic."+peerApp], 64)
		if volume <= 0 {
			continue
		}
		if volume > maxTraffic {
			maxTraffic = volume
		}
		peers = append(peers, peerPlacement{node: peerNode, traffic: volume})
	}

	metricsByNode := make(map[string][]nodeNetworkMetrics, len(candidateNodes))
	maxMetrics := nodeNetworkMetrics{}
	for _, candidate := range candidateNodes {
		if candidate == nil {
			continue
		}
		metricsForNode := make([]nodeNetworkMetrics, 0, len(peers))
		for _, peer := range peers {
			metrics := nodeNetworkMetrics{
				latency:    parseNodeNetworkAnnotation(candidate, "network-latency."+peer.node.Name),
				bandwidth:  parseNodeNetworkAnnotation(candidate, "network-bandwidth."+peer.node.Name),
				packetLoss: parseNodeNetworkAnnotation(candidate, "packet-loss."+peer.node.Name),
			}
			if ignoreSameZoneNetworkCost && sameZone(candidate, peer.node) {
				metrics.zeroCost = true
			}
			if metrics.latency > maxMetrics.latency {
				maxMetrics.latency = metrics.latency
			}
			if metrics.bandwidth > maxMetrics.bandwidth {
				maxMetrics.bandwidth = metrics.bandwidth
			}
			if metrics.packetLoss > maxMetrics.packetLoss {
				maxMetrics.packetLoss = metrics.packetLoss
			}
			metricsForNode = append(metricsForNode, metrics)
		}
		metricsByNode[candidate.Name] = metricsForNode
	}

	return preScoreState{
		peers:         peers,
		metricsByNode: metricsByNode,
		maxTraffic:    maxTraffic,
		maxMetrics:    maxMetrics,
	}
}

func sameZone(a, b *v1.Node) bool {
	if a == nil || b == nil {
		return false
	}
	zone := a.Labels[v1.LabelTopologyZone]
	return zone != "" && zone == b.Labels[v1.LabelTopologyZone]
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

func (s preScoreState) communicationCost(nodeName string) float64 {
	metrics := s.metricsByNode[nodeName]
	if len(metrics) == 0 {
		return 0
	}
	var total float64
	for i, peer := range s.peers {
		total += communicationCost(metrics[i], s.maxMetrics, peer.traffic, s.maxTraffic)
	}
	return total
}

func parseNodeNetworkAnnotation(node *v1.Node, key string) float64 {
	value, _ := strconv.ParseFloat(node.Annotations[key], 64)
	return value
}

func communicationCost(metrics, maxMetrics nodeNetworkMetrics, traffic, maxTraffic float64) float64 {
	if metrics.zeroCost {
		return 0
	}
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

	bandwidthRatio := 1.0
	if maxMetrics.bandwidth > 0 {
		if metrics.bandwidth <= 0 {
			bandwidthRatio = 0
		} else {
			bandwidthRatio = metrics.bandwidth / maxMetrics.bandwidth
		}
	}
	if bandwidthRatio > 1 {
		bandwidthRatio = 1
	}

	packetLossRatio := 0.0
	if metrics.packetLoss > 0 && maxMetrics.packetLoss > 0 {
		packetLossRatio = metrics.packetLoss / maxMetrics.packetLoss
	}
	if packetLossRatio > 1 {
		packetLossRatio = 1
	}

	return trafficRatio * (latencyRatio + 1 - bandwidthRatio + packetLossRatio)
}
