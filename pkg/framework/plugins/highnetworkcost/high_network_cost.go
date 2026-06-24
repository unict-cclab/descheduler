package highnetworkcost

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

const PluginName = "HighNetworkCost"

var _ frameworktypes.DeschedulePlugin = &HighNetworkCost{}

type HighNetworkCost struct {
	logger    klog.Logger
	handle    frameworktypes.Handle
	args      *HighNetworkCostArgs
	podFilter podutil.FilterFunc
}

type scoredPod struct {
	pod         *v1.Pod
	cost        float64
	targetNode  string
	targetCost  float64
	improvement float64
}

func New(ctx context.Context, obj runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	args, ok := obj.(*HighNetworkCostArgs)
	if !ok {
		return nil, fmt.Errorf("want args of type HighNetworkCostArgs, got %T", obj)
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
	return &HighNetworkCost{logger: klog.FromContext(ctx).WithValues("plugin", PluginName), handle: handle, args: args, podFilter: filter}, nil
}

func (d *HighNetworkCost) Name() string { return PluginName }

func (d *HighNetworkCost) Deschedule(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	d.logger.V(1).Info("starting high network cost descheduling cycle", "nodes", len(nodes), "minCommunicationCost", d.args.MinCommunicationCost, "minCostImprovement", d.args.MinCostImprovement)
	nodeByName := make(map[string]*v1.Node, len(nodes))
	var allPods, candidates []*v1.Pod
	for _, node := range nodes {
		nodeByName[node.Name] = node
		pods, err := podutil.ListPodsOnANode(node.Name, d.handle.GetPodsAssignedToNodeFunc(), nil)
		if err != nil {
			return &frameworktypes.Status{Err: err}
		}
		allPods = append(allPods, pods...)
		for _, pod := range pods {
			if !d.podFilter(pod) {
				d.logger.V(4).Info("pod rejected by eviction filters", "pod", klog.KObj(pod), "node", node.Name)
				continue
			}
			if !d.oldEnough(pod) {
				d.logger.V(3).Info("pod is younger than the configured minimum age", "pod", klog.KObj(pod), "node", node.Name, "minPodAgeSeconds", *d.args.MinPodAgeSeconds)
				continue
			}
			d.logger.V(3).Info("pod accepted as a cost-analysis candidate", "pod", klog.KObj(pod), "node", node.Name)
			candidates = append(candidates, pod)
		}
	}
	d.logger.V(2).Info("collected pods for cost analysis", "pods", len(allPods), "candidates", len(candidates))

	scored := make([]scoredPod, 0, len(candidates))
	for _, pod := range candidates {
		traffic, err := d.trafficAnnotations(ctx, pod)
		if err != nil {
			d.logger.Info("skipping pod without deployment traffic annotations", "pod", klog.KObj(pod), "error", err)
			continue
		}
		cost := communicationCost(pod, nodeByName[pod.Spec.NodeName], allPods, nodeByName, traffic)
		d.logger.V(2).Info("calculated current pod communication cost", "pod", klog.KObj(pod), "node", pod.Spec.NodeName, "cost", cost)
		if cost <= d.args.MinCommunicationCost {
			d.logger.V(2).Info("pod cost does not exceed the minimum communication cost", "pod", klog.KObj(pod), "cost", cost, "minimum", d.args.MinCommunicationCost)
			continue
		}
		targetNode, targetCost, found := d.bestAlternative(ctx, pod, cost, nodes, allPods, nodeByName, traffic)
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
	if len(scored) == 0 {
		d.logger.V(1).Info("completed cycle without eviction because no pod has a sufficiently better feasible placement")
		return nil
	}
	sortScoredPods(scored)
	winner := scored[0]
	d.logger.V(1).Info("selected pod with greatest feasible cost improvement", "pod", klog.KObj(winner.pod), "eligibleCandidates", len(scored), "currentNode", winner.pod.Spec.NodeName, "currentCost", winner.cost, "bestNode", winner.targetNode, "bestCost", winner.targetCost, "improvement", winner.improvement)
	if !d.handle.Evictor().PreEvictionFilter(winner.pod) {
		d.logger.Info("selected pod rejected by the final pre-eviction filter", "pod", klog.KObj(winner.pod))
		return nil
	}
	d.logger.Info("evicting pod with the greatest feasible communication-cost improvement", "pod", klog.KObj(winner.pod), "cost", winner.cost, "bestNode", winner.targetNode, "bestCost", winner.targetCost, "improvement", winner.improvement)
	if err := d.handle.Evictor().Evict(ctx, winner.pod, evictions.EvictOptions{StrategyName: PluginName}); err != nil {
		return &frameworktypes.Status{Err: err}
	}
	return nil
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

func (d *HighNetworkCost) bestAlternative(ctx context.Context, pod *v1.Pod, currentCost float64, nodes []*v1.Node, peers []*v1.Pod, nodeByName map[string]*v1.Node, traffic map[string]string) (string, float64, bool) {
	bestNode, bestCost, found := selectBestAlternative(
		pod.Spec.NodeName,
		currentCost,
		d.args.MinCostImprovement,
		nodes,
		func(candidate *v1.Node) bool {
			if err := nodeutil.NodeFit(ctx, d.handle.GetPodsAssignedToNodeFunc(), pod, candidate); err != nil {
				d.logger.V(3).Info("alternative node is not feasible for pod", "pod", klog.KObj(pod), "node", candidate.Name, "reason", err)
				return false
			}
			d.logger.V(3).Info("alternative node is feasible for pod", "pod", klog.KObj(pod), "node", candidate.Name)
			return true
		},
		func(candidate *v1.Node) float64 {
			cost := communicationCost(pod, candidate, peers, nodeByName, traffic)
			d.logger.V(3).Info("calculated projected pod communication cost", "pod", klog.KObj(pod), "currentNode", pod.Spec.NodeName, "candidateNode", candidate.Name, "currentCost", currentCost, "candidateCost", cost, "improvement", currentCost-cost)
			return cost
		},
	)
	return bestNode, bestCost, found
}

func selectBestAlternative(currentNode string, currentCost, minImprovement float64, nodes []*v1.Node, fits func(*v1.Node) bool, cost func(*v1.Node) float64) (string, float64, bool) {
	bestNode, bestCost := "", currentCost
	for _, candidate := range nodes {
		if candidate.Name == currentNode || !fits(candidate) {
			continue
		}
		candidateCost := cost(candidate)
		if candidateCost < bestCost {
			bestNode, bestCost = candidate.Name, candidateCost
		}
	}
	return bestNode, bestCost, bestNode != "" && currentCost-bestCost > minImprovement
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

func communicationCost(pod *v1.Pod, node *v1.Node, peers []*v1.Pod, nodes map[string]*v1.Node, traffic map[string]string) float64 {
	if node == nil {
		return 0
	}
	group := pod.Labels["group"]
	var total float64
	for _, peer := range peers {
		if peer.UID == pod.UID || peer.Namespace != pod.Namespace || group == "" || peer.Labels["group"] != group {
			continue
		}
		peerNode := nodes[peer.Spec.NodeName]
		peerApp := peer.Labels["app"]
		if peerNode == nil || peerApp == "" {
			continue
		}
		latency, _ := strconv.ParseFloat(node.Annotations["network-latency."+peerNode.Name], 64)
		volume, _ := strconv.ParseFloat(traffic["traffic."+peerApp], 64)
		total += latency * volume
	}
	return total
}
