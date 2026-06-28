package highnetworkcost

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
)

const PluginName = "HighNetworkCost"

const (
	SelectionPolicyHighestImprovement = "HighestImprovement"
	SelectionPolicyWeightedRandom     = "WeightedRandom"
)

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
	d.logger.V(1).Info("starting high network cost descheduling cycle", "nodes", len(nodes), "minCommunicationCost", d.args.MinCommunicationCost, "minCostImprovement", d.args.MinCostImprovement, "selectionPolicy", d.args.SelectionPolicy)

	pods, err := d.collectPodsForCostAnalysis(nodes)
	if err != nil {
		return &frameworktypes.Status{Err: err}
	}

	scored := d.scoreCandidates(ctx, nodes, pods)
	if len(scored) == 0 {
		d.logger.V(1).Info("completed cycle without eviction because no pod has a sufficiently better feasible placement")
		return nil
	}
	selected := selectScoredPods(scored, d.args.SelectionPolicy, defaultRandomFloat)
	evicted := 0
	for _, candidate := range selected {
		d.logger.V(1).Info("selected pod for feasible cost-improving eviction", "pod", klog.KObj(candidate.pod), "eligibleCandidates", len(scored), "currentNode", candidate.pod.Spec.NodeName, "currentCost", candidate.cost, "bestNode", candidate.targetNode, "bestCost", candidate.targetCost, "improvement", candidate.improvement, "selectionPolicy", d.args.SelectionPolicy)
		if !d.handle.Evictor().PreEvictionFilter(candidate.pod) {
			d.logger.Info("selected pod rejected by the final pre-eviction filter", "pod", klog.KObj(candidate.pod))
			continue
		}
		d.logger.Info("evicting pod with feasible communication-cost improvement", "pod", klog.KObj(candidate.pod), "cost", candidate.cost, "bestNode", candidate.targetNode, "bestCost", candidate.targetCost, "improvement", candidate.improvement)
		if err := d.handle.Evictor().Evict(ctx, candidate.pod, evictions.EvictOptions{StrategyName: PluginName}); err != nil {
			switch err.(type) {
			case *evictions.EvictionTotalLimitError:
				d.logger.V(1).Info("stopping high network cost evictions because the total eviction limit was reached", "evicted", evicted)
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
	d.logger.V(1).Info("completed high network cost eviction cycle", "eligibleCandidates", len(scored), "evicted", evicted)
	return nil
}
