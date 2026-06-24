package highnetworkcost

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/descheduler/pkg/api"
)

// HighNetworkCostArgs configures selection of the pod with the greatest feasible cost reduction.
type HighNetworkCostArgs struct {
	metav1.TypeMeta      `json:",inline"`
	Namespaces           *api.Namespaces       `json:"namespaces,omitempty"`
	LabelSelector        *metav1.LabelSelector `json:"labelSelector,omitempty"`
	MinPodAgeSeconds     *uint                 `json:"minPodAgeSeconds,omitempty"`
	MinCommunicationCost float64               `json:"minCommunicationCost,omitempty"`
	MinCostImprovement   float64               `json:"minCostImprovement,omitempty"`
}

func (in *HighNetworkCostArgs) DeepCopyInto(out *HighNetworkCostArgs) {
	*out = *in
	if in.Namespaces != nil {
		out.Namespaces = in.Namespaces.DeepCopy()
	}
	if in.LabelSelector != nil {
		out.LabelSelector = in.LabelSelector.DeepCopy()
	}
	if in.MinPodAgeSeconds != nil {
		value := *in.MinPodAgeSeconds
		out.MinPodAgeSeconds = &value
	}
}

func (in *HighNetworkCostArgs) DeepCopy() *HighNetworkCostArgs {
	if in == nil {
		return nil
	}
	out := new(HighNetworkCostArgs)
	in.DeepCopyInto(out)
	return out
}

func (in *HighNetworkCostArgs) DeepCopyObject() runtime.Object { return in.DeepCopy() }
