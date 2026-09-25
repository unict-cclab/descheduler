package networkaware

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/descheduler/pkg/api"
)

type NetworkAwareArgs struct {
	metav1.TypeMeta           `json:",inline"`
	Namespaces                *api.Namespaces       `json:"namespaces,omitempty"`
	LabelSelector             *metav1.LabelSelector `json:"labelSelector,omitempty"`
	MinPodAgeSeconds          *uint                 `json:"minPodAgeSeconds,omitempty"`
	MinPodIndex               *int                  `json:"minPodIndex,omitempty"`
	MinCommunicationCost      float64               `json:"minCommunicationCost,omitempty"`
	MinCostImprovement        float64               `json:"minCostImprovement,omitempty"`
	MaxPodsToEvict            *uint                 `json:"maxPodsToEvict,omitempty"`
	IgnoreSameZoneNetworkCost bool                  `json:"ignoreSameZoneNetworkCost,omitempty"`
}

func (in *NetworkAwareArgs) DeepCopyInto(out *NetworkAwareArgs) {
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
	if in.MinPodIndex != nil {
		value := *in.MinPodIndex
		out.MinPodIndex = &value
	}
	if in.MaxPodsToEvict != nil {
		value := *in.MaxPodsToEvict
		out.MaxPodsToEvict = &value
	}
}

func (in *NetworkAwareArgs) DeepCopy() *NetworkAwareArgs {
	if in == nil {
		return nil
	}
	out := new(NetworkAwareArgs)
	in.DeepCopyInto(out)
	return out
}

func (in *NetworkAwareArgs) DeepCopyObject() runtime.Object { return in.DeepCopy() }
