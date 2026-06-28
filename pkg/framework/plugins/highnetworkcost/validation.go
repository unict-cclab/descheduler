package highnetworkcost

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

func ValidateHighNetworkCostArgs(obj runtime.Object) error {
	args := obj.(*HighNetworkCostArgs)
	var errs []error
	if args.Namespaces != nil && len(args.Namespaces.Include) > 0 && len(args.Namespaces.Exclude) > 0 {
		errs = append(errs, fmt.Errorf("only one of Include/Exclude namespaces can be set"))
	}
	if args.LabelSelector != nil {
		if _, err := metav1.LabelSelectorAsSelector(args.LabelSelector); err != nil {
			errs = append(errs, fmt.Errorf("invalid labelSelector: %w", err))
		}
	}
	if args.MinCommunicationCost < 0 {
		errs = append(errs, fmt.Errorf("minCommunicationCost must be non-negative"))
	}
	if args.MinCostImprovement < 0 {
		errs = append(errs, fmt.Errorf("minCostImprovement must be non-negative"))
	}
	if args.SelectionPolicy != "" && args.SelectionPolicy != SelectionPolicyHighestImprovement && args.SelectionPolicy != SelectionPolicyWeightedRandom {
		errs = append(errs, fmt.Errorf("selectionPolicy must be one of %q or %q", SelectionPolicyHighestImprovement, SelectionPolicyWeightedRandom))
	}
	return utilerrors.NewAggregate(errs)
}
