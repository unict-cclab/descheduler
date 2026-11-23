package networkaware

import (
	"fmt"
	"math"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

func ValidateNetworkAwareArgs(obj runtime.Object) error {
	args := obj.(*NetworkAwareArgs)
	var errs []error
	if _, err := validateWeights(args.LatencyWeight, args.BandwidthWeight, args.PacketLossWeight); err != nil {
		errs = append(errs, err)
	}
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
	return utilerrors.NewAggregate(errs)
}

func validateWeights(latency, bandwidth, packetLoss *float64) ([3]float64, error) {
	values := [3]*float64{latency, bandwidth, packetLoss}
	names := [3]string{"latencyWeight", "bandwidthWeight", "packetLossWeight"}
	var weights [3]float64
	for i, value := range values {
		if value == nil {
			return weights, fmt.Errorf("%s is required", names[i])
		}
		if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 {
			return weights, fmt.Errorf("%s must be finite and non-negative", names[i])
		}
		weights[i] = *value
	}
	return weights, nil
}
