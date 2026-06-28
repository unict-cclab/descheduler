package highnetworkcost

import "k8s.io/apimachinery/pkg/runtime"

func SetDefaults_HighNetworkCostArgs(obj runtime.Object) {
	args := obj.(*HighNetworkCostArgs)
	if args.MinPodAgeSeconds == nil {
		value := uint(60)
		args.MinPodAgeSeconds = &value
	}
	if args.SelectionPolicy == "" {
		args.SelectionPolicy = SelectionPolicyHighestImprovement
	}
}
