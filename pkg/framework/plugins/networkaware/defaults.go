package networkaware

import "k8s.io/apimachinery/pkg/runtime"

func SetDefaults_NetworkAwareArgs(obj runtime.Object) {
	args := obj.(*NetworkAwareArgs)
	if args.MinPodAgeSeconds == nil {
		value := uint(60)
		args.MinPodAgeSeconds = &value
	}
}
