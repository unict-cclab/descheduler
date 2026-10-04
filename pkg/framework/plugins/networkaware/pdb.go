package networkaware

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
)

func (d *NetworkAware) pdbAllowsEviction(pod *v1.Pod, remaining map[string]int32) (string, bool) {
	pdbs, err := d.handle.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister().PodDisruptionBudgets(pod.Namespace).List(labels.Everything())
	if err != nil {
		d.logger.Error(err, "unable to check pod disruption budget", "pod", klog.KObj(pod))
		return "", false
	}
	key, err := checkPDBs(pod, pdbs, remaining)
	if err != nil {
		d.logger.V(2).Info("pod rejected by preventive PDB check", "pod", klog.KObj(pod), "reason", err)
		return "", false
	}
	return key, true
}

func checkPDBs(pod *v1.Pod, pdbs []*policyv1.PodDisruptionBudget, remaining map[string]int32) (string, error) {
	var matching *policyv1.PodDisruptionBudget
	for _, pdb := range pdbs {
		if pdb.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			return "", fmt.Errorf("invalid PDB selector: %w", err)
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		if matching != nil {
			return "", fmt.Errorf("multiple PDBs select the pod")
		}
		matching = pdb
	}
	if matching == nil {
		return "", nil
	}
	if pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return "", nil
	}
	ready := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			ready = true
		}
	}
	if pod.Status.Phase == v1.PodRunning && !ready && matching.Spec.UnhealthyPodEvictionPolicy != nil && *matching.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow {
		return "", nil
	}
	if !ready && matching.Spec.UnhealthyPodEvictionPolicy != nil && *matching.Spec.UnhealthyPodEvictionPolicy != policyv1.IfHealthyBudget {
		return "", fmt.Errorf("unrecognized unhealthy pod eviction policy")
	}
	if matching.Status.ObservedGeneration != matching.Generation {
		return "", fmt.Errorf("PDB status is not up to date")
	}
	if pod.Status.Phase == v1.PodRunning && !ready && (matching.Spec.UnhealthyPodEvictionPolicy == nil || *matching.Spec.UnhealthyPodEvictionPolicy == policyv1.IfHealthyBudget) && matching.Status.CurrentHealthy >= matching.Status.DesiredHealthy && matching.Status.DesiredHealthy > 0 {
		return "", nil
	}
	key := matching.Namespace + "/" + matching.Name
	allowed := matching.Status.DisruptionsAllowed
	if remaining != nil {
		if previous, ok := remaining[key]; ok && previous < allowed {
			allowed = previous
		}
		remaining[key] = allowed
	}
	if allowed <= 0 {
		return "", fmt.Errorf("PDB %s has no remaining disruption allowance", key)
	}
	return key, nil
}
