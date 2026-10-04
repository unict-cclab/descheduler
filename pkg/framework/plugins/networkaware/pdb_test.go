package networkaware

import (
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCheckPDBs(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		budget                                         int32
		stale, unmatched, duplicate, unhealthy, always bool
		want                                           bool
	}{
		{name: "available", budget: 1, want: true},
		{name: "exhausted"},
		{name: "stale", budget: 1, stale: true},
		{name: "no matching PDB", unmatched: true, want: true},
		{name: "overlapping PDBs", budget: 2, duplicate: true},
		{name: "unhealthy always allowed", unhealthy: true, always: true, want: true},
		{name: "unhealthy disrupted application", unhealthy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Labels: map[string]string{"app": "shop"}}, Status: v1.PodStatus{Phase: v1.PodRunning}}
			if !tc.unhealthy {
				p.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
			}
			pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "shop", Namespace: "default", Generation: 1}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "shop"}}}, Status: policyv1.PodDisruptionBudgetStatus{ObservedGeneration: 1, DisruptionsAllowed: tc.budget, DesiredHealthy: 1}}
			if tc.stale {
				pdb.Status.ObservedGeneration = 0
			}
			if tc.unmatched {
				pdb.Spec.Selector.MatchLabels["app"] = "other"
			}
			if tc.always {
				policy := policyv1.AlwaysAllow
				pdb.Spec.UnhealthyPodEvictionPolicy = &policy
			}
			pdbs := []*policyv1.PodDisruptionBudget{pdb}
			if tc.duplicate {
				pdbs = append(pdbs, pdb.DeepCopy())
			}
			_, err := checkPDBs(p, pdbs, nil)
			if (err == nil) != tc.want {
				t.Fatalf("allowed=%v, want %v (error %v)", err == nil, tc.want, err)
			}
		})
	}
}

func TestCheckPDBsTracksCycleAllowance(t *testing.T) {
	p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}, Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}}}
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{}}, Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1}}
	remaining := map[string]int32{}
	key, err := checkPDBs(p, []*policyv1.PodDisruptionBudget{pdb}, remaining)
	if err != nil {
		t.Fatal(err)
	}
	remaining[key]--
	if _, err := checkPDBs(p, []*policyv1.PodDisruptionBudget{pdb}, remaining); err == nil {
		t.Fatal("expected second eviction to be blocked")
	}
}
