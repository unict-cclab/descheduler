# HighNetworkCost

`HighNetworkCost` evicts at most one pod per descheduling cycle: the eligible
pod with the greatest achievable communication-cost reduction on a feasible
alternative node.

The cost model mirrors the Sophos scheduler `NetworkAware` score:

```text
sum(peer pods) node_latency(current, peer) * traffic(application, peer application)
```

Node latency comes from `network-latency.<node>` annotations and application
traffic comes from `traffic.<app>` Deployment annotations. Pods must share the
same `group` label to contribute to one another's cost.

```yaml
- name: HighNetworkCost
  args:
    minPodAgeSeconds: 60
    minCommunicationCost: 0
    minCostImprovement: 0
    namespaces:
      include: [default]
    labelSelector:
      matchLabels:
        group: onlineboutique
```

Use `DefaultEvictor` protections and global eviction limits as usual. A minimum
pod age is recommended to prevent rapid re-eviction after rescheduling.
`minCostImprovement` can suppress moves whose absolute cost reduction is too
small. Eligible pods are ranked by `currentCost - bestAlternativeCost`, with
current cost and pod identity used as deterministic tie-breakers. Feasibility
checks cover node selectors/required affinity, taints,
inter-pod anti-affinity, unschedulable nodes, and available requested resources.
The scheduler still makes the final placement decision after eviction.

Run the descheduler with `--v=3` to trace candidate selection, current costs,
node feasibility checks, projected costs, improvements, and the final eviction
decision. `--v=4` additionally logs pods rejected by the eviction filters.
