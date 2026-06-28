# HighNetworkCost

`HighNetworkCost` evicts eligible pods with achievable communication-cost
reduction on feasible alternative nodes until the configured eviction limits are
reached.

The cost model mirrors the Sophos scheduler `NetworkAware` score:

```text
sum(peer pods) trafficRatio * networkCost

trafficRatio = min(traffic(application, peer application) / maxPeerTraffic, 1)
networkCost  = latencyRatio + bandwidthRatio + packetLossRatio

latencyRatio    = min(node_latency(current, peer) / maxPeerLatency, 1)
bandwidthRatio  = 1 - min(node_bandwidth(current, peer) / maxPeerBandwidth, 1)
packetLossRatio = min(packet_loss(current, peer) / maxPeerPacketLoss, 1)
```

Node latency, bandwidth, and packet loss come from `network-latency.<node>`,
`network-bandwidth.<node>`, and `packet-loss.<node>` annotations. Application
traffic comes from `traffic.<app>` Deployment annotations. The maximum peer
traffic and peer-node metric values are derived from the candidate placement
set for the pod, matching the scheduler `NetworkAware` PreScore model. Pods
must share the same `group` label and, when the pod has an `index` label, peers
must have `index <= pod.index` to contribute to one another's cost. Alternative
costs are calculated only for the nodes that pass the descheduler `NodeFit`
feasibility check.

```yaml
- name: HighNetworkCost
  args:
    minPodAgeSeconds: 60
    minCommunicationCost: 0
    minCostImprovement: 0
    selectionPolicy: WeightedRandom
    namespaces:
      include: [default]
    labelSelector:
      matchLabels:
        group: onlineboutique
```

Use `DefaultEvictor` protections and global eviction limits as usual. A minimum
pod age is recommended to prevent rapid re-eviction after rescheduling.
`minCostImprovement` can suppress moves whose normalized cost reduction is too
small. `selectionPolicy` controls the order in which eligible pods are evicted:
`HighestImprovement` picks only the pod with the largest
`currentCost - bestAlternativeCost`, while `WeightedRandom` samples eligible
pods without replacement with probability proportional to that improvement.
Current cost and pod identity are used as deterministic tie-breakers before selection. Feasibility
checks cover node selectors/required affinity, taints,
inter-pod anti-affinity, unschedulable nodes, and available requested resources.
Use PodDisruptionBudgets for workload availability constraints during eviction.
The scheduler still makes the final placement decision after eviction.

Run the descheduler with `--v=3` to trace candidate selection, current costs,
node feasibility checks, projected costs, improvements, and the final eviction
decision. `--v=4` additionally logs pods rejected by the eviction filters.
