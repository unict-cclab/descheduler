# NetworkAware

`NetworkAware` evicts pods using the same communication-cost model as the
Sophos scheduler `NetworkAware` plugin, but it evaluates pods by `index` layer
to encourage staged convergence.

Each descheduler cycle starts at `minPodIndex` and scans every index upward.
Pods with a feasible cost-improving move are attempted by highest
`currentCost - bestAlternativeCost` within each layer, then processing continues
with the next layer. Failed eviction attempts are skipped and do not prevent
later pods or layers from being considered. Evicted pods enter the scheduler
queue in layer order: every eviction request in a layer is accepted or rejected
before the next layer is evaluated. This allows the scheduler to place lower
indexes before higher indexes without making the descheduler wait for pod
deletion or rescheduling to finish.

```yaml
- name: NetworkAware
  args:
    minPodAgeSeconds: 60
    minPodIndex: 0
    minCommunicationCost: 0
    minCostImprovement: 0
    ignoreSameZoneNetworkCost: true
    # Optional. If omitted, every improving pod across all index layers may be
    # evicted, subject to DefaultEvictor and global descheduler limits.
    maxPodsToEvict: 3
    namespaces:
      include: [default]
    labelSelector:
      matchLabels:
        group: onlineboutique
```

Pods without a valid integer `index` label are ignored. The cost model uses
`traffic.<app>` Deployment annotations and node annotations named
`network-latency.<node>`, `network-bandwidth.<node>`, and `packet-loss.<node>`.
Migration candidates in the pod's current zone are excluded before node-fit
checks and scoring, so an eviction is proposed only when a feasible target in
a different zone has sufficient cost improvement. Nodes with a missing zone
label remain eligible because their zone relationship cannot be determined.
When `ignoreSameZoneNetworkCost` is true, communication between nodes with the
same non-empty `topology.kubernetes.io/zone` label contributes zero network
cost, making same-zone placement equivalent to same-node placement for this
model.
