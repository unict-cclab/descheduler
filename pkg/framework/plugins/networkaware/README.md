# NetworkAware cost weights

Both scheduler and descheduler require `latencyWeight`, `bandwidthWeight` and
`packetLossWeight` in the NetworkAware plugin arguments. Each value must be finite
and non-negative. Missing values are rejected. Values above 1 are allowed; no
sum constraint or weight normalization is applied. Zero disables a term, and all
three weights may be zero to disable the communication cost entirely.

Equal weighting, preserving the previous cost scale:

```yaml
latencyWeight: 1
bandwidthWeight: 1
packetLossWeight: 1
```

Latency only:

```yaml
latencyWeight: 1
bandwidthWeight: 0
packetLossWeight: 0
```

The cost per peer is the normalized traffic multiplied by the weighted sum of
normalized latency, `1 - normalized bandwidth`, and normalized packet loss.
Metric normalization and the same-zone exclusion are unchanged. Use identical
weights in scheduler and descheduler.

Existing configurations must explicitly specify the three weights before using
this version. Setting them all to 1 preserves the previous communication costs
and descheduler thresholds. Changing their absolute scale changes the meaning
of `minCommunicationCost` and `minCostImprovement`.
