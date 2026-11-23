# NetworkAware cost weights

Both scheduler and descheduler require `latencyWeight`, `bandwidthWeight` and
`packetLossWeight` in the NetworkAware plugin arguments. Each value must be finite
and between 0 and 1. Their sum must equal 1 within an absolute tolerance of 1e-9.
Missing values are rejected; explicitly set zero to disable a term.

Equal weighting:

```yaml
latencyWeight: 0.3333333333333333
bandwidthWeight: 0.3333333333333333
packetLossWeight: 0.3333333333333333
```

Latency only:

```yaml
latencyWeight: 1
bandwidthWeight: 0
packetLossWeight: 0
```

The cost per peer is the normalized traffic multiplied by the weighted sum of
normalized latency, `1 - normalized bandwidth`, and normalized packet loss.
The same-zone exclusion and metric normalization are unchanged. Use identical
weights in scheduler and descheduler.

Existing configurations must specify the three weights before using this version.
Equal weights preserve the relative order of costs but divide their old values
by three. To retain equivalent descheduler thresholds, divide
`minCommunicationCost` and `minCostImprovement` by three as well.
