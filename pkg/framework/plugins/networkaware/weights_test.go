package networkaware

import (
	"context"
	"math"
	"testing"
)

func TestRequiredWeights(t *testing.T) {
	ptr := func(v float64) *float64 { return &v }
	for _, tc := range []struct {
		name   string
		values [3]*float64
		valid  bool
	}{
		{"missing", [3]*float64{}, false},
		{"missingZero", [3]*float64{ptr(1), nil, ptr(0)}, false},
		{"latencyOnly", [3]*float64{ptr(1), ptr(0), ptr(0)}, true},
		{"equal", [3]*float64{ptr(1.0 / 3), ptr(1.0 / 3), ptr(1.0 / 3)}, true},
		{"negative", [3]*float64{ptr(-0.1), ptr(0.6), ptr(0.5)}, false},
		{"zeroSum", [3]*float64{ptr(0), ptr(0), ptr(0)}, false},
		{"wrongSum", [3]*float64{ptr(0.2), ptr(0.3), ptr(0.4)}, false},
		{"nan", [3]*float64{ptr(math.NaN()), ptr(0), ptr(0)}, false},
		{"infinity", [3]*float64{ptr(math.Inf(1)), ptr(0), ptr(0)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateWeights(tc.values[0], tc.values[1], tc.values[2])
			if (err == nil) != tc.valid {
				t.Fatalf("validation error = %v, valid = %v", err, tc.valid)
			}
		})
	}
}

func TestWeightedCommunicationCost(t *testing.T) {
	metrics := nodeNetworkMetrics{latency: 100, bandwidth: 250, packetLoss: 2}
	maxima := nodeNetworkMetrics{latency: 200, bandwidth: 1000, packetLoss: 10}
	for _, tc := range []struct {
		name    string
		weights [3]float64
		want    float64
	}{
		{"latency", [3]float64{1, 0, 0}, 0.25},
		{"bandwidth", [3]float64{0, 1, 0}, 0.375},
		{"packetLoss", [3]float64{0, 0, 1}, 0.1},
		{"mixed", [3]float64{0.5, 0.3, 0.2}, 0.2575},
		{"equal", [3]float64{1.0 / 3, 1.0 / 3, 1.0 / 3}, 0.725 / 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := communicationCost(metrics, maxima, 50, 100, tc.weights)
			if math.Abs(got-tc.want) > 1e-12 {
				t.Fatalf("cost = %v, want %v", got, tc.want)
			}
			local := metrics
			local.zeroCost = true
			if got := communicationCost(local, maxima, 50, 100, tc.weights); got != 0 {
				t.Fatalf("local cost = %v", got)
			}
		})
	}
	for _, bw := range []float64{0, 1, 1000, math.NaN()} {
		metrics.bandwidth = bw
		if got := communicationCost(metrics, maxima, 50, 100, [3]float64{1, 0, 0}); got != 0.25 {
			t.Fatalf("disabled bandwidth changed cost: %v", got)
		}
	}
}

func TestNewRejectsMissingWeights(t *testing.T) {
	if _, err := New(context.Background(), &NetworkAwareArgs{}, nil); err == nil {
		t.Fatal("missing weights accepted")
	}
}
