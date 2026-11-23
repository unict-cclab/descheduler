package networkaware

import (
	"fmt"
	"math"
)

func validateWeights(latency, bandwidth, packetLoss *float64) ([3]float64, error) {
	values := [3]*float64{latency, bandwidth, packetLoss}
	names := [3]string{"latencyWeight", "bandwidthWeight", "packetLossWeight"}
	var weights [3]float64
	for i, value := range values {
		if value == nil {
			return weights, fmt.Errorf("%s is required", names[i])
		}
		if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 || *value > 1 {
			return weights, fmt.Errorf("%s must be finite and between 0 and 1", names[i])
		}
		weights[i] = *value
	}
	if math.Abs(weights[0]+weights[1]+weights[2]-1) > 1e-9 {
		return weights, fmt.Errorf("latencyWeight, bandwidthWeight and packetLossWeight must sum to 1")
	}
	return weights, nil
}
