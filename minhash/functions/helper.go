package functions

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"
)

// HashToFloat deterministically maps an integer to [0,1) using optional hash index, mimics multiple hash functions.
func HashToFloat(n int64, hashIndex int) float64 {
	h := sha256.New()
	// Write directly as bytes to avoid string allocations
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(n))
	binary.BigEndian.PutUint64(buf[8:], uint64(hashIndex))
	h.Write(buf[:])
	hashBytes := h.Sum(nil)
	val := binary.BigEndian.Uint64(hashBytes[:8])
	return float64(val) / float64(math.MaxUint64)
}

func MinimumFunc(a, b float64) float64 {
	return math.Min(a, b)
}

func GetEstimate(minVal float64) float64 {
	return math.Ceil(1/minVal - 1)
}

func MedianOfMeans(minVals []float64, medianGroupSize int) float64 {
	numHashes := len(minVals)

	// simple average if group size is invalid
	if medianGroupSize <= 0 || medianGroupSize >= numHashes {
		sum := 0.0
		for _, v := range minVals {
			sum += GetEstimate(v)
		}
		return sum / float64(numHashes)
	}

	numGroups := (numHashes + medianGroupSize - 1) / medianGroupSize // Round up to include remainder
	groupAverages := make([]float64, numGroups)

	for g := 0; g < numGroups; g++ {
		start := g * medianGroupSize
		end := start + medianGroupSize
		if end > numHashes {
			end = numHashes // Handle remainder group
		}
		sum := 0.0
		for i := start; i < end; i++ {
			sum += GetEstimate(minVals[i])
		}
		groupAverages[g] = sum / float64(end-start)
	}

	sort.Float64s(groupAverages)

	if numGroups%2 == 1 {
		return groupAverages[numGroups/2]
	}

	mid := numGroups / 2
	return (groupAverages[mid-1] + groupAverages[mid]) / 2
}