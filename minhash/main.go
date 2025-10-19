package main

import (
	"514-assignment/functions"
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// compute both estimates using helper functions
func computeEstimates(minVals []float64, medianGroupSize int) (avgEstimate, medianEstimate float64) {
	sum := 0.0
	for _, v := range minVals {
		sum += functions.GetEstimate(v)
	}
	avgEstimate = sum / float64(len(minVals))
	medianEstimate = functions.MedianOfMeans(minVals, medianGroupSize)
	return
}

// processStream consumes a given stream of integers
func processStream(stream []int64, outputFile string, numHashes, printEvery, medianGroupSize int) {
	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	minVals := make([]float64, numHashes)
	for i := range minVals {
		minVals[i] = 1.0
	}

	seen := make(map[int64]struct{}) // track unique integers
	totalUnique := 0

	for i, n := range stream {
		// track unique elements
		if _, exists := seen[n]; !exists {
			seen[n] = struct{}{}
			totalUnique++
		}

		// update hash sketches
		for h := 0; h < numHashes; h++ {
			hashVal := functions.HashToFloat(n, h)
			minVals[h] = functions.MinimumFunc(minVals[h], hashVal)
		}

		if (i+1)%printEvery == 0 {
			avgEstimate, medianEstimate := computeEstimates(minVals, medianGroupSize)
			line := fmt.Sprintf(
				"Processed %d numbers: avg=%.2f, median=%.2f, uniques=%d\n",
				i+1, avgEstimate, medianEstimate, totalUnique,
			)
			fmt.Print(line)
			writer.WriteString(line)
		}
	}

	avgEstimate, medianEstimate := computeEstimates(minVals, medianGroupSize)
	final := fmt.Sprintf(
		"Final after %d numbers: avg=%.2f, median=%.2f, total uniques=%d\n",
		len(stream), avgEstimate, medianEstimate, totalUnique,
	)
	fmt.Print(final)
	writer.WriteString(final)
}

func main() {
	printEvery := 1000
	medianGroupSize := 5
	totalCount := 10_000_000 // generate 10 million numbers

	// ✅ generate ONE stream for all hash configurations
	stream := make([]int64, totalCount)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < totalCount; i++ {
		stream[i] = rand.Int63n(int64(totalCount / 2)) // duplicates likely
	}

	hashCounts := []int{1, 5, 50, 500, 5000}

	for _, H := range hashCounts {
		outputFile := fmt.Sprintf("output/estimate_sharedstream_%d_hashes.txt", H)
		fmt.Printf("\nRunning with H=%d, T=%d, total=%d...\n", H, medianGroupSize, totalCount)
		processStream(stream, outputFile, H, printEvery, medianGroupSize)
	}
}
