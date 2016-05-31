package main

import (
	"testing"

	"math/rand"
	"time"
)

var TimeRefSliceLength int = 30

// Compute a time slice with deterministically
// random offsets
func GetTimeRefSlice() []time.Time {

	// Deterministic offsets
	r := rand.New(rand.NewSource(91235))

	// Start of time, convenient
	last := time.Unix(0, 0)

	result := make([]time.Time, 0)
	for i := 0; i < TimeRefSliceLength; i++ {
		result = append(result, last)
		// Set last to be the previous last plus some number
		// of hours that ensure this is strictly ascending
		last = last.Add(time.Hour*time.Duration(r.Uint32()>>19) + 1)
	}

	return result
}

// Sanity test on TimeColumn's After
func TestTimeAfter(t *testing.T) {

	ref := GetTimeRefSlice()

	col := NewTimeColumn()
	col.Push(ref)

	// Middle item of slice
	middleIndex := TimeRefSliceLength / 2
	query := col.After(ref[middleIndex])
	computed := query.TruthyIndices()

	// We know how long the result should be
	if (TimeRefSliceLength-middleIndex)-1 != len(computed) {
		t.Fatalf("after has unexpected length %v != %v",
			TimeRefSliceLength-middleIndex, len(computed))
	}

	// Check every item computed is after the middle
	for _, refIndex := range computed {
		if refIndex < middleIndex {
			t.Fatalf("strictly increasing item in subset found as After")
		}
	}

}

// Sanity test on TimeColumn's ANDAfter
func TestTimeANDAfter(t *testing.T) {

	ref := GetTimeRefSlice()

	col := NewTimeColumn()
	col.Push(ref)

	// Middle item of slice
	middleIndex := TimeRefSliceLength / 2
	// Start from all truthy bools
	query := NewBoolColumn()
	query.PushTrue(len(ref))
	// Filter out bad results
	col.ANDAfter(ref[middleIndex], query)
	computed := query.TruthyIndices()

	// We know how long the result should be
	if (TimeRefSliceLength-middleIndex)-1 != len(computed) {
		t.Fatalf("after has unexpected length %v != %v",
			TimeRefSliceLength-middleIndex, len(computed))
	}

	// Check every item computed is after the middle
	for _, refIndex := range computed {
		if refIndex < middleIndex {
			t.Fatalf("strictly increasing item in subset found as After")
		}
	}

}
