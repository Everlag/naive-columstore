package main

import (
	"testing"
)

var RLEUInt32TestSlice []uint32 = []uint32{1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 60, 2, 1, 1}

// Sanity test on RLEUInt32Column's sum
func TestRLEUInt32Sum(t *testing.T) {
	col := NewRLEUInt32Column(len(RLEUInt32TestSlice))
	col.Push(RLEUInt32TestSlice)

	if col.Sum() != 82 {
		t.Fatalf("sum is not as expected 82 != %v", col.Sum())
	}

}

func TestRLEUInt32Equal(t *testing.T) {
	col := NewRLEUInt32Column(len(RLEUInt32TestSlice))
	col.Push(RLEUInt32TestSlice)

	// Compute indices
	reference := []int{6, 7, 8, 12}
	query := col.Equal(2)
	computed := query.TruthyIndices()
	if len(reference) != len(computed) {
		t.Fatalf("equal has unexpected result '%v'", computed)
	}

	for i, refIndex := range reference {
		if computed[i] != refIndex {
			t.Fatalf("equal has unexpected result '%v'", computed)
		}
	}

}
