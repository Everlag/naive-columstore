package main

import (
	"testing"
)

// Sanity test on RLEUInt32Column's sum
func TestRLEUInt32Sum(t *testing.T) {
	col := NewRLEUInt32Column(20)
	col.Push([]uint32{1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 60, 2, 1, 1})

	if col.Sum() != 82 {
		t.Fatalf("sum is not as expected 82 != %v", col.Sum())
	}

}
