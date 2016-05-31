package main

import (
	"fmt"
	"github.com/biogo/store/step"
)

type RLEUint32 uint32

func (t RLEUint32) Equal(e step.Equaler) bool {
	return t == e.(RLEUint32)
}

// A run length encoded Uint32 column supporting a subset
// of the operations applied to a standard UInt32Column
//
// This is typically the storage end of a FiniteString32Column
// rather than directly storing user's uints
type RLEUInt32Column struct {
	contents *step.Vector
	length   int
}

func NewRLEUInt32Column(capacity int) RLEUInt32Column {
	rle, err := step.New(0, capacity, RLEUint32(0))
	if err != nil {
		panic(fmt.Sprintf("failed to create rle vector '%v'", err))
	}

	return RLEUInt32Column{
		contents: rle,
		length:   0,
	}
}

func (c *RLEUInt32Column) Push(values []uint32) {
	for i, v := range values {
		c.contents.Set(c.length+i, RLEUint32(v))
	}
	c.length += len(values)
}

// Access the value stored at the named index
//
// This performs no range checking so an invalid
// index will cause a panic. The caller is responsible
// for ensuring index is within bounds
func (c *RLEUInt32Column) Access(index int) uint32 {
	rleVal, err := c.contents.At(index)
	if err != nil {
		panic(fmt.Sprintf("failed to read value from vector '%v'", err))
	}

	return uint32(rleVal.(RLEUint32))
}

// Determine the length of this column
func (c *RLEUInt32Column) Length() int {
	return c.contents.Len()
}

// Sum all values in the column
func (c *RLEUInt32Column) Sum() uint64 {
	var result uint64

	VecStepAfter := func(start, end int, rleVal step.Equaler) {
		v := uint64(rleVal.(RLEUint32))
		length := uint64(end - start)

		result = result + length*v
	}
	c.contents.Do(VecStepAfter)

	return result
}

// Determine all values equal a provided value
// and return them positionally as a BoolColumn
func (c *RLEUInt32Column) Equal(value uint32) BoolColumn {
	results := NewBoolColumn()
	VecStepAfter := func(start, end int, rleVal step.Equaler) {

		length := end - start

		v := uint32(rleVal.(RLEUint32))
		if v == value {
			results.PushTrue(length)
		} else {
			results.PushFalse(length)
		}

	}
	c.contents.Do(VecStepAfter)

	return results
}
