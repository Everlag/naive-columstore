package main

import (
	"fmt"
	"time"

	"github.com/biogo/store/step"
)

type RLETime time.Time

func (t RLETime) Equal(e step.Equaler) bool {
	return t == e.(RLETime)
}

type TimeColumn struct {
	contents *step.Vector
	length   int
}

func NewTimeColumn(capacity int) TimeColumn {
	rle, err := step.New(0, capacity, RLETime{})
	if err != nil {
		panic(fmt.Sprintf("failed to create rle vector '%v'", err))
	}

	return TimeColumn{
		contents: rle,
		length:   0,
	}
}

func (c *TimeColumn) Push(values []time.Time) {
	for i, v := range values {
		c.contents.Set(c.length+i, RLETime(v))
	}
	c.length += len(values)
}

// Access the value stored at the named index
//
// This performs no range checking so an invalid
// index will cause a panic. The caller is responsible
// for ensuring index is within bounds
func (c *TimeColumn) Access(index int) time.Time {
	rleTime, err := c.contents.At(index)
	if err != nil {
		panic(fmt.Sprintf("failed to read value from vector '%v'", err))
	}

	return time.Time(rleTime.(RLETime))
}

// Determine all times happening after a certain point
// and return them positionally as a BoolColumn
func (c *TimeColumn) After(when time.Time) BoolColumn {

	results := NewBoolColumn()
	VecStepAfter := func(start, end int, rleTime step.Equaler) {
		fill := false

		t := time.Time(rleTime.(RLETime))
		if t.After(when) {
			fill = true
		}

		intermediate := make([]bool, end-start)
		for i := 0; i < len(intermediate); i++ {
			intermediate[i] = fill
		}

		results.Push(intermediate)
	}
	c.contents.Do(VecStepAfter)

	return results
}
