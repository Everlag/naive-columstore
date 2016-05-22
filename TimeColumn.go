package main

import (
	"time"
)

type TimeColumn struct {
	contents []time.Time
}

func NewTimeColumn() TimeColumn {
	return TimeColumn{
		contents: make([]time.Time, 0),
	}
}

func (c *TimeColumn) Push(values []time.Time) {
	c.contents = append(c.contents, values...)
}

// Access the value stored at the named index
//
// This performs no range checking so an invalid
// index will cause a panic. The caller is responsible
// for ensuring index is within bounds
func (c *TimeColumn) Access(index int) time.Time {
	return c.contents[index]
}

// Determine all times happening after a certain point
// and return them positionally as a BoolColumn
func (c *TimeColumn) After(when time.Time) BoolColumn {
	results := NewBoolColumn()
	for _, v := range c.contents {
		results.Push([]bool{v.After(when)})
	}

	return results
}
