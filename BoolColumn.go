package main

import "github.com/willf/bitset"

// A typically temporary column efficiently
// storing boolean values
type BoolColumn struct {

	// Third party package feature stupid fast speeds
	contents *bitset.BitSet

	end uint
}

func NewBoolColumn() BoolColumn {
	return BoolColumn{
		contents: bitset.New(1000000),
		end:      0,
	}
}

func (c *BoolColumn) Push(values []bool) {

	start := c.end
	for i, v := range values {
		c.contents.SetTo(start+uint(i), v)
	}

	c.end += uint(len(values))
}

// Push a length of true booleans onto the column
//
// Useful for unpacking compressed runs.
func (c *BoolColumn) PushTrue(length int) {

	start := c.end
	for i := 0; i < length; i++ {
		c.contents.Set(start + uint(i))
	}
	c.end += uint(length)
}

// Push a length of false booleans onto the column
//
// Useful for unpacking compressed runs.
func (c *BoolColumn) PushFalse(length int) {

	start := c.end
	for i := 0; i < length; i++ {
		c.contents.Clear(start + uint(i))
	}
	c.end += uint(length)
}

// Negate every value of the column and return it
func (c *BoolColumn) Not() BoolColumn {
	c.contents = c.contents.Complement()

	return *c
}

// AND every value of this column and another column
// that is assumed to be of equal length and organization
// and overwrite this column
func (c *BoolColumn) AND(other BoolColumn) BoolColumn {

	c.contents.InPlaceIntersection(other.contents)

	return *c
}

// OR every value of this column and another column
// that is assumed to be of equal length and organization
// and overwrite this column
func (c *BoolColumn) OR(other BoolColumn) BoolColumn {

	c.contents.InPlaceUnion(other.contents)

	return *c
}

// Returns all indices for which this column
// has truthy values
func (c *BoolColumn) TruthyIndices() []int {

	indices := make([]int, 0)

	// Shorthand, save some horizontal space
	set := c.contents

	for i, found := set.NextSet(0); found; i, found = set.NextSet(i + 1) {
		indices = append(indices, int(i))
	}

	return indices
}
