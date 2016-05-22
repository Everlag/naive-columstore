package main

// A typically temporary column efficiently
// storing boolean values
type BoolColumn struct {
	contents []bool
}

func NewBoolColumn() BoolColumn {
	return BoolColumn{
		contents: make([]bool, 0),
	}
}

func (c *BoolColumn) Push(values []bool) {
	c.contents = append(c.contents, values...)
}

// Negate every value of the column and return it
func (c *BoolColumn) Not() BoolColumn {
	for i, v := range c.contents {
		c.contents[i] = !v
	}

	return *c
}

// AND every value of this column and another column
// that is assumed to be of equal length and organization
// and overwrite this column
func (c *BoolColumn) AND(other BoolColumn) BoolColumn {
	for i, v := range c.contents {
		c.contents[i] = v && other.contents[i]
	}

	return *c
}

// Returns all indices for which this column
// has truthy values
func (c *BoolColumn) TruthyIndices() []int {

	indices := make([]int, 0)
	for i, v := range c.contents {
		if v {
			indices = append(indices, i)
		}
	}

	return indices
}
