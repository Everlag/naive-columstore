package main

type UInt32Column struct {
	contents []uint32
}

func NewUInt32Column() UInt32Column {
	return UInt32Column{
		contents: make([]uint32, 0),
	}
}

func (c *UInt32Column) Push(values []uint32) {
	c.contents = append(c.contents, values...)
}

// Access the value stored at the named index
//
// This performs no range checking so an invalid
// index will cause a panic. The caller is responsible
// for ensuring index is within bounds
func (c *UInt32Column) Access(index int) uint32 {
	return c.contents[index]
}

// Determine the length of this column
func (c *UInt32Column) Length() int {
	return len(c.contents)
}

// Determine the difference between a provided value
// and each value in the column as {column} - {value}
func (c *UInt32Column) Delta(value uint32) UInt32Column {
	results := NewUInt32Column()

	for _, v := range c.contents {
		results.Push([]uint32{v - value})
	}

	return results
}

// Sum all values in the column
func (c *UInt32Column) Sum() uint64 {
	var result uint64
	for _, v := range c.contents {
		result = result + uint64(v)
	}

	return result
}

// Determine all values less than a provided value
// and return them positionally as a BoolColumn
func (c *UInt32Column) Less(value uint32) BoolColumn {
	results := NewBoolColumn()
	for _, v := range c.contents {
		results.Push([]bool{v < value})
	}

	return results
}

// Determine all values less than a provided value
// and return them positionally as a BoolColumn
func (c *UInt32Column) More(value uint32) BoolColumn {
	less := c.Less(value)
	return less.Not()
}

// Determine all values equal a provided value
// and return them positionally as a BoolColumn
func (c *UInt32Column) Equal(value uint32) BoolColumn {
	results := NewBoolColumn()
	for _, v := range c.contents {
		results.Push([]bool{v == value})
	}

	return results
}
