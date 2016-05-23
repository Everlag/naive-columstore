package main

type FiniteString32Column struct {
	// Underlying storage exploits all properties of ints
	contents UInt32Column

	// Translation and inversion structure
	// for compressing strings into flat ints
	translator        map[string]uint32
	inverter          map[uint32]string
	translatorCounter uint32
}

func NewFiniteString32Column() FiniteString32Column {
	return FiniteString32Column{
		contents: NewUInt32Column(),

		translator:        make(map[string]uint32),
		inverter:          make(map[uint32]string),
		translatorCounter: 0,
	}
}

func (c *FiniteString32Column) Push(values []string) {
	translated := make([]uint32, len(values))

	for i, v := range values {
		key, ok := c.translator[v]
		if !ok {
			// Increment translator counter
			c.translatorCounter += 1
			key = c.translatorCounter

			// Add key to translator and inverter
			c.translator[v] = key
			c.inverter[key] = v
		}
		translated[i] = key
	}

	// Push to underlying storage
	c.contents.Push(translated)
}

// Access the value stored at the named index
//
// Provides some guarantees as Access method for the
// columns underlying storage regarding panicking
func (c *FiniteString32Column) Access(index int) string {
	// Fetch compact representation
	raw := c.contents.Access(index)

	// Return the readable string
	return c.inverter[raw]
}

// Determine all values equal a provided value
// and return them positionally as a BoolColumn
func (c *FiniteString32Column) Equal(value string) BoolColumn {

	// Translate the string into something
	// our underlying storage can handle
	translated := c.translator[value]

	return c.contents.Equal(translated)
}

// Determine all values equal to a member of the provided values
// and return them positionally as a BoolColumn
//
// Cannot handle empty slices, for single values call Equal instead
func (c *FiniteString32Column) Within(values []string) BoolColumn {

	var query *BoolColumn
	for _, v := range values {
		// Translate the string into something
		// our underlying storage can handle
		translated := c.translator[v]
		result := c.contents.Equal(translated)
		if query == nil {
			query = &result
		} else {
			result = query.OR(result)
			query = &result
		}
	}

	return *query
}
