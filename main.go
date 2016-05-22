// A toy implementation of a bespoke column oriented store
// to help figure out how certain concepts are implemented in reality.
package main

import (
	"fmt"
	"log"

	"bufio"
	"encoding/csv"
	"io"
	"os"

	"time"

	"strconv"

	"runtime"

	"github.com/bjwbell/gensimd/simd"
)

// This is really just documentation at this point...
type Column interface {
	Push([]interface{})
}

// A toy price database
type PriceDB struct {
	Names FiniteString32Column
	Sets  FiniteString32Column

	Prices UInt32Column

	Times TimeColumn
}

func NewPriceDB() PriceDB {
	return PriceDB{
		Names:  NewFiniteString32Column(),
		Sets:   NewFiniteString32Column(),
		Prices: NewUInt32Column(),
		Times:  NewTimeColumn(),
	}
}

// Materialize all PriceTuples that are truthy from
// the provided BoolColumn
//
// The assumption is that the provided BoolColumn is
// the result of a predicate executed on this database.
// As a result, we do no range checking.
//
// Passing a BoolColumn that was not created by this
// database instance has no guarantees regarding safety.
func (db *PriceDB) MaterializeFromBools(b BoolColumn) []PriceTuple {

	// Grab all indices which this column is truthy
	//
	// This is efficient on selective queries but terrible
	// against sparse queries where a list of FalseIndices
	// would work better to blacklist against. Oh well.
	positions := b.TruthyIndices()

	// Keep columns separate for as long as possible
	names := make([]string, len(positions))
	sets := make([]string, len(positions))
	prices := make([]uint32, len(positions))
	times := make([]time.Time, len(positions))
	for i, p := range positions {
		names[i] = db.Names.Access(p)
		sets[i] = db.Sets.Access(p)
		prices[i] = db.Prices.Access(p)
		times[i] = db.Times.Access(p)
	}

	// Stitch tuples back together into fancy structs
	tuples := make([]PriceTuple, len(positions))
	for i := range positions {
		tuples[i] = PriceTuple{
			Name:  names[i],
			Set:   sets[i],
			Price: prices[i],
			Time:  times[i],
		}
	}

	return tuples

}

// Stream a CSV into the database
//
// This reads a CSV in as 4k clumps then adds it to the database
func (db *PriceDB) IngestCSV(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}

	buffered := bufio.NewReader(f)
	parser := csv.NewReader(buffered)

	tuples := make([]PriceTuple, 0)
	var record []string
	for err != io.EOF {
		record, err = parser.Read()
		if err != nil && err != io.EOF {
			return err
		}

		// Ignore header and footer...
		if len(record) == 0 || record[3] == "price" {
			continue
		}

		tuple, err := RawTuple(record).ToPrice()
		if err != nil {
			return err
		}
		tuples = append(tuples, tuple)

		if len(tuples) >= 4096 {
			db.Push(tuples)
			tuples = make([]PriceTuple, 0)
		}

	}

	// Clear off the remaining tuples
	db.Push(tuples)

	return nil
}

func (db *PriceDB) Push(values []PriceTuple) {
	names := make([]string, len(values))
	sets := make([]string, len(values))
	prices := make([]uint32, len(values))
	times := make([]time.Time, len(values))
	for i, p := range values {
		names[i] = p.Name
		sets[i] = p.Set
		prices[i] = p.Price
		times[i] = p.Time
	}
	db.Names.Push(names)
	db.Sets.Push(sets)
	db.Prices.Push(prices)
	db.Times.Push(times)
}

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

type ColumnFlavor uint32

const (
	UInt32 ColumnFlavor = iota
	String
)

func main() {
	fmt.Println(simd.Available())

	db := NewPriceDB()

	err := db.IngestCSV("prices.csv")
	if err != nil {
		log.Fatal(err)
	}

	// Block oriented
	stats := runtime.MemStats{}
	runtime.ReadMemStats(&stats)
	fmt.Println(stats.Alloc/1024,
		stats.Lookups, stats.Mallocs, stats.Frees)

	// tuples, err := parseTuples("prices.csv")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println(len(tuples))
	// parsed, err := tuples[20].ToPrice()
	// fmt.Println(parsed, err)
}

type RawTuple []string

type PriceTuple struct {
	Name, Set string
	Price     uint32

	Time time.Time
}

// Convert a raw tuple to a price tuple
func (r RawTuple) ToPrice() (PriceTuple, error) {
	if len(r) < 4 {
		return PriceTuple{}, fmt.Errorf("invalid price tuples")
	}

	tuple := PriceTuple{
		Name: r[0],
		Set:  r[1],
	}

	price64, err := strconv.ParseUint(r[3], 10, 32)
	if err != nil {
		return PriceTuple{}, fmt.Errorf("malformed price '%v'", err)
	}
	tuple.Price = uint32(price64)

	when, err := time.Parse("2006-01-02 15:04:05", r[2])
	if err != nil {
		return PriceTuple{}, fmt.Errorf("malformed time '%v'", r[2])
	}
	tuple.Time = when

	return tuple, nil
}

// Parse tuples from a provided file encoded as csv
func parseTuples(file string) ([]RawTuple, error) {
	f, err := os.Open(file)
	if err != nil {
		fmt.Println("uh")
		log.Fatalf("failed to open prices.csv '%v'", err)
	}

	buffered := bufio.NewReader(f)
	parser := csv.NewReader(buffered)

	tuples := make([]RawTuple, 0)
	var record []string
	for err != io.EOF {
		record, err = parser.Read()
		if err != nil && err != io.EOF {
			return nil, err
		}

		tuples = append(tuples, RawTuple(record))
	}

	return tuples, nil
}
