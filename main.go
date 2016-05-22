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

	"sort"

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

// Materialize all PriceTuples that are truthy from the provided
// BoolColumn then sort them in descending order of time.
func (db *PriceDB) MaterializeTimeSortAsc(b BoolColumn) []PriceTuple {

	// Materialize
	tuples := db.MaterializeFromBools(b)

	// Sort by time
	sort.Sort(TimeOrderedTuples(tuples))

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

type TimeOrderedTuples []PriceTuple

func (a TimeOrderedTuples) Len() int {
	return len(a)
}
func (a TimeOrderedTuples) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a TimeOrderedTuples) Less(i, j int) bool {
	return a[i].Time.Before(a[j].Time)
}
