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
}

func NewPriceDB() PriceDB {
	return PriceDB{
		Names: NewFiniteString32Column(),
	}
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
	for i, p := range values {
		names[i] = p.Name
		sets[i] = p.Set
		prices[i] = p.Price
	}
	db.Names.Push(names)
	db.Sets.Push(sets)
	db.Prices.Push(prices)
}

type BoolColumn struct {
	blocks []BoolBlock

	latestBlock BoolBlock

	BlockSize int
}

func NewBoolColumn(BlockSize int) BoolColumn {
	return BoolColumn{
		BlockSize: BlockSize,

		blocks: make([]BoolBlock, 0),

		latestBlock: BoolBlock{},
	}
}

func (c *BoolColumn) Push(values []bool) {
	// Check against block sizing
	if c.latestBlock.Length()+len(values) > c.BlockSize {
		// Lazily transfer full block to full storage
		c.blocks = append(c.blocks, c.latestBlock)

		// Grab a new block
		c.latestBlock = BoolBlock{}
	}
	// Add our values to the latest block
	c.latestBlock.Push(values)
}

type BoolBlock struct {
	contents []bool
}

// Get current length for a block
//
// Direct access to contents is discouraged due to future
// compression that may be applied
func (b *BoolBlock) Length() int {
	return len(b.contents)
}

func (b *BoolBlock) Push(values []bool) {
	b.contents = append(b.contents, values...)
}

type UInt32Column struct {
	blocks []UInt32Block

	latestBlock UInt32Block

	BlockSize int
}

func NewUInt32Column(BlockSize int) UInt32Column {
	return UInt32Column{
		BlockSize: BlockSize,

		blocks: make([]UInt32Block, 0),

		latestBlock: UInt32Block{},
	}
}

func (c *UInt32Column) Push(values []uint32) {
	// Check against block sizing
	if c.latestBlock.Length()+len(values) > c.BlockSize {
		// Lazily transfer full block to full storage
		c.blocks = append(c.blocks, c.latestBlock)

		// Grab a new block
		c.latestBlock = UInt32Block{}
	}
	// Add our values to the latest block
	c.latestBlock.Push(values)
}

// A block of a UInt32Column
type UInt32Block struct {
	contents []uint32
}

// Get current length for a block
//
// Direct access to contents is discouraged due to future
// compression that may be applied
func (b *UInt32Block) Length() int {
	return len(b.contents)
}

func (b *UInt32Block) Push(values []uint32) {
	b.contents = append(b.contents, values...)
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
		contents: NewUInt32Column(8192),

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
		}
		translated[i] = key
	}

	// Push to underlying storage
	c.contents.Push(translated)
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

	// Time
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
