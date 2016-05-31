package main

import (
	"sort"
	"time"
)

// Define a projection sorted first by name
// then by time
type NameTimeProjection struct {
	Names RLEFiniteString32Column
	Sets  FiniteString32Column

	Prices UInt32Column

	Times TimeColumn
}

// Generate a NameTimeProjection from a fully
// filled PriceDB.
//
// I'm not handling updates so this is fine... in theory.
func NameTimeProjectionFromPriceDB(db PriceDB) NameTimeProjection {
	proj := NameTimeProjection{
		Names:  NewRLEFiniteString32Column(),
		Sets:   NewFiniteString32Column(),
		Prices: NewUInt32Column(),
		Times:  NewTimeColumn(),
	}

	// Determine full length of database
	length := db.Prices.Length()

	// Fetch fully materialized tuples
	//
	// TODO: avoid full materialization of the entire
	// freaking dataset...
	tuples := make([]PriceTuple, length)
	for i := 0; i < length; i++ {
		tuple := PriceTuple{
			Name:  db.Names.Access(i),
			Set:   db.Sets.Access(i),
			Price: db.Prices.Access(i),
			Time:  db.Times.Access(i),
		}

		tuples[i] = tuple
	}

	// Sort the tuples according to Name then time
	sort.Sort(NameTimeOrderedTuples(tuples))

	proj.Push(tuples)

	return proj
}

func (proj *NameTimeProjection) Push(values []PriceTuple) {
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
	proj.Names.Push(names)
	proj.Sets.Push(sets)
	proj.Prices.Push(prices)
	proj.Times.Push(times)
}

// Materialize all PriceTuples that are truthy from
// the provided BoolColumn
//
// The assumption is that the provided BoolColumn is
// the result of a predicate executed on this database.
// As a result, we do no range checking.
//
// Passing a BoolColumn that was not created by this
// projection instance has no guarantees regarding safety.
func (proj *NameTimeProjection) MaterializeFromBools(b BoolColumn) []PriceTuple {

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
		names[i] = proj.Names.Access(p)
		sets[i] = proj.Sets.Access(p)
		prices[i] = proj.Prices.Access(p)
		times[i] = proj.Times.Access(p)
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

// Query for the latest group of prices in the column for a card
func (proj *NameTimeProjection) Latest(name string) BoolColumn {
	// Determine latest names and their indices
	query := proj.Names.Equal(name)
	truthy := query.TruthyIndices()

	if len(truthy) == 0 {
		return NewBoolColumn()
	}

	// Find last truthy index, our sort invariant
	// has that as the point with the latest time
	lastIndex := truthy[len(truthy)-1]
	// Offset latestTime by a small amount so we can
	// be perform an After easily
	latestTime := proj.Times.Access(lastIndex).Add(-time.Minute)

	proj.Times.ANDAfter(latestTime, query)

	return query
}

type NameTimeOrderedTuples []PriceTuple

func (a NameTimeOrderedTuples) Len() int {
	return len(a)
}
func (a NameTimeOrderedTuples) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a NameTimeOrderedTuples) Less(i, j int) bool {
	first := a[i]
	second := a[j]

	// If names are same, fall back to time based
	if first.Name == second.Name {
		return first.Time.Before(second.Time)
	}

	return first.Name < second.Name
}
