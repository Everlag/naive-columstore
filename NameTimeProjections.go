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
