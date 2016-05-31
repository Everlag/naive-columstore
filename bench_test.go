package main

import (
	"testing"

	"time"
)

// Always save benchmark results here to
// ensure the compiler doesn't optimize them away
var garbage PriceDB
var uselessTuples []PriceTuple
var garbageQuery BoolColumn
var trashUint64 uint64

var BenchDB *PriceDB
var BenchNameTimeProjection *NameTimeProjection

func setupPriceBenchmark(b *testing.B) PriceDB {
	if BenchDB != nil {
		return *BenchDB
	}
	db := NewPriceDB()

	err := db.IngestCSV("prices.csv")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	return db
}

func setupNameTimeProjectionBench(b *testing.B) NameTimeProjection {
	// Make sure the underlying db was created first
	db := setupPriceBenchmark(b)
	if BenchNameTimeProjection == nil {
		proj := NameTimeProjectionFromPriceDB(db)
		BenchNameTimeProjection = &proj
	}

	return *BenchNameTimeProjection
}

func BenchmarkUint32More(b *testing.B) {

	db := setupPriceBenchmark(b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		db.Prices.More(120)
	}

	garbage = db
}

func BenchmarkUint32Less(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		db.Prices.Less(120)
	}

	garbage = db

}

func BenchmarkUint32Delta(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		db.Prices.Delta(120)
	}

	garbage = db
}

func BenchmarkUint32Sum(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		trashUint64 += db.Prices.Sum()
	}
}

// AND two queries together
//
// This is an important operation as it allows
// merging two predicates.
func BenchmarkBoolAND(b *testing.B) {
	db := setupPriceBenchmark(b)

	// Find prices higher and lower than our
	// current threshold
	lowerBound := db.Prices.More(100)
	upperBound := db.Prices.Less(1000)
	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {

		// Determine values between and materialize
		garbageQuery = upperBound.AND(lowerBound)
	}
}

// Select all prices more than 100 cents = $1
func BenchmarkSelectAllMoreThanDollar(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {
		garbageQuery = db.Prices.More(100)
	}
}

// Select all names equal to 'Griselbrand' or 'Avacyn, Angel of Hope'
func BenchmarkFiniteString32Within(b *testing.B) {
	db := setupPriceBenchmark(b)

	names := []string{
		"Griselbrand",
		"Avacyn, Angel of Hope",
	}

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {
		garbageQuery = db.Names.Within(names)
	}
}

// Select all prices more than 100 cents = $1 and
// rematerialize them into tuples
func BenchmarkSelectAllMoreThanDollarMaterial(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {
		query := db.Prices.More(100)
		uselessTuples = db.MaterializeFromBools(query)
	}
}

// Select all prices more than 100 cents = $1 and
// less than 1000 cents = $10
func BenchmarkSelectAllMoreDollarLessTen(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {
		// Find prices higher and lower than our
		// current threshold
		lowerBound := db.Prices.More(100)
		upperBound := db.Prices.Less(1000)

		// Determine values between and materialize
		garbageQuery = upperBound.AND(lowerBound)
	}
}

// Select all prices more than 100 cents = $1 and
// less than 1000 cents = $10 then rematerialize them into tuples
func BenchmarkSelectAllMoreDollarLessTenMaterial(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {
		// Find prices higher and lower than our
		// current threshold
		lowerBound := db.Prices.More(100)
		upperBound := db.Prices.Less(1000)

		// Determine values between and materialize
		innerBound := upperBound.AND(lowerBound)

		uselessTuples = db.MaterializeFromBools(innerBound)
	}
}

// Select all prices after the time of the 500000th price
func BenchmarkSelectAfterTimeWiseMidPoint(b *testing.B) {
	db := setupPriceBenchmark(b)

	when, err := time.Parse("2006-01-02 15:04:05", "2015-11-24 20:39:29")
	if err != nil {
		b.Fatalf("failed to parse threshold '%v'", err)
	}

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {

		garbageQuery = db.Times.After(when)

	}
}

// Select all prices for a specific card/set combo
//
// This requires materialization as sort is currently
// only implemented on tuples rather than on columns.
func BenchmarkLatestPriceMaterial(b *testing.B) {

	testTupleTime, err := time.Parse("2006-01-02 15:04:05",
		"2016-04-09 03:51:45")
	if err != nil {
		b.Fatalf("failed to parse time, %v", err)
	}

	testTuple := PriceTuple{
		Name:  "Griselbrand",
		Set:   "Avacyn Restored Foil",
		Price: 5523,
		Time:  testTupleTime,
	}

	db := setupPriceBenchmark(b)

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {

		nameEq := db.Names.Equal(testTuple.Name)
		setEq := db.Sets.Equal(testTuple.Set)
		innerBound := nameEq.AND(setEq)

		tuples := db.MaterializeTimeSortAsc(innerBound)
		// We only want one but have no way of ensuring we only
		// get one, so we have to handle that
		if len(tuples) < 1 {
			b.Fatalf("found fewer than two tuples")
		}

		uselessTuples = []PriceTuple{tuples[len(tuples)-1]}

	}

}

// Select the latest, highest price for a card
//
// This requires materialization as sort is currently
// only implemented on tuples rather than on columns.
func BenchmarkLatestHighestPriceMaterial(b *testing.B) {

	testTupleTime, err := time.Parse("2006-01-02 15:04:05",
		"2016-04-09 03:51:45")
	if err != nil {
		b.Fatalf("failed to parse time, %v", err)
	}

	testTuple := PriceTuple{
		Name:  "Windswept Heath",
		Set:   "Onslaught Foil",
		Price: 15499,
		Time:  testTupleTime,
	}

	proj := setupNameTimeProjectionBench(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		query := proj.Latest(testTuple.Name)

		tuples := proj.MaterializeFromBools(query)
		// We only want one but have no way of ensuring we only
		// get one, so we have to handle that
		if len(tuples) < 1 {
			b.Fatalf("found fewer than two tuples")
		}

		// Select tuple with highest price
		var found PriceTuple
		for _, t := range tuples {
			if t.Price > found.Price {
				found = t
			}
		}

		uselessTuples = []PriceTuple{found}
	}
}

func BenchmarkLatestHighestPriceMaterialOld(b *testing.B) {

	testTupleTime, err := time.Parse("2006-01-02 15:04:05",
		"2016-04-09 03:51:45")
	if err != nil {
		b.Fatalf("failed to parse time, %v", err)
	}

	testTuple := PriceTuple{
		Name:  "Windswept Heath",
		Set:   "Onslaught Foil",
		Price: 15499,
		Time:  testTupleTime,
	}

	db := setupPriceBenchmark(b)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		nameEq := db.Names.Equal(testTuple.Name)
		innerBound := nameEq

		tuples := db.MaterializeTimeSortAsc(innerBound)
		// We only want one but have no way of ensuring we only
		// get one, so we have to handle that
		if len(tuples) < 1 {
			b.Fatalf("found fewer than two tuples")
		}

		// Select last tuples and find the last with lowest time
		var found PriceTuple
		for i := len(tuples) - 1; i >= 0; i-- {
			t := tuples[i]
			if found.Time.Before(t.Time) || found.Time.Equal(t.Time) {
				if t.Price > found.Price || found.Time.Before(t.Time) {
					found = t
				}
			}
		}

		uselessTuples = []PriceTuple{found}
	}
}
