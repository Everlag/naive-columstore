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

func setupPriceBenchmark(b *testing.B) PriceDB {
	db := NewPriceDB()

	err := db.IngestCSV("prices.csv")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	return db
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
