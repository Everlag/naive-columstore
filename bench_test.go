package main

import (
	"testing"
)

// Always save benchmark results here to
// ensure the compiler doesn't optimize them away
var garbage PriceDB
var uselessTuples []PriceTuple
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

// Select all prices more than 100 cents = $1 and
// rematerialize them into tuples
func BenchmarkSelectAllMoreThanDollar(b *testing.B) {
	db := setupPriceBenchmark(b)

	b.ResetTimer()

	// Find prices higher than our threshold
	for n := 0; n < b.N; n++ {
		query := db.Prices.More(100)
		uselessTuples = db.MaterializeFromBools(query)
	}
}
