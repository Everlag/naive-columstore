package main

import (
	"testing"
)

// Always save benchmark results here to
// ensure the compiler doesn't optimize them away
var garbage PriceDB

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
}
