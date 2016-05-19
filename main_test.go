package main

import (
	"testing"
)

func setupPriceTest(t *testing.T) PriceDB {
	db := NewPriceDB()

	err := db.IngestCSV("prices.csv")
	if err != nil {
		t.Fatal(err)
	}

	return db
}

// Select all prices more than 1 000 000 cents = $10K and
// rematerialize them into tuples
func TestSimpleSelect(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher than our threshold
	query := db.Prices.More(1000000)
	tuples := db.MaterializeFromBools(query)
	// Rough
	if len(tuples) == 0 {
		t.Fatal("bad query, none found")
	}
	if len(tuples) > 1000 {
		t.Fatal("bad query, too many")
	}
	// Exact
	if len(tuples) != 54 {
		t.Fatalf("bad query, %v found instead of 54", len(tuples))
	}
}

// Select all prices more than 1 000 000 cents = $10K and
// less than our upper threshold of #11K then
// rematerialize them into tuples
func TestUpperLowerANDSelect(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher and lower than our
	// current threshold
	lowerBound := db.Prices.More(1000000)
	upperBound := db.Prices.Less(1100000)

	// Determine values between and materialize
	innerBound := upperBound.AND(lowerBound)

	tuples := db.MaterializeFromBools(innerBound)
	// Rough
	if len(tuples) == 0 {
		t.Fatal("bad query, none found")
	}
	if len(tuples) > 1000 {
		t.Fatal("bad query, too many")
	}
	// Exact
	if len(tuples) != 24 {
		t.Fatalf("bad query, %v found instead of 24", len(tuples))
	}
}
