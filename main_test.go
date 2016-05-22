package main

import (
	"testing"

	"time"
)

var TestDB *PriceDB

func setupPriceTest(t *testing.T) PriceDB {
	if TestDB != nil {
		return *TestDB
	}

	db := NewPriceDB()

	err := db.IngestCSV("prices.csv")
	if err != nil {
		t.Fatal(err)
	}

	TestDB = &db

	return db
}

// Select all prices more than than 90 000 000 cents = $900K
//
// This should always return 0 results
func TestPriceSelectNone(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher than our threshold
	query := db.Prices.More(9000000)
	truthy := query.TruthyIndices()
	// We should never have prices higher than our threshold
	if len(truthy) != 0 {
		t.Fatal("bad query, found tuples", db.MaterializeFromBools(query))
	}
}

// Select all prices less than than 90 000 000 cents = $900K
//
// This should always return 0 results
func TestPriceSelectAll(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher than our threshold
	query := db.Prices.Less(9000000)
	truthy := query.TruthyIndices()
	// We should never have prices higher than our threshold
	if len(truthy) != 1000000 {
		t.Fatalf("bad query, found %v tuples, expected 1000000", len(truthy))
	}
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

// Select all prices happening after 2015-11-13 15:07:12 - 1 day
// that should cover all prices in the test dataset
//
// We have a million values in the dataset...
func TestTimeAfterSelectAll(t *testing.T) {
	db := setupPriceTest(t)

	when, err := time.Parse("2006-01-02 15:04:05", "2015-11-13 15:07:12")
	if err != nil {
		t.Fatalf("failed to parse threshold '%v'", err)
	}

	when = when.Add(time.Hour * 24 * -1)
	query := db.Times.After(when)
	truthy := query.TruthyIndices()
	if len(truthy) != 1000000 {
		t.Fatalf("found %v tuples, not 1 million!", len(truthy))
	}

}

// Select all prices happening after 2017-11-13 15:07:12 - 1 day
// that should cover no prices in the test dataset
//
// We have a million values in the dataset...
func TestTimeAfterSelectNone(t *testing.T) {
	db := setupPriceTest(t)

	when, err := time.Parse("2006-01-02 15:04:05", "2017-11-13 15:07:12")
	if err != nil {
		t.Fatalf("failed to parse threshold '%v'", err)
	}

	query := db.Times.After(when)
	truthy := query.TruthyIndices()
	if len(truthy) != 0 {
		t.Fatalf("found %v tuples, not 0!", len(truthy))
	}

}
