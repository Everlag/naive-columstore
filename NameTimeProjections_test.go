package main

import (
	"testing"

	"time"
)

var TestNameTimeProjection *NameTimeProjection

// Create the testing projection we use
//
// This allows tests to run faster as anything using this projection
// can use the same copy
func setupNameTimeProjectionTest(t *testing.T) NameTimeProjection {

	// Make sure the underlying db was created first
	db := setupPriceTest(t)
	if TestNameTimeProjection == nil {
		proj := NameTimeProjectionFromPriceDB(db)
		TestNameTimeProjection = &proj
	}

	return *TestNameTimeProjection
}

// Create a projection out of the fully formed db
//
// Postgres equivalent
//  n/a
func TestNameTimeProjectionFromPriceDB(t *testing.T) {
	// Ensure test DB and projection are setup
	//
	// Not strictly necessary but we use TestDB anyway
	proj := setupNameTimeProjectionTest(t)
	db := setupPriceTest(t)

	// Ensure price length is equal
	priceLengthDB := db.Prices.Length()
	priceLengthProj := proj.Prices.Length()
	if priceLengthDB != priceLengthProj {
		t.Fatalf("mismatching projection and db lengths %v != %v", priceLengthDB, priceLengthProj)
	}

	// Ensure sort was successful in making time ascending
	badTransitions := 0

	var prevName string
	var prevTime time.Time
	for i := 0; i < priceLengthProj; i++ {
		name := proj.Names.Access(i)
		time := proj.Times.Access(i)

		// Unequal names should have previous time AFTER current time
		if name != prevName && prevTime.Before(time) {
			// Dataset can be a little dirty due to unhinged and such
			// so we keep track of how many times this happeened
			// and threshold the test failure
			badTransitions++
			if badTransitions > 5 {
				t.Fatalf("not asc sort order for index %v: {%v, %v} {%v, %v}",
					i, prevName, prevTime, name, time)
			}
		}

		// Equal names should have previous time BEFORE current time
		if name == prevName && prevTime.After(time) {
			t.Fatalf("not asc sort order for index %v: {%v, %v} {%v, %v}",
				i, prevName, prevTime, name, time)
		}

		prevName = name
		prevTime = time
	}

}

// Find the number of tuples for a certain card's latest value
//
// Postgres equivalent(result # only, not performance equiv)
// SELECT * FROM prices.mtgprice
// 	WHERE name='Windswept Heath' AND
// 	      time = timestamp '2016-04-09 03:51:45'  ORDER BY time DESC, price DESC;
func TestNameTimeProjectionLatest(t *testing.T) {

	proj := setupNameTimeProjectionTest(t)

	referenceCount := 6
	query := proj.Latest("Windswept Heath")
	count := len(query.TruthyIndices())
	if referenceCount != referenceCount {
		t.Fatalf("mismatching coubt %v != %v", referenceCount, count)
	}

}
