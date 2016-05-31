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
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where price > 9000000;
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
// This should always return 1000000 results
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where price < 9000000;
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

// Select all prices equal to 1458 cents = $10.48 and
// rematerialize them into tuples
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where price = 1458;
func TestUint32Equal(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher than our threshold
	query := db.Prices.Equal(1458)
	tuples := db.MaterializeFromBools(query)
	// Rough
	if len(tuples) == 0 {
		t.Fatal("bad query, none found")
	}
	if len(tuples) > 1000 {
		t.Fatal("bad query, too many")
	}
	// Exact
	if len(tuples) != 22 {
		t.Fatalf("bad query, %v found instead of 22", len(tuples))
	}
}

// Select all names equal to 'Griselbrand' and rematerialize
// them into tuples
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where name = 'Griselbrand';
func TestFiniteString32Equal(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher than our threshold
	query := db.Names.Equal("Griselbrand")
	tuples := db.MaterializeFromBools(query)
	// Rough
	if len(tuples) == 0 {
		t.Fatal("bad query, none found")
	}
	if len(tuples) > 1000 {
		t.Fatal("bad query, too many")
	}
	// Exact
	if len(tuples) != 75 {
		t.Fatalf("bad query, %v found instead of 22", len(tuples))
	}
}

// Select all names equal to 'Griselbrand' or 'Avacyn, Angel of Hope'
// and rematerialize them into tuples
//
// Postgres equivalent
// select count(*) from prices.mtgprice where
// 	name = 'Griselbrand' or name = 'Avacyn, Angel of Hope';
func TestFiniteString32Within(t *testing.T) {
	db := setupPriceTest(t)

	names := []string{
		"Griselbrand",
		"Avacyn, Angel of Hope",
	}

	// Find prices higher than our threshold
	query := db.Names.Within(names)
	tuples := db.MaterializeFromBools(query)
	// Rough
	if len(tuples) == 0 {
		t.Fatal("bad query, none found")
	}
	// Exact
	if len(tuples) != 125 {
		t.Fatalf("bad query, %v found instead of 125", len(tuples))
	}
}

// Select all prices more than 1 000 000 cents = $10K and
// rematerialize them into tuples
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where price > 1000000;
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
// less than our upper threshold of $11K then
// rematerialize them into tuples
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where price > 1000000 and price < 1100000;
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

// Select all prices less than 1 000 000 cents = $10K or
// less than our upper threshold of $11K then
// rematerialize them into tuples
//
// Postgres equivalent
//  select count(*) from prices.mtgprice where price > 1000000 or price > 1100000;
func TestUpperLowerORSelect(t *testing.T) {
	db := setupPriceTest(t)

	// Find prices higher and lower than our
	// current threshold
	lowerBound := db.Prices.More(1000000)
	upperBound := db.Prices.More(1100000)

	// Determine values between and materialize
	innerBound := upperBound.OR(lowerBound)

	tuples := db.MaterializeFromBools(innerBound)
	// Rough
	if len(tuples) == 0 {
		t.Fatal("bad query, none found")
	}
	// Exact
	if len(tuples) != 54 {
		t.Fatalf("bad query, %v found instead of 54", len(tuples))
	}
}

// Select all prices happening after 2015-11-13 15:07:12 - 1 day
// that should cover all prices in the test dataset
//
// We have a million values in the dataset...
//
// Postgres equivalent
// with threshold as
// 	(select to_timestamp('2015-10-13 15:07:12', 'YYYY-MM-DD-HH24-MI-SS'))
// 	select count(*) from prices.mtgprice where
// 		time > (select * from threshold);
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
//
// Postgres equivalent
// with threshold as
// 	(select to_timestamp('2015-10-13 15:07:12', 'YYYY-MM-DD-HH24-MI-SS'))
// 	select count(*) from prices.mtgprice where
// 		time < (select * from threshold);
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

// Select all prices happening after the middle tuple at ~2015-11-24
//
// Postgres equivalent
// with threshold as
// 	(select to_timestamp('2015-11-24 20:39:29', 'YYYY-MM-DD-HH24-MI-SS'))
// 	select count(*) from prices.mtgprice where
// 		time > (select * from threshold);
func TestTimeAfterSelecAfterTimeWiseMidPoint(t *testing.T) {
	db := setupPriceTest(t)

	when, err := time.Parse("2006-01-02 15:04:05", "2015-11-24 20:39:29")
	if err != nil {
		t.Fatalf("failed to parse threshold '%v'", err)
	}

	query := db.Times.After(when)
	truthy := query.TruthyIndices()
	if len(truthy) != 491318 {
		t.Fatalf("found %v tuples, not 0!", len(truthy))
	}

}

// Select a single price for a card-set combination
// which is the latest price.
//
// Postgres equivalent
// SELECT name, set, time, price FROM prices.mtgprice
// 	WHERE name='Griselbrand' AND set='Avacyn Restored Foil'
// 	ORDER BY time DESC LIMIT 1;
func TestLatestPrice(t *testing.T) {

	testTupleTime, err := time.Parse("2006-01-02 15:04:05",
		"2016-04-09 03:51:45")
	if err != nil {
		t.Fatalf("failed to parse time, %v", err)
	}

	testTuple := PriceTuple{
		Name:  "Griselbrand",
		Set:   "Avacyn Restored Foil",
		Price: 5523,
		Time:  testTupleTime,
	}

	db := setupPriceTest(t)

	nameEq := db.Names.Equal(testTuple.Name)
	setEq := db.Sets.Equal(testTuple.Set)
	innerBound := nameEq.AND(setEq)

	tuples := db.MaterializeTimeSortAsc(innerBound)
	// We only want one but have no way of ensuring we only
	// get one, so we have to handle that
	if len(tuples) < 1 {
		t.Fatalf("found fewer than two tuples")
	}

	found := tuples[len(tuples)-1]
	if found != testTuple {
		t.Fatalf("found tuple not equal to expected result, got %v", found)
	}

}

// Select the highest, latest price for a card-set combination
//
// We perform a lot of naive work on materialized tuples,
// hopefully projections can let us avoid that in the future.
//
// Postgres equivalent
// SELECT * FROM prices.mtgprice
// 	WHERE name='Windswept Heath' ORDER BY time DESC, price DESC LIMIT 1;
func TestLatestHighestPrice(t *testing.T) {

	testTupleTime, err := time.Parse("2006-01-02 15:04:05",
		"2016-04-09 03:51:45")
	if err != nil {
		t.Fatalf("failed to parse time, %v", err)
	}

	testTuple := PriceTuple{
		Name:  "Windswept Heath",
		Set:   "Onslaught Foil",
		Price: 15499,
		Time:  testTupleTime,
	}

	proj := setupNameTimeProjectionTest(t)

	query := proj.Latest(testTuple.Name)

	tuples := proj.MaterializeFromBools(query)
	// We only want one but have no way of ensuring we only
	// get one, so we have to handle that
	if len(tuples) < 1 {
		t.Fatalf("found fewer than two tuples")
	}

	// Select tuple with highest price
	var found PriceTuple
	for _, t := range tuples {
		if t.Price > found.Price {
			found = t
		}
	}

	if found != testTuple {
		t.Fatalf("found tuple not equal to expected result, got %v", found)
	}
}
