# naive-columstore

This is a simple, toy column store implemented as a result of reading 'The Design and Implementation of Modern Column-Oriented Database Systems'[1]. To keep everything sane and typesafe, this is a bespoke implementation for a dataset collected from mtgprice.

I'm starting to benchmark early and releases will be tagged for when benchmarks can be stably compared.


## Testing Bootstrap

As all queries written here have direct equivalents to SQL, it makes sense to bootstrap all test results against a known-good implementation.

As a result, queries implemented here are tested against the same query run in postgres.

The following are the commands to run in psql as the `postgres` user to import the data file. Open psql from the repo root, `\copy` is used relatively to import the testing data.

	/*Create a database to test under*/
	CREATE DATABASE pricedata_tests WITH
		OWNER     postgres
		ENCODING 'UTF8';

	\connect pricedata_tests

	/*Add a schema to work under*/
	CREATE SCHEMA prices;

	/*Add a table to work under*/
	CREATE TABLE prices.mtgprice (

		name TEXT NOT NULL,
		set TEXT NOT NULL,
		time timestamp NOT NULL,

		price int NOT NULL,
		CONSTRAINT uniqueMTGpriceEntryKey UNIQUE (name, set, time)

	);

	/*Import data, will take a minute or two*/
	\copy prices.mtgprice from 'prices.csv' DELIMITER ',' CSV HEADER;

	/*Ensure data is present, should return true*/
	select (count(*) = 1000000) as valid from prices.mtgprice;

## Historical Benchmarks

When settling on a bitmap to use, I performed some benchmarks. Here they are formatted with github.com/cespare/prettybench

	willf/bitset ~240MB
	[]bool ~270MB
	RoaringBitmap/roaring ~ 200MB

	willf/bitset
	BenchmarkUint32More-4                                100          16320800 ns/op         262208 B/op          4 allocs/op
	BenchmarkUint32Less-4                                100          15615659 ns/op         131104 B/op          2 allocs/op
	BenchmarkUint32Delta-4                               100          15973511 ns/op       22995204 B/op         39 allocs/op
	BenchmarkUint32Sum-4                                2000            743859 ns/op              0 B/op          0 allocs/op
	BenchmarkBoolAND-4                                 50000             31699 ns/op              0 B/op          0 allocs/op
	BenchmarkSelectAllMoreThanDollar-4                   100          16020921 ns/op         262208 B/op          4 allocs/op
	BenchmarkFiniteString32Within-4                       50          25190530 ns/op         262241 B/op          6 allocs/op
	BenchmarkSelectAllMoreThanDollarMaterial-4            20          74621560 ns/op       47334222 B/op         45 allocs/op
	BenchmarkSelectAllMoreDollarLessTen-4                 50          30496544 ns/op         393313 B/op          6 allocs/op
	BenchmarkSelectAllMoreDollarLessTenMaterial-4         20          76079955 ns/op       37299048 B/op         45 allocs/op
	BenchmarkSelectAfterTimeWiseMidPoint-4               100          17992860 ns/op         131104 B/op          2 allocs/op
	BenchmarkLatestPriceMaterial-4                        50          25104048 ns/op         266057 B/op         17 allocs/op
	ok      github.com/Everlag/test 100.352s

	[]bool
	PASS
	BenchmarkUint32More-4                                100          12300487 ns/op       5863804 B/op          35 allocs/op
	BenchmarkUint32Less-4                                100          10776367 ns/op       5863804 B/op          35 allocs/op
	BenchmarkUint32Delta-4                               100          17705522 ns/op      22995210 B/op          39 allocs/op
	BenchmarkUint32Sum-4                                2000            760287 ns/op             0 B/op           0 allocs/op
	BenchmarkBoolAND-4                                   500           3306324 ns/op             0 B/op           0 allocs/op
	BenchmarkSelectAllMoreThanDollar-4                   100          12233875 ns/op       5863804 B/op          35 allocs/op
	BenchmarkFiniteString32Within-4                      100          23427404 ns/op      11727673 B/op          72 allocs/op
	BenchmarkSelectAllMoreThanDollarMaterial-4            20          77372820 ns/op      52935826 B/op          76 allocs/op
	BenchmarkSelectAllMoreDollarLessTen-4                 50          27022548 ns/op      11727608 B/op          70 allocs/op
	BenchmarkSelectAllMoreDollarLessTenMaterial-4         20          73027185 ns/op      48633354 B/op         111 allocs/op
	BenchmarkSelectAfterTimeWiseMidPoint-4               100          15291031 ns/op       5863803 B/op          35 allocs/op
	BenchmarkLatestPriceMaterial-4                       100          24598447 ns/op      11731457 B/op          83 allocs/op
	ok      github.com/Everlag/test 104.711s


	RoaringBitmap/roaring
	PASS(one off errors, I skipped...)
	BenchmarkUint32More-4                                 50          27632560 ns/op        595473 B/op         283 allocs/op
	BenchmarkUint32Less-4                                 50          27661220 ns/op        595473 B/op         283 allocs/op
	BenchmarkUint32Delta-4                               100          18106963 ns/op      22995212 B/op          40 allocs/op
	BenchmarkUint32Sum-4                                2000            751310 ns/op             0 B/op           0 allocs/op
	BenchmarkBoolAND-4                                 20000             96504 ns/op             0 B/op           0 allocs/op
	BenchmarkSelectAllMoreThanDollar-4                    50          26896896 ns/op        595473 B/op         283 allocs/op
	BenchmarkFiniteString32Within-4                      100          13598676 ns/op          4288 B/op         141 allocs/op
	BenchmarkSelectAllMoreThanDollarMaterial-4            20          98438055 ns/op      47667796 B/op         341 allocs/op
	BenchmarkSelectAllMoreDollarLessTen-4                 20          58682050 ns/op       1199171 B/op         568 allocs/op
	BenchmarkSelectAllMoreDollarLessTenMaterial-4         10         117794540 ns/op      38105236 B/op         625 allocs/op
	BenchmarkSelectAfterTimeWiseMidPoint-4                50          23716366 ns/op        297760 B/op         144 allocs/op
	BenchmarkLatestPriceMaterial-4                       100          14929403 ns/op         33240 B/op         230 allocs/op
	ok      github.com/Everlag/test 98.223s

## References

[1] Abadi D, Boncz P, Harizopoulos S, Idreos S, Madden S. The design and implementation of modern column-oriented database systems. Now; 2013 Nov 25.