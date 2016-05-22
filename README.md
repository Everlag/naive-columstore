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

## References

[1] Abadi D, Boncz P, Harizopoulos S, Idreos S, Madden S. The design and implementation of modern column-oriented database systems. Now; 2013 Nov 25.