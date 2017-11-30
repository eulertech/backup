-- Script to create all JODI tables.
DROP TABLE IF EXISTS {schemaName}.{tableName}_primary CASCADE;
CREATE TABLE {schemaName}.{tableName}_primary (
	country VARCHAR(20) ENCODE LZO,
	product VARCHAR(20) ENCODE LZO,
	flow VARCHAR(20) ENCODE LZO,
	unit VARCHAR(20) ENCODE LZO,
	date DATE,
	quantity REAL,
	code INTEGER ENCODE LZO,
	Qualifier VARCHAR(20) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}_secondary CASCADE;
CREATE TABLE {schemaName}.{tableName}_secondary (
	country VARCHAR(20) ENCODE LZO,
	product VARCHAR(20) ENCODE LZO,
	flow VARCHAR(20) ENCODE LZO,
	unit VARCHAR(20) ENCODE LZO,
	date DATE,
	quantity REAL,
	code INTEGER ENCODE LZO,
	Qualifier VARCHAR(20) ENCODE LZO
);

DROP TABLE IF EXISTS  {schemaName}.{tableName}_flow CASCADE;
CREATE TABLE {schemaName}.{tableName}_flow
(
	name		VARCHAR(20) ENCODE	LZO,
	file_table	VARCHAR(20) not null ENCODE LZO,
   	full_name	VARCHAR(200) ENCODE	LZO
);

DROP TABLE IF EXISTS  {schemaName}.{tableName}_product CASCADE;
CREATE TABLE {schemaName}.{tableName}_product
(
	name		VARCHAR(20) ENCODE	LZO,
	file_table	VARCHAR(20) not null ENCODE LZO,
   	full_name	VARCHAR(200) ENCODE	LZO
);

DROP TABLE IF EXISTS  {schemaName}.{tableName}_qualifier CASCADE;
CREATE TABLE {schemaName}.{tableName}_qualifier
(
	code		INTEGER,
   	description	VARCHAR(200) ENCODE	LZO
);

DROP TABLE IF EXISTS  {schemaName}.{tableName}_units CASCADE;
CREATE TABLE {schemaName}.{tableName}_units
(
	name		VARCHAR(20) ENCODE	LZO,
   	description	VARCHAR(200) ENCODE	LZO,
   	uom			VARCHAR(20) ENCODE	LZO
);