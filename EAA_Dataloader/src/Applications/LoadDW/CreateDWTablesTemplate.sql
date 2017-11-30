-- Script to create the data warehouse tables.

DROP TABLE IF EXISTS {schemaName}.{tableName}_data CASCADE;
CREATE TABLE {schemaName}.{tableName}_data (
	name VARCHAR(512) ENCODE LZO,
	date DATE,
	value REAL
);

DROP TABLE IF EXISTS {schemaName}.{tableName}_attributes CASCADE;
CREATE TABLE {schemaName}.{tableName}_attributes (
	name VARCHAR(512) ENCODE LZO,
	label VARCHAR(1024) ENCODE LZO,
	description VARCHAR(1024) ENCODE LZO,
	source VARCHAR(50) ENCODE LZO,
	concept VARCHAR(50) ENCODE LZO,
	frequency VARCHAR(50) ENCODE LZO,
	forecast BOOLEAN,
	startDate DATE,
	endDate DATE
);
