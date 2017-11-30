-- Script to create the {schemaName}.{tableName} tables
DROP TABLE IF EXISTS {schemaName}.{tableName};
CREATE TABLE {schemaName}.{tableName} (
	flow						VARCHAR(200)	ENCODE LZO,
	country						VARCHAR(200)	ENCODE LZO,
	product						VARCHAR(200)	ENCODE LZO,
	period_type					VARCHAR(2)	ENCODE LZO,
	period						VARCHAR(7)	ENCODE LZO,
	value						DOUBLE PRECISION
);


DROP TABLE IF EXISTS {schemaName}.WORKING_{tableName};
CREATE TABLE {schemaName}.WORKING_{tableName} (
	flow						VARCHAR(200)	ENCODE LZO,
	country						VARCHAR(200)	ENCODE LZO,
	product						VARCHAR(200)	ENCODE LZO,
	period						VARCHAR(10)	ENCODE LZO,
	value						DOUBLE PRECISION
);
