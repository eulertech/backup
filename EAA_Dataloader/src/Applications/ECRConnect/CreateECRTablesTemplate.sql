-- Script to create the ECR Connect tables

DROP TABLE IF EXISTS {schemaName}.{tableName} CASCADE;
CREATE TABLE {schemaName}.{tableName} (
	country				VARCHAR(50) ENCODE LZO,
	risk_name			VARCHAR(50) ENCODE LZO,
	risk_value			REAL,
	risk_description	VARCHAR(250) ENCODE LZO,
	risk_class			VARCHAR(50) ENCODE LZO,
	risk_class_avg		REAL,
	updated_on			DATE
);

DROP TABLE IF EXISTS {schemaName}.{tableName}_history CASCADE;
CREATE TABLE {schemaName}.{tableName}_history (
	country				VARCHAR(50) ENCODE LZO,
	risk_name			VARCHAR(50) ENCODE LZO,
	risk_value			REAL,
	updated_on			DATE
);

DROP TABLE IF EXISTS {schemaName}.{tableName}_xref_class CASCADE;
CREATE TABLE {schemaName}.{tableName}_xref_class (
	class_name			VARCHAR(50) ENCODE LZO,
	risk_name			VARCHAR(50) ENCODE LZO,
	risk_desc			VARCHAR(50) ENCODE LZO
);