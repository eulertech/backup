-- Script to create the Consensus tables

DROP TABLE IF EXISTS {schemaName}.{tableName} CASCADE;
CREATE TABLE {schemaName}.{tableName} (
	survey_date		DATE,
	bank_name		VARCHAR(50) ENCODE LZO,
	forecast_date	DATE,
	forecast_price	FLOAT	
);
