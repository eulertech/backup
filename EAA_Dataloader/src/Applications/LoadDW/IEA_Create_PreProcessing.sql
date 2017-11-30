-- Pre process script for IEA app... 

DROP TABLE IF EXISTS {schemaName}.{tableName} CASCADE;
CREATE TABLE {schemaName}.{tableName} (
	source VARCHAR(12) ENCODE LZO,
	field VARCHAR(12) ENCODE LZO,
	shortname VARCHAR(12) ENCODE LZO,
	description VARCHAR(100) ENCODE LZO
);
