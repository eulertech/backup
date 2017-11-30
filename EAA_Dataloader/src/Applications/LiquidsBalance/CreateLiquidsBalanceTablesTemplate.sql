--Script to create LiquidsBalance Final Tables, temporary tables are created dynamically on run time.

DROP TABLE IF EXISTS  {schemaName}.{tableName}_crude CASCADE;
CREATE TABLE {schemaName}.{tableName}_crude
(
	category				varchar(20) ENCODE LZO,
	region					varchar(50) ENCODE LZO,
	country					varchar(50) ENCODE LZO,
   	year			   		varchar(4)	ENCODE LZO,
	value				    REAL
);

DROP TABLE IF EXISTS  {schemaName}.{tableName}_tightoil CASCADE;
CREATE TABLE {schemaName}.{tableName}_tightoil
(
	region					varchar(50) ENCODE LZO,
	country					varchar(50) ENCODE LZO,
   	year			   		varchar(4)	ENCODE LZO,
	value				    REAL
);