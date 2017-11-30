--Script to create RigCount Tables
DROP TABLE IF EXISTS  {schemaName}.{tableName};

CREATE TABLE {schemaName}.{tableName}
(
	category				varchar(100) ENCODE LZO,
	frequency				varchar(1) ENCODE LZO,
	description				varchar(255) ENCODE LZO,
	source					varchar(255) ENCODE LZO,
	unit					varchar(20) ENCODE LZO,
   	valuationdate   		varchar(10) ENCODE	LZO,
 	value				    DOUBLE PRECISION
);

--GRANT SELECT, UNKNOWN ON {schemaName}.{tableName} TO group analysts;

