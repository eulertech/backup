--Script to create RigCount Tables
DROP TABLE IF EXISTS  {schemaName}.{tableName};

CREATE TABLE {schemaName}.{tableName}
(
	category				varchar(200) ENCODE LZO,
   	valuationdate   		DATE,
   	name					varchar(200) ENCODE	LZO,
	wells				    DOUBLE PRECISION
);

--GRANT SELECT, UNKNOWN ON {schemaName}.{tableName} TO group analysts;

