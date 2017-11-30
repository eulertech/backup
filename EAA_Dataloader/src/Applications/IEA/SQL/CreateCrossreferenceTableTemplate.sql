--Script to create {schemaName}.{tableName} Tables
DROP TABLE IF EXISTS  {schemaName}.{tableName};

CREATE TABLE {schemaName}.{tableName}
(
	source				varchar(200) not null ENCODE LZO,
   	destination   		varchar(200) ENCODE	LZO
);

ALTER TABLE {schemaName}.{tableName}
   ADD CONSTRAINT {tableName}_pkey
   PRIMARY KEY (source);
--GRANT SELECT, UNKNOWN ON {schemaName}.{tableName} TO group analysts;

