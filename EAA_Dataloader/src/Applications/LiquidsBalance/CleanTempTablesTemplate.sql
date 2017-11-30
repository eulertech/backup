--SQL Script template to clean up the LiquidsBalance temporal tables

DROP TABLE IF EXISTS  {schemaName}.{tableName}_crude_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_asw2016_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_deepwater_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_offshore_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_onshore_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_rivalry2016_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_shallowwater_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_ultradeepwater_temp;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_tightoil_temp;