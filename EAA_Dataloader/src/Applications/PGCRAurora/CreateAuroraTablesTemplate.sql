-- Script to create the Aurora tables
DROP TABLE IF EXISTS {schemaName}.{tableName}2017H1;
CREATE TABLE {schemaName}.{tableName}2017H1
(
Hour_Beginning varchar(30) ENCODE LZO,
ERCOT_Houston float,
ERCOT_South float,
ERCOT_West float,
ERCOT_North float
);

DROP TABLE IF EXISTS {schemaName}.{tableName}HistoricalPrices;
CREATE TABLE {schemaName}.{tableName}HistoricalPrices
(
Hour_Beginning varchar(30) ENCODE LZO,
ERCOT_Houston float,
ERCOT_South float,
ERCOT_West float,
ERCOT_North float
);

DROP TABLE IF EXISTS {schemaName}.{tableName}HistoricalLoad;
CREATE TABLE {schemaName}.{tableName}HistoricalLoad
(
Hour_Beginning varchar(30) ENCODE LZO,
ERCOT_Houston float,
ERCOT_South float,
ERCOT_West float,
ERCOT_North float
);