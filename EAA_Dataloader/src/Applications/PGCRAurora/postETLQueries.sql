-- Script to create the consolidated Aurora tables

DROP TABLE IF EXISTS {schemaName}.{tableName}L22017H1;
select cast(Hour_Beginning as TIMESTAMP),
ERCOT_Houston,
ERCOT_South,
ERCOT_West,
ERCOT_North
into {schemaName}.{tableName}L22017H1 
from {schemaName}.{tableName}2017H1;

DROP TABLE IF EXISTS {schemaName}.{tableName}L2HistoricalPrices;
select cast(Hour_Beginning as TIMESTAMP),
ERCOT_Houston,
ERCOT_South,
ERCOT_West,
ERCOT_North
into {schemaName}.{tableName}L2HistoricalPrices 
from {schemaName}.{tableName}HistoricalPrices;

DROP TABLE IF EXISTS {schemaName}.{tableName}L2HistoricalLoad;
select cast(Hour_Beginning as TIMESTAMP),
ERCOT_Houston,
ERCOT_South,
ERCOT_West,
ERCOT_North
into {schemaName}.{tableName}L2HistoricalLoad 
from {schemaName}.{tableName}HistoricalLoad;

DROP TABLE IF EXISTS {schemaName}.{tableName}2017H1;
DROP TABLE IF EXISTS {schemaName}.{tableName}HistoricalPrices;
DROP TABLE IF EXISTS {schemaName}.{tableName}HistoricalLoad;

--Changing to Long form

DROP TABLE IF EXISTS {schemaName}.{tableName}l32017h1;
select * into {schemaName}.{tableName}l32017h1
from(
select hour_beginning,'ercot_houston' as Zone, ercot_houston Price from {schemaName}.{tableName}l22017h1
union
select hour_beginning,'ercot_south' as Zone, ercot_south Price from {schemaName}.{tableName}l22017h1
union
select hour_beginning,'ercot_west' as Zone, ercot_west Price from {schemaName}.{tableName}l22017h1
union
select hour_beginning,'ercot_north' as Zone, ercot_north Price from {schemaName}.{tableName}l22017h1);

DROP TABLE IF EXISTS {schemaName}.{tableName}l3historicalload;
select * into {schemaName}.{tableName}l3historicalload
from(
select hour_beginning,'ercot_houston' as Zone, ercot_houston as Load from {schemaName}.{tableName}l2historicalload
union
select hour_beginning,'ercot_south' as Zone, ercot_south as Load from {schemaName}.{tableName}l2historicalload
union
select hour_beginning,'ercot_west' as Zone, ercot_west as Load from {schemaName}.{tableName}l2historicalload
union
select hour_beginning,'ercot_north' as Zone, ercot_north as Load from {schemaName}.{tableName}l2historicalload);

DROP TABLE IF EXISTS {schemaName}.{tableName}l3historicalprices;
select * into {schemaName}.{tableName}l3historicalprices
from(
select hour_beginning,'ercot_houston' as Zone, ercot_houston Price from {schemaName}.{tableName}l2historicalprices
union
select hour_beginning,'ercot_south' as Zone, ercot_south Price from {schemaName}.{tableName}l2historicalprices
union
select hour_beginning,'ercot_west' as Zone, ercot_west Price from {schemaName}.{tableName}l2historicalprices
union
select hour_beginning,'ercot_north' as Zone, ercot_north Price from {schemaName}.{tableName}l2historicalprices);

DROP TABLE IF EXISTS {schemaName}.{tableName}l22017h1;
DROP TABLE IF EXISTS {schemaName}.{tableName}l2historicalload;
DROP TABLE IF EXISTS {schemaName}.{tableName}l2historicalprices;

