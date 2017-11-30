-- Script to create the consolidated Aurora tables

DROP TABLE IF EXISTS {schemaName}.{tableName}L2Balance;
select BA,
cast(UTC_DATETIME as TIMESTAMP),
cast(LOCAL_TIMESTAMP as TIMESTAMP),
cast(LOCAL_DATE as TIMESTAMP),
LOCAL_HOUR_INT,
VAL_D,
VAL_DF,
VAL_NG,
VAL_TI
into {schemaName}.{tableName}L2Balance 
from {schemaName}.{tableName}Balance;

DROP TABLE IF EXISTS {schemaName}.{tableName}L2Interchange;
select 
BA,
cast(UTC_DATETIME as TIMESTAMP),
cast(LOCAL_TIMESTAMP as TIMESTAMP),
cast(LOCAL_DATE as TIMESTAMP),
LOCAL_HOUR_INT,
DIBA,
VALUE
into {schemaName}.{tableName}L2Interchange 
from {schemaName}.{tableName}Interchange;

DROP TABLE IF EXISTS {schemaName}.{tableName}Balance;
DROP TABLE IF EXISTS {schemaName}.{tableName}Interchange;
