-- Script to create the objects needed for the RIGPOINT app

DROP TABLE IF EXISTS {schemaName}.{tableName}_utilization_monthly;

CREATE TABLE {schemaName}.{tableName}_utilization_monthly (
	month DATE,
	rig_type VARCHAR(30) ENCODE LZO,
	country VARCHAR(30) ENCODE LZO,
	total_supply REAL,
	marketed_supply REAL,
	working REAL,
	total_util REAL,
	marketed_util REAL
);
