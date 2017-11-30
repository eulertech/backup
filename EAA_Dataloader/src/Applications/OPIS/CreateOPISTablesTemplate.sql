-- Script to create the OPIS tables
DROP TABLE IF EXISTS {schemaName}.{tableName}retail_price;
CREATE TABLE {schemaName}.{tableName}retail_price (
	fuel_location_id integer,
	address1 VARCHAR(100) ENCODE LZO,
	address2 VARCHAR(100) ENCODE LZO,
	zip VARCHAR(50) ENCODE LZO,
	brand_name VARCHAR(50) ENCODE LZO,
	retail_product_name VARCHAR(50) ENCODE LZO,
	days_with_prices integer,
	price REAL
);

DROP TABLE IF EXISTS {schemaName}.{tableName}retail_volume;
CREATE TABLE {schemaName}.{tableName}retail_volume (
	date date,
	volume_location_id integer,
	address1 VARCHAR(100) ENCODE LZO,
	address2 VARCHAR(100) ENCODE LZO,
	zip VARCHAR(50) ENCODE LZO,
	retail_product_name VARCHAR(50) ENCODE LZO,
	volume_type_name VARCHAR(40) ENCODE LZO,
	volume_amount REAL
);

--SELECT top 50 * FROM stl_load_errors order by starttime desc;