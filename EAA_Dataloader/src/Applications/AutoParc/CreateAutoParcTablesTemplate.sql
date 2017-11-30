-- Script to create the automotive PARC tables
DROP TABLE IF EXISTS {schemaName}.{tableName};
CREATE TABLE {schemaName}.{tableName} (
	country						VARCHAR(40)	ENCODE LZO,
	registration_type			VARCHAR(40)	ENCODE LZO,
	make						VARCHAR(40)	ENCODE LZO,
	model						VARCHAR(40)	ENCODE LZO,
	body_type					VARCHAR(40)	ENCODE LZO,
	fuel_type					VARCHAR(20)	ENCODE LZO,
	engine_displacement_ccm		INTEGER,
	engine_displacement_litres	VARCHAR(10)	ENCODE LZO,
	engine_power				INTEGER,
	number_of_cylinders			INTEGER,
	gvw							INTEGER,
	transmission_type			VARCHAR(40)	ENCODE LZO,
	axle_configuration			VARCHAR(10)	ENCODE LZO,
	History_Forecast_kode		VARCHAR(2)	ENCODE LZO,
	Modms_txt					VARCHAR(40)	ENCODE LZO,
	model_kode_fc				VARCHAR(10)	ENCODE LZO,
	model_kode					VARCHAR(10)	ENCODE LZO,
	model_txt					VARCHAR(40)	ENCODE LZO,
	year					INTEGER,
	market						VARCHAR(10)	ENCODE LZO,
	total						INTEGER,
	model_age					INTEGER
);

--SELECT top 50 * FROM stl_load_errors order by starttime desc;
