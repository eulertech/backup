-- Script to create the EIA PET IMPORTS tables
DROP TABLE IF EXISTS {schemaName}.{tableName}pet_imports_series_attributes;
CREATE TABLE {schemaName}.{tableName}pet_imports_series_attributes (
	series_id		VARCHAR(50)	    ENCODE LZO,
	name			VARCHAR(256)	ENCODE LZO,
	units			VARCHAR(256)	ENCODE LZO,
	f			    VARCHAR(2)	    ENCODE LZO,
	copyright		VARCHAR(256)	ENCODE LZO,
	source			VARCHAR(256)	ENCODE LZO,
    lat             VARCHAR(256)	ENCODE LZO,
	lon             VARCHAR(256)	ENCODE LZO,
	geography		VARCHAR(256)	ENCODE LZO,
	geography2      VARCHAR(256)	ENCODE LZO,
	start			VARCHAR(8)	    ENCODE LZO,
	"end"			VARCHAR(8)	    ENCODE LZO,
	last_updated	DATE,
	geoset_id       VARCHAR(256)	ENCODE LZO,
	latlon          VARCHAR(256)	ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}pet_imports_series_data;
CREATE TABLE {schemaName}.{tableName}pet_imports_series_data (
	series_id		VARCHAR(50)	ENCODE LZO,
	period			VARCHAR(50)	ENCODE LZO,
	value           REAL
);

--SELECT top 50 * FROM stl_load_errors order by starttime desc;
