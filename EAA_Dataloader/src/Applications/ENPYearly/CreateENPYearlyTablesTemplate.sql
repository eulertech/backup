--Script to create ENP Yearly Tables
DROP TABLE IF EXISTS  {schemaName}.{tableName};

CREATE TABLE {schemaName}.{tableName}
(
   	Data_Level   			varchar(50) ENCODE	LZO,
   	Region					varchar(100) ENCODE	LZO,
   	Country					varchar(200) ENCODE	LZO,
   	Iris_ID					DOUBLE PRECISION,
   	Crude_Stream			varchar(100) ENCODE	LZO,
   	Project_ID				varchar(100) ENCODE	LZO,
   	Project_Name			varchar(100) ENCODE	LZO,
   	Hydrocarbon				varchar(100) ENCODE	LZO,
   	Basin					varchar(100) ENCODE	LZO,
   	Subbasin				varchar(100) ENCODE	LZO,
   	Operator				varchar(512) ENCODE	LZO,
   	Start_Year				integer,
	Terrain					varchar(100) ENCODE	LZO,   
	Sanctioned				varchar(100) ENCODE	LZO,
	Omit					varchar(100) ENCODE	LZO,
	Latitude_Dec_Deg		DOUBLE PRECISION,
	Longitude_Dec_Deg		DOUBLE PRECISION,
	BEP						DOUBLE PRECISION,
	Tranches				varchar(100) ENCODE	LZO,
	Years					REAL,
	Production_Kbbl_d		DOUBLE PRECISION,
	Prod_2015_2040_MMbbl	DOUBLE PRECISION,
	Produced_to_date_MMbbl	varchar(100) ENCODE	LZO
);

--GRANT SELECT, UNKNOWN ON {schemaName}.{tableName} TO group analysts;