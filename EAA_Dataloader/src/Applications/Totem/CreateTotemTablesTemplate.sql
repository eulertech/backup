--Script to create Totem Tables
DROP TABLE IF EXISTS  {schemaName}.{tableName};

CREATE TABLE {schemaName}.{tableName}
(
   	valuationdate   		varchar(10) ENCODE	LZO,
   	clientid                integer,
   	name					varchar(200) ENCODE	LZO,
   	totemgroup				varchar(100) ENCODE	LZO,
   	units                   varchar(50) ENCODE	LZO,
   	pricingtime             varchar(50) ENCODE	LZO,
   	period                  varchar(30) ENCODE	LZO,
   	startDate               varchar(10) ENCODE	LZO,
   	endDate                	varchar(10) ENCODE	LZO,
   	totemtype              	varchar(10) ENCODE	LZO,
   	price                   REAL,
   	consensusPrice			REAl,
	compositePrice			REAL,   
	priceRange				REAL,
	contributors			integer,
	priceStddev				REAL,
	strike					REAL,
	vol						REAL,
	reconstitutedForward	REAL,
	consensusVol			REAL,
	compositeVol			REAL,
	volRange				REAL,
	expiryDate				varchar(10) ENCODE	LZO,
	volStddev				REAL
);

--GRANT SELECT, UNKNOWN ON {schemaName}.{tableName} TO group analysts;

