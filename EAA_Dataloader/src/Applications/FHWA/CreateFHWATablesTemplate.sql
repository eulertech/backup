-- Script to create the FHWA tables

DROP TABLE IF EXISTS {schemaName}.{tableName}_faf34 CASCADE;
CREATE TABLE {schemaName}.{tableName}_faf34 (
	ID				INTEGER,
	SIGN1			VARCHAR(10) ENCODE LZO,
	USLRS_KEY		VARCHAR(20) ENCODE LZO,
	STATE			VARCHAR(10) ENCODE LZO,
	CTFIPS			REAL,
	BEG_MP			REAL,
	END_MP			REAL,
	VERSION			VARCHAR(10) ENCODE LZO,
	AADT07			REAL,
	AADTT07			REAL,
	FAF07			REAL,
	NONFAF07		REAL,
	AADT40			REAL,
	AADTT40			REAL,
	FAF40			REAL,
	NONFAF40		REAL,
	CAP07			REAL,
	SF07			REAL,
	VCR07			REAL,
	SPEED07			REAL,
	DELAY07			REAL,
	CAP40			REAL,
	SF40			REAL,
	VCR40			REAL,
	SPEED40			REAL,
	DELAY40			REAL,
	VMT_07			REAL,
	VMT_40			REAL,
	TVMT_07			REAL,
	TVMT_40			REAL,
	FAF_VMT_07		REAL,
	FAF_VMT_40		REAL,
	YKTONS_07		REAL,
	YKTONS_40		REAL
);
