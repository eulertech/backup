-- Script to create the OPEC tables

DROP TABLE IF EXISTS {schemaName}.{tableName}_calendar CASCADE;
CREATE TABLE {schemaName}.{tableName}_calendar (
	idx_number INTEGER,
	effect_month VARCHAR(20) ENCODE LZO,
	meeting_number VARCHAR(10) ENCODE LZO,
	meeting_date VARCHAR(100) ENCODE LZO,
	meeting_ending_date VARCHAR(100) ENCODE LZO,
	quota REAL,
	change REAL,
	algeria REAL,
	angola REAL,
	ecuador REAL,
	indonesia REAL,
	ir REAL,
	iraq REAL,
	kuwait REAL,
	libya REAL,
	nigeria REAL,
	qatar REAL,
	saudiarabia REAL,
	uae REAL,
	venezuela REAL,
	total REAL,
	notes VARCHAR(50) ENCODE LZO
);
