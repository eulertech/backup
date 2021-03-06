-- Script to create the tables
DROP TABLE IF EXISTS {schemaName}.{tableName}Balance;
CREATE TABLE {schemaName}.{tableName}Balance
(
BA	VARCHAR(10) ENCODE LZO,
UTC_DATETIME	VARCHAR(30) ENCODE LZO,
LOCAL_TIMESTAMP	VARCHAR(30) ENCODE LZO,
LOCAL_DATE	VARCHAR(30) ENCODE LZO,
LOCAL_HOUR_INT	INTEGER,
VAL_D	INTEGER,
VAL_DF	INTEGER,
VAL_NG	INTEGER,
VAL_TI	INTEGER
);

DROP TABLE IF EXISTS {schemaName}.{tableName}Interchange;
CREATE TABLE {schemaName}.{tableName}Interchange
(
BA	VARCHAR(10) ENCODE LZO,
UTC_DATETIME	VARCHAR(30) ENCODE LZO,
LOCAL_TIMESTAMP	VARCHAR(30) ENCODE LZO,
LOCAL_DATE	VARCHAR(30) ENCODE LZO,
LOCAL_HOUR_INT	INTEGER,
DIBA	VARCHAR(10) ENCODE LZO,
VALUE	INTEGER
);

--SELECT top 50 * FROM stl_load_errors order by starttime desc;