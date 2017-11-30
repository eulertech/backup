DROP TABLE IF EXISTS {schemaName}.{tableName}DAM;
CREATE TABLE {schemaName}.{tableName}DAM
(
    DeliveryDate            VARCHAR(100) ENCODE LZO,
    HourEnding              VARCHAR(100) ENCODE LZO,
    BusName                 VARCHAR(100) ENCODE LZO,
    LMP                     VARCHAR(100) ENCODE LZO,
    DSTFlag					CHAR(1) NULL ENCODE LZO
);

--this is a one-time execution script    
CREATE TABLE IF NOT EXISTS {schemaName}.{tableName}l2DAM_ts_Incremental
(
    BusName     VARCHAR(100) ENCODE LZO,
    ts          DATETIME,
    ISO		    VARCHAR(20) ENCODE LZO,
    LMP         FLOAT,
    LoadDate    DATE
);

DROP TABLE IF EXISTS {schemaName}.{tableName}dst_start_end;
CREATE TABLE {schemaName}.{tableName}dst_start_end
(
    year                    INT,
    dst_start               DATETIME,
    dst_end                 DATETIME    
);

DROP TABLE IF EXISTS {schemaName}.{tableName}dst_adjustment;
CREATE TABLE {schemaName}.{tableName}dst_adjustment
(
    DeliveryDate                VARCHAR(100) ENCODE LZO,
    HourEnding                  VARCHAR(100) ENCODE LZO,
    BusName                     VARCHAR(100) ENCODE LZO,
    LMP                         VARCHAR(100) ENCODE LZO,
    DSTFlag					    CHAR(1) NULL ENCODE LZO,
    DeliveryDate_ts             DATETIME,
    adjusted_DeliveryDate_ts    DATETIME
);
