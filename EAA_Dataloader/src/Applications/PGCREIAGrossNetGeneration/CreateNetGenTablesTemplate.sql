DROP TABLE IF EXISTS {schemaName}.{tableName}GrossAndNetGen;
CREATE TABLE {schemaName}.{tableName}GrossAndNetGen
(
    Year                INT,
    Month               INT,
    FACILITY_CODE       INT,
    STATE_CODE          VARCHAR(2) ENCODE LZO,
    SECTOR_CODE         INT,
    FUEL_2002           VARCHAR(100) ENCODE LZO,
    FUEL_TYPE           VARCHAR(100) ENCODE LZO,
    GROSS_GENERATION    FLOAT,
    GENERATION          FLOAT,
    ESTIMATION_FLAG     VARCHAR(1) ENCODE LZO,
    PRIME_MOVER         VARCHAR(2) ENCODE LZO,
    GENERATOR_ID        VARCHAR(100) ENCODE LZO
);

