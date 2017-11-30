DROP TABLE IF EXISTS {schemaName}.{tableName}plant_details CASCADE;
CREATE TABLE {schemaName}.{tableName}plant_details
(
    name                VARCHAR(100) ENCODE LZO,
    settlementPoint     VARCHAR(100) ENCODE LZO,
    plantName           VARCHAR(100) ENCODE LZO,
    plantAddress        VARCHAR(100) ENCODE LZO,
    county				VARCHAR(100) NULL ENCODE LZO,
    utility				VARCHAR(100) NULL ENCODE LZO,
    latitude			VARCHAR(100) NULL ENCODE LZO,
    longitude			VARCHAR(100) NULL ENCODE LZO
);

--This table has data which is part manually curated. Not part of this data load.
--When moving into production, copy from this table
CREATE TABLE IF NOT EXISTS {schemaName}.{tableName}bus_plant_mapping
(
    Match                                                    VARCHAR(100) ENCODE LZO,
    k_lat                                                    VARCHAR(100) ENCODE LZO,
    k_lon                                                    VARCHAR(100) ENCODE LZO,
    p_lat                                                    VARCHAR(100) ENCODE LZO,
    p_lon                                                    VARCHAR(100) ENCODE LZO,
    k_plantname                                              VARCHAR(100) ENCODE LZO,
    p_plantname                                              VARCHAR(100) ENCODE LZO,
    k_settlementpoint                                        VARCHAR(100) ENCODE LZO,
    k_plantaddres                                            VARCHAR(100) ENCODE LZO,
    k_county                                                 VARCHAR(100) ENCODE LZO,
    k_utility                                                VARCHAR(100) ENCODE LZO,
    p_utilityid                                              VARCHAR(100) ENCODE LZO,
    p_utilityname                                            VARCHAR(100) ENCODE LZO,
    p_plantcode                                              VARCHAR(100) ENCODE LZO,
    p_streetaddress                                          VARCHAR(100) ENCODE LZO,
    p_city                                                   VARCHAR(100) ENCODE LZO,
    p_state                                                  VARCHAR(100) ENCODE LZO,
    p_zip                                                    VARCHAR(100) ENCODE LZO,
    p_county                                                 VARCHAR(100) ENCODE LZO,
    p_latitude                                               VARCHAR(100) ENCODE LZO,
    p_longitude                                              VARCHAR(100) ENCODE LZO,
    p_nercregion                                             VARCHAR(100) ENCODE LZO,
    p_balancingauthoritycode                                 VARCHAR(100) ENCODE LZO,
    p_balancingauthorityname                                 VARCHAR(100) ENCODE LZO,
    p_nameofwatersource                                      VARCHAR(100) ENCODE LZO,
    p_primarypurpose_naicscode                               VARCHAR(100) ENCODE LZO,
    p_regulatorystatus                                       VARCHAR(100) ENCODE LZO,
    p_sector                                                 VARCHAR(100) ENCODE LZO,
    p_sectorname                                             VARCHAR(100) ENCODE LZO,
    p_netmetering_forfacilitieswithsolarorwindgeneration     VARCHAR(100) ENCODE LZO,
    p_ferccogenerationstatus                                 VARCHAR(100) ENCODE LZO,
    p_ferccogenerationdocketnumber                           VARCHAR(100) ENCODE LZO,
    p_fercsmallpowerproducerstatus                           VARCHAR(100) ENCODE LZO,
    p_fercsmallpowerproducerdocketnumber                     VARCHAR(100) ENCODE LZO,
    p_fercexemptwholesalegeneratorstatus                     VARCHAR(100) ENCODE LZO,
    p_fercexemptwholesalegeneratordocketnumber               VARCHAR(100) ENCODE LZO,
    p_ashimpoundment                                         VARCHAR(100) ENCODE LZO,
    p_ashimpoundmentlined                                    VARCHAR(100) ENCODE LZO,
    p_ashimpoundmentstatus                                   VARCHAR(100) ENCODE LZO,
    p_transmissionordistributionsystemowner                  VARCHAR(100) ENCODE LZO,
    p_transmissionordistributionsystemownerid                VARCHAR(100) ENCODE LZO,
    p_transmissionordistributionsystemownerstate             VARCHAR(100) ENCODE LZO,
    p_gridvoltage_kv                                         VARCHAR(100) ENCODE LZO,
    p_gridvoltage2_kv                                        VARCHAR(100) ENCODE LZO,
    p_gridvoltage3_kv                                        VARCHAR(100) ENCODE LZO,
    p_naturalgaspipelinename                                 VARCHAR(100) ENCODE LZO
);
