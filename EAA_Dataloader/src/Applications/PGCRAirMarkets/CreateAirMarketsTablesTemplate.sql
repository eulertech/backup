-- Script to create the AirMarkets tables
DROP TABLE IF EXISTS {schemaName}.{tableName}emission;
CREATE TABLE {schemaName}.{tableName}emission (
   state                     varchar(100) ENCODE LZO,
   facility_name             varchar(1000) ENCODE LZO,
   facility_id_orispl        BIGINT,
   unit_id                   varchar(10) ENCODE LZO,
   associated_stacks         varchar(100) ENCODE LZO,
   year                      integer,
   date                      varchar(10) ENCODE LZO,
   hour                      integer,
   programs                  varchar(200) ENCODE LZO,
   operating_time            float8,
   gross_load_mw             float8,
   steam_load_5000lb_hr      float8,
   so2_pounds                float8,
   avg_nox_rate_lb_mmbtu     float8,
   nox_pounds                float8,
   co2_short_tons            float8,
   heat_input_mmbtu          float8,
   epa_region                integer,
   nerc_region               varchar(100) ENCODE LZO,
   county                    varchar(100) ENCODE LZO,
   source_category           varchar(100) ENCODE LZO,
   owner                     varchar(2000) ENCODE LZO,
   operator                  varchar(2000) ENCODE LZO,
   representative_primary    varchar(3000) ENCODE LZO,
   representative_secondary  varchar(3000) ENCODE LZO,
   so2_phase                 varchar(200) ENCODE LZO,
   nox_phase                 varchar(200) ENCODE LZO,
   operating_status          varchar(200) ENCODE LZO,
   unit_type                 varchar(200) ENCODE LZO,
   fuel_type_primary         varchar(200) ENCODE LZO,
   fuel_type_secondary       varchar(200) ENCODE LZO,
   so2_controls              varchar(200) ENCODE LZO,
   nox_controls              varchar(200) ENCODE LZO,
   pm_controls               varchar(200) ENCODE LZO,
   hg_controls               varchar(200) ENCODE LZO,
   facility_latitude         float8,
   facility_longitude        float8
);

-- Needs to be added upon request and see how to summarize or aggregate the data first.
--DROP TABLE IF EXISTS {schemaName}.{tableName}data_caveats;
--CREATE TABLE {schemaName}.{tableName}data_caveats (
--   state               varchar(10) ENCODE LZO,
--   facility_name       varchar(100) ENCODE LZO,
--   facility_id_orispl  bigint,
--   unit_id             varchar(10) ENCODE LZO,
--   caveat              varchar(500)  ENCODE LZO
--);

--SELECT top 50 * FROM stl_load_errors order by starttime desc;