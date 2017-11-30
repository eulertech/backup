-- Script to create the AirMarkets tables
DROP TABLE IF EXISTS {schemaName}.{tableName}emissions;
CREATE TABLE {schemaName}.{tableName}emissions (
   state                     varchar(20),
   facility_name             varchar(100),
   facility_id_orispl        bigint,
   unit_id                   varchar(10),
   associated_stacks         varchar(100),
   year                      integer,
   date                      date,
   hour                      integer,
   programs                  varchar(200),
   operating_time            float8,
   gross_load_mw             float8,
   steam_load_5000lb_hr      float8,
   so2_pounds                float8,
   avg_nox_rate_lb_mmbtu     float8,
   nox_pounds                float8,
   co2_short_tons            float8,
   heat_input_mmbtu          float8,
   epa_region                integer,
   nerc_region               varchar(100),
   county                    varchar(100),
   source_category           varchar(100),
   owner                     varchar(2000),
   operator                  varchar(2000),
   representative_primary    varchar(2000),
   representative_secondary  varchar(2000),
   so2_phase                 varchar(200),
   nox_phase                 varchar(200),
   operating_status          varchar(200),
   unit_type                 varchar(200),
   fuel_type_primary         varchar(200),
   fuel_type_secondary       varchar(200),
   so2_controls              varchar(200),
   nox_controls              varchar(200),
   pm_controls               varchar(200),
   hg_controls               varchar(200),
   facility_latitude         float8,
   facility_longitude        float8
);

DROP TABLE IF EXISTS {schemaName}.{tableName}data_caveats;
CREATE TABLE {schemaName}.{tableName}data_caveats (
   state               varchar(10),
   facility_name       varchar(100),
   facility_id_orispl  bigint,
   unit_id             varchar(10),
   caveat              varchar(500)
);

--SELECT top 50 * FROM stl_load_errors order by starttime desc;