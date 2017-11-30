DROP TABLE IF EXISTS {schemaName}.{tableName}caiso CASCADE;
CREATE TABLE {schemaName}.{tableName}caiso
(
   intervalstarttime_gmt  varchar(30),
   intervalendtime_gmt    varchar(30),
   opr_dt                 varchar(20),
   opr_hr                 integer,
   opr_interval           integer,
   node_id_xml            varchar(30),
   node_id                varchar(30),
   node                   varchar(30),
   market_run_id          varchar(10),
   lmp_type               varchar(10),
   xml_data_item          varchar(20),
   pnode_resmrid          varchar(30),
   grp_type               varchar(20),
   pos                    integer,
   mw                     float8,
   group1                 integer
);


DROP TABLE IF EXISTS {schemaName}.{tableName}ercot CASCADE;
CREATE TABLE {schemaName}.{tableName}ercot
(
   deliverydate  varchar(20),
   hourending    varchar(10),
   busname       varchar(20),
   lmp           float8,
   dstflag       varchar(10)
);


DROP TABLE IF EXISTS {schemaName}.{tableName}miso CASCADE;
CREATE TABLE {schemaName}.{tableName}miso
(
   name  varchar(20),
   lmp1  float8,
   mlc1  float8,
   mcc1  float8,
   lmp2  float8,
   mlc2  float8,
   mcc2  float8,
   lmp3  float8,
   mlc3  float8,
   mcc3  float8,
   lmp4  float8,
   mlc4  float8,
   mcc4  float8
);


DROP TABLE IF EXISTS {schemaName}.{tableName}neiso CASCADE;
CREATE TABLE {schemaName}.{tableName}neiso
(
   h                        varchar(10),
   date                     varchar(20),
   hourending               float8,
   locationid               float8,
   locationname             varchar(30),
   locationtype             varchar(20),
   locationalmarginalprice  float8,
   energycomponent          float8,
   congestioncomponent      float8,
   marginallosscomponent    float8
);



DROP TABLE IF EXISTS {schemaName}.{tableName}nyiso CASCADE;
CREATE TABLE {schemaName}.{tableName}nyiso
(
   timestamp1              varchar(20),
   name                    varchar(10),
   ptid                    integer,
   lbmp                    float8,
   marginalcostlosses      float8,
   marginalcostcongestion  float8
);


DROP TABLE IF EXISTS {schemaName}.{tableName}pjm CASCADE;
CREATE TABLE {schemaName}.{tableName}pjm
(
   publishdate  varchar(20),
   version      integer,
   zone         varchar(10),
   pnodeid      integer,
   pnodename    varchar(30),
   pnodetype    varchar(10),
   pricingtype  varchar(20),
   h1           float8,
   h2           float8,
   h3           float8,
   h4           float8,
   h5           float8,
   h6           float8,
   h7           float8,
   h8           float8,
   h9           float8,
   h10          float8,
   h11          float8,
   h12          float8,
   h13          float8,
   h14          float8,
   h15          float8,
   h16          float8,
   h17          float8,
   h18          float8,
   h19          float8,
   h20          float8,
   h21          float8,
   h22          float8,
   h23          float8,
   h24          float8
);



DROP TABLE IF EXISTS {schemaName}.{tableName}spp CASCADE;
CREATE TABLE {schemaName}.{tableName}spp
(
   interval1           varchar(20),
   gmtintervalend      varchar(20),
   settlementlocation  varchar(30),
   pnode               varchar(40),
   lmp                 float8,
   mlc                 float8,
   mcc                 float8,
   mec                 float8
);


