DROP TABLE IF EXISTS {schemaName}.{tableName}Balancing_authority_map;
CREATE TABLE {schemaName}.{tableName}Balancing_authority_map
(
    BalancingAuthority                             VARCHAR(100) ENCODE LZO,
    Abbreviation                                   VARCHAR(100) ENCODE LZO,
    BalancingAuthorityAreaNERCName                 VARCHAR(100) ENCODE LZO,
    BalancingAuthorityAreaNERCSubRegionName        VARCHAR(100) ENCODE LZO,
    NERC                                           VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}Buyers_map;
CREATE TABLE {schemaName}.{tableName}Buyers_map
(
    Name                VARCHAR(100) ENCODE LZO, 
    Commonname          VARCHAR(100) ENCODE LZO,
    Type                VARCHAR(100) ENCODE LZO
);
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Class_name_map;                                                          
CREATE TABLE {schemaName}.{tableName}Class_name_map                                                          
(                                                          
    ClassName                           VARCHAR(100) ENCODE LZO,
    StandardizedClassName               VARCHAR(100) ENCODE LZO    
);                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}IHS_Project_Data;                                                          
CREATE TABLE {schemaName}.{tableName}IHS_Project_Data                                                          
(                                                          
    EERID                               VARCHAR(100) ENCODE LZO,                           
    EVID                                VARCHAR(100) ENCODE LZO,                          
    EIAID                               VARCHAR(100) ENCODE LZO,                           
    RECRegistry                         VARCHAR(100) ENCODE LZO,                                 
    RECRegistryID                       VARCHAR(100) ENCODE LZO,                                   
    StateRegistryID                     VARCHAR(100) ENCODE LZO,                                     
    QFID                                VARCHAR(100) ENCODE LZO,                          
    TexasRegion                         VARCHAR(100) ENCODE LZO,                                 
    Country                             VARCHAR(100) ENCODE LZO,                             
    Status                              VARCHAR(100) ENCODE LZO,                            
    Region                              VARCHAR(100) ENCODE LZO,                            
    Grid                                VARCHAR(100) ENCODE LZO,                          
    Subregion                           VARCHAR(100) ENCODE LZO,                                
    ProjectName                         VARCHAR(100) ENCODE LZO,                                 
    Technology                          VARCHAR(100) ENCODE LZO,                                
    ProjectPhaseName                    VARCHAR(100) ENCODE LZO,                                      
    NameplateCapacity                   VARCHAR(100) ENCODE LZO,                                       
    Yearonline                          VARCHAR(100) ENCODE LZO,     
    Unnamed                             VARCHAR(100) ENCODE LZO,     
    ProjectDeveloper1                   VARCHAR(100) ENCODE LZO,                                       
    ProjectDeveloper2                   VARCHAR(100) ENCODE LZO,                                       
    ProjectOwner1                       VARCHAR(100) ENCODE LZO,                                   
    Owner1stake_Percent                 VARCHAR(100) ENCODE LZO,                                         
    ProjectOwner2                       VARCHAR(100) ENCODE LZO,                                   
    Owner2stake_Percent                 VARCHAR(100) ENCODE LZO,                                         
    ProjectOwner3                       VARCHAR(100) ENCODE LZO,                                   
    Owner3stake_Percent                 VARCHAR(100) ENCODE LZO,                                         
    TurbineVendor                       VARCHAR(100) ENCODE LZO,                                   
    TurbineModel                        VARCHAR(100) ENCODE LZO,                                  
    Turbinenameplate                    VARCHAR(100) ENCODE LZO,                                      
    ofTurbines                          VARCHAR(100) ENCODE LZO,                                
    RotorDiameter                       VARCHAR(100) ENCODE LZO,                                   
    OfftakeMode                         VARCHAR(100) ENCODE LZO,                                 
    PPA1                                VARCHAR(100) ENCODE LZO,                          
    PPA2                                VARCHAR(100) ENCODE LZO,                          
    PPA2_Percent                        VARCHAR(100) ENCODE LZO,                                  
    PPA3                                VARCHAR(100) ENCODE LZO,                          
    PPA3_Percent                        VARCHAR(100) ENCODE LZO,                                  
    PPA4                                VARCHAR(100) ENCODE LZO,                          
    PPA4_Percent                        VARCHAR(100) ENCODE LZO,                                  
    OtherRECPurchaser                   VARCHAR(100) ENCODE LZO,                                       
    PPAdate                             VARCHAR(100) ENCODE LZO,                             
    InitialPPAPrice_US                  VARCHAR(100) ENCODE LZO,                                        
    Repowered                           VARCHAR(100) ENCODE LZO,                               
    RegimeID1                           VARCHAR(100) ENCODE LZO,                               
    Notes                               VARCHAR(500) ENCODE LZO,                           
    Source                              VARCHAR(100) ENCODE LZO,                            
    AnnouncementDate                    VARCHAR(100) ENCODE LZO,                                      
    UCUpdate                            VARCHAR(100) ENCODE LZO,                              
    OnlineUpdated                       VARCHAR(100) ENCODE LZO,                                   
    LatestDateUpdated                   VARCHAR(100) ENCODE LZO,                                       
    LastUpdateby_Analyst                VARCHAR(100) ENCODE LZO
);                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Increment_peaking_name_map;                                                          
CREATE TABLE {schemaName}.{tableName}Increment_peaking_name_map                                                          
(                                                          
    IncrementPeakingName            VARCHAR(100) ENCODE LZO,
    StandardizedPeakingName         VARCHAR(100) ENCODE LZO                                             
);                                                          
                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Product_map CASCADE;                                                          
CREATE TABLE {schemaName}.{tableName}Product_map                                                          
(                                                          
    ProductNameRaw          VARCHAR(100) ENCODE LZO,
    ProdcutNameMapped       VARCHAR(100) ENCODE LZO                                                          
);                                                          
                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Rate_units_map;                                                          
CREATE TABLE {schemaName}.{tableName}Rate_units_map                                                          
(                                                          
    Units                   VARCHAR(100) ENCODE LZO,    
    StandardizedUnits       VARCHAR(100) ENCODE LZO                                                          
);                                                          

                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Seller_EIA_ID CASCADE;
CREATE TABLE {schemaName}.{tableName}Seller_EIA_ID
(
    Seller			VARCHAR(100) ENCODE LZO,
    EIA_ID			INTEGER
);

                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Sellers_map;                                                          
CREATE TABLE {schemaName}.{tableName}Sellers_map                                                          
(                                                          
    Name                    VARCHAR(100) ENCODE LZO,
    Type                    VARCHAR(100) ENCODE LZO,
    EERID                   VARCHAR(100) ENCODE LZO                                                          
);                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Super_EER_ID_Jaime_map;                                                          
CREATE TABLE {schemaName}.{tableName}Super_EER_ID_Jaime_map                                                          
(                                                          
    SuperEERID              VARCHAR(100) ENCODE LZO,    
    IndividualEERID         VARCHAR(100) ENCODE LZO
);                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Time_Zone_map;                                                          
CREATE TABLE {schemaName}.{tableName}Time_Zone_map                                                          
(                                                          
    TimeZone                VARCHAR(100) ENCODE LZO,
	Definition              VARCHAR(100) ENCODE LZO                                                          
);                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}Trading_Hubs_map;                                                          
CREATE TABLE {schemaName}.{tableName}Trading_Hubs_map                                                          
(                                                          
    PointOfDeliverySpecificLocation         VARCHAR(100) ENCODE LZO,
    TradingHubStandardized                  VARCHAR(100) ENCODE LZO
);                                                          
                                                          
                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}bilinear_lat_lon;                                                          
CREATE TABLE {schemaName}.{tableName}bilinear_lat_lon                                                          
(
	Date                        VARCHAR(100) ENCODE LZO,                                  
	Wind_Speed_22m              VARCHAR(100) ENCODE LZO,                                            
	Wind_Direction_22m          VARCHAR(100) ENCODE LZO,                                                
	Temperature_22m             VARCHAR(100) ENCODE LZO,                                             
	Height_22m                  VARCHAR(100) ENCODE LZO,                                        
	Pressure_22m                VARCHAR(100) ENCODE LZO,                                          
	Density_22m                 VARCHAR(100) ENCODE LZO,
    dataProvider                VARCHAR(100) ENCODE LZO,
    extractedOn                 VARCHAR(100) ENCODE LZO,
    model                       VARCHAR(100) ENCODE LZO,
    verticalLevel               VARCHAR(100) ENCODE LZO,
    horizontalInte              VARCHAR(100) ENCODE LZO,
    iValue                      VARCHAR(100) ENCODE LZO,
    jValue                      VARCHAR(100) ENCODE LZO,
    latitude                    VARCHAR(100) ENCODE LZO,
    longitude                   VARCHAR(100) ENCODE LZO
);                                                          
                                                                          
DROP TABLE IF EXISTS {schemaName}.{tableName}ERCOT_Node_Zone_Map;                                                          
CREATE TABLE {schemaName}.{tableName}ERCOT_Node_Zone_Map                                                          
(
ISO_Name	                    varchar(100) ENCODE LZO,
ISO_ID                      	integer,
Model_Type						varchar(100) ENCODE LZO,
Model_Name						varchar(100) ENCODE LZO,
Model_ID						integer,
Price_Node_Name					varchar(100) ENCODE LZO,
Price_Node_ID					integer,
Price_Node_Type					varchar(100) ENCODE LZO,
Model_Weighting_Factor			float8,
Record_Count					integer
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}Electric_Plant_in_Service_Row_Map;
CREATE TABLE {schemaName}.{tableName}Electric_Plant_in_Service_Row_Map                                                          
(
Row_Number 						integer,
Description						varchar(100) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}O_and_M_Row_Map;
CREATE TABLE {schemaName}.{tableName}O_and_M_Row_Map                                                          
(
Row_Number 						integer,
Description						varchar(200) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}OandM_Fields;
CREATE TABLE {schemaName}.{tableName}OandM_Fields                                                          
(
Description						varchar(200) ENCODE LZO,
Responden2						varchar(200) ENCODE LZO,
Crnt_Yr_Am 						BIGINT
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}OandMTotals;
CREATE TABLE {schemaName}.{tableName}OandMTotals                                                          
(
Sum_of_Crnt_Yr_Am_Column_Labels	varchar(500) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}Payroll_Map;
CREATE TABLE {schemaName}.{tableName}Payroll_Map                                                          
(
Row_Number 						integer,
Description						varchar(200) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}ErcotBus_923Plant_Map;
CREATE TABLE {schemaName}.{tableName}ErcotBus_923Plant_Map                                                          
(
price_node_name					varchar(100) ENCODE LZO,
k_settlementpoint				varchar(100) ENCODE LZO,
k_lat							float8,
k_lon							float8,
p_utilityid 					integer,
p_utilityname					varchar(100) ENCODE LZO,
p_plantname						varchar(100) ENCODE LZO,
p_plantcode 					integer,
p_latitude						float8,
p_longitude						float8
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}Bus2FuelType_Map;
CREATE TABLE {schemaName}.{tableName}Bus2FuelType_Map                                                          
(
price_node_name					varchar(100) ENCODE LZO,
fueltype						varchar(100) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}ErcotGenBus_923Plant_Map CASCADE;
CREATE TABLE {schemaName}.{tableName}ErcotGenBus_923Plant_Map                                                          
(
ISO_Name						varchar(100) ENCODE LZO,
price_node_name					varchar(100) ENCODE LZO,
Model_Name						varchar(100) ENCODE LZO,
Price_Node_Type					varchar(100) ENCODE LZO,
p_plantcode 					integer,
superbusname					varchar(100) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}CountyZone_Map CASCADE;
CREATE TABLE {schemaName}.{tableName}CountyZone_Map                                                          
(
County							varchar(100) ENCODE LZO,
zone							varchar(100) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}FederalHolidays CASCADE;
CREATE TABLE {schemaName}.{tableName}FederalHolidays                                                          
(
Holiday							varchar(100) ENCODE LZO,
Schedule						varchar(100) ENCODE LZO,
Date							varchar(100) ENCODE LZO
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}ERCOT_HourlyDemand CASCADE;
CREATE TABLE {schemaName}.{tableName}ERCOT_HourlyDemand                                                          
(
Time_Period						varchar(100) ENCODE LZO,
Hour_Beginning					varchar(100) ENCODE LZO,	
ERCOT_Houston					float8,
ERCOT_North						float8,
ERCOT_South						float8,	
ERCOT_West						float8,
ERCOT_Panhandle					float8
);                                                          

DROP TABLE IF EXISTS {schemaName}.{tableName}ERCOT_Zonal_Wind CASCADE;
CREATE TABLE {schemaName}.{tableName}ERCOT_Zonal_Wind                                                          
(
Date							varchar(100) ENCODE LZO,
South_Houston					float8,
West_North						float8,
Houston							float8,
North							float8,
South							float8,
West							float8
);

DROP TABLE IF EXISTS {schemaName}.{tableName}Coal_Mapping_AM_860 CASCADE;
CREATE TABLE {schemaName}.{tableName}Coal_Mapping_AM_860                                                          
(
AM_facility_id_orispl			varchar(10) ENCODE LZO,
AM_unit_id						varchar(10) ENCODE LZO,
EIA860_Plantcode				varchar(10) ENCODE LZO,
EIA860_generatorid				varchar(10) ENCODE LZO,
Flag_Manually_Mapped_by_AA		float8
);

DROP TABLE IF EXISTS {schemaName}.{tableName}ERCOT_HourlyPrices CASCADE;
CREATE TABLE {schemaName}.{tableName}ERCOT_HourlyPrices
(	
	Hour_Beginning					varchar(100) ENCODE LZO,	
	ERCOT_Houston					float8,
	ERCOT_South						float8,	
	ERCOT_West						float8,
	ERCOT_North						float8
);   

DROP TABLE IF EXISTS {schemaName}.{tableName}ERCOT_Aurora_Prices CASCADE;
CREATE TABLE {schemaName}.{tableName}ERCOT_Aurora_Prices
(	
	Hour_Beginning					varchar(100) ENCODE LZO,	
	ERCOT_Houston					float8,
	ERCOT_South						float8,	
	ERCOT_West						float8,
	ERCOT_North						float8
);   

DROP TABLE IF EXISTS {schemaName}.{tableName}Coal_Mapping_AM_860_inclERCOT CASCADE;
CREATE TABLE {schemaName}.{tableName}Coal_Mapping_AM_860_inclERCOT                                                          
(
AM_facility_id_orispl			varchar(10) ENCODE LZO,
AM_unit_id						varchar(10) ENCODE LZO,
EIA860_Plantcode				varchar(10) ENCODE LZO,
EIA860_generatorid				varchar(10) ENCODE LZO,
Flag_Manually_Mapped_by_AA		float8,
IHS_ID							varchar(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}Hourly_ERCOT_Zonal_Forecast CASCADE;
CREATE TABLE {schemaName}.{tableName}Hourly_ERCOT_Zonal_Forecast
(
	Hour_Beginning_Standard_Time    varchar(24) ENCODE LZO,
	zonal_price_Houston             FLOAT,
	zonal_price_North               FLOAT,
	zonal_price_South               FLOAT,
	zonal_price_West                FLOAT,
	zonal_price_Panhandle           FLOAT,
	demand_Houston                  FLOAT,
	demand_North                    FLOAT,
	demand_South                    FLOAT,
	demand_West                     FLOAT,
	demand_Panhandle                FLOAT,
	generation_Houston_Hydro        FLOAT,
	generation_North_Hydro          FLOAT,
	generation_South_Hydro          FLOAT,
	generation_North_Nuclear        FLOAT,
	generation_South_Nuclear        FLOAT,
	generation_North_Wind           FLOAT,
	generation_South_Wind           FLOAT,
	generation_West_Wind            FLOAT,
	generation_Panhandle_Wind       FLOAT,
	generation_Houston_Solar        FLOAT,
	generation_North_Solar          FLOAT,
	generation_South_Solar          FLOAT,
	generation_West_Solar           FLOAT
);
