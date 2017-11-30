-- Script to create the Form-923 tables
DROP TABLE IF EXISTS {schemaName}.{tableName}1_Utility CASCADE;
CREATE TABLE {schemaName}.{tableName}1_Utility
(
    UtilityID						INTEGER,
    UtilityName						VARCHAR(100) ENCODE LZO,
    StreetAddress					VARCHAR(100) ENCODE LZO,
    City							VARCHAR(100) ENCODE LZO,
    State							VARCHAR(10) ENCODE LZO,
    Zip								VARCHAR(10) ENCODE LZO,
    OwnerofPlants					VARCHAR(10) ENCODE LZO,
    OperatorofPlants				VARCHAR(10) ENCODE LZO,
    AssetManagerofPlants			VARCHAR(10) ENCODE LZO,
    OtherRelationshipswithPlants	VARCHAR(10) ENCODE LZO,
    EntityType						VARCHAR(10) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}2_Plant CASCADE;
CREATE TABLE {schemaName}.{tableName}2_Plant
(
    UtilityID													INTEGER,
    UtilityName													VARCHAR(50) ENCODE LZO,
    PlantCode													INTEGER,
    PlantName													VARCHAR(100) ENCODE LZO,
    StreetAddress												VARCHAR(100) ENCODE LZO,
    City														VARCHAR(100) ENCODE LZO,
    State														VARCHAR(10) ENCODE LZO,
    Zip															INTEGER,
    County														VARCHAR(50) ENCODE LZO,
    Latitude													FLOAT8,
    Longitude													FLOAT8,
    NERCRegion													VARCHAR(10) ENCODE LZO,
    BalancingAuthorityCode										VARCHAR(10) ENCODE LZO,
    BalancingAuthorityName										VARCHAR(100) ENCODE LZO,
    NameofWaterSource											VARCHAR(100) ENCODE LZO,
    PrimaryPurpose_NAICSCode									INTEGER,
    RegulatoryStatus											VARCHAR(10) ENCODE LZO,
    Sector														INTEGER,
    SectorName													VARCHAR(20) ENCODE LZO,
    NetMetering_forfacilitieswithsolarorwindgeneration			VARCHAR(10) ENCODE LZO,
    FERCCogenerationStatus										VARCHAR(10) ENCODE LZO,
    FERCCogenerationDocketNumber								VARCHAR(100) ENCODE LZO,
    FERCSmallPowerProducerStatus								VARCHAR(10) ENCODE LZO,
    FERCSmallPowerProducerDocketNumber							VARCHAR(100) ENCODE LZO,
    FERCExemptWholesaleGeneratorStatus							VARCHAR(10) ENCODE LZO,
    FERCExemptWholesaleGeneratorDocketNumber					VARCHAR(100) ENCODE LZO,
    AshImpoundment												VARCHAR(10) ENCODE LZO,
    AshImpoundmentLined											VARCHAR(10) ENCODE LZO,
    AshImpoundmentStatus										VARCHAR(10) ENCODE LZO,
    TransmissionorDistributionSystemOwner						VARCHAR(100) ENCODE LZO,
    TransmissionorDistributionSystemOwnerID						INTEGER,
    TransmissionorDistributionSystemOwnerState					VARCHAR(10) ENCODE LZO,
    GridVoltage_kV												FLOAT8,
    GridVoltage2_kV												FLOAT8,
    GridVoltage3_kV												FLOAT8,
    NaturalGasPipelineName										VARCHAR(100) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}3_1_GeneratorOperable CASCADE;
CREATE TABLE {schemaName}.{tableName}3_1_GeneratorOperable
(
    UtilityID	                                                    INTEGER,
    UtilityName	                                                    VARCHAR(100) ENCODE LZO,
    PlantCode	                                                    INTEGER,
    PlantName	                                                    VARCHAR(100) ENCODE LZO,
    State		                                                    VARCHAR(100) ENCODE LZO,
    County		                                                    VARCHAR(100) ENCODE LZO,
    GeneratorID	                                                    VARCHAR(100) ENCODE LZO,
    Technology	                                                    VARCHAR(100) ENCODE LZO,
    PrimeMover	                                                    VARCHAR(100) ENCODE LZO,
    UnitCode	                                                    VARCHAR(100) ENCODE LZO,
    Ownership														VARCHAR(100) ENCODE LZO,
    DuctBurners														VARCHAR(100) ENCODE LZO,
    CanBypassHeatRecoverySteamGenerator								VARCHAR(100) ENCODE LZO,
    RTO_ISOLMPNodeDesignation										VARCHAR(100) ENCODE LZO,
    RTO_ISOLocationDesignationforReportingWholesaleSalesDatatoFERC	VARCHAR(100) ENCODE LZO,
    NameplateCapacity_MW	                                        VARCHAR(100) ENCODE LZO,
    NameplatePowerFactor	                                        VARCHAR(100) ENCODE LZO,
    SummerCapacity_MW	                                            VARCHAR(100) ENCODE LZO,
    WinterCapacity_MW	                                            VARCHAR(100) ENCODE LZO,
    MinimumLoad_MW	                                                VARCHAR(100) ENCODE LZO,
    UprateorDerateCompletedDuringYear	                            VARCHAR(100) ENCODE LZO,
    MonthUprateorDerateCompleted	                                VARCHAR(100) ENCODE LZO,
    YearUprateorDerateCompleted	                                    VARCHAR(100) ENCODE LZO,
    Status	                                                        VARCHAR(100) ENCODE LZO,
    SynchronizedtoTransmissionGrid	                                VARCHAR(100) ENCODE LZO,
    OperatingMonth	                                                VARCHAR(100) ENCODE LZO,
    OperatingYear	                                                VARCHAR(100) ENCODE LZO,
    PlannedRetirementMonth	                                        VARCHAR(100) ENCODE LZO,
    PlannedRetirementYear	                                        VARCHAR(100) ENCODE LZO,
    AssociatedwithCombinedHeatandPowerSystem	                    VARCHAR(100) ENCODE LZO,
    SectorName	                                                    VARCHAR(100) ENCODE LZO,
    Sector	                                                        VARCHAR(100) ENCODE LZO,
    ToppingorBottoming	                                            VARCHAR(100) ENCODE LZO,
    EnergySource1	                                                VARCHAR(100) ENCODE LZO,
    EnergySource2	                                                VARCHAR(100) ENCODE LZO,
    EnergySource3	                                                VARCHAR(100) ENCODE LZO,
    EnergySource4	                                                VARCHAR(100) ENCODE LZO,
    EnergySource5	                                                VARCHAR(100) ENCODE LZO,
    EnergySource6	                                                VARCHAR(100) ENCODE LZO,
    StartupSource1	                                                VARCHAR(100) ENCODE LZO,
    StartupSource2	                                                VARCHAR(100) ENCODE LZO,
    StartupSource3	                                                VARCHAR(100) ENCODE LZO,
    StartupSource4	                                                VARCHAR(100) ENCODE LZO,
    SolidFuelGasificationSystem	                                    VARCHAR(100) ENCODE LZO,
    CarbonCaptureTechnology	                                        VARCHAR(100) ENCODE LZO,
    Turbines_Inverters_orHydrokineticBuoys	                        VARCHAR(100) ENCODE LZO,
    TimefromColdShutdowntoFullLoad	                                VARCHAR(100) ENCODE LZO,
    FluidizedBedTechnology	                                        VARCHAR(100) ENCODE LZO,
    PulverizedCoalTechnology	                                    VARCHAR(100) ENCODE LZO,
    StokerTechnology	                                            VARCHAR(100) ENCODE LZO,
    OtherCombustionTechnology	                                    VARCHAR(100) ENCODE LZO,
    SubcriticalTechnology	                                        VARCHAR(100) ENCODE LZO,
    SupercriticalTechnology	                                        VARCHAR(100) ENCODE LZO,
    UltrasupercriticalTechnology	                                VARCHAR(100) ENCODE LZO,
    PlannedNetSummerCapacityUprate_MW	                            VARCHAR(100) ENCODE LZO,
    PlannedNetWinterCapacityUprate_MW	                            VARCHAR(100) ENCODE LZO,
    PlannedUprateMonth	                                            VARCHAR(100) ENCODE LZO,
    PlannedUprateYear	                                            VARCHAR(100) ENCODE LZO,
    PlannedNetSummerCapacityDerate_MW	                            VARCHAR(100) ENCODE LZO,
    PlannedNetWinterCapacityDerate_MW	                            VARCHAR(100) ENCODE LZO,
    PlannedDerateMonth	                                            VARCHAR(100) ENCODE LZO,
    PlannedDerateYear	                                            VARCHAR(100) ENCODE LZO,
    PlannedNewPrimeMover	                                        VARCHAR(100) ENCODE LZO,
    PlannedEnergySource1	                                        VARCHAR(100) ENCODE LZO,
    PlannedNewNameplateCapacity_MW	                                VARCHAR(100) ENCODE LZO,
    PlannedRepowerMonth	                                            VARCHAR(100) ENCODE LZO,
    PlannedRepowerYear	                                            VARCHAR(100) ENCODE LZO,
    OtherPlannedModifications	                                    VARCHAR(100) ENCODE LZO,
    OtherModificationsMonth	                                        VARCHAR(100) ENCODE LZO,
    OtherModificationsYear	                                        VARCHAR(100) ENCODE LZO,
    CofireFuels	                                                    VARCHAR(100) ENCODE LZO,
    SwitchBetweenOilandNaturalGas	                                VARCHAR(100) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}3_1_GeneratorProposed CASCADE;
CREATE TABLE {schemaName}.{tableName}3_1_GeneratorProposed
(
	UtilityID								    INTEGER,
	UtilityName								    VARCHAR(100) ENCODE LZO,
	PlantCode								    VARCHAR(100) ENCODE LZO,
	PlantName								    VARCHAR(100) ENCODE LZO,
	State									    VARCHAR(100) ENCODE LZO,
	County									    VARCHAR(100) ENCODE LZO,
	GeneratorID								    VARCHAR(100) ENCODE LZO,
	Technology								    VARCHAR(100) ENCODE LZO,
	PrimeMover								    VARCHAR(100) ENCODE LZO,
	UnitCode								    VARCHAR(100) ENCODE LZO,
	Ownership								    VARCHAR(100) ENCODE LZO,
	DuctBurners								    VARCHAR(100) ENCODE LZO,
	CanBypassHeatRecoverySteamGenerator		    VARCHAR(100) ENCODE LZO,
	RTO_ISOLMPNodeDesignation				    VARCHAR(100) ENCODE LZO,
	RTO_ISOLocationDesignation				    VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW					    VARCHAR(100) ENCODE LZO,
	NameplatePowerFactor					    VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW						    VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW						    VARCHAR(100) ENCODE LZO,
	Status									    VARCHAR(100) ENCODE LZO,
	EffectiveMonth							    VARCHAR(100) ENCODE LZO,
	EffectiveYear							    VARCHAR(100) ENCODE LZO,
	CurrentMonth							    VARCHAR(100) ENCODE LZO,
	CurrentYear								    VARCHAR(100) ENCODE LZO,
	AssociatedwithCombinedHeatandPowerSystem    VARCHAR(100) ENCODE LZO,
	SectorName								    VARCHAR(100) ENCODE LZO,
	Sector									    VARCHAR(100) ENCODE LZO,
	PreviouslyCanceled						    VARCHAR(100) ENCODE LZO,
	EnergySource1							    VARCHAR(100) ENCODE LZO,
	EnergySource2							    VARCHAR(100) ENCODE LZO,
	EnergySource3							    VARCHAR(100) ENCODE LZO,
	EnergySource4							    VARCHAR(100) ENCODE LZO,
	EnergySource5							    VARCHAR(100) ENCODE LZO,
	EnergySource6							    VARCHAR(100) ENCODE LZO,
	Turbines_Inverters_orHydrokineticBuoys	    VARCHAR(100) ENCODE LZO,
	FluidizedBedTechnology					    VARCHAR(100) ENCODE LZO,
	PulverizedCoalTechnology				    VARCHAR(100) ENCODE LZO,
	StokerTechnology						    VARCHAR(100) ENCODE LZO,
	OtherCombustionTechnology				    VARCHAR(100) ENCODE LZO,
	SubcriticalTechnology					    VARCHAR(100) ENCODE LZO,
	SupercriticalTechnology					    VARCHAR(100) ENCODE LZO,
	UltrasupercriticalTechnology			    VARCHAR(100) ENCODE LZO,
	SolidFuelGasificationSystem				    VARCHAR(100) ENCODE LZO,
	CarbonCaptureTechnology					    VARCHAR(100) ENCODE LZO,
	SwitchBetweenOilandNaturalGas			    VARCHAR(100) ENCODE LZO,
	CofireFuels								    VARCHAR(100) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}3_1_GeneratorRetCancelled CASCADE;
CREATE TABLE {schemaName}.{tableName}3_1_GeneratorRetCancelled
(
	UtilityID								    INTEGER,
	UtilityName								    VARCHAR(100) ENCODE LZO,
	PlantCode								    VARCHAR(100) ENCODE LZO,
	PlantName								    VARCHAR(100) ENCODE LZO,
	State									    VARCHAR(100) ENCODE LZO,
	County									    VARCHAR(100) ENCODE LZO,
	GeneratorID								    VARCHAR(100) ENCODE LZO,
	Technology								    VARCHAR(100) ENCODE LZO,
	PrimeMover								    VARCHAR(100) ENCODE LZO,
	UnitCode								    VARCHAR(100) ENCODE LZO,
	Ownership								    VARCHAR(100) ENCODE LZO,
	DuctBurners								    VARCHAR(100) ENCODE LZO,
	CanBypassHeatRecoverySteamGenerator		    VARCHAR(100) ENCODE LZO,
	RTO_ISOLMPNodeDesignation				    VARCHAR(100) ENCODE LZO,
	RTO_ISOLocationDesignation				    VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW					    VARCHAR(100) ENCODE LZO,
	NameplatePowerFactor					    VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW						    VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW						    VARCHAR(100) ENCODE LZO,
	MinimumLoad_MW							    VARCHAR(100) ENCODE LZO,
	UprateorDerateCompletedDuringYear		    VARCHAR(100) ENCODE LZO,
	MonthUprateorDerateCompleted			    VARCHAR(100) ENCODE LZO,
	YearUprateorDerateCompleted				    VARCHAR(100) ENCODE LZO,
	Status									    VARCHAR(100) ENCODE LZO,
	SynchronizedtoTransmissionGrid			    VARCHAR(100) ENCODE LZO,
	OperatingMonth							    VARCHAR(100) ENCODE LZO,
	OperatingYear							    VARCHAR(100) ENCODE LZO,
	RetirementMonth							    VARCHAR(100) ENCODE LZO,
	RetirementYear							    VARCHAR(100) ENCODE LZO,
	AssociatedwithCombinedHeatandPowerSystem    VARCHAR(100) ENCODE LZO,
	SectorName								    VARCHAR(100) ENCODE LZO,
	Sector									    VARCHAR(100) ENCODE LZO,
	ToppingorBottoming						    VARCHAR(100) ENCODE LZO,
	EnergySource1							    VARCHAR(100) ENCODE LZO,
	EnergySource2							    VARCHAR(100) ENCODE LZO,
	EnergySource3							    VARCHAR(100) ENCODE LZO,
	EnergySource4							    VARCHAR(100) ENCODE LZO,
	EnergySource5							    VARCHAR(100) ENCODE LZO,
	EnergySource6							    VARCHAR(100) ENCODE LZO,
	StartupSource1							    VARCHAR(100) ENCODE LZO,
	StartupSource2							    VARCHAR(100) ENCODE LZO,
	StartupSource3							    VARCHAR(100) ENCODE LZO,
	StartupSource4							    VARCHAR(100) ENCODE LZO,
	SolidFuelGasificationSystem				    VARCHAR(100) ENCODE LZO,
	CarbonCaptureTechnology					    VARCHAR(100) ENCODE LZO,
	Turbines_Inverters_orHydrokineticBuoys	    VARCHAR(100) ENCODE LZO,
	TimefromColdShutdowntoFullLoad			    VARCHAR(100) ENCODE LZO,
	FluidizedBedTechnology					    VARCHAR(100) ENCODE LZO,
	PulverizedCoalTechnology				    VARCHAR(100) ENCODE LZO,
	StokerTechnology						    VARCHAR(100) ENCODE LZO,
	OtherCombustionTechnology				    VARCHAR(100) ENCODE LZO,
	SubcriticalTechnology					    VARCHAR(100) ENCODE LZO,
	SupercriticalTechnology					    VARCHAR(100) ENCODE LZO,
	UltrasupercriticalTechnology			    VARCHAR(100) ENCODE LZO,
	CofireFuels								    VARCHAR(100) ENCODE LZO,
	SwitchBetweenOilandNaturalGas			    VARCHAR(100) ENCODE LZO
);



DROP TABLE IF EXISTS {schemaName}.{tableName}3_2_WindOperable CASCADE;
CREATE TABLE {schemaName}.{tableName}3_2_WindOperable
(
	UtilityID                           INTEGER,
	UtilityName                         VARCHAR(100) ENCODE LZO,
	PlantCode                           VARCHAR(100) ENCODE LZO,
	PlantName                           VARCHAR(100) ENCODE LZO,
	State                               VARCHAR(100) ENCODE LZO,
	County                              VARCHAR(100) ENCODE LZO,
	GeneratorID                         VARCHAR(100) ENCODE LZO,
	Status                              VARCHAR(100) ENCODE LZO,
	Technology                          VARCHAR(100) ENCODE LZO,
	PrimeMover                          VARCHAR(100) ENCODE LZO,
	SectorName                          VARCHAR(100) ENCODE LZO,
	Sector                              VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW                VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW                   VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW                   VARCHAR(100) ENCODE LZO,
	OperatingMonth                      VARCHAR(100) ENCODE LZO,
	OperatingYear                       VARCHAR(100) ENCODE LZO,
	NumberofTurbines                    VARCHAR(100) ENCODE LZO,
	PredominantTurbineManufacturer      VARCHAR(100) ENCODE LZO,
	PredominantTurbineModelNumber       VARCHAR(100) ENCODE LZO,
	DesignWindSpeed_mph                 VARCHAR(100) ENCODE LZO,
	WindQualityClass                    VARCHAR(100) ENCODE LZO,
	TurbineHubHeight_Feet               VARCHAR(100) ENCODE LZO,
	FAAObstacleNumber                   VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}3_2_WindRetCancelled CASCADE;
CREATE TABLE {schemaName}.{tableName}3_2_WindRetCancelled
(
	UtilityID                           INTEGER,
	UtilityName                         VARCHAR(100) ENCODE LZO,
	PlantCode                           VARCHAR(100) ENCODE LZO,
	PlantName                           VARCHAR(100) ENCODE LZO,
	State                               VARCHAR(100) ENCODE LZO,
	County                              VARCHAR(100) ENCODE LZO,
	GeneratorID                         VARCHAR(100) ENCODE LZO,
	Status                              VARCHAR(100) ENCODE LZO,
	Technology                          VARCHAR(100) ENCODE LZO,
	PrimeMover                          VARCHAR(100) ENCODE LZO,
	SectorName                          VARCHAR(100) ENCODE LZO,
	Sector                              VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW                VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW                   VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW                   VARCHAR(100) ENCODE LZO,
	OperatingMonth                      VARCHAR(100) ENCODE LZO,
	OperatingYear                       VARCHAR(100) ENCODE LZO,
	NumberofTurbines                    VARCHAR(100) ENCODE LZO,
	PredominantTurbineManufacturer      VARCHAR(100) ENCODE LZO,
	PredominantTurbineModelNumber       VARCHAR(100) ENCODE LZO,
	DesignWindSpeed_mph                 VARCHAR(100) ENCODE LZO,
	WindQualityClass                    VARCHAR(100) ENCODE LZO,
	TurbineHubHeight_Feet               VARCHAR(100) ENCODE LZO,
	FAAObstacleNumber                   VARCHAR(100) ENCODE LZO
);



DROP TABLE IF EXISTS {schemaName}.{tableName}3_3_SolarOperable CASCADE;
CREATE TABLE {schemaName}.{tableName}3_3_SolarOperable
(
	UtilityID               INTEGER,
	UtilityName             VARCHAR(100) ENCODE LZO,
	PlantCode               VARCHAR(100) ENCODE LZO,
	PlantName               VARCHAR(100) ENCODE LZO,
	State                   VARCHAR(100) ENCODE LZO,
	County                  VARCHAR(100) ENCODE LZO,
	GeneratorID             VARCHAR(100) ENCODE LZO,
	Status                  VARCHAR(100) ENCODE LZO,
	Technology              VARCHAR(100) ENCODE LZO,
	PrimeMover              VARCHAR(100) ENCODE LZO,
	SectorName              VARCHAR(100) ENCODE LZO,
	Sector                  VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW    VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW       VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW       VARCHAR(100) ENCODE LZO,
	OperatingMonth          VARCHAR(100) ENCODE LZO,
	OperatingYear           VARCHAR(100) ENCODE LZO,
	LensesMirrors          	VARCHAR(100) ENCODE LZO,
	SingleAxisTracking	    VARCHAR(100) ENCODE LZO,
	DualAxisTracking        VARCHAR(100) ENCODE LZO,
	FixedTilt               VARCHAR(100) ENCODE LZO,
	ParabolicTrough         VARCHAR(100) ENCODE LZO,
	LinearFresnel           VARCHAR(100) ENCODE LZO,
	PowerTower              VARCHAR(100) ENCODE LZO,
	DishEngine              VARCHAR(100) ENCODE LZO,
	OtherSolarTechnology    VARCHAR(100) ENCODE LZO,
	DCNetCapacity_MW        VARCHAR(100) ENCODE LZO,
	CrystallineSilicon      VARCHAR(100) ENCODE LZO,
	ThinFilm_CdTe          	VARCHAR(100) ENCODE LZO,
	ThinFilm_ASi          	VARCHAR(100) ENCODE LZO,
	ThinFilm_CIGS          	VARCHAR(100) ENCODE LZO,
	ThinFilm_Other         	VARCHAR(100) ENCODE LZO,
	OtherMaterials          VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}3_3_SolarRetCancelled CASCADE;
CREATE TABLE {schemaName}.{tableName}3_3_SolarRetCancelled
(
	UtilityID               INTEGER,
	UtilityName             VARCHAR(100) ENCODE LZO,
	PlantCode               VARCHAR(100) ENCODE LZO,
	PlantName               VARCHAR(100) ENCODE LZO,
	State                   VARCHAR(100) ENCODE LZO,
	County                  VARCHAR(100) ENCODE LZO,
	GeneratorID             VARCHAR(100) ENCODE LZO,
	Status                  VARCHAR(100) ENCODE LZO,
	Technology              VARCHAR(100) ENCODE LZO,
	PrimeMover              VARCHAR(100) ENCODE LZO,
	SectorName              VARCHAR(100) ENCODE LZO,
	Sector                  VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW    VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW       VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW       VARCHAR(100) ENCODE LZO,
	OperatingMonth          VARCHAR(100) ENCODE LZO,
	OperatingYear           VARCHAR(100) ENCODE LZO,
	LensesMirrors           VARCHAR(100) ENCODE LZO,
	SingleAxisTracking      VARCHAR(100) ENCODE LZO,
	DualAxisTracking        VARCHAR(100) ENCODE LZO,
	FixedTilt               VARCHAR(100) ENCODE LZO,
	ParabolicTrough         VARCHAR(100) ENCODE LZO,
	LinearFresnel           VARCHAR(100) ENCODE LZO,
	PowerTower              VARCHAR(100) ENCODE LZO,
	DishEngine              VARCHAR(100) ENCODE LZO,
	OtherSolarTechnology    VARCHAR(100) ENCODE LZO,
	DCNetCapacity_MW        VARCHAR(100) ENCODE LZO,
	CrystallineSilicon      VARCHAR(100) ENCODE LZO,
	ThinFilm_CdTe           VARCHAR(100) ENCODE LZO,
	ThinFilm_ASi            VARCHAR(100) ENCODE LZO,
	ThinFilm_CIGS           VARCHAR(100) ENCODE LZO,
	ThinFilm_Other          VARCHAR(100) ENCODE LZO,
	OtherMaterials          VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}3_4_MultifuelOperable CASCADE;
CREATE TABLE {schemaName}.{tableName}3_4_MultifuelOperable
(
	UtilityID                               INTEGER,
	UtilityName                             VARCHAR(100) ENCODE LZO,
	PlantCode                               VARCHAR(100) ENCODE LZO,
	PlantName                               VARCHAR(100) ENCODE LZO,
	State                                   VARCHAR(100) ENCODE LZO,
	County                                  VARCHAR(100) ENCODE LZO,
	GeneratorID                             VARCHAR(100) ENCODE LZO,
	Status                                  VARCHAR(100) ENCODE LZO,
	Technology                              VARCHAR(100) ENCODE LZO,
	PrimeMover                              VARCHAR(100) ENCODE LZO,
	SectorName                              VARCHAR(100) ENCODE LZO,
	Sector                                  VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW                    VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW                       VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW                       VARCHAR(100) ENCODE LZO,
	EnergySource1                           VARCHAR(100) ENCODE LZO,
	EnergySource2                           VARCHAR(100) ENCODE LZO,
	CofireFuels                             VARCHAR(100) ENCODE LZO,
	CofireEnergySource1                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource2                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource3                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource4                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource5                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource6                     VARCHAR(100) ENCODE LZO,
	SwitchBetweenOilandNaturalGas           VARCHAR(100) ENCODE LZO,
	SwitchWhenOperating                     VARCHAR(100) ENCODE LZO,
	NetSummerCapacitywithNaturalGas_MW      VARCHAR(100) ENCODE LZO,
	NetWinterCapacitywithNaturalGas_MW      VARCHAR(100) ENCODE LZO,
	NetSummerCapacitywithOil_MW             VARCHAR(100) ENCODE LZO,
	NetWinterCapacitywithOil_MW             VARCHAR(100) ENCODE LZO,
	TimetoSwitchFromGastoOil                VARCHAR(100) ENCODE LZO,
	TimetoSwitchFromOiltoGas                VARCHAR(100) ENCODE LZO,
	FactorsthatLimitSwitching               VARCHAR(100) ENCODE LZO,
	StorageLimits                           VARCHAR(100) ENCODE LZO,
	AirPermitLimits                         VARCHAR(100) ENCODE LZO,
	OtherLimits                             VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}3_4_MultifuelProposed CASCADE;
CREATE TABLE {schemaName}.{tableName}3_4_MultifuelProposed
(
	UtilityID                       INTEGER,
	UtilityName                     VARCHAR(100) ENCODE LZO,
	PlantCode                       VARCHAR(100) ENCODE LZO,
	PlantName                       VARCHAR(100) ENCODE LZO,
	State                           VARCHAR(100) ENCODE LZO,
	County                          VARCHAR(100) ENCODE LZO,
	GeneratorID                     VARCHAR(100) ENCODE LZO,
	Status                          VARCHAR(100) ENCODE LZO,
	Technology                      VARCHAR(100) ENCODE LZO,
	PrimeMover                      VARCHAR(100) ENCODE LZO,
	SectorName                      VARCHAR(100) ENCODE LZO,
	Sector                          VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW            VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW               VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW               VARCHAR(100) ENCODE LZO,
	EnergySource1                   VARCHAR(100) ENCODE LZO,
	EnergySource2                   VARCHAR(100) ENCODE LZO,
	CofireFuels                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource1             VARCHAR(100) ENCODE LZO,
	CofireEnergySource2             VARCHAR(100) ENCODE LZO,
	CofireEnergySource3             VARCHAR(100) ENCODE LZO,
	CofireEnergySource4             VARCHAR(100) ENCODE LZO,
	CofireEnergySource5             VARCHAR(100) ENCODE LZO,
	CofireEnergySource6             VARCHAR(100) ENCODE LZO,
	SwitchBetweenOilandNaturalGas   VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}3_4_MultifuelRetCancelled CASCADE;
CREATE TABLE {schemaName}.{tableName}3_4_MultifuelRetCancelled
(
	UtilityID                               INTEGER,
	UtilityName                             VARCHAR(100) ENCODE LZO,
	PlantCode                               VARCHAR(100) ENCODE LZO,
	PlantName                               VARCHAR(100) ENCODE LZO,
	State                                   VARCHAR(100) ENCODE LZO,
	County                                  VARCHAR(100) ENCODE LZO,
	GeneratorID                             VARCHAR(100) ENCODE LZO,
	Status                                  VARCHAR(100) ENCODE LZO,
	Technology                              VARCHAR(100) ENCODE LZO,
	PrimeMover                              VARCHAR(100) ENCODE LZO,
	SectorName                              VARCHAR(100) ENCODE LZO,
	Sector                                  VARCHAR(100) ENCODE LZO,
	NameplateCapacity_MW                    VARCHAR(100) ENCODE LZO,
	SummerCapacity_MW                       VARCHAR(100) ENCODE LZO,
	WinterCapacity_MW                       VARCHAR(100) ENCODE LZO,
	EnergySource1                           VARCHAR(100) ENCODE LZO,
	EnergySource2                           VARCHAR(100) ENCODE LZO,
	CofireFuels                             VARCHAR(100) ENCODE LZO,
	CofireEnergySource1                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource2                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource3                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource4                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource5                     VARCHAR(100) ENCODE LZO,
	CofireEnergySource6                     VARCHAR(100) ENCODE LZO,
	SwitchBetweenOilandNaturalGas           VARCHAR(100) ENCODE LZO,
	SwitchWhenOperating                     VARCHAR(100) ENCODE LZO,
	NetSummerCapacitywithNaturalGas_MW      VARCHAR(100) ENCODE LZO,
	NetWinterCapacitywithNaturalGas_MW      VARCHAR(100) ENCODE LZO,
	NetSummerCapacitywithOil_MW             VARCHAR(100) ENCODE LZO,
	NetWinterCapacitywithOil_MW             VARCHAR(100) ENCODE LZO,
	TimetoSwitchFromGastoOil                VARCHAR(100) ENCODE LZO,
	TimetoSwitchFromOiltoGas                VARCHAR(100) ENCODE LZO,
	FactorsthatLimitSwitching               VARCHAR(100) ENCODE LZO,
	StorageLimits                           VARCHAR(100) ENCODE LZO,
	AirPermitLimits                         VARCHAR(100) ENCODE LZO,
	OtherLimits                             VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}OwnerOwnership CASCADE;
CREATE TABLE {schemaName}.{tableName}OwnerOwnership
(
	UtilityID               INTEGER,
	UtilityName             VARCHAR(100) ENCODE LZO,
	PlantCode               VARCHAR(100) ENCODE LZO,
	PlantName               VARCHAR(100) ENCODE LZO,
	State                   VARCHAR(100) ENCODE LZO,
	GeneratorID             VARCHAR(100) ENCODE LZO,
	Status                  VARCHAR(100) ENCODE LZO,
	OwnerName               VARCHAR(100) ENCODE LZO,
	OwnerStreetAddress      VARCHAR(100) ENCODE LZO,
	OwnerCity               VARCHAR(100) ENCODE LZO,
	OwnerState              VARCHAR(100) ENCODE LZO,
	OwnerZip                VARCHAR(100) ENCODE LZO,
	OwnershipID             VARCHAR(100) ENCODE LZO,
	PercentOwned            VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerGenerator CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerGenerator
(
	UtilityID       INTEGER,
	UtilityName     VARCHAR(100) ENCODE LZO,
	PlantCode       VARCHAR(100) ENCODE LZO,
	PlantName       VARCHAR(100) ENCODE LZO,
	BoilerID        VARCHAR(100) ENCODE LZO,
	GeneratorID     VARCHAR(100) ENCODE LZO,
	SteamPlantType  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerCooling CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerCooling
(
	UtilityID       INTEGER,
	UtilityName     VARCHAR(100) ENCODE LZO,
	PlantCode       VARCHAR(100) ENCODE LZO,
	PlantName       VARCHAR(100) ENCODE LZO,
	BoilerID        VARCHAR(100) ENCODE LZO,
	CoolingID     	VARCHAR(100) ENCODE LZO,
	SteamPlantType  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerParticulateMatter CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerParticulateMatter
(
	UtilityID       				INTEGER,
	UtilityName     				VARCHAR(100) ENCODE LZO,
	PlantCode       				VARCHAR(100) ENCODE LZO,
	PlantName       				VARCHAR(100) ENCODE LZO,
	BoilerID        				VARCHAR(100) ENCODE LZO,
	ParticulateMatterControlID     	VARCHAR(100) ENCODE LZO,
	SteamPlantType  				VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerSO2 CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerSO2
(
	UtilityID       	INTEGER,
	UtilityName     	VARCHAR(100) ENCODE LZO,
	PlantCode       	VARCHAR(100) ENCODE LZO,
	PlantName       	VARCHAR(100) ENCODE LZO,
	BoilerID        	VARCHAR(100) ENCODE LZO,
	SO2ControlID     	VARCHAR(100) ENCODE LZO,
	SteamPlantType  	VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerNOx CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerNOx
(
    UtilityID       	INTEGER,
	UtilityName     	VARCHAR(100) ENCODE LZO,
	PlantCode       	VARCHAR(100) ENCODE LZO,
	PlantName       	VARCHAR(100) ENCODE LZO,
	BoilerID        	VARCHAR(100) ENCODE LZO,
	NOxControlID     	VARCHAR(100) ENCODE LZO,
	SteamPlantType  	VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerMercury CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerMercury
(
    UtilityID       	    INTEGER,
	UtilityName     	    VARCHAR(100) ENCODE LZO,
	PlantCode       	    VARCHAR(100) ENCODE LZO,
	PlantName       	    VARCHAR(100) ENCODE LZO,
	BoilerID        	    VARCHAR(100) ENCODE LZO,
	MercuryControlID     	VARCHAR(100) ENCODE LZO,
	SteamPlantType  	    VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocBoilerStackFlue CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocBoilerStackFlue
(
    UtilityID       	INTEGER,
	UtilityName     	VARCHAR(100) ENCODE LZO,
	PlantCode       	VARCHAR(100) ENCODE LZO,
	PlantName       	VARCHAR(100) ENCODE LZO,
	BoilerID        	VARCHAR(100) ENCODE LZO,
	StackFlueID     	VARCHAR(100) ENCODE LZO,
	SteamPlantType  	VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroAssocEmissionsControlEquipment CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroAssocEmissionsControlEquipment
(
    UtilityID                       INTEGER,
    UtilityName                     VARCHAR(100) ENCODE LZO,
    PlantCode                       VARCHAR(100) ENCODE LZO,
    PlantName                       VARCHAR(100) ENCODE LZO,
    EquipmentType                   VARCHAR(100) ENCODE LZO,
    ParticulateMatterControlID      VARCHAR(100) ENCODE LZO,
    SO2ControlID                    VARCHAR(100) ENCODE LZO,
    NOxControlID                    VARCHAR(100) ENCODE LZO,
    MercuryControlID                VARCHAR(100) ENCODE LZO,
    AcidGasControl                  VARCHAR(100) ENCODE LZO,
    Status                          VARCHAR(100) ENCODE LZO,
    InserviceMonth                  VARCHAR(100) ENCODE LZO,
    InserviceYear                   VARCHAR(100) ENCODE LZO,
    TotalCost_ThousandDollars       VARCHAR(100) ENCODE LZO,
    SteamPlantType                  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroEquipEmissionStandardsStrategies CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroEquipEmissionStandardsStrategies
(
    UtilityID                                       INTEGER,
    UtilityName                                     VARCHAR(100) ENCODE LZO,
    PlantCode                                       VARCHAR(100) ENCODE LZO,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    State                                           VARCHAR(100) ENCODE LZO,
    BoilerID                                        VARCHAR(100) ENCODE LZO,
    BoilerStatus                                    VARCHAR(100) ENCODE LZO,
    TypeofBoiler                                    VARCHAR(100) ENCODE LZO,
    NewSourceReview                                 VARCHAR(100) ENCODE LZO,
    NewSourceReviewPermit                           VARCHAR(100) ENCODE LZO,
    NewSourceReviewMonth                            VARCHAR(100) ENCODE LZO,
    NewSourceReviewYear                             VARCHAR(100) ENCODE LZO,
    RegulationSulfur                                VARCHAR(100) ENCODE LZO,
    StandardSulfurRate                              VARCHAR(100) ENCODE LZO,
    StandardSulfurPercentScrubbed                   VARCHAR(100) ENCODE LZO,
    UnitSulfur                                      VARCHAR(100) ENCODE LZO,
    PeriodSulfur                                    VARCHAR(100) ENCODE LZO,
    ComplianceYearSulfur                            VARCHAR(100) ENCODE LZO,
    SulfurDioxideControlExistingStrategy1           VARCHAR(100) ENCODE LZO,
    SulfurDioxideControlExistingStrategy2           VARCHAR(100) ENCODE LZO,
    SulfurDioxideControlExistingStrategy3           VARCHAR(100) ENCODE LZO,
    SulfurDioxideControlProposedStrategy1           VARCHAR(100) ENCODE LZO,
    SulfurDioxideControlProposedStrategy2           VARCHAR(100) ENCODE LZO,
    SulfurDioxideControlProposedStrategy3           VARCHAR(100) ENCODE LZO,
    RegulationNitrogen                              VARCHAR(100) ENCODE LZO,
    StandardNitrogenRate                            VARCHAR(100) ENCODE LZO,
    UnitNitrogen                                    VARCHAR(100) ENCODE LZO,
    PeriodNitrogen                                  VARCHAR(100) ENCODE LZO,
    ComplianceYearNitrogen                          VARCHAR(100) ENCODE LZO,
    NitrogenOxideControlExistingStrategy1           VARCHAR(100) ENCODE LZO,
    NitrogenOxideControlExistingStrategy2           VARCHAR(100) ENCODE LZO,
    NitrogenOxideControlExistingStrategy3           VARCHAR(100) ENCODE LZO,
    NitrogenOxideControlProposedStrategy1           VARCHAR(100) ENCODE LZO,
    NitrogenOxideControlProposedStrategy2           VARCHAR(100) ENCODE LZO,
    NitrogenOxideControlProposedStrategy3           VARCHAR(100) ENCODE LZO,
    RegulationParticulate                           VARCHAR(100) ENCODE LZO,
    StandardParticulateRate                         VARCHAR(100) ENCODE LZO,
    UnitParticulate                                 VARCHAR(100) ENCODE LZO,
    PeriodParticulate                               VARCHAR(100) ENCODE LZO,
    ComplianceYearParticulate                       VARCHAR(100) ENCODE LZO,
    RegulationMercury                               VARCHAR(100) ENCODE LZO,
    ComplianceYearMercury                           VARCHAR(100) ENCODE LZO,
    MercuryControlExistingStrategy1                 VARCHAR(100) ENCODE LZO,
    MercuryControlExistingStrategy2                 VARCHAR(100) ENCODE LZO,
    MercuryControlExistingStrategy3                 VARCHAR(100) ENCODE LZO,
    MercuryControlProposedStrategy1                 VARCHAR(100) ENCODE LZO,
    MercuryControlProposedStrategy2                 VARCHAR(100) ENCODE LZO,
    MercuryControlProposedStrategy3                 VARCHAR(100) ENCODE LZO,
    SteamPlantType                                  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroEquipBoilerInfoDesignParameters CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroEquipBoilerInfoDesignParameters
(
    UtilityID                                       INTEGER,
    UtilityName                                     VARCHAR(100) ENCODE LZO,
    PlantCode                                       VARCHAR(100) ENCODE LZO,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    State                                           VARCHAR(100) ENCODE LZO,
    BoilerID                                        VARCHAR(100) ENCODE LZO,
    BoilerStatus                                    VARCHAR(100) ENCODE LZO,
    TypeofBoiler                                    VARCHAR(100) ENCODE LZO,
    InserviceMonth                                  VARCHAR(100) ENCODE LZO,
    InserviceYear                                   VARCHAR(100) ENCODE LZO,
    RetirementMonth                                 VARCHAR(100) ENCODE LZO,
    RetirementYear                                  VARCHAR(100) ENCODE LZO,
    FiringType1                                     VARCHAR(100) ENCODE LZO,
    FiringType2                                     VARCHAR(100) ENCODE LZO,
    FiringType3                                     VARCHAR(100) ENCODE LZO,
    MaxSteamFlow_ThousandPoundsperHour              VARCHAR(100) ENCODE LZO,
    FiringRateUsingCoal_01TonsperHour               VARCHAR(100) ENCODE LZO,
    FiringRateUsingPetroleum_01BarrelsperHour       VARCHAR(100) ENCODE LZO,
    FiringRateUsingGas_01MCFperHour                 VARCHAR(100) ENCODE LZO,
    FiringRateUsingOtherFuels                       VARCHAR(100) ENCODE LZO,
    WasteHeatInput_MillionBTUperHour                VARCHAR(100) ENCODE LZO,
    PrimaryFuel1                                    VARCHAR(100) ENCODE LZO,
    PrimaryFuel2                                    VARCHAR(100) ENCODE LZO,
    PrimaryFuel3                                    VARCHAR(100) ENCODE LZO,
    PrimaryFuel4                                    VARCHAR(100) ENCODE LZO,
    TurndownRatio                                   VARCHAR(100) ENCODE LZO,
    Efficiency100PercentLoad                        VARCHAR(100) ENCODE LZO,
    Efficiency50PercentLoad                         VARCHAR(100) ENCODE LZO,
    AirFlow100PercentLoad_CubicFeetperMinute        VARCHAR(100) ENCODE LZO,
    WetDryBottom                                    VARCHAR(100) ENCODE LZO,
    FlyAshReinjection                               VARCHAR(100) ENCODE LZO,
    SteamPlantType                                  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroEquipCooling CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroEquipCooling
(
    UtilityID                                       INTEGER,
    UtilityName                                     VARCHAR(100) ENCODE LZO,
    PlantCode                                       VARCHAR(100) ENCODE LZO,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    State                                           VARCHAR(100) ENCODE LZO,
    CoolingID                                       VARCHAR(100) ENCODE LZO,
    CoolingStatus                                   VARCHAR(100) ENCODE LZO,
    InserviceMonth                                  VARCHAR(100) ENCODE LZO,
    InserviceYear                                   VARCHAR(100) ENCODE LZO,
    CoolingType1                                    VARCHAR(100) ENCODE LZO,
    CoolingType2                                    VARCHAR(100) ENCODE LZO,
    CoolingType3                                    VARCHAR(100) ENCODE LZO,
    CoolingType4                                    VARCHAR(100) ENCODE LZO,
    PercentDryCooling                               VARCHAR(100) ENCODE LZO,
    CoolingWaterSource                              VARCHAR(100) ENCODE LZO,
    CoolingWaterDischarge                           VARCHAR(100) ENCODE LZO,
    WaterSourceCode                                 VARCHAR(100) ENCODE LZO,
    WaterTypeCode                                   VARCHAR(100) ENCODE LZO,
    IntakeRateat100Percent_GallonsperMinute         VARCHAR(100) ENCODE LZO,
    ChlorineInserviceMonth                          VARCHAR(100) ENCODE LZO,
    ChlorineInserviceYear                           VARCHAR(100) ENCODE LZO,
    PondInserviceMonth                              VARCHAR(100) ENCODE LZO,
    PondInserviceYear                               VARCHAR(100) ENCODE LZO,
    PondSurfaceArea_Acres                           VARCHAR(100) ENCODE LZO,
    PondVolume_AcreFeet                             VARCHAR(100) ENCODE LZO,
    TowerInserviceMonth                             VARCHAR(100) ENCODE LZO,
    TowerInserviceYear                              VARCHAR(100) ENCODE LZO,
    TowerType1                                      VARCHAR(100) ENCODE LZO,
    TowerType2                                      VARCHAR(100) ENCODE LZO,
    TowerType3                                      VARCHAR(100) ENCODE LZO,
    TowerType4                                      VARCHAR(100) ENCODE LZO,
    TowerWaterRate_GallonsperMinute                 VARCHAR(100) ENCODE LZO,
    PowerRequirement_MW                             VARCHAR(100) ENCODE LZO,
    CostTotal_ThousandDollars                       VARCHAR(100) ENCODE LZO,
    CostPonds_ThousandDollars                       VARCHAR(100) ENCODE LZO,
    CostTowers_ThousandDollars                      VARCHAR(100) ENCODE LZO,
    CostChlorineEquipment_ThousandDollars           VARCHAR(100) ENCODE LZO,
    IntakeDistanceShore_Feet                        VARCHAR(100) ENCODE LZO,
    OutletDistanceShore_Feet                        VARCHAR(100) ENCODE LZO,
    IntakeDistanceSurface_Feet                      VARCHAR(100) ENCODE LZO,
    OutletDistanceSurface_Feet                      VARCHAR(100) ENCODE LZO,
    SteamPlantType                                  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroEquipFGP CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroEquipFGP
(
    UtilityID                                       INTEGER,
    UtilityName                                     VARCHAR(100) ENCODE LZO,
    PlantCode                                       VARCHAR(100) ENCODE LZO,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    State                                           VARCHAR(100) ENCODE LZO,
    ParticulateMatterControlID                      VARCHAR(100) ENCODE LZO,
    CollectorType1                                  VARCHAR(100) ENCODE LZO,
    CollectorType2                                  VARCHAR(100) ENCODE LZO,
    CollectorType3                                  VARCHAR(100) ENCODE LZO,
    FuelSpecificationAshCoal                        VARCHAR(100) ENCODE LZO,
    FuelSpecificationAshPetroleum                   VARCHAR(100) ENCODE LZO,
    FuelSpecificationSulfurCoal                     VARCHAR(100) ENCODE LZO,
    FuelSpecificationSulfurPetroleum                VARCHAR(100) ENCODE LZO,
    CollectionEfficiency                            VARCHAR(100) ENCODE LZO,
    EmissionRate_PoundsperHour                      VARCHAR(100) ENCODE LZO,
    GasExitRate_CubicFeetperMinute                  VARCHAR(100) ENCODE LZO,
    GasExitTemperature_Fahrenheit                   VARCHAR(100) ENCODE LZO,
    SteamPlantType                                  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroEquipFGD CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroEquipFGD
(
    UtilityID                                       INTEGER,
    UtilityName                                     VARCHAR(100) ENCODE LZO,
    PlantCode                                       VARCHAR(100) ENCODE LZO,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    State                                           VARCHAR(100) ENCODE LZO,
    SO2ControlID                                    VARCHAR(100) ENCODE LZO,
    SO2Type1                                        VARCHAR(100) ENCODE LZO,
    SO2Type2                                        VARCHAR(100) ENCODE LZO,
    SO2Type3                                        VARCHAR(100) ENCODE LZO,
    SO2Type4                                        VARCHAR(100) ENCODE LZO,
    SorbentType1                                    VARCHAR(100) ENCODE LZO,
    SorbentType2                                    VARCHAR(100) ENCODE LZO,
    SorbentType3                                    VARCHAR(100) ENCODE LZO,
    SorbentType4                                    VARCHAR(100) ENCODE LZO,
    ByproductRecovery                               VARCHAR(100) ENCODE LZO,
    PondLandfillRequirements_AcreFootperYear        VARCHAR(100) ENCODE LZO,
    SludgePondLined                                 VARCHAR(100) ENCODE LZO,
    FlueGasBypassFGD                                VARCHAR(100) ENCODE LZO,
    SpecificationsofCoalAsh                         VARCHAR(100) ENCODE LZO,
    SpecificationsofCoalSulfur                      VARCHAR(100) ENCODE LZO,
    FGDTrainsTotal                                  VARCHAR(100) ENCODE LZO,
    FGDTrains100Percent                             VARCHAR(100) ENCODE LZO,
    RemovalEfficiencyofSulfur                       VARCHAR(100) ENCODE LZO,
    SulfurEmissionRate_PoundsperHour                VARCHAR(100) ENCODE LZO,
    FlueGasExitRate_CubicFeetperMinute              VARCHAR(100) ENCODE LZO,
    FlueGasExitTemperature_Fahrenheit               VARCHAR(100) ENCODE LZO,
    FlueGasEnteringFGD_PercentofTotal               VARCHAR(100) ENCODE LZO,
    CostStructure_ThousandDollars                   VARCHAR(100) ENCODE LZO,
    CostDisposal_ThousandDollars                    VARCHAR(100) ENCODE LZO,
    CostOther_ThousandDollars                       VARCHAR(100) ENCODE LZO,
    CostTotal_ThousandDollars                       VARCHAR(100) ENCODE LZO,
    SteamPlantType                                  VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}EnviroEquipStackFlue CASCADE;
CREATE TABLE {schemaName}.{tableName}EnviroEquipStackFlue
(
    UtilityID                                       INTEGER,
    UtilityName                                     VARCHAR(100) ENCODE LZO,
    PlantCode                                       VARCHAR(100) ENCODE LZO,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    State                                           VARCHAR(100) ENCODE LZO,
    StackorFlueID                                   VARCHAR(100) ENCODE LZO,
    InserviceMonth                                  VARCHAR(100) ENCODE LZO,
    InserviceYear                                   VARCHAR(100) ENCODE LZO,
    StackFlueStatus                                 VARCHAR(100) ENCODE LZO,
    StackHeight_Feet                                VARCHAR(100) ENCODE LZO,
    AreaatTop_SquareFeet                            VARCHAR(100) ENCODE LZO,
    ExitRate100Percent_CubicFeetperMinute           VARCHAR(100) ENCODE LZO,
    ExitRate50Percent_CubicFeetperMinute            VARCHAR(100) ENCODE LZO,
    ExitTemperature100Percent_Fahrenheit            VARCHAR(100) ENCODE LZO,
    ExitTemperature50Percent_Fahrenheit             VARCHAR(100) ENCODE LZO,
    ExitVelocity100Percent_FeetperSecond            VARCHAR(100) ENCODE LZO,
    ExitVelocity100Percent_FeetperSecond2           VARCHAR(100) ENCODE LZO,
    ExitTemperatureSummer_Fahrenheit                VARCHAR(100) ENCODE LZO,
    ExitTemperatureWinter_Fahrenheit                VARCHAR(100) ENCODE LZO,
    ExitTemperatureMeasuredorEstimated              VARCHAR(100) ENCODE LZO,
    SteamPlantType                                  VARCHAR(100) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}M_GeneratorOperating CASCADE;
CREATE TABLE {schemaName}.{tableName}M_GeneratorOperating
(
    EntityID                                        INTEGER,
    EntityName                                      VARCHAR(100) ENCODE LZO,
    PlantID                                         INTEGER,
    PlantName                                       VARCHAR(100) ENCODE LZO,
    Sector                                          VARCHAR(100) ENCODE LZO,
    PlantState                                      VARCHAR(100) ENCODE LZO,
    GeneratorID                                     VARCHAR(100) ENCODE LZO,
    NameplateCapacity_MW                            FLOAT8,
    NetSummerCapacity_MW                            FLOAT8,
    NetWinterCapacity_MW                            FLOAT8,
    Technology                                      VARCHAR(100) ENCODE LZO,
    EnergySourceCode                                VARCHAR(100) ENCODE LZO,
    PrimeMoverCode                                  VARCHAR(100) ENCODE LZO,
    OperatingMonth                                  INTEGER,
    OperatingYear                                   INTEGER,
    PlannedRetirementMonth                          INTEGER,
    PlannedRetirementYear                           INTEGER,
    Status                                          VARCHAR(100) ENCODE LZO,
    PlannedDerateYear                               INTEGER,
    PlannedDerateMonth                              INTEGER,
    PlannedDerateofSummerCapacity_MW                FLOAT8,
    PlannedUprateYear                               INTEGER,
    PlannedUprateMonth                              INTEGER,
    PlannedUprateofSummerCapacity_MW                FLOAT8,
    County                                          VARCHAR(100) ENCODE LZO,
    Latitude                                        FLOAT8,
    Longitude                                       FLOAT8,
    BalancingAuthorityCode                          VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}M_GeneratorPlanned CASCADE;
CREATE TABLE {schemaName}.{tableName}M_GeneratorPlanned
(
    EntityID                            INTEGER,
    EntityName                          VARCHAR(100) ENCODE LZO,
    PlantID                             INTEGER,
    PlantName                           VARCHAR(100) ENCODE LZO,
    Sector                              VARCHAR(100) ENCODE LZO,
    PlantState                          VARCHAR(100) ENCODE LZO,
    GeneratorID                         VARCHAR(100) ENCODE LZO,
    NameplateCapacity_MW                FLOAT8,
    NetSummerCapacity_MW                FLOAT8,
    NetWinterCapacity_MW                FLOAT8,
    Technology                          VARCHAR(100) ENCODE LZO,
    EnergySourceCode                    VARCHAR(100) ENCODE LZO,
    PrimeMoverCode                      VARCHAR(100) ENCODE LZO,
    PlannedOperationMonth               INTEGER,
    PlannedOperationYear                INTEGER,
    Status                              VARCHAR(100) ENCODE LZO,
    County                              VARCHAR(100) ENCODE LZO,
    Latitude                            FLOAT8,
    Longitude                           FLOAT8,
    BalancingAuthorityCode              VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}M_GeneratorRetired CASCADE;
CREATE TABLE {schemaName}.{tableName}M_GeneratorRetired
(
    EntityID                            INTEGER,
    EntityName                          VARCHAR(100) ENCODE LZO,
    PlantID                             INTEGER,
    PlantName                           VARCHAR(100) ENCODE LZO,
    Sector                              VARCHAR(100) ENCODE LZO,
    PlantState                          VARCHAR(100) ENCODE LZO,
    GeneratorID                         VARCHAR(100) ENCODE LZO,
    NameplateCapacity_MW                FLOAT8,
    NetSummerCapacity_MW                FLOAT8,
    NetWinterCapacity_MW                FLOAT8,
    Technology                          VARCHAR(100) ENCODE LZO,
    EnergySourceCode                    VARCHAR(100) ENCODE LZO,
    PrimeMoverCode                      VARCHAR(100) ENCODE LZO,
    RetirementMonth                     FLOAT8,
    RetirementYear                      INTEGER,
    OperatingMonth                      INTEGER,
    OperatingYear                       INTEGER,
    County                              VARCHAR(100) ENCODE LZO,
    Latitude                            FLOAT8,
    Longitude                           FLOAT8,
    BalancingAuthorityCode              VARCHAR(100) ENCODE LZO
);


DROP TABLE IF EXISTS {schemaName}.{tableName}M_GeneratorCanceledorPostponed CASCADE;
CREATE TABLE {schemaName}.{tableName}M_GeneratorCanceledorPostponed
(
    EntityID                            INTEGER,
    EntityName                          VARCHAR(100) ENCODE LZO,
    PlantID                             INTEGER,
    PlantName                           VARCHAR(100) ENCODE LZO,
    Sector                              VARCHAR(100) ENCODE LZO,
    PlantState                          VARCHAR(100) ENCODE LZO,
    GeneratorID                         VARCHAR(100) ENCODE LZO,
    NameplateCapacity_MW                FLOAT8,
    NetSummerCapacity_MW                FLOAT8,
    NetWinterCapacity_MW                FLOAT8,
    Technology                          VARCHAR(100) ENCODE LZO,
    EnergySourceCode                    VARCHAR(100) ENCODE LZO,
    PrimeMoverCode                      VARCHAR(100) ENCODE LZO,
    County                              VARCHAR(100) ENCODE LZO,
    Latitude                            FLOAT8,
    Longitude                           FLOAT8,
    BalancingAuthorityCode              VARCHAR(100) ENCODE LZO
);

