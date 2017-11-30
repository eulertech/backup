-- Clean up script for the whole process, this is executed even if the process failed... 

DROP TABLE IF EXISTS {schemaName}.jodi_primary_vw CASCADE;
DROP TABLE IF EXISTS {schemaName}.jodi_secondary_vw CASCADE;
DROP TABLE IF EXISTS {schemaName}.rigcount_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.rigpointutilmon_transposed CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_worldoilsupplydemand_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_supply_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_stocks_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_fieldbyfield_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_demandoecdde_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_demandnoecdde_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.chemicals_fixed CASCADE;
DROP TABLE IF EXISTS {schemaName}.auto_parc_grouped CASCADE;
DROP TABLE IF EXISTS {schemaName}.auto_lv_sales_grouped CASCADE;
DROP TABLE IF EXISTS {schemaName}.alvsales_transposed CASCADE;
DROP TABLE IF EXISTS {schemaName}.auto_lv_production_grouped CASCADE;
DROP TABLE IF EXISTS {schemaName}.alvprod_transposed CASCADE;
DROP TABLE IF EXISTS {schemaName}.eia_pet_series_data_cleaned CASCADE;
DROP TABLE IF EXISTS {schemaName}.eia_pet_series_attributes_cleaned CASCADE;
DROP TABLE IF EXISTS {schemaName}.eia_steo_series_data_cleaned CASCADE;
DROP TABLE IF EXISTS {schemaName}.eia_steo_series_attributes_cleaned CASCADE;
DROP TABLE IF EXISTS {schemaName}.econconnect_data_cleaned CASCADE;
DROP TABLE IF EXISTS {schemaName}.econconnect_attributes_cleaned CASCADE;
DROP TABLE IF EXISTS {schemaName}.iea_dictionary CASCADE;
DROP TABLE IF EXISTS {schemaName}.scenarios_transposed CASCADE;