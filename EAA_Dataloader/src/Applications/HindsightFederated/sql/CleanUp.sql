-- Clean up script for the whole process, this is executed even if the process failed... 

DROP VIEW IF EXISTS {schemaName}.vw_eaa_data;
DROP VIEW IF EXISTS {schemaName}.vw_eaa_attributes;
DROP VIEW IF EXISTS {schemaName}.vw_magellan_data;
DROP VIEW IF EXISTS {schemaName}.vw_magellan_attributes;
DROP VIEW IF EXISTS {schemaName}.vw_geforecast_giif_data;
DROP VIEW IF EXISTS {schemaName}.vw_geforecast_giif_attributes;
DROP VIEW IF EXISTS {schemaName}.vw_geforecast_srs_attributes;
DROP VIEW IF EXISTS {schemaName}.vw_geforecast_srs_data;
DROP VIEW IF EXISTS {schemaName}.vw_geforecast_wes_attributes;
DROP VIEW IF EXISTS {schemaName}.vw_geforecast_wes_data;

vacuum {schemaName}.federated_series_attributes_history;
vacuum {schemaName}.federated_series_attributes;
vacuum {schemaName}.federated_series_data_history;
vacuum {schemaName}.federated_series_data;
