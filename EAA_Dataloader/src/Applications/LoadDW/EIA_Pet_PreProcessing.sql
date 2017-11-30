-- Pre process script for EIA Pet... 

SELECT * INTO {schemaName}.eia_pet_series_data_cleaned
FROM {sourceSchema}.eia_pet_series_data
WHERE series_id NOT LIKE '%.W' 
	AND  series_id NOT LIKE '%.D'
	AND  series_id NOT LIKE '%.4' ;
 
COMMIT;


SELECT * INTO {schemaName}.eia_pet_series_attributes_cleaned
FROM {sourceSchema}.eia_pet_series_attributes
WHERE series_id NOT LIKE '%.W' 
	AND  series_id NOT LIKE '%.D'
	AND  series_id NOT LIKE '%.4' ;
 
COMMIT;