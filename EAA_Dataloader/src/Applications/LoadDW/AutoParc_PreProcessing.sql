-- Pre process script for Auto Parc... 

SELECT country, fuel_type, year, sum(total) as total
INTO {schemaName}.auto_parc_grouped
FROM {sourceSchema}.auto_parc
GROUP BY country, fuel_type, year;

COMMIT;
