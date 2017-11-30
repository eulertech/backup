-- Pre process script for load stage... 

SELECT * INTO {schemaName}.jodi_secondary_vw
FROM {sourceSchema}.jodi_secondary
WHERE quantity IS NOT NULL;

COMMIT;