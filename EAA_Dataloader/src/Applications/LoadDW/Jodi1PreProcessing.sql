-- Pre process script for load stage... 

SELECT * INTO {schemaName}.jodi_primary_vw
FROM {sourceSchema}.jodi_primary
WHERE quantity IS NOT NULL;

COMMIT;