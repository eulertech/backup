-- Pre process script for Chemicals... 

SELECT DISTINCT productid, product, locationid, location, category_id, category, years,
  (
    CASE
      WHEN (value IS NULL OR value = '') THEN 0
      ELSE value
    END
  )
INTO {schemaName}.chemicals_fixed
FROM {sourceSchema}.chemicals

COMMIT;
