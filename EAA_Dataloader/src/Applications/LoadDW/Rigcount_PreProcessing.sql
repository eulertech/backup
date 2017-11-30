-- Pre process script for RigCount... 

SELECT category, SPLIT_PART(category, '(', 1) AS category_cleaned, valuationdate, name, wells
INTO {schemaName}.rigcount_fixed
FROM {sourceSchema}.rigcount

COMMIT;
