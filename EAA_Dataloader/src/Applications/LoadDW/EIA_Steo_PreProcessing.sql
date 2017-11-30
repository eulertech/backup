-- Pre process script for EIA Pet... 

SELECT series_id,
	(
		CASE
		  WHEN (POSITION('Q' IN period) > 0) THEN TO_DATE(REPLACE(period, 'Q', ''), 'YYYYQ')
		  ELSE TO_DATE(period, 'YYYY') 
		END
	) as period,
	value
INTO {schemaName}.eia_steo_series_data_cleaned
FROM {sourceSchema}.eia_steo_series_data;
 
COMMIT;


SELECT series_id, name, units, f, source, description,
 	(
		CASE
			WHEN (POSITION('Q' IN start) > 0) THEN TO_DATE(REPLACE(start, 'Q', ''), 'YYYYQ')
			ELSE TO_DATE(start, 'YYYY') 
		END
	) as start,
	(
		CASE
			WHEN (POSITION('Q' IN "end") > 0) THEN TO_DATE(REPLACE("end", 'Q', ''), 'YYYYQ')
			ELSE TO_DATE("end", 'YYYY') 
		END
	) as "end"
INTO {schemaName}.eia_steo_series_attributes_cleaned
FROM {sourceSchema}.eia_steo_series_attributes;

COMMIT;
