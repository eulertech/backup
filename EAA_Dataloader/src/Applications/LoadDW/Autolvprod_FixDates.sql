-- Transform script for Auto LV Production... 

ALTER TABLE {schemaName}.alvprod_transposed ADD COLUMN dateConcept_temp VARCHAR(10);
ALTER TABLE {schemaName}.alvprod_transposed ADD COLUMN frequency VARCHAR(1);

UPDATE {schemaName}.alvprod_transposed 
  SET dateConcept_temp = (
	  CASE
	      WHEN (POSITION('q' IN dateConcept) = 1) THEN TO_DATE(REPLACE(dateConcept, 'q', ''), 'QYYYY')
	      WHEN (POSITION('cy' IN dateConcept) = 1) THEN TO_DATE(REPLACE(dateConcept, 'cy', ''), 'YYYY')
	      WHEN (POSITION('Jan' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Feb' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Mar' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Apr' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('May' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Jun' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Jul' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Aug' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Sep' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Oct' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Nov' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	      WHEN (POSITION('Dec' IN dateConcept) = 1) THEN TO_DATE(dateConcept, 'MonYY')
	  END
	),
  	frequency = (
	  CASE
	      WHEN (position('q' IN dateConcept) = 1) THEN 'Q'
	      WHEN (POSITION('cy' IN dateConcept) = 1) THEN 'Y'
	      WHEN (POSITION('Jan' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Feb' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Mar' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Apr' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('May' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Jun' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Jul' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Aug' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Sep' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Oct' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Nov' IN dateConcept) = 1) THEN 'M'
	      WHEN (POSITION('Dec' IN dateConcept) = 1) THEN 'M'
	  END
	);

ALTER TABLE {schemaName}.alvprod_transposed  DROP COLUMN dateConcept;
ALTER TABLE {schemaName}.alvprod_transposed  RENAME COLUMN dateConcept_temp TO dateConcept;