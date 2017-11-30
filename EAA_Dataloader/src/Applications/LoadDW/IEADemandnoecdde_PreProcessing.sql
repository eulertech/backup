-- Pre process script for IEA_Demandnoecdde... 

SELECT country, period_type, 
  (
	  CASE
	      WHEN period_type = 'Q' THEN TO_DATE(REPLACE(period, 'Q', ''), 'QYYYY')
	      WHEN period_type = 'Y' THEN TO_DATE(period, 'YYYY')
	      ELSE TO_DATE(period, 'MONYYYY')
	  END
	) AS period, 
	value
INTO {schemaName}.iea_demandnoecdde_fixed
FROM {sourceSchema}.iea_demandnoecdde;

COMMIT;
