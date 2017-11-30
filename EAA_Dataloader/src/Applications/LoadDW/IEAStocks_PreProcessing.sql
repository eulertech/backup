-- Pre process script for IEA Stocks... 

SELECT flow, country, product, period_type,  
  (
	  CASE
	      WHEN period_type = 'Q' THEN TO_DATE(REPLACE(period, 'Q', ''), 'QYYYY')
	      WHEN period_type = 'M' THEN TO_DATE(period, 'MONYYYY')
	      WHEN period_type = 'Y' THEN TO_DATE(period, 'YYYY')
	  END
	) AS period, 
	value
INTO {schemaName}.iea_stocks_fixed
FROM {sourceSchema}.iea_stocks;

COMMIT;
