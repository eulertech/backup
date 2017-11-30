UNLOAD ('select series_id,series_key,mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,source,timestamp as timestamp,wefa_version,bank,version from hindsight_prod.series_attributes_history') 
  TO 's3://ihs-temp/varun/series_attributes_history.dmp' 
  WITH CREDENTIALS AS 'aws_access_key_id=AKIAJYXVBNHTJNSA27EQ;aws_secret_access_key=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' 
  GZIP
  NULL AS ''
  DELIMITER AS '|'
  ALLOWOVERWRITE
  PARALLEL OFF;  

  
UNLOAD ('select series_id,date,datavalue,version,status from hindsight_prod.series_data_history') 
  TO 's3://ihs-temp/varun/series_data_history.dmp' 
  WITH CREDENTIALS AS 'aws_access_key_id=AKIAJYXVBNHTJNSA27EQ;aws_secret_access_key=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' 
  GZIP
  NULL AS ''
  DELIMITER AS '|'
  ALLOWOVERWRITE
  PARALLEL OFF;
  

UNLOAD ('select tablename, rowcount, date, exec_order from hindsight_prod.row_count_history') 
  TO 's3://ihs-temp/varun/row_count_history.dmp' 
  WITH CREDENTIALS AS 'aws_access_key_id=AKIAJYXVBNHTJNSA27EQ;aws_secret_access_key=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' 
  GZIP
  NULL AS ''
  DELIMITER AS '|'
  ALLOWOVERWRITE
  PARALLEL OFF;
  

UNLOAD ('select series_id, prior_rowcount, prior_version, current_rowcount, current_version, change, ratio from hindsight_etl.mismatch_from_prior') 
  TO 's3://ihs-temp/varun/mismatch_from_prior.dmp' 
  WITH CREDENTIALS AS 'aws_access_key_id=AKIAJYXVBNHTJNSA27EQ;aws_secret_access_key=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' 
  GZIP
  NULL AS ''
  DELIMITER AS '|'
  ALLOWOVERWRITE
  PARALLEL OFF;

  
