--Script to create SampleResults Tables
DROP TABLE IF EXISTS  {schemaName}.SampleResults;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_attributes;
DROP TABLE IF EXISTS  {schemaName}.{tableName}_data;

CREATE TABLE {schemaName}.SampleResults
(
   valuationdate    character varying(10), 
   GLM_value        double precision,
   ARIMA_value      double precision,
   LASSO_value      double precision,
   NN_value         double precision,
   SPECTRE_value    double precision
) 
WITH (
  OIDS = FALSE
);

CREATE TABLE {schemaName}.{tableName}_attributes
(
  id SERIAL,
  attr_id character varying(10),
  name character varying(2000),
  category character varying(100)  
)
WITH (
  OIDS=FALSE
);

  
commit;

CREATE INDEX series_attributes_ndx
ON eaa_dev.series_attributes (attr_id, category);


CREATE TABLE {schemaName}.{tableName}_data
(
  id SERIAL,
  attr_id character varying(10),
  date date,
  type character varying(10),
  value character varying(2000),
  CONSTRAINT series_data_pk
   PRIMARY KEY (attr_id, date, type)  
)
WITH (
  OIDS=FALSE
);

