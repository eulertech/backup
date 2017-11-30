-- Table: eaa_dev.series_data

 DROP TABLE eaa_dev.series_data;

CREATE TABLE eaa_dev.series_data
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

