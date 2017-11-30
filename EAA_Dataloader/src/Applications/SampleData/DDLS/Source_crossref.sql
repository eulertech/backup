-- Table: eaa_dev.series_data

 DROP TABLE eaa_dev.source_crossref;

CREATE TABLE eaa_dev.source_crossref
(
  id SERIAL,
  scenario_id character varying(10),
  source_name character varying(200),
  source_id character varying(200)
)
WITH (
  OIDS=FALSE
);

insert into eaa_dev.source_crossref
	(scenario_id,source_name,source_id)
values
('1000', 'Phoenix','1'),
('2000', 'Phoenix','2'),
('3000', 'Phoenix','3')
;

commit;
	