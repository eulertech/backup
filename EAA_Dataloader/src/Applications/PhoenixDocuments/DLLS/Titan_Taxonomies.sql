-- Table: eaa_dev.Titan_Taxonomies

-- DROP TABLE eaa_dev.Titan_Taxonomies;

CREATE TABLE eaa_dev.Titan_Taxonomies
(
  id serial NOT NULL,
  category character varying(100),
  attr_id integer,
  Name character varying(200),
  displayName character varying(200),
  parent character varying(200)
)
WITH (
  OIDS=FALSE
);

commit;	

insert into eaa_dev.Titan_Taxonomies
	(category, attr_id, name, displayName, parent)
values 
	('EAA Drivers', 1, 'Shale Development' ,'Shale Development', ''),
	('EAA Drivers', 2, 'Energy Fundamentals' ,'Energy Fundamentals', ''),	
	('EAA Drivers', 3, 'Technology' ,'Technology', ''),
	('EAA Drivers', 4, 'Demand Drivers' ,'Demand Drivers', ''),
	('EAA Drivers', 5, 'Supply Drivers' ,'Supply Drivers', ''),
	('EAA Drivers', 6, 'Oil' ,'Oil', ''),
	('EAA Scenario(Taxonomy)', 1, 'Scenario 1' ,'Scenario 1 Rivalry', '<ROOT>'),
	('EAA Scenario(Taxonomy)', 2, 'Scenario 2' ,'Scenario 2 Epoch', '<ROOT>'),
	('EAA Scenario(Taxonomy)', 3, 'Scenario 3' ,'Scenario 3 Peak', '<ROOT>'),
	('EAA Scenario(Taxonomy)', 4, 'Rivalry' ,'Rivalry', '<ROOT>'),
	('EAA Scenario(Taxonomy)', 5, 'R Q1' ,'Q1', 'Rivalry'),
	('EAA Scenario(Taxonomy)', 6, 'R Q2' ,'Q2', 'Rivalry'),
	('EAA Scenario(Taxonomy)', 7, 'R Q3' ,'Q3', 'Rivalry'),
	('EAA Scenario(Taxonomy)', 8, 'R Q4' ,'Q4', 'Rivalry'),
	('EAA Scenario(Taxonomy)', 9, 'Autonomy' ,'Autonomy', '<ROOT>'),
	('EAA Scenario(Taxonomy)', 10, 'A Q1' ,'Q1', 'Autonomy'),
	('EAA Scenario(Taxonomy)', 11, 'A Q2' ,'Q2', 'Autonomy'),
	('EAA Scenario(Taxonomy)', 12, 'Veritigo' ,'Veritigo', '<ROOT>'),
	('EAA Scenario(Taxonomy)', 13, 'V Q4' ,'Q4', 'Veritigo')
	
commit;	