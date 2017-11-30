-- This script is used to populate the Product reference data for JODI World files.

INSERT INTO {schemaName}.jodi_product
	(name, file_table, full_name)
VALUES 
	('CRUDEOIL', 'primary', 'Crude oil'),
	('NGL', 'primary', 'NGL'),
	('OTHERCRUDE', 'primary', 'Other Crude'),
	('TOTCRUDE', 'primary', 'Total Crude'),
	('KEROSENE', 'secondary', 'Kerosene (incl. jetkero/other kero)'),
	('GASOLINE', 'secondary', 'Motor Gasoline (incl. aviation gasoline)'),
	('GASDIES', 'secondary', 'Gas/Diesel Oil'),
	('LPG', 'secondary', 'Liquefied Petroleum Gases'),
	('RESFUEL', 'secondary', 'Residual Fuel Oil'),
	('NAPHTHA', 'secondary', 'Naphtha'),
	('JETKERO', 'secondary', 'Kerosene type jet fuel'),
	('ONONSPEC', 'secondary', 'Other oil products'),
	('TOTPRODSC', 'secondary', 'Total oil products'),
	('TOTPRODS', 'secondary', 'Total oil products');

COMMIT;