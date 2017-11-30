-- This script is used to populate the Flow reference data for the JODI World files.

INSERT INTO {schemaName}.jodi_flow
	(name, file_table, full_name)
VALUES 
	('CSNATTER', 'primary', 'Closing stocks'),
	('DIRECTUSE', 'primary', 'Direct use'),
	('OTHSOURCES', 'primary', 'From other sources'),
	('PRODREFOUT', 'primary', 'Production/Refinery Output'),
	('PTRANSFBF', 'primary', 'Products transferred/Backflows'),
	('REFOBSDEM', 'primary', 'Refinery intake/Demand'),
	('STATDIFF', 'primary', 'Statistical difference'),
	('STCHANAT', 'primary', 'Stock change'),
	('TOTEXPSB', 'primary', 'Exports'),
	('TOTIMPSB', 'primary', 'Imports'),
	('PRECEIPTS', 'secondary', 'Receipts'),
	('PRODREFOUT', 'secondary', 'Refinery output'),
	('TOTIMPSB', 'secondary', 'Imports'),
	('TOTEXPSB', 'secondary', 'Exports'),
	('TRANSF', 'secondary', 'Products transferred'),
	('INTPRODTRANSF', 'secondary', 'Interproduct transfers'),
	('STCHANAT', 'secondary', 'Stock change'),
	('STATDIFF', 'secondary', 'Statistical difference'),
	('REFOBSDEM', 'secondary', 'Demand'),
	('CSNATTER', 'secondary', 'Closing stocks');

COMMIT;