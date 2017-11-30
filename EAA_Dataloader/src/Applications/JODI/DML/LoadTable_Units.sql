-- This script is used to populate the Units reference data for the JODI World files.

INSERT INTO {schemaName}.jodi_units
	(name, description, uom)
VALUES
	('KBBL', 'Thousand Barrels', 'kbbl'),
	('TONS', 'Thousand Metric Tons', 'kmt'),
	('KL', 'Thousand Kilolitres', 'kl'),
	('KBD', 'Thousand Barrels per day', 'kb/d'),
	('CONVBBL', 'Conversion factor', 'barrels/ktons');

COMMIT;