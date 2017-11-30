-- This script is used to populate the Qualifier reference data for the JODI World files.

INSERT INTO {schemaName}.jodi_qualifier
	(code, description)
VALUES
	(1, 'Results of the assessment show reasonable levels of comparability'),
	(2, 'Consult metadata/Use with caution'),
	(3, 'Data has not been assessed'),
	(4, 'Data under verification');

COMMIT;