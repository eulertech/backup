-- Pre process script for econConnect (Hindsight)... 

SELECT dat.series_id, REPLACE(REPLACE(REPLACE(REPLACE(att.mnemonic, '$', 'd'), '%', 'p'), '&', 'n'), '@', 'a') as mnemonic, 
	dat.date, dat.datavalue
INTO {schemaName}.econconnect_data_cleaned
FROM {sourceSchema}.series_data dat
	INNER JOIN {sourceSchema}.series_attributes att ON att.series_id = dat.series_id
WHERE datavalue IS NOT NULL 
	AND att.mnemonic IS NOT NULL;

COMMIT;


SELECT att.series_id, REPLACE(REPLACE(REPLACE(REPLACE(att.mnemonic, '$', 'd'), '%', 'p'), '&', 'n'), '@', 'a') as mnemonic, 
    att.seriestype, att.shortlabel, att.frequency, att.startdate, att.enddate, att.concept, att.unit, 
    att.industry, att.geo, att.source
INTO {schemaName}.econconnect_attributes_cleaned
FROM {sourceSchema}.series_attributes att
WHERE att.mnemonic IS NOT NULL
	AND att.series_id IN(SELECT DISTINCT series_id FROM {schemaName}.econconnect_data_cleaned);;

COMMIT;