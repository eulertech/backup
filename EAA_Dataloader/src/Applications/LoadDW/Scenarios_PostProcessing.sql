-- Sets Start and End dates based on the first and last date where the data value is greather than 0. 
DROP TABLE IF EXISTS {schemaName}.ScenariosStartingDatesTemp;
DROP TABLE IF EXISTS {schemaName}.ScenariosEndingDatesTemp;

SELECT vw.name, vw.date into {schemaName}.ScenariosStartingDatesTemp
FROM(SELECT dat.name, dat.date, RANK() OVER (PARTITION BY dat.name ORDER BY "date" ASC ) AS rankValue
    FROM {schemaName}.{tableName}_data dat
      INNER JOIN {schemaName}.{tableName}_attributes attr ON attr.name = dat.name AND attr.source = 'Scenarios by Country'
    where dat.value > 0
    )vw
WHERE vw.rankValue = 1;

SELECT vw.name, vw.date into {schemaName}.ScenariosEndingDatesTemp
FROM(SELECT dat.name, dat.date, RANK() OVER (PARTITION BY dat.name ORDER BY "date" DESC ) AS rankValue
    FROM {schemaName}.{tableName}_data dat
      INNER JOIN {schemaName}.{tableName}_attributes attr ON attr.name = dat.name AND attr.source = 'Scenarios by Country'
    where dat.value > 0
    )vw
WHERE vw.rankValue = 1;

UPDATE {schemaName}.{tableName}_attributes SET  startDate = st.date FROM {schemaName}.ScenariosStartingDatesTemp st WHERE st.name = {schemaName}.{tableName}_attributes.name;
UPDATE {schemaName}.{tableName}_attributes SET  enddate = en.date FROM {schemaName}.ScenariosEndingDatesTemp en WHERE en.name = {schemaName}.{tableName}_attributes.name;

COMMIT;

UPDATE {schemaName}.{tableName}_attributes 
    SET forecast = (CASE WHEN (en.date > DATE_PART_YEAR(TO_DATE(CURRENT_DATE, 'YYYY-MM-DD'))) THEN true ELSE false END) 
FROM {schemaName}.ScenariosEndingDatesTemp en WHERE en.name = {schemaName}.{tableName}_attributes.name;

COMMIT;

DROP TABLE {schemaName}.ScenariosStartingDatesTemp;
DROP TABLE {schemaName}.ScenariosEndingDatesTemp;

-- Sets Start and End dates as blank because all of their series data values are 0. 
UPDATE {schemaName}.{tableName}_attributes
  SET startdate = NULL,
      enddate = NULL
WHERE name IN(SELECT dat.name 
              FROM {schemaName}.{tableName}_data dat 
                  INNER JOIN {schemaName}.{tableName}_attributes attr ON attr.name = dat.name AND attr.source = 'Scenarios by Country' 
              GROUP BY dat.name  
              HAVING SUM(dat.value) = 0);

COMMIT;
