INSERT INTO {schemaName}.{historyTable}(name, date, status, version)
SELECT O.name, O.date, 'D' AS status, to_date('{seriesVersion}', 'YYYY-MM-DD') AS version
FROM
(
    SELECT name, date FROM {schemaName}.{historyTable} WHERE 1 = 1 {incrementalFilter}
    EXCEPT
    SELECT name, date FROM {schemaName}.{viewName}
) AS O;
