INSERT INTO {schemaName}.{historyTable}
SELECT L.name, L.date, L.value, 'I' AS status, to_date('{seriesVersion}', 'YYYY-MM-DD') AS version
FROM( SELECT name, date FROM {schemaName}.{viewName}
      EXCEPT 
      SELECT h.name, h.date 
      FROM {schemaName}.{historyTable} h
	      JOIN (SELECT name, date, max(version) as version
	            FROM {schemaName}.{historyTable}
	            WHERE status <> 'D' {incrementalFilter}
	            GROUP BY name, date) lh 
	      ON lh.name = h.name
	        AND lh.date = h.date
	        AND lh.version = h.version
) AS O
INNER JOIN {schemaName}.{viewName} AS L ON O.name = L.name
    AND O.date = L.date;
