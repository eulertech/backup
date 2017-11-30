INSERT INTO {schemaName}.{historyTable}({targetFields},version)
SELECT {targetFields}, to_date('{seriesVersion}', 'YYYY-MM-DD') AS version
FROM {schemaName}.{viewName}

EXCEPT

SELECT {historyFields},to_date('{seriesVersion}', 'YYYY-MM-DD') AS version
FROM {schemaName}.{historyTable} AS h
INNER JOIN ( SELECT name, MAX(version) AS version
			 FROM {schemaName}.{historyTable}
			 WHERE 1 = 1 {incrementalFilter}
			 GROUP BY name
			) AS m ON h.name= m.name AND h.version = m.version;
