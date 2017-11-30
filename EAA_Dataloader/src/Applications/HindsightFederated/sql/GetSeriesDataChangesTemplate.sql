create table {schemaName}.{historyTable}_partitioned(like {schemaName}.{historyTable});

insert into {schemaName}.{historyTable}_partitioned
select h.*
from {schemaName}.{historyTable} h
      join (select name, date, max(version) as version
            from {schemaName}.{historyTable}
            where status <> 'D' {incrementalFilter}
            group by name, date) lh 
      on lh.name = h.name
        and lh.date = h.date
        and lh.version = h.version;


INSERT INTO {schemaName}.{historyTable}
SELECT L.name, L.date , L.value, 'M' AS status, to_date('{seriesVersion}', 'YYYY-MM-DD') AS version
FROM( SELECT name, date 
      FROM(
          SELECT name, date, value FROM {schemaName}.{viewName}
          EXCEPT 
          SELECT name, date, value FROM {schemaName}.{historyTable}_partitioned
        ) AS O1 --includes status M and I

      INTERSECT

      SELECT name, date 
      FROM(
          SELECT name, date FROM {schemaName}.{viewName}
          INTERSECT
          SELECT name, date FROM {schemaName}.{historyTable}_partitioned
      ) AS O2 --includes status M and NO_CHANGE
  ) AS O3
INNER JOIN {schemaName}.{viewName} AS L
    ON O3.name=L.name
    AND O3.date=L.date;

drop table {schemaName}.{historyTable}_partitioned;
