create table {schemaName}.{viewName}
as
select {distinct} {sourceFields}
from {sourceSchema}.{sourceTable}
{viewJoins}
{viewFilters};