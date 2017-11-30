-- Build the latest version for the Series Attributes...

insert into {schemaName}.{federatedAttributesLatestTable}
select h.name, h.label, h.description, h.source, h.concept, h.frequency, h.forecast, h.startdate, h.enddate, h.qualifier, h.latlon, h.last_update_date, h.source_id
from {schemaName}.{federatedAttributesHistoryTable} h 
      inner join (
              select name, max(version) as version
              from {schemaName}.{federatedAttributesHistoryTable}
              group by name
      ) as m on h.name = m.name and h.version = m.version
where h.name = m.name 
    and h.version = m.version;
