-- Sets the latest version for Series Data ...

create table {schemaName}.series_names_chunk(name varchar(512) encode zstd);

copy {schemaName}.series_names_chunk from '{s3TempBucket}{s3FileName}' 
credentials 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}';


create table {schemaName}.hfhistory_calculated as(
	select o.name, o.date, max(o.version) as version
	from {schemaName}.{federatedDataHistoryTable} o
		join {schemaName}.series_names_chunk ch on ch.name = o.name
	where o.status <> 'D'
	group by o.name, o.date
);


insert into {schemaName}.{federatedDataLatestTable}
select h.name, h.date, h.value
from {schemaName}.{federatedDataHistoryTable} h
	join {schemaName}.hfhistory_calculated lh 
	on lh.name = h.name
		and lh.date = h.date
		and lh.version = h.version;


drop table {schemaName}.hfhistory_calculated;
drop table {schemaName}.series_names_chunk;