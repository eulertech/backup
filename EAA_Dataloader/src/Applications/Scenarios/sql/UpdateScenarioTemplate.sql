---  parameters
--- {schemaName} -> the associated scheme
--- {tbworkingsourceName} -> working table name 
--- {tbattributedestinationName} -> final attribute name
--- {tbdatadestinationName} -> final data name
--- {tbstats} -> the name of the statics stable
--- {procid} -> the processid


Begin;
--
-- create tmp working table
--
create TEMP table tmp_{tbworkingsourceName}_working(like {schemaName}.{tbworkingsourceName});
insert into  tmp_{tbworkingsourceName}_working
  select a.scenario_key, a.scenario_name, a.region, a.mnemonic, a.longname,
         a.date, a.value, a.unit, a.modifieddate
  from {schemaName}.{tbworkingsourceName} a
  group by a.scenario_key, a.scenario_name, a.region, a.mnemonic, a.longname,
           a.date, a.value, a.unit, a.modifieddate;

--
--  create insert tables
--
	create TEMP table tmp_{tbattributedestinationName}_inserts(like {schemaName}.{tbattributedestinationName});
	insert into tmp_{tbattributedestinationName}_inserts
	(
		select a.scenario_key, a.scenario_name, a.region, a.mnemonic, a.longname, a.unit
			from tmp_{tbworkingsourceName}_working a
			left outer join {schemaName}.{tbattributedestinationName} b
			on a.scenario_key = b.scenario_key
			where
			   b.scenario_key is null
			group by a.scenario_key, a.scenario_name, a.region, a.mnemonic, a.longname, a.unit
	);

	create TEMP table tmp_{tbdatadestinationName}_inserts(like {schemaName}.{tbdatadestinationName});
	insert into tmp_{tbdatadestinationName}_inserts
	(
		select a.scenario_key, a.date, a.value 
			from tmp_{tbworkingsourceName}_working a
			left outer join {schemaName}.{tbdatadestinationName} b
			on a.scenario_key = b.scenario_key
			and a.date = b.date
			where
			   b.scenario_key is null
			   and
			   b.date is null
	);

--
-- create change tables
--
	create TEMP table tmp_{tbattributedestinationName}_changed(like {schemaName}.{tbattributedestinationName});
	insert into tmp_{tbattributedestinationName}_changed
	(
		select distinct a.scenario_key, a.scenario_name, a.region, a.mnemonic, a.longname, a.unit
			from tmp_{tbworkingsourceName}_working a
			join  {schemaName}.{tbattributedestinationName} b
			on a.scenario_key = b.scenario_key
	);


	create TEMP table tmp_{tbdatadestinationName}_changed(like {schemaName}.{tbdatadestinationName});
	insert into tmp_{tbdatadestinationName}_changed
	(
	select distinct  a.scenario_key, a.date, a.value
		from tmp_{tbworkingsourceName}_working a
		join {schemaName}.{tbdatadestinationName} b
		on a.scenario_key = b.scenario_key
		and a.date = b.date
	);

	update {tbstats}
	set recsinserted = (select count(*) from tmp_{tbattributedestinationName}_inserts),
		recsmodified = (select count(*) from tmp_{tbattributedestinationName}_changed)
	where runid = {procid};
--
--remove records first
---
	delete from {schemaName}.{tbattributedestinationName}
	where (scenario_key)
	  in
	  (
	   select scenario_key
	   from tmp_{tbattributedestinationName}_changed
	  );
	  
	delete from {schemaName}.{tbdatadestinationName}
	where (scenario_key, date)
	  in
	  (
	   select scenario_key, date
	   from tmp_{tbdatadestinationName}_changed
	  );
--
--  insert new and changed records
--
	insert into {schemaName}.{tbattributedestinationName}
	(
	   select *
	   from tmp_{tbattributedestinationName}_inserts
	);	  
	
	insert into {schemaName}.{tbdatadestinationName}
	(
	   select *
	   from tmp_{tbdatadestinationName}_inserts
	);	 	
	  
	insert into {schemaName}.{tbattributedestinationName}
	(
	   select *
	   from tmp_{tbattributedestinationName}_changed
	);	  
	
	insert into {schemaName}.{tbdatadestinationName}
	(
	   select *
	   from tmp_{tbdatadestinationName}_changed
	);	 
	
DROP TABLE IF EXISTS {schemaName}.{tbworkingsourceName};
			  
End;

vacuum {schemaName}.{tbattributedestinationName};
vacuum {schemaName}.{tbdatadestinationName};
