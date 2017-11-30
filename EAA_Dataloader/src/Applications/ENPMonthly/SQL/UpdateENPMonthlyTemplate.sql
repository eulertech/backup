---  parameters
--- {schemaName} -> the associated scheme
--- {tbworkingsourceName} -> working table name 
--- {tdestinationName} -> final attribute name
--- {tbstats} -> the name of the statics stable
--- {procid} -> the processid


Begin;
--
-- create tmp working table
--
create TEMP table tmp_{tbworkingsourceName}(like {schemaName}.{tbworkingsourceName});
insert into  tmp_{tbworkingsourceName}
  select a.category, a.frequency, a.description, a.source, a.unit,
         a.valuationdate, a.value
  from {schemaName}.{tbworkingsourceName} a
  group by a.category, a.frequency, a.description, a.source, a.unit, a.valuationdate, a.value;

--
--  create insert tables
--
	create TEMP table tmp_{tdestinationName}_inserts(like {schemaName}.{tdestinationName});
	insert into tmp_{tdestinationName}_inserts
	(
		select  a.category, a.frequency, a.description, a.source, a.unit,
         		a.valuationdate, a.value
			from tmp_{tbworkingsourceName} a
			left outer join {schemaName}.{tdestinationName} b
				on a.category = b.category
				and a.frequency = b.frequency
				and a.description = b.description
				and a.source = b.source
				and a.unit = b.unit
				and a.valuationdate = b.valuationdate
			where
			   b.category is null
			group by a.category, a.frequency, a.description, a.source, a.unit,
         		a.valuationdate, a.value	
	);

--
-- create change tables
--
	create TEMP table tmp_{tdestinationName}_changed(like {schemaName}.{tdestinationName});
	insert into tmp_{tdestinationName}_changed
	(
		select distinct a.category, a.frequency, a.description, a.source, a.unit,
         		a.valuationdate, a.value
			from tmp_{tbworkingsourceName} a
			join  {schemaName}.{tdestinationName} b
				on a.category = b.category
				and a.frequency = b.frequency
				and a.description = b.description
				and a.source = b.source
				and a.unit = b.unit
				and a.valuationdate = b.valuationdate
			where
			 	md5(a.category||a.frequency||a.description||a.source||a.unit||
	                a.valuationdate||a.value::TEXT) != md5(b.category||b.frequency||b.description||b.source||b.unit||
	                b.valuationdate||b.value::TEXT)  
	);

	update {tbstats}
	set recsinserted = (select count(*) from tmp_{tdestinationName}_inserts),
		recsmodified = (select count(*) from tmp_{tdestinationName}_changed)
	where runid = {procid};
--
--remove records first
---
	delete from {schemaName}.{tdestinationName}
	where (category, frequency, description, source, unit, valuationdate)
	  in
	  (
	   select category, frequency, description, source, unit, valuationdate
	   from tmp_{tdestinationName}_changed
	  );
--
--  insert new and changed records
--
	insert into {schemaName}.{tdestinationName}
	(
	   select *
	   from tmp_{tdestinationName}_inserts
	);	  
	
	  
	insert into {schemaName}.{tdestinationName}
	(
	   select *
	   from tmp_{tdestinationName}_changed
	);	  
	
DROP TABLE IF EXISTS {schemaName}.{tbworkingsourceName};
			  
End;

vacuum {schemaName}.{tdestinationName};
