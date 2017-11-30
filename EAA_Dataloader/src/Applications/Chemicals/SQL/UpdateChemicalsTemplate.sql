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
  select a.productid, a.product, a.locationid, a.location, a.category_id,
         a.category, a.years, a.value
  from {schemaName}.{tbworkingsourceName} a
  group by a.productid, a.product, a.locationid, a.location, a.category_id,
         a.category, a.years, a.value;

--
--  create insert tables
--
	create TEMP table tmp_{tdestinationName}_inserts(like {schemaName}.{tdestinationName});
	insert into tmp_{tdestinationName}_inserts
	(
		select  a.productid, a.product, a.locationid, a.location, a.category_id,
         		a.category, a.years, a.value
			from tmp_{tbworkingsourceName} a
			left outer join {schemaName}.{tdestinationName} b
				on a.productid = b.productid
				and a.product = b.product
				and a.locationid = b.locationid
				and a.location = b.location
				and a.category_id = b.category_id
				and a.category = b.category
				and a.years = b.years
			where
			   b.productid is null
			group by a.productid, a.product, a.locationid, a.location, a.category_id,
         			 a.category, a.years, a.value	
	);

--
-- create change tables
--
	create TEMP table tmp_{tdestinationName}_changed(like {schemaName}.{tdestinationName});
	insert into tmp_{tdestinationName}_changed
	(
		select distinct a.productid, a.product, a.locationid, a.location, a.category_id,
         				a.category, a.years, a.value
			from tmp_{tbworkingsourceName} a
			join  {schemaName}.{tdestinationName} b
				on a.productid = b.productid
				and a.product = b.product
				and a.locationid = b.locationid
				and a.location = b.location
				and a.category_id = b.category_id
				and a.category = b.category
				and a.years = b.years
			where
			 	md5(a.productid||a.product||a.locationid||a.location||a.category_id||
	                a.category||a.years||a.value::TEXT) != md5(b.productid||b.product||b.locationid||b.location||b.category_id||
	                b.category||b.years||b.value::TEXT)  
	);

	update {tbstats}
	set recsinserted = (select count(*) from tmp_{tdestinationName}_inserts),
		recsmodified = (select count(*) from tmp_{tdestinationName}_changed)
	where runid = {procid};
--
--remove records first
---
	delete from {schemaName}.{tdestinationName}
	where (productid, product, locationid, location, category_id, category, years,  value)
	  in
	  (
	   select productid, product, locationid, location, category_id, category, years,  value
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
