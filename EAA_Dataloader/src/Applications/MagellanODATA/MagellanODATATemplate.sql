---  parameters
--- {schemaName} -> the associated scheme
--- {tbattributesourceName} -> working attribute name 
--- {tbattributedestinationName} -> final attribute name
--- {tbdatasourceName} ->  working data name
--- {tbdatadestinationName} -> final data name
--- {tbstats} -> the name of the statics stable
--- {procid} -> the processid


Begin;
--
-- create tmp attributes working table
--
create TEMP table tmp_magellan_attributes_working(like {schemaName}.{tbattributedestinationName});
insert into  tmp_magellan_attributes_working
  select a.source_id, a.dri_mnemonic, a.start_date, a.end_date,
	       a.base_period_value, a.short_label, a.long_label, a.explorer_label,
	       a.last_update_date, a.document_type, a.wefa_mnemonic
  from {schemaName}.{tbattributesourceName} a
  group by  a.source_id, a.dri_mnemonic, a.start_date, a.end_date,
	       a.base_period_value, a.short_label, a.long_label, a.explorer_label,
	       a.last_update_date, a.document_type, a.wefa_mnemonic;

--
-- create tmp data working table	
create TEMP table tmp_magellan_data_working(like {schemaName}.{tbdatadestinationName});
insert into  tmp_magellan_data_working
  select a.source_id, a.date, a.value
  from {schemaName}.{tbdatasourceName} a
  group by  a.source_id, a.date, a.value;

--
--  create insert tables
--
	create TEMP table tmp_{tbattributedestinationName}_inserts(like {schemaName}.{tbattributedestinationName});
	insert into tmp_{tbattributedestinationName}_inserts
	(
	select a.source_id, a.dri_mnemonic, a.start_date, a.end_date,
	       a.base_period_value, a.short_label, a.long_label, a.explorer_label,
	       a.last_update_date, a.document_type, a.wefa_mnemonic
		from tmp_magellan_attributes_working a
		left outer join {schemaName}.{tbattributedestinationName} b
		on a.source_id = b.source_id
		where
		   b.source_id is null
		group by a.source_id, a.dri_mnemonic, a.start_date, a.end_date,
	       a.base_period_value, a.short_label, a.long_label, a.explorer_label,
	       a.last_update_date, a.document_type, a.wefa_mnemonic		   
	);

	create TEMP table tmp_{tbdatadestinationName}_inserts(like {schemaName}.{tbdatadestinationName});
	insert into tmp_{tbdatadestinationName}_inserts
	(
	select a.source_id, a.date, a.value
		from tmp_magellan_data_working a
		left outer join {schemaName}.{tbdatadestinationName} b
		on a.source_id = b.source_id
		and a.date = b.date
		where
		   b.source_id is null
		   and
		   b.date is null
   		group by  a.source_id, a.date, a.value			   
	);

--
-- create change tables
--
	create TEMP table tmp_{tbattributedestinationName}_changed(like {schemaName}.{tbattributedestinationName});
	insert into tmp_{tbattributedestinationName}_changed
	(
	select distinct a.source_id, a.dri_mnemonic, a.start_date, a.end_date,
	       a.base_period_value, a.short_label, a.long_label, a.explorer_label,
	       a.last_update_date, a.document_type, a.wefa_mnemonic
		from {schemaName}.{tbattributesourceName} a
		join {schemaName}.{tbattributedestinationName} b
		on a.source_id = b.source_id
	);


	create TEMP table tmp_{tbdatadestinationName}_changed(like {schemaName}.{tbdatadestinationName});
	insert into tmp_{tbdatadestinationName}_changed
	(
	select distinct a.source_id, a.date, a.value
		from {schemaName}.{tbdatasourceName} a
		join {schemaName}.{tbdatadestinationName} b
		on a.source_id = b.source_id
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
	where (source_id)
	  in
	  (
	   select source_id
	   from tmp_{tbattributedestinationName}_changed
	  );
	  
	delete from {schemaName}.{tbdatadestinationName}
	where (source_id, date)
	  in
	  (
	   select source_id, date
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

DROP TABLE IF EXISTS {schemaName}.{tbattributesourceName};

DROP TABLE IF EXISTS {schemaName}.{tbdatasourceName};
			  
End;

--vacuum {schemaName}.{tbattributedestinationName};
--vacuum {schemaName}.{tbdatadestinationName};
select 'Update all done';