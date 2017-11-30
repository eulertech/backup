---  parameters
--- {schemaName} -> the associated scheme
--- {tbstats} -> the name of the statics stable
--- {procid} -> the processid

Begin;
--
--  create temp insert tables
--
	create TEMP table tmp_geforecast_giif_attributes_inserts(like {schemaName}.geforecast_giif_attributes);
	create TEMP table tmp_geforecast_srs_attributes_inserts(like {schemaName}.geforecast_srs_attributes);
	create TEMP table tmp_geforecast_wes_attributes_inserts(like {schemaName}.geforecast_wes_attributes);

	create TEMP table tmp_geforecast_giif_data_inserts(like {schemaName}.geforecast_giif_data);
	create TEMP table tmp_geforecast_srs_data_inserts(like {schemaName}.geforecast_srs_data);
	create TEMP table tmp_geforecast_wes_data_inserts(like {schemaName}.geforecast_wes_data);
	
--
--  create temp change tables  
--
	create TEMP table tmp_geforecast_giif_attributes_change(like {schemaName}.geforecast_giif_attributes);
	create TEMP table tmp_geforecast_srs_attributes_change(like {schemaName}.geforecast_srs_attributes);
	create TEMP table tmp_geforecast_wes_attributes_change(like {schemaName}.geforecast_wes_attributes);

	create TEMP table tmp_geforecast_giif_data_change(like {schemaName}.geforecast_giif_data);
	create TEMP table tmp_geforecast_srs_data_change(like {schemaName}.geforecast_srs_data);
	create TEMP table tmp_geforecast_wes_data_change(like {schemaName}.geforecast_wes_data);

---
-- fill the insert tables
---
	
	insert into tmp_geforecast_giif_attributes_inserts
	(
	select a.object_id, a.name, a.mnemonic, a.frequencychar,
	       a.geo, a.startdate, a.enddate, a.updateddate,
	       a.publisheddate, a.longlabel, a.dataedge
		from {schemaName}.geforecast_giif_attributes_working a
		left outer join {schemaName}.geforecast_giif_attributes b
		on a.object_id = b.object_id
		where
		   b.object_id is null
	);

	insert into tmp_geforecast_srs_attributes_inserts
	(
	select a.object_id, a.name, a.mnemonic, a.frequencychar,
	       a.geo, a.startdate, a.enddate, a.updateddate,
	       a.publisheddate, a.longlabel, a.dataedge
		from {schemaName}.geforecast_srs_attributes_working a
		left outer join {schemaName}.geforecast_srs_attributes b
		on a.object_id = b.object_id
		where
		   b.object_id is null
	);


	insert into tmp_geforecast_wes_attributes_inserts
	(
	select a.object_id, a.name, a.mnemonic, a.frequencychar,
	       a.geo, a.startdate, a.enddate, a.updateddate,
	       a.publisheddate, a.longlabel, a.dataedge
		from {schemaName}.geforecast_wes_attributes_working a
		left outer join {schemaName}.geforecast_giif_attributes b
		on a.object_id = b.object_id
		where
		   b.object_id is null
	);

---
-- data
---

	insert into tmp_geforecast_giif_data_inserts
	(
	select a.object_id, a.date, a.value
		from {schemaName}.geforecast_giif_data_working a
		left outer join {schemaName}.geforecast_giif_data b
		on a.object_id = b.object_id
		and a.date = b.date
		where
		   b.object_id is null
		   and
		   b.date is null
   		group by  a.object_id, a.date, a.value			   
	);

	insert into tmp_geforecast_srs_data_inserts
	(
	select a.object_id, a.date, a.value
		from {schemaName}.geforecast_srs_data_working a
		left outer join {schemaName}.geforecast_giif_data b
		on a.object_id = b.object_id
		and a.date = b.date
		where
		   b.object_id is null
		   and
		   b.date is null
   		group by  a.object_id, a.date, a.value			   
	);

	insert into tmp_geforecast_wes_data_inserts
	(
	select a.object_id, a.date, a.value
		from {schemaName}.geforecast_wes_data_working a
		left outer join {schemaName}.geforecast_giif_data b
		on a.object_id = b.object_id
		and a.date = b.date
		where
		   b.object_id is null
		   and
		   b.date is null
   		group by  a.object_id, a.date, a.value			   
	);

--
-- fill the change tables
--
	insert into tmp_geforecast_giif_attributes_change
	(
	select distinct a.object_id, a.name, a.mnemonic, a.frequencychar,
	       a.geo, a.startdate, a.enddate, a.updateddate,
	       a.publisheddate, a.longlabel, a.dataedge
		from {schemaName}.geforecast_giif_attributes_working a
		join {schemaName}.geforecast_giif_attributes b
		on a.object_id = b.object_id
	);

	insert into tmp_geforecast_srs_attributes_change
	(
	select distinct a.object_id, a.name, a.mnemonic, a.frequencychar,
	       a.geo, a.startdate, a.enddate, a.updateddate,
	       a.publisheddate, a.longlabel, a.dataedge
		from {schemaName}.geforecast_srs_attributes_working a
		join {schemaName}.geforecast_srs_attributes b
		on a.object_id = b.object_id
	);

	insert into tmp_geforecast_wes_attributes_change
	(
	select distinct a.object_id, a.name, a.mnemonic, a.frequencychar,
	       a.geo, a.startdate, a.enddate, a.updateddate,
	       a.publisheddate, a.longlabel, a.dataedge
		from {schemaName}.geforecast_wes_attributes_working a
		join {schemaName}.geforecast_wes_attributes b
		on a.object_id = b.object_id
	);

---
-- data
---

	insert into tmp_geforecast_giif_data_change
	(
	select distinct a.object_id, a.date, a.value
		from {schemaName}.geforecast_giif_data_working a
		join {schemaName}.geforecast_giif_data b
		on a.object_id = b.object_id
		and a.date = b.date
	);

	insert into tmp_geforecast_srs_data_change
	(
	select distinct a.object_id, a.date, a.value
		from {schemaName}.geforecast_srs_data_working a
		join {schemaName}.geforecast_srs_data b
		on a.object_id = b.object_id
		and a.date = b.date
	);

	insert into tmp_geforecast_wes_data_change
	(
	select distinct a.object_id, a.date, a.value
		from {schemaName}.geforecast_wes_data_working a
		join {schemaName}.geforecast_wes_data b
		on a.object_id = b.object_id
		and a.date = b.date
	);

create temp table tmp_counters as select
	0 as recsinserted,
	0 as recsmodified;
	
insert into tmp_counters(recsinserted,recsmodified) 
values(
	(select count(*) from tmp_geforecast_giif_attributes_inserts),
	(select count(*) from tmp_geforecast_giif_attributes_change)
);	

insert into tmp_counters(recsinserted,recsmodified)
values(
	(select count(*) from tmp_geforecast_srs_attributes_inserts),
	(select count(*) from tmp_geforecast_srs_attributes_change)
);	

insert into tmp_counters(recsinserted,recsmodified)
values(
	(select count(*) from tmp_geforecast_wes_attributes_inserts),
	(select count(*) from tmp_geforecast_wes_attributes_change)
);	

	update {tbstats}
	set recsinserted = (select sum(recsinserted) from tmp_counters),
		recsmodified = (select sum(recsmodified) from tmp_counters)
	where runid = {procid};
--
--remove records first
---
	delete from {schemaName}.geforecast_giif_attributes
	where (object_id)
	  in
	  (
	   select object_id
	   from tmp_geforecast_giif_attributes_change
	  );
	  
	delete from {schemaName}.geforecast_srs_attributes
	where (object_id)
	  in
	  (
	   select object_id
	   from tmp_geforecast_srs_attributes_change
	  );

	delete from {schemaName}.geforecast_wes_attributes
	where (object_id)
	  in
	  (
	   select object_id
	   from tmp_geforecast_wes_attributes_change
	  );

---
-- data
---

	delete from {schemaName}.geforecast_giif_data
	where (object_id, date)
	  in
	  (
	   select object_id, date
	   from tmp_geforecast_giif_data_change
	  );

	delete from {schemaName}.geforecast_srs_data
	where (object_id, date)
	  in
	  (
	   select object_id, date
	   from tmp_geforecast_srs_data_change
	  );

	delete from {schemaName}.geforecast_wes_data
	where (object_id, date)
	  in
	  (
	   select object_id, date
	   from tmp_geforecast_wes_data_change
	  );

--
--  insert new and changed records
--
	insert into {schemaName}.geforecast_giif_attributes
	(
	   select *
	   from tmp_geforecast_giif_attributes_inserts
	);	  
	
	insert into {schemaName}.geforecast_srs_attributes
	(
	   select *
	   from tmp_geforecast_srs_attributes_inserts
	);	  

	insert into {schemaName}.geforecast_wes_attributes
	(
	   select *
	   from tmp_geforecast_wes_attributes_inserts
	);	  
	
	insert into {schemaName}.geforecast_giif_attributes
	(
	   select *
	   from tmp_geforecast_giif_attributes_change
	);	  
	
	insert into {schemaName}.geforecast_srs_attributes
	(
	   select *
	   from tmp_geforecast_srs_attributes_change
	);	  

	insert into {schemaName}.geforecast_wes_attributes
	(
	   select *
	   from tmp_geforecast_wes_attributes_change
	);	  
	

---
-- data
---	
	  
	insert into {schemaName}.geforecast_giif_data
	(
	   select *
	   from tmp_geforecast_giif_data_inserts
	);	  
	
	insert into {schemaName}.geforecast_srs_data
	(
	   select *
	   from tmp_geforecast_srs_data_inserts
	);	  

	insert into {schemaName}.geforecast_wes_data
	(
	   select *
	   from tmp_geforecast_wes_data_inserts
	);	  
	
	
	insert into {schemaName}.geforecast_giif_data
	(
	   select *
	   from tmp_geforecast_giif_data_change
	);	  
	
	insert into {schemaName}.geforecast_srs_data
	(
	   select *
	   from tmp_geforecast_srs_data_change
	);	  

	insert into {schemaName}.geforecast_wes_data
	(
	   select *
	   from tmp_geforecast_wes_data_change
	);	

--DROP TABLE IF EXISTS {schemaName}.geforecast_giif_attributes_working;
--DROP TABLE IF EXISTS {schemaName}.geforecast_srs_attributes_working;
--DROP TABLE IF EXISTS {schemaName}.geforecast_wes_attributes_working;

--DROP TABLE IF EXISTS {schemaName}.geforecast_giif_data_working;
--DROP TABLE IF EXISTS {schemaName}.geforecast_srs_data_working;
--DROP TABLE IF EXISTS {schemaName}.geforecast_wes_data_working;
			  
End;

vacuum {schemaName}.geforecast_giif_attributes;
vacuum {schemaName}.geforecast_giif_data;
vacuum {schemaName}.geforecast_srs_attributes;
vacuum {schemaName}.geforecast_srs_data;
vacuum {schemaName}.geforecast_wes_attributes;
vacuum {schemaName}.geforecast_wes_data;

select 'Update all done';