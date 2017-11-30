---  parameters
-- {schemaName} -> the associated scheme
--- {attrSrc} -> attribute source table 
--- {dataSrc} -> data source table
--- {attrDest} -> final table for attributes
--- {dataDest} -> final table for data
--- {orderByFields} -> this is the value that is used in order of the sql, it is also the value that will be used to create the incremental history.

Begin;

create temp table tmp_objectids as  (
	select object_id, mnemonic
	from {schemaName}.{attrSrc} where publisheddate = (select min(publisheddate) from {schemaName}.{attrSrc})
);	 

---
--  copy the attribute records over to history
---
	insert into {schemaName}.{attrDest}
	(
	select a.*
	  from {schemaName}.{attrSrc} a
	    join tmp_objectids b
	    on a.object_id = b.object_id
	);

--
-- now copy over the data records
--	
	insert into {schemaName}.{dataDest}
	(
	select b.mnemonic, a.date, a.value
	  from {schemaName}.{dataSrc} a
	    join tmp_objectids b
	    on a.object_id = b.object_id
	);
	
--
-- remove records from the source
--
  	delete from {schemaName}.{attrSrc}
  	where (object_id)
  	  in 
  	  (
  	    select object_id from tmp_objectids
  	  );

  	delete from {schemaName}.{dataSrc}
  	where (object_id)
  	  in 
  	  (
  	    select object_id from tmp_objectids
  	  );
	
End;