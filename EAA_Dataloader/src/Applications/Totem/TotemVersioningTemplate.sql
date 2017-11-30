---  parameters
--- tbname -> represent the application name 
--- schemaName -> the associated scheme
--- tbtotem -> the destination table name
--- tbstats -> the name of the statics stable
--- procid -> the processid
--- fromdate -> the date from which the record first became valid  {fromdate}

begin;
-- create temp tables for insert/change/delete references
	create TEMP table tmp_{tbname}_inserts(like {schemaName}.{tbtotem});
	insert into tmp_totem_inserts
	(
	select a.*
		from {schemaName}.{tbtotem}_working a
		left outer join {schemaName}.{tbtotem} b
		on a.valuationdate = b.valuationdate
		and a.clientid = b.clientid
		and a.name = b.name
		and a.totemgroup = b.totemgroup
		and a.units = b.units
		and a.period = b.period
		and a.startdate = b.startdate
		and a.enddate = b.enddate
		and a.totemtype = b.totemtype	
		and nvl(a.strike,'') = nvl(b.strike,'')
		where
		   b.valuationdate is null
	);

	create TEMP table tmp_totem_changed(like {schemaName}.{tbtotem});
	insert into tmp_totem_changed
	(
	    select a.valuationdate, a.clientid, a.name, a.totemgroup, a.units, a.pricingtime, a.period, a.startdate, a.enddate,
	           a.totemtype, a.price, a.consensusprice, a.compositeprice, a.pricerange, a.contributors, a.pricestddev,
	           a.strike, a.vol, a.reconstitutedforward, a.consensusvol, a.compositevol, a.volrange, a.expirydate,
	           a.volstddev
       	from {schemaName}.{tbtotem}_working a
          	join {schemaName}.{tbtotem} b
          	on a.valuationdate = b.valuationdate
				and a.clientid = b.clientid
				and a.name = b.name
				and a.totemgroup = b.totemgroup
				and a.units = b.units
				and a.period = b.period
				and a.startdate = b.startdate
				and a.enddate = b.enddate
				and a.totemtype = b.totemtype	
				and nvl(a.strike,'') = nvl(b.strike,'')
		        and b.recstatus = 'I'
	          	where
	          	       md5(a.valuationdate||a.clientid||a.name||a.totemgroup||a.units||
	                    a.pricingtime||a.period||a.startdate||a.enddate||a.totemtype||nvl(a.price,0)||nvl(a.consensusprice,0)||
	                    nvl(a.compositeprice,'')||nvl(a.pricerange,'')||a.contributors||nvl(a.pricestddev,'')||
	                    nvl(a.strike,'')||nvl(a.vol,'')||nvl(a.reconstitutedforward,'')||nvl(a.consensusvol,'')||
	                    nvl(a.compositevol,'')||nvl(a.volrange,'')||nvl(a.expirydate,'')
	                    ::TEXT) !=  md5(b.valuationdate||b.clientid||b.name||b.totemgroup||b.units||
	                    b.pricingtime||b.period||b.startdate||b.enddate||b.totemtype||nvl(b.price,0)||nvl(b.consensusprice,0)||
	                    nvl(b.compositeprice,'')||nvl(b.pricerange,'')||b.contributors||nvl(b.pricestddev,'')||
	                    nvl(b.strike,'')||nvl(b.vol,'')||nvl(b.reconstitutedforward,'')||nvl(b.consensusvol,'')||
	                    nvl(b.compositevol,'')||nvl(b.volrange,'')||nvl(b.expirydate,'')
	                    ::TEXT)
	);

	create TEMP table tmp_totem_deleted(like {schemaName}.{tbtotem});
	insert into tmp_totem_deleted
	(
	select b.*
		from {schemaName}.{tbtotem}_working a
		right outer join {schemaName}.{tbtotem} b
		on a.valuationdate = b.valuationdate
			and a.clientid = b.clientid
			and a.name = b.name
			and a.totemgroup = b.totemgroup
			and a.units = b.units
			and a.period = b.period
			and a.startdate = b.startdate
			and a.enddate = b.enddate
			and a.totemtype = b.totemtype	
			and nvl(a.strike,'') = nvl(b.strike,'')	
		where
		   a.valuationdate is null
		   and b.recstatus = 'I'
	);

	update {schemaName}.{tbstats}
	set recsinserted = (select count(*) from tmp_{tbname}_inserts),
		recsmodified = (select count(*) from tmp_{tbname}_changed),
		recsdeleted = (select count(*) from tmp_{tbname}_deleted)
	where processid = {procid};

--- mark the deleted ones
	update {schemaName}.{tbtotem}
	set recexpirydate = cast(getdate() as varchar(10)),
	    recstatus = 'D'
	where (valuationdate, name, startdate, period, totemtype) in
	  ( select valuationdate, name, startdate, period, totemtype from tmp_{tbname}_deleted);
--
--  insert the new records with no expiration date and the 
--  from date to the earliest available date
--
	insert into {schemaName}.{tbtotem}
	(
		valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	    totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	    strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate,
	    volstddev, recstatus, recstatusdate, recfromdate
	)
	(
	   select valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	          totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	          strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate,
	          volstddev, 
	          'I', getDate(), '{fromdate}-01'
	   from tmp_{tbname}_inserts a
	);
--
-- enddate the ones that we are going to have to modify
--
	update {schemaName}.{tbtotem}
	 set recstatus = 'C',
	    recstatusdate = getDate(),
	    recexpirydate = cast(getdate() as varchar(10))
	 where 
	 (valuationdate, name, startdate, period, totemtype) in
	  ( select valuationdate, name, startdate, period, totemtype from tmp_{tbname}_changed);
	  
--
-- now that we have expired the older records we need new records
-- and we will mark its from date to the current date
-- and the expirydate to way out in the future
--
	insert into {schemaName}.{tbtotem}
	(
		valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	    totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	    strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate,
	    volstddev, recstatus, recstatusdate, recfromdate
	)
	(
	   select valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	          totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	          strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate,
	          volstddev, 
	          'I', getDate(), getDate()      
	   from tmp_{tbname}_changed a
	);
select 'versioning all done';
end;
