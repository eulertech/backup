---  parameters
--- tbname -> represent the application name 
--- schemaName -> the associated scheme
--- tbtotem -> the destination table name
--- tbstats -> the name of the statics stable
--- procid -> the processid

begin;
-- create temp tables for insert/change/delete references
	create TEMP table tmp_{tbname}_inserts(like {schemaName}.{tbtotem});
	insert into tmp_{tbname}_inserts
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
	insert into tmp_{tbname}_changed
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

	update {tbstats}
	set recsinserted = (select count(*) from tmp_{tbname}_inserts),
		recsmodified = (select count(*) from tmp_{tbname}_changed)
	where runid = {procid};

--
-- remove records that match the records for changed first
--
	delete from {schemaName}.{tbtotem}
	where (valuationdate, clientid, name, totemgroup, units, period, startdate, enddate, totemtype, nvl(strike,''))
	  in
	  (
	   select valuationdate, clientid, name, totemgroup, units, period, startdate, enddate,totemtype, nvl(strike,'')
	   from tmp_{tbname}_changed
	  );

--
--  insert the new records 
--
	insert into {schemaName}.{tbtotem}
	(
		valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	    totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	    strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate,
	    volstddev
	)
	(
	   select valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	          totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	          strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate, volstddev
	   from tmp_{tbname}_inserts
	);

--
--  insert the change records 
--
	insert into {schemaName}.{tbtotem}
	(
		valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	    totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	    strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate,
	    volstddev
	)
	(
	   select valuationdate, clientid, name, totemgroup, units, pricingtime, period, startdate, enddate,
	          totemtype, price, consensusprice, compositeprice, pricerange, contributors, pricestddev,
	          strike, vol, reconstitutedforward, consensusvol, compositevol, volrange, expirydate, volstddev
	   from tmp_{tbname}_changed
	);
	
DROP TABLE IF EXISTS {schemaName}.{tbtotem}_working;

end;
vacuum {schemaName}.{tbtotem};
select 'versioning all done';