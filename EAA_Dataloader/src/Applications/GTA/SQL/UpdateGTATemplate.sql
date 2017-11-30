--
--  read the working table and place the new data into the final table
--  {workingtable} --  the name of the working table
--  {desttable}  --  name of the table to be updated
--  {workingschemaname} -- the schema that we are working in
--  {destschemaname} -- the schema that we are working in
--  {keys} -- key fields
--  {join} -- Join statement

begin;

	create TEMP table tmp_{desttable}_inserts(like {destschemaname}.{desttable});
	insert into tmp_{desttable}_inserts
	(
		select a.*
			from {workingschemaname}.{workingtable} a
			left outer join {destschemaname}.{desttable} b
			on {join}
			where
			   b.{keys} is null
	);

insert into {destschemaname}.{desttable}
	select * from tmp_{desttable}_inserts;

DROP TABLE IF EXISTS {workingschemaname}.{workingtable};
end;

vacuum {destschemaname}.{desttable};
