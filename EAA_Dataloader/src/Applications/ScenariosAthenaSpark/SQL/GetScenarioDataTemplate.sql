/*
  pull scenarios data from predefined view based on last run date of {lastrundate}
*/
select 
       replace(	
			replace(
				replace(
					'Scenarios' + '.' + Scenario + '.' + mnemonic,
					' ', ''),
				 ')',''),
			 '(',''
		) as scenario_key,
	   scenario,
       region,
	   mnemonic,
	   Name as longname,
	   case
		  when [year] is not null Then '01/01/'+ltrim(rtrim(str([year])))
	   end as date,
	   val,
	   unitname as unit,
	   convert(date, modifieddate,101) as modifieddate
from vwAA_DemandSeriesv2
where modifieddate > convert(date, '{lastrundate}',101)
