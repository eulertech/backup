--SQL Script template to unpivot columns from temporal tables

insert into {schemaName}.{tableName}_crude
select * from (
	  select 'Total' as category, region, country, '1980' as year, "1980" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1981' as year, "1981" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1982' as year, "1982" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1983' as year, "1983" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1984' as year, "1984" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1985' as year, "1985" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1986' as year, "1986" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1987' as year, "1987" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1988' as year, "1988" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1989' as year, "1989" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1990' as year, "1990" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1991' as year, "1991" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1992' as year, "1992" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1993' as year, "1993" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1994' as year, "1994" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1995' as year, "1995" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1996' as year, "1996" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1997' as year, "1997" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1998' as year, "1998" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '1999' as year, "1999" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2000' as year, "2000" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2001' as year, "2001" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2002' as year, "2002" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2003' as year, "2003" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2004' as year, "2004" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'Total' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_crude_temp union all 
	  select 'ASW 2016' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'ASW 2016' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_asw2016_temp union all 
	  select 'Deep Water' as category, region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Deep Water' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_deepwater_temp union all 
	  select 'Offshore' as category, region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Offshore' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_offshore_temp union all 
	  select 'Onshore' as category, region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Onshore' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_onshore_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Rivalry 2016' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_rivalry2016_temp union all 
	  select 'Shallow Water' as category, region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Shallow Water' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_shallowwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_ultradeepwater_temp union all 
	  select 'Ultra Deep Water' as category, region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_ultradeepwater_temp
)

commit;

insert into {schemaName}.{tableName}_tightoil
select * from (
  select region, country, '1980' as year, "1980" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1981' as year, "1981" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1982' as year, "1982" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1983' as year, "1983" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1984' as year, "1984" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1985' as year, "1985" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1986' as year, "1986" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1987' as year, "1987" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1988' as year, "1988" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1989' as year, "1989" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1990' as year, "1990" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1991' as year, "1991" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1992' as year, "1992" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1993' as year, "1993" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1994' as year, "1994" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1995' as year, "1995" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1996' as year, "1996" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1997' as year, "1997" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1998' as year, "1998" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '1999' as year, "1999" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2000' as year, "2000" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2001' as year, "2001" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2002' as year, "2002" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2003' as year, "2003" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2004' as year, "2004" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2005' as year, "2005" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2006' as year, "2006" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2007' as year, "2007" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2008' as year, "2008" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2009' as year, "2009" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2010' as year, "2010" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2011' as year, "2011" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2012' as year, "2012" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2013' as year, "2013" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2014' as year, "2014" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2015' as year, "2015" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2016' as year, "2016" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2017' as year, "2017" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2018' as year, "2018" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2019' as year, "2019" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2020' as year, "2020" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2021' as year, "2021" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2022' as year, "2022" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2023' as year, "2023" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2024' as year, "2024" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2025' as year, "2025" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2026' as year, "2026" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2027' as year, "2027" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2028' as year, "2028" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2029' as year, "2029" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2030' as year, "2030" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2031' as year, "2031" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2032' as year, "2032" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2033' as year, "2033" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2034' as year, "2034" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2035' as year, "2035" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2036' as year, "2036" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2037' as year, "2037" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2038' as year, "2038" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2039' as year, "2039" as value from {schemaName}.{tableName}_tightoil_temp union all 
  select region, country, '2040' as year, "2040" as value from {schemaName}.{tableName}_tightoil_temp
)

commit;

