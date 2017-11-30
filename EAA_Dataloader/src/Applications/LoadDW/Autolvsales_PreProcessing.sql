-- Pre process script for Auto LV Sales... 

SELECT country, cartruck, 
    SUM(cy2006) AS cy2006,
	SUM(cy2007) AS cy2007,
	SUM(cy2008) AS cy2008,
	SUM(cy2009) AS cy2009,
	SUM(cy2010) AS cy2010,
	SUM(cy2011) AS cy2011,
	SUM(cy2012) AS cy2012,
	SUM(cy2013) AS cy2013,
	SUM(cy2014) AS cy2014,
	SUM(cy2015) AS cy2015,
	SUM(cy2016) AS cy2016,
	SUM(cy2017) AS cy2017,
	SUM(cy2018) AS cy2018,
	SUM(cy2019) AS cy2019,
	SUM(cy2020) AS cy2020,
	SUM(cy2021) AS cy2021,
	SUM(cy2022) AS cy2022,
	SUM(cy2023) AS cy2023
INTO {schemaName}.auto_lv_sales_grouped
FROM {sourceSchema}.auto_lv_sales
GROUP BY country, cartruck;

COMMIT;