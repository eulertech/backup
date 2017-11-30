-- 
-- this is the DDL for the new series_attribute table  Data
--

insert into eaa_dev.series_attributes
	(attr_id,name,category)
values 
	('1000', 'Rivalry','TOP'),
	('2000', 'Epoch','TOP'),
	('3000', 'Peak','TOP'),
	('1000','bluescenario','color'),
	('2000','orangescenario','color'),
	('3000','greenscenario','color'),
	('1000', '35','Percent'),
	('2000', '55','Percent'),
	('3000', '10','Percent'),
	('1000','Most intense competition in history amoung enery sources for market share amid evolutionary social and technology change','Description'),
	('2000','Transition to an energy mix away from fossil fuels at a much faster pace than expected','Description'),
	('3000','World economy and energy markets like weather on a mountaintop - pleasant one moment, then brutal storms the next','Description'),
	('1000', 'Shale Development','MENU'),	
	('1000', 'Energy Fundamentals','MENU'),	
	('1000', 'Technology','MENU'),	
	('1000', 'Demand Drivers','MENU'),	
	('1000', 'Supply Drivers','MENU'),	
	('1000', 'Oil','MENU'),
	('2000', 'Energy Fundamentals','MENU'),	
	('2000', 'Technology','MENU'),	
	('2000', 'Demand Drivers','MENU'),	
	('2000', 'Technology','MENU'),	
	('2000', 'Demand Drivers','MENU'),	
	('2000', 'Supply Drivers','MENU'),
	('2000', 'Oil','MENU'),
	('3000', 'Shale Development','MENU'),
	('3000', 'Energy Fundamentals','MENU'),	
	('3000', 'Technology','MENU'),	
	('3000', 'Demand Drivers','MENU'),	
	('3000', 'Supply Drivers','MENU'),	
	('3000', 'Oil','MENU'),
	('1000', 'EAA Drivers','TITAN_DRIVERS'),
	('1000', 'EAA Scenario(Taxonomy)','TITAN_SCENARIOS'),
	('2000', 'EAA Drivers','TITAN_DRIVERS'),
	('2000', 'EAA Scenario(Taxonomy)','TITAN_SCENARIOS'),
	('3000', 'EAA Drivers','TITAN_DRIVERS'),
	('3000', 'EAA Scenario(Taxonomy)','TITAN_SCENARIOS');

commit;

         