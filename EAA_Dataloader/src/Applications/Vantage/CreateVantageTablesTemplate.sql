-- Creates all Vantage tables in Redshift...


-- Conventional and Unconventional Summary Data (Asset Level)
DROP TABLE IF EXISTS {schemaName}.{tableName}asset_summary CASCADE;
CREATE TABLE {schemaName}.{tableName}asset_summary (
	asset_name												VARCHAR(50) ENCODE LZO, 
	basin_name												VARCHAR(50) ENCODE LZO, 
	price_scenario_description								VARCHAR(50) ENCODE LZO, 
	inflation_value											FLOAT, 
	country													VARCHAR(50) ENCODE LZO, 
	asset_is_unconventional									INT,
	decomm_total											FLOAT,
	capital_dev_total										FLOAT,
	capital_total											FLOAT,
	revenue_total											FLOAT,
	taxes_income											FLOAT,
	co2_emissions											FLOAT,
	costs_total												FLOAT,
	co2_content												FLOAT,
	oil_recoverable_reserves								FLOAT,
	break_even_gas_10_perc_disc_rate						FLOAT,
	irr														FLOAT,
	decomm_cost_per_gas_equivalent							FLOAT,
	decomm_cost_per_oil_equivalent							FLOAT,
	total_cost_per_gas_equivalent							FLOAT,
	total_cost_per_oil_equivalent							FLOAT,
	capital_drilling_dev_per_oil_equivalent					FLOAT,
	capital_drilling_dev_per_gas_equivalent					FLOAT,
	capital_e_and_a_per_oil_equivalent						FLOAT,
	capital_e_and_a_per_gas_equivalent						FLOAT,
	opcost_per_gas_equivalent								FLOAT,
	opcost_per_oil_equivalent								FLOAT,
	number_of_projects										INT,
	dev_cost_per_gas_equivalent								FLOAT,
	dev_cost_per_oil_equivalent								FLOAT
);

-- Conventional and Unconventional Annual Data (Asset Level)
DROP TABLE IF EXISTS {schemaName}.{tableName}asset_annual CASCADE;
CREATE TABLE {schemaName}.{tableName}asset_annual (
	asset_name										VARCHAR(50) ENCODE LZO,
	basin_name										VARCHAR(50) ENCODE LZO,
	price_scenario_description						VARCHAR(50) ENCODE LZO,
	inflation_value									FLOAT,
	consolidation_type								VARCHAR(25) ENCODE LZO,
	country											VARCHAR(50) ENCODE LZO,
	year_id											INT,
	asset_is_unconventional							INT,
	percentage_remaining_reserves					FLOAT,
	decomm_total									FLOAT,
	capital_total									FLOAT,
	capital_dev_total								FLOAT,
	cumulative_production_gas_equivalent			FLOAT,
	cumulative_production_oil_equivalent			FLOAT,
	drilling_cost_total								FLOAT,
	price_gas										FLOAT,
	production_gas_rate								FLOAT,
	production_oil_rate								FLOAT,
	gross_gas_revenue								FLOAT,
	gross_liquids_revenue							FLOAT,
	gross_oil_revenue								FLOAT,
	revenue_total									FLOAT,
	taxes_income									FLOAT,
	oil_price										FLOAT,
	opcost_total									FLOAT,
	opcost_total_including_bonuses_fees				FLOAT,
	opcost_variable									FLOAT,
	co2_emissions									FLOAT,
	production_oil_volume							FLOAT,
	production_gas_volume							FLOAT,
	profit_gas										FLOAT,
	profit_oil										FLOAT,
	costs_total										FLOAT,
	cost_recovery									FLOAT
);


-- Conventional and Unconventional Summary Data (Project Level)
DROP TABLE IF EXISTS {schemaName}.{tableName}project_summary CASCADE;
CREATE TABLE {schemaName}.{tableName}project_summary (
	asset_name										VARCHAR(50) ENCODE LZO, 
	basin_name										VARCHAR(50) ENCODE LZO, 
	project_name									VARCHAR(50) ENCODE LZO,
	is_sanctioned									INT,
	price_scenario_description						VARCHAR(50) ENCODE LZO, 
	inflation_value									FLOAT, 
	country											VARCHAR(50) ENCODE LZO, 
	asset_is_unconventional							INT,
	decomm_total									FLOAT,
	capital_dev_total								FLOAT,
	capital_total									FLOAT,
	revenue_total									FLOAT,
	taxes_income									FLOAT,
	co2_emissions									FLOAT,
	costs_total										FLOAT,
	co2_content										FLOAT,
	oil_recoverable_reserves						FLOAT,
	break_even_gas_10_perc_disc_rate				FLOAT,
	irr												FLOAT,
	decomm_cost_per_gas_equivalent					FLOAT,
	decomm_cost_per_oil_equivalent					FLOAT,
	total_cost_per_gas_equivalent					FLOAT,
	total_cost_per_oil_equivalent					FLOAT,
	capital_drilling_dev_per_oil_equivalent			FLOAT,
	capital_drilling_dev_per_gas_equivalent			FLOAT,
	capital_e_and_a_per_oil_equivalent				FLOAT,
	capital_e_and_a_per_gas_equivalent				FLOAT,
	opcost_per_gas_equivalent						FLOAT,
	opcost_per_oil_equivalent						FLOAT,
	cost_per_well_completion						FLOAT,
	cost_per_well_driling							FLOAT,
	cost_per_well_facilities						FLOAT,
	number_of_projects								INT,
	dev_cost_per_gas_equivalent						FLOAT,
	dev_cost_per_oil_equivalent						FLOAT
);


-- Conventional and Unconventional Annual Data (Project Level)
DROP TABLE IF EXISTS {schemaName}.{tableName}project_annual CASCADE;
CREATE TABLE {schemaName}.{tableName}project_annual (
	asset_name										VARCHAR(50) ENCODE LZO,
	basin_name										VARCHAR(50) ENCODE LZO,
	project_name									VARCHAR(50) ENCODE LZO,
	price_scenario_description						VARCHAR(50) ENCODE LZO,
	inflation_value									FLOAT,
	consolidation_type								VARCHAR(25) ENCODE LZO,
	country											VARCHAR(50) ENCODE LZO,
	year_id											INT,
	asset_is_unconventional							INT,
	percentage_remaining_reserves					FLOAT,
	decomm_total									FLOAT,
	capital_total									FLOAT,
	capital_dev_total								FLOAT,
	cumulative_production_gas_equivalent			FLOAT,
	cumulative_production_oil_equivalent			FLOAT,
	drilling_cost_total								FLOAT,
	price_gas										FLOAT,
	production_gas_rate								FLOAT,
	production_oil_rate								FLOAT,
	gross_gas_revenue								FLOAT,
	gross_liquids_revenue							FLOAT,
	gross_oil_revenue								FLOAT,
	revenue_total									FLOAT,
	taxes_income									FLOAT,
	oil_price										FLOAT,
	opcost_total									FLOAT,
	opcost_total_including_bonuses_fees				FLOAT,
	opcost_variable									FLOAT,
	co2_emissions									FLOAT,
	production_oil_volume							FLOAT,
	production_gas_volume							FLOAT,
	profit_gas										FLOAT,
	profit_oil										FLOAT,
	costs_total										FLOAT,
	cost_recovery									FLOAT
);


-- Unconventional Monthly Data (Project Level)
DROP TABLE IF EXISTS {schemaName}.{tableName}project_monthly CASCADE;
CREATE TABLE {schemaName}.{tableName}project_monthly (
	asset_name					VARCHAR(50) ENCODE LZO,
	project_name				VARCHAR(50) ENCODE LZO,
	date_date					DATE,
	price_scenario_description	VARCHAR(50) ENCODE LZO,
	company_name				VARCHAR(100) ENCODE LZO,
	rig_count					INT,
	well_count					INT
);


-- Type Well Summary Data
DROP TABLE IF EXISTS {schemaName}.{tableName}well_summary CASCADE;
CREATE TABLE {schemaName}.{tableName}well_summary (
	well_type_id										INT,
	asset_name											VARCHAR(50) ENCODE LZO,
	basin_name											VARCHAR(50) ENCODE LZO,
	project_name										VARCHAR(50) ENCODE LZO,
	country												VARCHAR(50) ENCODE LZO,
	inflation_value										FLOAT,
	price_scenario_description							VARCHAR(50) ENCODE LZO,
	decomm_total										FLOAT,
	capital_total										FLOAT,
	drilling_costs_per_oil_equivalent					INT,
	drilling_costs_per_gas_equivalent					INT,
	drilling_costs_total								INT,
	f_and_d_per_gas_equivalent							FLOAT,
	f_and_d_per_oil_equivalent							FLOAT,
	price_gas											FLOAT,
	gross_gas_revenue									FLOAT,
	gross_liquids_revenue								FLOAT,
	gross_oil_revenue									FLOAT,
	revenue_total										FLOAT,
	taxes_income										FLOAT,
	irr													FLOAT,
	price_oil											FLOAT,
	operational_cost_total								FLOAT,
	opcost_total_including_bonuses_fees					FLOAT,
	opcost_variable										FLOAT,
	profit_gas											FLOAT,
	profit_oil											FLOAT,
	costs_total											FLOAT,
	total_cost_per_gas_equivalent						FLOAT,
	total_cost_per_oil_equivalent						FLOAT,
	cost_recovery										FLOAT,
	opcost_per_oil_equivalent							FLOAT,
	opcost_per_gas_equivalent							FLOAT,
	co2_emissions										FLOAT,
	production_oil_volume								FLOAT,
	number_of_projects									INT,
	gas_break_even_price								FLOAT,
	break_even_price									FLOAT,
	production_ngl_volume								FLOAT,
	opcost_trans_oil									FLOAT
);


-- Type Well Annual Data
DROP TABLE IF EXISTS {schemaName}.{tableName}well_annual CASCADE;
CREATE TABLE {schemaName}.{tableName}well_annual (
	well_id											INT, 
	asset_name										VARCHAR(50) ENCODE LZO,
	basin_name										VARCHAR(50) ENCODE LZO,
	project_name									VARCHAR(50) ENCODE LZO,
	country											VARCHAR(50) ENCODE LZO,
	price_scenario_description						VARCHAR(50) ENCODE LZO,
	year											INT,
	decomm_total									FLOAT, 
	capital_total									FLOAT, 
	drilling_costs_total							FLOAT, 
	price_gas										FLOAT, 
	revenue_gas										FLOAT, 
	revenue_liquid									FLOAT, 
	revenue_oil										FLOAT, 
	revenue_total									FLOAT, 
	taxes_income									FLOAT,
	price_oil										FLOAT, 
	opcost_total									FLOAT, 
	opcost_total_including_bonuses_fees				FLOAT, 
	opcost_variable									FLOAT, 
	co2_emissions									FLOAT,
	remaining_reserves								FLOAT,
	production_oil_volume							FLOAT,
	production_gas_rate								FLOAT,
	production_oil_rate								FLOAT,
	profit_gas										FLOAT,
	profit_oil										FLOAT, 
	costs_total										FLOAT, 
	cost_recovery									FLOAT,
	production_ngl_volume							FLOAT
);


-- Type Well Monthly Data
DROP TABLE IF EXISTS {schemaName}.{tableName}well_monthly CASCADE;
CREATE TABLE {schemaName}.{tableName}well_monthly (
	asset_name						VARCHAR(50) ENCODE LZO,
	project_name					VARCHAR(50) ENCODE LZO,
	month							INT,
	quarter							VARCHAR(13) ENCODE LZO,
	year							VARCHAR(8) ENCODE LZO,
	production_gas_rate				FLOAT,
	production_oil_rate				FLOAT
);