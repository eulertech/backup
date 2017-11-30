DROP TABLE IF EXISTS {schemaName}.{tableName}contracts CASCADE;
CREATE TABLE {schemaName}.{tableName}contracts
(
   contract_unique_id                     VARCHAR(100) ENCODE LZO,
   seller_company_name                    VARCHAR(100) ENCODE LZO,
   seller_history_name                    VARCHAR(100) ENCODE LZO,
   customer_company_name                  VARCHAR(500) ENCODE LZO,
   contract_affiliate                     VARCHAR(100) ENCODE LZO,
   ferc_tariff_reference                  VARCHAR(500) ENCODE LZO,
   contract_service_agreement_id          VARCHAR(100) ENCODE LZO,
   contract_execution_date                VARCHAR(100) ENCODE LZO,
   commencement_date_of_contract_term     VARCHAR(100) ENCODE LZO,
   contract_termination_date              VARCHAR(100) ENCODE LZO,
   actual_termination_date                VARCHAR(100) ENCODE LZO,
   extension_provision_description        VARCHAR(1024) ENCODE LZO,
   class_name                             VARCHAR(100) ENCODE LZO,
   term_name                              VARCHAR(100) ENCODE LZO,
   increment_name                         VARCHAR(100) ENCODE LZO,
   increment_peaking_name                 VARCHAR(100) ENCODE LZO,
   product_type_name                      VARCHAR(100) ENCODE LZO,
   product_name                           VARCHAR(100) ENCODE LZO,
   quantity                               VARCHAR(100) ENCODE LZO,
   units                                  VARCHAR(100) ENCODE LZO,
   rate                                   VARCHAR(100) ENCODE LZO,
   rate_minimum                           VARCHAR(100) ENCODE LZO,
   rate_maximum                           VARCHAR(100) ENCODE LZO,
   rate_description                       VARCHAR(1024) ENCODE LZO,
   rate_units                             VARCHAR(100) ENCODE LZO,
   point_of_receipt_balancing_authority   VARCHAR(100) ENCODE LZO,
   point_of_receipt_specific_location     VARCHAR(100) ENCODE LZO,
   point_of_delivery_balancing_authority  VARCHAR(100) ENCODE LZO,
   point_of_delivery_specific_location    VARCHAR(100) ENCODE LZO,
   begin_date                             VARCHAR(100) ENCODE LZO,
   end_date                               VARCHAR(100) ENCODE LZO	
);	


DROP TABLE IF EXISTS {schemaName}.{tableName}ident CASCADE;
CREATE TABLE {schemaName}.{tableName}ident
(
   filer_unique_id                                  VARCHAR(10) ENCODE LZO,
   company_name                                     VARCHAR(100) ENCODE LZO,
   company_identifier                               VARCHAR(20) ENCODE LZO,
   contact_name                                     VARCHAR(40) ENCODE LZO,
   contact_title                                    VARCHAR(60) ENCODE LZO,
   contact_address                                  VARCHAR(90) ENCODE LZO,
   contact_city                                     VARCHAR(30) ENCODE LZO,
   contact_state                                    VARCHAR(10) ENCODE LZO,
   contact_zip                                      VARCHAR(20) ENCODE LZO,
   contact_country_name                             VARCHAR(10) ENCODE LZO,
   contact_phone                                    VARCHAR(30) ENCODE LZO,
   contact_email                                    VARCHAR(50) ENCODE LZO,
   transactions_reported_to_index_price_publishers  VARCHAR(10) ENCODE LZO,
   filing_quarter                                   VARCHAR(20) ENCODE LZO	
);

DROP TABLE IF EXISTS {schemaName}.{tableName}indexPub CASCADE;
CREATE TABLE {schemaName}.{tableName}indexPub
(
   filer_unique_id                                                        VARCHAR(10) ENCODE LZO,
   seller_company_name                                                    VARCHAR(80) ENCODE LZO,
   index_price_publishers_to_which_sales_transactions_have_been_reported  VARCHAR(10) ENCODE LZO,
   transactions_reported                                                  VARCHAR(110) ENCODE LZO
);

DROP TABLE IF EXISTS {schemaName}.{tableName}transactions CASCADE;
CREATE TABLE {schemaName}.{tableName}transactions
(
	transaction_unique_id					VARCHAR(100) ENCODE LZO,
	seller_company_name						VARCHAR(100) ENCODE LZO,
	customer_company_name					VARCHAR(1024) ENCODE LZO,
	ferc_tariff_reference					VARCHAR(100) ENCODE LZO,
	contract_service_agreement				VARCHAR(100) ENCODE LZO,
	transaction_unique_identifier			VARCHAR(100) ENCODE LZO,
	transaction_begin_date					VARCHAR(100) ENCODE LZO,
	transaction_end_date					VARCHAR(100) ENCODE LZO,
	trade_date								VARCHAR(100) ENCODE LZO,
	exchange_brokerage_service				VARCHAR(100) ENCODE LZO,
	type_of_rate							VARCHAR(100) ENCODE LZO,
	time_zone								VARCHAR(100) ENCODE LZO,
	point_of_delivery_balancing_authority	VARCHAR(100) ENCODE LZO,
	point_of_delivery_specific_location		VARCHAR(100) ENCODE LZO,
	class_name								VARCHAR(100) ENCODE LZO,
	term_name								VARCHAR(100) ENCODE LZO,
	increment_name							VARCHAR(100) ENCODE LZO,
	increment_peaking_name					VARCHAR(100) ENCODE LZO,
	product_name							VARCHAR(100) ENCODE LZO,
	transaction_quantity					VARCHAR(100) ENCODE LZO,
	price									VARCHAR(100) ENCODE LZO,
	rate_units								VARCHAR(100) ENCODE LZO,
	standardized_quantity					VARCHAR(100) ENCODE LZO,
	standardized_price						VARCHAR(100) ENCODE LZO,
	total_transmission_charge				VARCHAR(100) ENCODE LZO,
	total_transaction_charge				VARCHAR(100) ENCODE LZO
);

