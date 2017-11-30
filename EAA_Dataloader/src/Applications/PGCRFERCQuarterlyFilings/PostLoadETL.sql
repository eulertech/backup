DROP TABLE IF EXISTS {schemaName}.{tableName}temp_transactions;
CREATE TABLE {schemaName}.{tableName}temp_transactions
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
	total_transaction_charge				VARCHAR(100) ENCODE LZO,
    trade_date_cleaned                      DATE,
    transaction_begin_date_cleaned          TIMESTAMP,
    transaction_end_date_cleaned            TIMESTAMP,
    transaction_quantity_cleaned            FLOAT,
    price_cleaned                           FLOAT,
    standardized_quantity_cleaned           FLOAT,
    standardized_price_cleaned              FLOAT,
    total_transmission_charge_cleaned       FLOAT,
    total_transaction_charge_cleaned        FLOAT    
);


INSERT INTO {schemaName}.{tableName}temp_transactions
(
    transaction_unique_id,
    seller_company_name,
    customer_company_name,
    ferc_tariff_reference,
    contract_service_agreement,
    transaction_unique_identifier,
    transaction_begin_date,
    transaction_end_date,
    trade_date,
    exchange_brokerage_service,
    type_of_rate,
    time_zone,
    point_of_delivery_balancing_authority,
    point_of_delivery_specific_location,
    class_name,
    term_name,
    increment_name,
    increment_peaking_name,
    product_name,
    transaction_quantity,
    price,
    rate_units,
    standardized_quantity,
    standardized_price,
    total_transmission_charge,
    total_transaction_charge
)
SELECT
    transaction_unique_id,
    seller_company_name,
    customer_company_name,
    ferc_tariff_reference,
    contract_service_agreement,
    transaction_unique_identifier,
    transaction_begin_date,
    transaction_end_date,
    trade_date,
    exchange_brokerage_service,
    type_of_rate,
    time_zone,
    point_of_delivery_balancing_authority,
    point_of_delivery_specific_location,
    class_name,
    term_name,
    increment_name,
    increment_peaking_name,
    product_name,
    transaction_quantity,
    price,
    rate_units,
    standardized_quantity,
    standardized_price,
    total_transmission_charge,
    total_transaction_charge
FROM {schemaName}.{tableName}transactions;



--cleaning trade_date column
--eg 1. 20130704
--eg 2. 19970408.0
UPDATE {schemaName}.{tableName}temp_transactions 
SET trade_date_cleaned=TO_DATE(TRIM(REPLACE(trade_date, '.0','')), 'YYYYMMDD')
WHERE len(trim(trade_date))=8 OR len(trim(trade_date))=10;

--eg 1. 141218
--eg 2. 161019
UPDATE {schemaName}.{tableName}temp_transactions 
SET trade_date_cleaned=TO_DATE(TRIM(trade_date), 'YYMMDD')
WHERE len(trim(trade_date))=6;

UPDATE {schemaName}.{tableName}temp_transactions SET transaction_begin_date_cleaned=TO_TIMESTAMP(transaction_begin_date, 'YYYYMMDDHH24MI');
UPDATE {schemaName}.{tableName}temp_transactions SET transaction_end_date_cleaned=TO_TIMESTAMP(transaction_end_date, 'YYYYMMDDHH24MI');
UPDATE {schemaName}.{tableName}temp_transactions SET transaction_quantity_cleaned=CAST(transaction_quantity AS FLOAT);
UPDATE {schemaName}.{tableName}temp_transactions SET price_cleaned=CAST(price AS FLOAT);
UPDATE {schemaName}.{tableName}temp_transactions SET standardized_quantity_cleaned=CAST(standardized_quantity AS FLOAT);
UPDATE {schemaName}.{tableName}temp_transactions SET standardized_price_cleaned=CAST(standardized_price AS FLOAT);
UPDATE {schemaName}.{tableName}temp_transactions SET total_transmission_charge_cleaned=CAST(total_transmission_charge AS FLOAT);
UPDATE {schemaName}.{tableName}temp_transactions SET total_transaction_charge_cleaned=CAST(total_transaction_charge AS FLOAT);

 

DROP TABLE IF EXISTS {schemaName}.{tableName}final_transactions;
CREATE TABLE {schemaName}.{tableName}final_transactions
(
	transaction_unique_id					VARCHAR(100) ENCODE LZO,
	seller_company_name						VARCHAR(100) ENCODE LZO,
	customer_company_name					VARCHAR(1024) ENCODE LZO,
	ferc_tariff_reference					VARCHAR(100) ENCODE LZO,
	contract_service_agreement				VARCHAR(100) ENCODE LZO,
	transaction_unique_identifier			VARCHAR(100) ENCODE LZO,
	transaction_begin_date					TIMESTAMP,
	transaction_end_date					TIMESTAMP,
	trade_date								DATE,
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
	transaction_quantity					FLOAT,
	price									FLOAT,
	rate_units								VARCHAR(100) ENCODE LZO,
	standardized_quantity					FLOAT,
	standardized_price						FLOAT,
	total_transmission_charge				FLOAT,
	total_transaction_charge				FLOAT
);


INSERT INTO {schemaName}.{tableName}final_transactions
(
    transaction_unique_id, 
    seller_company_name, 
    customer_company_name, 
    ferc_tariff_reference, 
    contract_service_agreement, 
    transaction_unique_identifier, 
    transaction_begin_date, 
    transaction_end_date, 
    trade_date, 
    exchange_brokerage_service, 
    type_of_rate, 
    time_zone, 
    point_of_delivery_balancing_authority, 
    point_of_delivery_specific_location, 
    class_name, 
    term_name, 
    increment_name, 
    increment_peaking_name, 
    product_name, 
    transaction_quantity, 
    price, 
    rate_units, 
    standardized_quantity, 
    standardized_price, 
    total_transmission_charge, 
    total_transaction_charge
)
SELECT
    transaction_unique_id, 
    seller_company_name, 
    customer_company_name, 
    ferc_tariff_reference, 
    contract_service_agreement, 
    transaction_unique_identifier, 
    transaction_begin_date_cleaned,
    transaction_end_date_cleaned, 
    trade_date_cleaned, 
    exchange_brokerage_service, 
    type_of_rate, 
    time_zone, 
    point_of_delivery_balancing_authority, 
    point_of_delivery_specific_location, 
    class_name, 
    term_name, 
    increment_name, 
    increment_peaking_name, 
    product_name, 
    transaction_quantity_cleaned, 
    price_cleaned, 
    rate_units, 
    standardized_quantity_cleaned, 
    standardized_price_cleaned, 
    total_transmission_charge_cleaned, 
    total_transaction_charge_cleaned
FROM {schemaName}.{tableName}temp_transactions;

DROP TABLE IF EXISTS {schemaName}.{tableName}temp_transactions;


DROP TABLE IF EXISTS {schemaName}.{tableName}l2transactions_with_pod;
CREATE TABLE {schemaName}.{tableName}l2transactions_with_pod
(
    seller_company_name                             VARCHAR(100) ENCODE LZO,
    customer_company_name                           VARCHAR(170) ENCODE LZO,
    product_name                                    VARCHAR(40)  ENCODE LZO,
    quarteryear                                     VARCHAR(500) ENCODE LZO,
    quarter                                         INTEGER,
    year                                            INTEGER,
    QuarterDate                                     DATE,
    rate_units                                      VARCHAR(20) ENCODE LZO,
    type_of_rate                                    VARCHAR(20) ENCODE LZO,
    class_name                                      VARCHAR(10) ENCODE LZO,
    point_of_delivery_balancing_authority           VARCHAR(10) ENCODE LZO,
    point_of_delivery_specific_location             VARCHAR(60) ENCODE LZO,
    total_transmission_charge                       FLOAT,
    total_transaction_charge                        FLOAT,
    standardized_quantity                           FLOAT,
    transaction_quantity                            FLOAT,
    standardized_price                              FLOAT
);

INSERT INTO {schemaName}.{tableName}l2transactions_with_pod(
    seller_company_name,
    customer_company_name,
    product_name,
    quarteryear,
    quarter,
    year,
    QuarterDate,
    rate_units,
    type_of_rate,
    class_name,
    point_of_delivery_balancing_authority,
    point_of_delivery_specific_location,
    total_transmission_charge,
    total_transaction_charge,
    standardized_quantity,
    transaction_quantity,
    standardized_price)
SELECT
    seller_company_name,
    customer_company_name,
    product_name,
    CASE 
        WHEN datepart('month',transaction_begin_date) in (1,2,3) THEN 'Q1'
        WHEN datepart('month',transaction_begin_date) in (4,5,6) THEN 'Q2'
        WHEN datepart('month',transaction_begin_date) in (7,8,9) THEN 'Q3'
        WHEN datepart('month',transaction_begin_date) in (10,11,12) THEN 'Q4'
    END || ' '  || CAST(datepart('year',transaction_begin_date) AS CHAR(4)) AS QuarterYear,
    datepart('quarter',transaction_begin_date) AS Quarter,
    datepart('year',transaction_begin_date) AS Year,
    CAST(CASE 
        WHEN datepart('month',transaction_begin_date) in (1,2,3) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-01-01'
        WHEN datepart('month',transaction_begin_date) in (4,5,6) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-04-01'
        WHEN datepart('month',transaction_begin_date) in (7,8,9) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-07-01'
        WHEN datepart('month',transaction_begin_date) in (10,11,12) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-10-01'
    END AS DATE) AS QuarterDate,
    rate_units,
    type_of_rate,
    class_name,
    point_of_delivery_balancing_authority,
    point_of_delivery_specific_location,
    SUM(total_transmission_charge) AS total_transmission_charge,
    SUM(total_transaction_charge) AS total_transaction_charge,
    SUM(standardized_quantity) AS standardized_quantity,
    SUM(transaction_quantity) AS transaction_quantity,
    AVG(standardized_price) AS standardized_price
FROM {schemaName}.{tableName}final_transactions
GROUP BY seller_company_name,
    customer_company_name,
    product_name,
    CASE 
        WHEN datepart('month',transaction_begin_date) in (1,2,3) THEN 'Q1'
        WHEN datepart('month',transaction_begin_date) in (4,5,6) THEN 'Q2'
        WHEN datepart('month',transaction_begin_date) in (7,8,9) THEN 'Q3'
        WHEN datepart('month',transaction_begin_date) in (10,11,12) THEN 'Q4'
    END || ' '  || CAST(datepart('year',transaction_begin_date) AS CHAR(4)),
    datepart('quarter',transaction_begin_date),
    datepart('year',transaction_begin_date),
    CAST(CASE 
        WHEN datepart('month',transaction_begin_date) in (1,2,3) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-01-01'
        WHEN datepart('month',transaction_begin_date) in (4,5,6) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-04-01'
        WHEN datepart('month',transaction_begin_date) in (7,8,9) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-07-01'
        WHEN datepart('month',transaction_begin_date) in (10,11,12) THEN CAST(datepart('year',transaction_begin_date) AS CHAR(4)) || '-10-01'
    END AS DATE),
    rate_units,
    type_of_rate,
    class_name,
    point_of_delivery_balancing_authority,
    point_of_delivery_specific_location;
    
--Drop these tables since the final table is {schemaName}.{tableName}l2transactions_with_pod
DROP TABLE IF EXISTS {schemaName}.{tableName}transactions;
DROP TABLE IF EXISTS {schemaName}.{tableName}final_transactions;

