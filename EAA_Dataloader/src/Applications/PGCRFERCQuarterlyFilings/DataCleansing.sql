--cleaning trade_date column
--eg 1. 20130704
--eg 2. 19970408.0
UPDATE pgcr_dev.temp_transactions 
SET trade_date_cleaned=TO_DATE(TRIM(REPLACE(trade_date, '.0','')), 'YYYYMMDD')
WHERE len(trim(trade_date))=8 OR len(trim(trade_date))=10;

--eg 1. 141218
--eg 2. 161019
UPDATE pgcr_dev.temp_transactions 
SET trade_date_cleaned=TO_DATE(TRIM(trade_date), 'YYMMDD')
WHERE len(trim(trade_date))=6;

UPDATE pgcr_dev.temp_transactions SET transaction_begin_date_cleaned=TO_TIMESTAMP(transaction_begin_date, 'YYYYMMDDHH24MI');
UPDATE pgcr_dev.temp_transactions SET transaction_end_date_cleaned=TO_TIMESTAMP(transaction_end_date, 'YYYYMMDDHH24MI');

UPDATE pgcr_dev.temp_transactions SET transaction_quantity_cleaned=CAST(transaction_quantity AS FLOAT);
UPDATE pgcr_dev.temp_transactions SET price_cleaned=CAST(price AS FLOAT);
UPDATE pgcr_dev.temp_transactions SET standardized_quantity_cleaned=CAST(standardized_quantity AS FLOAT);
UPDATE pgcr_dev.temp_transactions SET standardized_price_cleaned=CAST(standardized_price AS FLOAT);
UPDATE pgcr_dev.temp_transactions SET total_transmission_charge_cleaned=CAST(total_transmission_charge AS FLOAT);
UPDATE pgcr_dev.temp_transactions SET total_transaction_charge_cleaned=CAST(total_transaction_charge AS FLOAT);


TRUNCATE TABLE pgcr_dev.final_transactions;
INSERT INTO pgcr_dev.final_transactions(
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
    total_transaction_charge)
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
FROM pgcr_dev.temp_transactions;

    



