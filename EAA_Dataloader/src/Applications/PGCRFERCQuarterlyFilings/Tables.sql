DROP TABLE IF EXISTS pgcr_dev.final_contracts;
CREATE TABLE pgcr_dev.final_contracts
(
    contract_unique_id	                    VARCHAR(20) ENCODE LZO,
    seller_company_name	                    VARCHAR(100) ENCODE LZO,
    seller_history_name	                    VARCHAR(10) ENCODE LZO,
    customer_company_name	                VARCHAR(100) ENCODE LZO,
    contract_affiliate	                    VARCHAR(10) ENCODE LZO,
    ferc_tariff_reference	                VARCHAR(70) ENCODE LZO,
    contract_service_agreement_id	        VARCHAR(40) ENCODE LZO,
    contract_execution_date	                VARCHAR(20) ENCODE LZO,
    commencement_date_of_contract_term	    VARCHAR(20) ENCODE LZO,
    contract_termination_date	            VARCHAR(20) ENCODE LZO,
    actual_termination_date	                VARCHAR(20) ENCODE LZO,
    extension_provision_description	        VARCHAR(820) ENCODE LZO,
    class_name	                            VARCHAR(70) ENCODE LZO,
    term_name	                            VARCHAR(10) ENCODE LZO,
    increment_name	                        VARCHAR(10) ENCODE LZO,
    increment_peaking_name	                VARCHAR(10) ENCODE LZO,
    product_type_name	                    VARCHAR(20) ENCODE LZO,
    product_name	                        VARCHAR(50) ENCODE LZO,
    quantity	                            VARCHAR(40) ENCODE LZO,
    units	                                VARCHAR(20) ENCODE LZO,
    rate	                                VARCHAR(20) ENCODE LZO,
    rate_minimum	                        VARCHAR(20) ENCODE LZO,
    rate_maximum	                        VARCHAR(20) ENCODE LZO,
    rate_description	                    VARCHAR(310) ENCODE LZO,
    rate_units	                            VARCHAR(20) ENCODE LZO,
    point_of_receipt_balancing_authority	VARCHAR(10) ENCODE LZO,
    point_of_receipt_specific_location	    VARCHAR(60) ENCODE LZO,
    point_of_delivery_balancing_authority	VARCHAR(10) ENCODE LZO,
    point_of_delivery_specific_location	    VARCHAR(60) ENCODE LZO,
    begin_date	                            VARCHAR(20) ENCODE LZO,
    end_date	                            VARCHAR(20) ENCODE LZO    
);


DROP TABLE IF EXISTS pgcr_dev.final_transactions;
CREATE TABLE pgcr_dev.final_transactions
(
    transaction_unique_id	                VARCHAR(20) ENCODE LZO,
    seller_company_name	                    VARCHAR(100) ENCODE LZO,
    customer_company_name	                VARCHAR(170) ENCODE LZO,
    ferc_tariff_reference	                VARCHAR(70) ENCODE LZO,
    contract_service_agreement	            VARCHAR(40) ENCODE LZO,
    transaction_unique_identifier	        VARCHAR(30) ENCODE LZO,
    transaction_begin_date	                TIMESTAMP,
    transaction_end_date	                TIMESTAMP,
    trade_date	                            DATE,
    exchange_brokerage_service	            VARCHAR(20) ENCODE LZO,
    type_of_rate	                        VARCHAR(20) ENCODE LZO,
    time_zone	                            VARCHAR(10) ENCODE LZO,
    point_of_delivery_balancing_authority	VARCHAR(10) ENCODE LZO,
    point_of_delivery_specific_location	    VARCHAR(60) ENCODE LZO,
    class_name	                            VARCHAR(10) ENCODE LZO,
    term_name	                            VARCHAR(10) ENCODE LZO,
    increment_name	                        VARCHAR(10) ENCODE LZO,
    increment_peaking_name	                VARCHAR(10) ENCODE LZO,
    product_name	                        VARCHAR(40) ENCODE LZO,
    transaction_quantity	                float,
    price	                                float,
    rate_units	                            VARCHAR(20) ENCODE LZO,
    standardized_quantity	                float,
    standardized_price	                    float,
    total_transmission_charge	            float,
    total_transaction_charge	            float
);



DROP TABLE IF EXISTS pgcr_dev.final_ident;
CREATE TABLE pgcr_dev.final_ident
(
    filer_unique_id	                                VARCHAR(10) ENCODE LZO,
    company_name	                                VARCHAR(100) ENCODE LZO,
    company_identifier	                            VARCHAR(20) ENCODE LZO,
    contact_name	                                VARCHAR(40) ENCODE LZO,
    contact_title	                                VARCHAR(60) ENCODE LZO,
    contact_address	                                VARCHAR(90) ENCODE LZO,
    contact_city	                                VARCHAR(30) ENCODE LZO,
    contact_state	                                VARCHAR(10) ENCODE LZO,
    contact_zip	                                    VARCHAR(20) ENCODE LZO,
    contact_country_name	                        VARCHAR(10) ENCODE LZO,
    contact_phone	                                VARCHAR(30) ENCODE LZO,
    contact_email	                                VARCHAR(50) ENCODE LZO,
    transactions_reported_to_index_price_publishers	VARCHAR(10) ENCODE LZO,
    filing_quarter	                                VARCHAR(20) ENCODE LZO   
);


DROP TABLE IF EXISTS pgcr_dev.final_indexPub;
CREATE TABLE pgcr_dev.final_indexPub
(
    filer_unique_id	                                                        VARCHAR(10) ENCODE LZO,
    seller_company_name	                                                    VARCHAR(80) ENCODE LZO,
    index_price_publishers_to_which_sales_transactions_have_been_reported	VARCHAR(10) ENCODE LZO,
    transactions_reported	                                                VARCHAR(110) ENCODE LZO   
);
