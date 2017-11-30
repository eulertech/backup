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
FROM {schemaName}.{tableName}transactions
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
