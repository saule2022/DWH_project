
CREATE SCHEMA IF NOT EXISTS myschema;

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;

CREATE SCHEMA IF NOT EXISTS bl_3nf;
-- Create schema BL_CL (Cleansing Layer)
CREATE SCHEMA IF NOT EXISTS BL_CL;

-- Create table for logging data storing
CREATE TABLE IF NOT EXISTS bl_cl.logging_table(
	user_info VARCHAR,
	inserted_table VARCHAR,
	data_source VARCHAR,
	text_message VARCHAR,
	inserted_date DATE	
);


-- Create procedure to insert appropriate logging information
CREATE OR REPLACE PROCEDURE bl_cl.logging_info(p_user_info VARCHAR, p_table_name VARCHAR, p_data_source VARCHAR, p_text_message VARCHAR, p_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN 
	INSERT INTO bl_cl.logging_table (user_info, inserted_table, data_source, text_message, inserted_date)
	VALUES (p_user_info,  p_table_name, p_data_source, p_text_message,  p_date);
END;
$$;



CREATE FOREIGN TABLE IF NOT EXISTS myschema.shoe_table1(
    PRODUCT_ID INT,
    PRODUCT_SRC_ID INT,
    SHOE_BRAND_ID INT,
    Shoe_Brand VARCHAR(50),
    BRAND_CODE VARCHAR(10),
    CATEGORY_ID INT,
    CATEGORY_SCR_ID VARCHAR(50),
    CATEGORY_NAME VARCHAR(50),
    CATEGORY_CODE VARCHAR(50),
    STORE_ID VARCHAR(50),
    STORE_NAME VARCHAR(100),
    PRODUCT_DESC VARCHAR(100),
    IS_ACTIVE BOOLEAN,
    START_DT DATE,
    END_DT DATE,
    COLOUR_ID INT,
    COLOURS VARCHAR(50),
    MATERIAL_ID INT,
    MATERIAL VARCHAR(500),
    SALE_DATE DATE,
    SIZE_ID INT,
    SIZE VARCHAR(50),
    QUANTITY_SOLD INT,
    PRICES DECIMAL(10,  2),
    AMOUNT DECIMAL(10,  2),
    CUST_ID INT,
    cust_FIRST_NAME VARCHAR(50),
    cust_LAST_NAME VARCHAR(50),
    cust_PHONE VARCHAR(50),
    emp_id INT,
    emp_FIRST_NAME VARCHAR(50),
    emp_LAST_NAME VARCHAR(50),
    emp_PHONE VARCHAR(50),
    emp_country_id VARCHAR(50),
    emp_COUNTRY VARCHAR(50),
    cust_country_id VARCHAR(50),
    cust_COUNTRY VARCHAR(50),
    store_country_id VARCHAR(50),
    store_COUNTRY VARCHAR(50),
    cust_city_id VARCHAR(50),
    cust_city VARCHAR(50),
    store_city_id VARCHAR(50),
    store_city VARCHAR(50),
    emp_city_id VARCHAR(50),
    emp_city VARCHAR(50),
    cust_code VARCHAR(100),
    CUST_ADDRESS_ID VARCHAR(100),
    CUST_ADDRESS VARCHAR(100),
    emp_code VARCHAR(100),
    emp_ADDRESS_id VARCHAR(100),
    emp_ADDRESS VARCHAR(100),
    store_code VARCHAR(100),
    store_ADDRESS_ID VARCHAR(100),
    store_ADDRESS VARCHAR(100),
    REGION_SRC_ID VARCHAR(50),
    cust_region_id VARCHAR(50),
    cust_region VARCHAR(50),
    store_region_id VARCHAR(50),
    store_region VARCHAR(50),
    store_economic_region_id VARCHAR(50),
    store_economic_region VARCHAR(50),
    emp_economic_region_id VARCHAR(50),
    emp_economic_region VARCHAR(50),
    cust_economic_region VARCHAR(50),
    cust_economic_region_id VARCHAR(50),
    emp_region_id VARCHAR(50),
    emp_region VARCHAR(50),
    DISCOUNT_ID VARCHAR(50),
    DISCOUNT_name VARCHAR(255),
    DISCOUNT_PRC INT
) SERVER file_server OPTIONS (
    FORMAT 'csv',
    filename '/library/Postgresql/16/data/src_shoe_sales5.csv',
    HEADER 'true',
    DELIMITER ';'
);


CREATE TABLE IF NOT EXISTS myschema.scr_shoe_table(
    PRODUCT_ID INT,
    PRODUCT_SRC_ID INT,
    SHOE_BRAND_ID INT,
    Shoe_Brand VARCHAR(50),
    BRAND_CODE VARCHAR(10),
    CATEGORY_ID INT,
    CATEGORY_SCR_ID VARCHAR(50),
    CATEGORY_NAME VARCHAR(50),
    CATEGORY_CODE VARCHAR(50),
    STORE_ID VARCHAR(50),
    STORE_NAME VARCHAR(100),
    PRODUCT_DESC VARCHAR(100),
    IS_ACTIVE BOOLEAN,
    START_DT DATE,
    END_DT DATE,
    COLOUR_ID INT,
    COLOURS VARCHAR(50),
    MATERIAL_ID INT,
    MATERIAL VARCHAR(500),
    SALE_DATE DATE,
    SIZE_ID INT,
    SIZE VARCHAR(50),
    QUANTITY_SOLD INT,
    PRICES DECIMAL(10,  2),
    AMOUNT DECIMAL(10,  2),
    CUST_ID INT,
    cust_FIRST_NAME VARCHAR(50),
    cust_LAST_NAME VARCHAR(50),
    cust_PHONE VARCHAR(50),
    emp_id INT,
    emp_FIRST_NAME VARCHAR(50),
    emp_LAST_NAME VARCHAR(50),
    emp_PHONE VARCHAR(50),
    emp_country_id VARCHAR(50),
    emp_COUNTRY VARCHAR(50),
    cust_country_id VARCHAR(50),
    cust_COUNTRY VARCHAR(50),
    store_country_id VARCHAR(50),
    store_COUNTRY VARCHAR(50),
    cust_city_id VARCHAR(50),
    cust_city VARCHAR(50),
    store_city_id VARCHAR(50),
    store_city VARCHAR(50),
    emp_city_id VARCHAR(50),
    emp_city VARCHAR(50),
    cust_code VARCHAR(100),
    CUST_ADDRESS_ID VARCHAR(100),
    CUST_ADDRESS VARCHAR(100),
    emp_code VARCHAR(100),
    emp_ADDRESS_id VARCHAR(100),
    emp_ADDRESS VARCHAR(100),
    store_code VARCHAR(100),
    store_ADDRESS_ID VARCHAR(100),
    store_ADDRESS VARCHAR(100),
    REGION_SRC_ID VARCHAR(50),
    cust_region_id VARCHAR(50),
    cust_region VARCHAR(50),
    store_region_id VARCHAR(50),
    store_region VARCHAR(50),
    store_economic_region_id VARCHAR(50),
    store_economic_region VARCHAR(50),
    emp_economic_region_id VARCHAR(50),
    emp_economic_region VARCHAR(50),
    cust_economic_region VARCHAR(50),
    cust_economic_region_id VARCHAR(50),
    emp_region_id VARCHAR(50),
    emp_region VARCHAR(50),
    DISCOUNT_ID VARCHAR(50),
    DISCOUNT_name VARCHAR(255),
    DISCOUNT_PRC INT
);

--second table

CREATE FOREIGN TABLE IF NOT EXISTS myschema.shoe_table2(
    PRODUCT_ID INT,
    PRODUCT_SRC_ID INT,
    SHOE_BRAND_ID INT,
    Shoe_Brand VARCHAR(50),
    BRAND_CODE VARCHAR(10),
    CATEGORY_ID INT,
    CATEGORY_SCR_ID VARCHAR(50),
    CATEGORY_NAME VARCHAR(50),
    CATEGORY_CODE VARCHAR(50),
    STORE_ID VARCHAR(50),
    STORE_NAME VARCHAR(100),
    PRODUCT_DESC VARCHAR(100),
    IS_ACTIVE BOOLEAN,
    START_DT DATE,
    END_DT DATE,
    COLOUR_ID INT,
    COLOURS VARCHAR(50),
    MATERIAL_ID INT,
    MATERIAL VARCHAR(500),
    SALE_DATE DATE,
    SIZE_ID INT,
    SIZE VARCHAR(50),
    QUANTITY_SOLD INT,
    PRICES DECIMAL(10,  2),
    AMOUNT DECIMAL(10,  2),
    CUST_ID INT,
    cust_FIRST_NAME VARCHAR(50),
    cust_LAST_NAME VARCHAR(50),
    cust_PHONE VARCHAR(50),
    emp_id INT,
    emp_FIRST_NAME VARCHAR(50),
    emp_LAST_NAME VARCHAR(50),
    emp_PHONE VARCHAR(50),
    emp_country_id VARCHAR(50),
    emp_COUNTRY VARCHAR(50),
    cust_country_id VARCHAR(50),
    cust_COUNTRY VARCHAR(50),
    store_country_id VARCHAR(50),
    store_COUNTRY VARCHAR(50),
    cust_city_id VARCHAR(50),
    cust_city VARCHAR(50),
    store_city_id VARCHAR(50),
    store_city VARCHAR(50),
    emp_city_id VARCHAR(50),
    emp_city VARCHAR(50),
    cust_code VARCHAR(100),
    CUST_ADDRESS_ID VARCHAR(100),
    CUST_ADDRESS VARCHAR(100),
    emp_code VARCHAR(100),
    emp_ADDRESS_id VARCHAR(100),
    emp_ADDRESS VARCHAR(100),
    store_code VARCHAR(100),
    store_ADDRESS_ID VARCHAR(100),
    store_ADDRESS VARCHAR(100),
    REGION_SRC_ID VARCHAR(50),
    cust_region_id VARCHAR(50),
    cust_region VARCHAR(50),
    store_region_id VARCHAR(50),
    store_region VARCHAR(50),
    store_economic_region_id VARCHAR(50),
    store_economic_region VARCHAR(50),
    emp_economic_region_id VARCHAR(50),
    emp_economic_region VARCHAR(50),
    cust_economic_region VARCHAR(50),
    cust_economic_region_id VARCHAR(50),
    emp_region_id VARCHAR(50),
    emp_region VARCHAR(50),
    DISCOUNT_ID VARCHAR(50),
    DISCOUNT_name VARCHAR(255),
    DISCOUNT_PRC INT
) SERVER file_server OPTIONS (
    FORMAT 'csv',
    filename '/library/Postgresql/16/data/src_shoe_sales5secpart.csv',
    HEADER 'true',
    DELIMITER ';'
);

--drop  table myschema.scr_shoe_table2 cascade;



CREATE TABLE IF NOT EXISTS myschema.scr_shoe_table2(
    PRODUCT_ID INT,
    PRODUCT_SRC_ID INT,
    SHOE_BRAND_ID INT,
    Shoe_Brand VARCHAR(50),
    BRAND_CODE VARCHAR(10),
    CATEGORY_ID INT,
    CATEGORY_SCR_ID VARCHAR(50),
    CATEGORY_NAME VARCHAR(50),
    CATEGORY_CODE VARCHAR(50),
    STORE_ID VARCHAR(50),
    STORE_NAME VARCHAR(100),
    PRODUCT_DESC VARCHAR(100),
    IS_ACTIVE BOOLEAN,
    START_DT DATE,
    END_DT DATE,
    COLOUR_ID INT,
    COLOURS VARCHAR(50),
    MATERIAL_ID INT,
    MATERIAL VARCHAR(500),
    SALE_DATE DATE,
    SIZE_ID INT,
    SIZE VARCHAR(50),
    QUANTITY_SOLD INT,
    PRICES DECIMAL(10,  2),
    AMOUNT DECIMAL(10,  2),
    CUST_ID INT,
    cust_FIRST_NAME VARCHAR(50),
    cust_LAST_NAME VARCHAR(50),
    cust_PHONE VARCHAR(50),
    emp_id INT,
    emp_FIRST_NAME VARCHAR(50),
    emp_LAST_NAME VARCHAR(50),
    emp_PHONE VARCHAR(50),
    emp_country_id VARCHAR(50),
    emp_COUNTRY VARCHAR(50),
    cust_country_id VARCHAR(50),
    cust_COUNTRY VARCHAR(50),
    store_country_id VARCHAR(50),
    store_COUNTRY VARCHAR(50),
    cust_city_id VARCHAR(50),
    cust_city VARCHAR(50),
    store_city_id VARCHAR(50),
    store_city VARCHAR(50),
    emp_city_id VARCHAR(50),
    emp_city VARCHAR(50),
    cust_code VARCHAR(100),
    CUST_ADDRESS_ID VARCHAR(100),
    CUST_ADDRESS VARCHAR(100),
    emp_code VARCHAR(100),
    emp_ADDRESS_id VARCHAR(100),
    emp_ADDRESS VARCHAR(100),
    store_code VARCHAR(100),
    store_ADDRESS_ID VARCHAR(100),
    store_ADDRESS VARCHAR(100),
    REGION_SRC_ID VARCHAR(50),
    cust_region_id VARCHAR(50),
    cust_region VARCHAR(50),
    store_region_id VARCHAR(50),
    store_region VARCHAR(50),
    store_economic_region_id VARCHAR(50),
    store_economic_region VARCHAR(50),
    emp_economic_region_id VARCHAR(50),
    emp_economic_region VARCHAR(50),
    cust_economic_region VARCHAR(50),
    cust_economic_region_id VARCHAR(50),
    emp_region_id VARCHAR(50),
    emp_region VARCHAR(50),
    DISCOUNT_ID VARCHAR(50),
    DISCOUNT_name VARCHAR(255),
    DISCOUNT_PRC INT
);



CREATE TABLE IF NOT EXISTS bl_3nf.ce_categories (
    category_id BIGINT PRIMARY KEY DEFAULT nextval('category_id_key_value'),
    category_name VARCHAR(255),
    category_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    category_src_unique_id VARCHAR(255) UNIQUE
);

CREATE TABLE if not exists  bl_3nf.ce_shoe_brand (
    shoe_brand_id BIGINT PRIMARY KEY DEFAULT nextval('shoe_brand_id_key_value'),
    shoe_brand VARCHAR(255),
    shoe_brand_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    shoe_brand_src_unique_id VARCHAR(255) UNIQUE
);

CREATE TABLE if not exists  bl_3nf.ce_colours (
    colour_id BIGINT PRIMARY KEY DEFAULT nextval('colour_id_key_value'), 
    colour VARCHAR(255),
    colour_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    colour_src_unique_id VARCHAR(255) UNIQUE
);


CREATE TABLE IF NOT EXISTS bl_3nf.ce_sizes(
    size_id BIGINT PRIMARY KEY DEFAULT nextval('size_id_key_value'), 
    size VARCHAR(50),
    size_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    size_src_unique_id VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS bl_3nf.ce_materials (
    material_id int  primary key,
    material VARCHAR(255),
    material_src_id INTEGER,
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE BOOLEAN,
    material_src_unique_id VARCHAR(255) UNIQUE
   -- UNIQUE (material_src_id, start_dt)
);


CREATE TABLE IF NOT EXISTS bl_3nf.CE_PRODUCTS (
    product_id BIGINT PRIMARY KEY DEFAULT nextval('product_id_key_value'),
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE VARCHAR(40),
    PRODUCT_DESC VARCHAR(255),
    CATEGORY_ID BIGINT,
    category_name VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    PRODUCT_SRC_ID BIGINT,
    insert_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    quantity_sold INT,
    SHOE_BRAND_id BIGINT,
    SHOE_BRAND VARCHAR(255),
    colour_id BIGINT,
    COLOUR VARCHAR(255),
    MATERIAL_id BIGINT,
    MATERIAL VARCHAR(255),
    SIZE_id BIGINT,
    size VARCHAR(255),
    PRICES NUMERIC,
    CONSTRAINT unique_product_start_dt UNIQUE (PRODUCT_SRC_ID, START_DT)
);


CREATE TABLE IF NOT EXISTS bl_3nf.ce_regions(
    region_id BIGINT PRIMARY KEY DEFAULT nextval('region_id_key_value'),
    region_name VARCHAR(255),
    region_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    region_src_unique_id VARCHAR(100) UNIQUE
);

CREATE TABLE if not exists bl_3nf.CE_ECONOMIC_REGIONS (
    ECONOMIC_REGION_ID BIGINT PRIMARY KEY DEFAULT nextval('ECONOMIC_REGION_id_key_value'),
    ECONOMIC_REGION_SRC_ID VARCHAR(255),
    ECONOMIC_REGION VARCHAR(255),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    UPDATE_DT DATE,
    INSERT_DT DATE,
    ECONOMIC_region_src_unique_id VARCHAR(255) UNIQUE
);

CREATE TABLE if not exists bl_3nf.ce_countries (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255),
    region_id INT,
    economic_region_id INT,
    country_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    country_src_unique_id VARCHAR(100) unique
   
);

CREATE TABLE if not exists bl_3nf.ce_cities (
    city_id BIGINT PRIMARY KEY DEFAULT nextval('city_id_key_value'),  
    city_name VARCHAR(50),
    country_id INT,
    city_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    city_src_unique_id VARCHAR(100) UNIQUE
);



CREATE TABLE if not exists bl_3nf.CE_ADDRESSES (
    address_id BIGINT PRIMARY KEY DEFAULT nextval('address_id_key_value'),  
    address VARCHAR(255),
    zipcode VARCHAR(10),
    city_id INT,
    address_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    address_src_unique_id VARCHAR(100) UNIQUE
);


CREATE table if not exists  bl_3nf.ce_customers (
    customer_id BIGINT PRIMARY KEY DEFAULT nextval('customer_id_key_value'),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    cust_phone VARCHAR(15),
    address_id INT,  
    customer_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    customer_src_unique_id VARCHAR(255) UNIQUE
);

CREATE TABLE if not exists  bl_3nf.ce_employess(
    emp_id BIGINT PRIMARY KEY DEFAULT nextval('emp_id_key_value'),  
    emp_first_name VARCHAR(255),
    emp_last_name VARCHAR(255),
    emp_phone VARCHAR(15),
    address_id INT,  
    employee_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    employee_src_unique_id VARCHAR(255) UNIQUE
);

CREATE TABLE if not exists bl_3nf.ce_stores (
    store_id BIGINT PRIMARY KEY DEFAULT nextval('store_id_key_value'),
    store_address_id INT,  --INT REFERENCES ce_addresses(address_id),
    store_src_id VARCHAR(500),
    source_system VARCHAR(500),
    source_system_entity VARCHAR(500),
    store_name VARCHAR(500),
    emp_id VARCHAR(500), -- REFERENCES ce_employess(address_id),    
    insert_dt DATE,
    update_dt DATE,
    stores_src_unique_id VARCHAR(255) UNIQUE
);


 CREATE TABLE if not exists bl_3nf.ce_discount(
    discount_id BIGINT PRIMARY KEY DEFAULT nextval('discount_id_key_value'),    
    DISCOUNT_name VARCHAR(255),
    DISCOUNT_PRC INT,  
    discount_src_id VARCHAR(50),
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    discount_src_unique_id VARCHAR(255) UNIQUE
); 

   CREATE TABLE if not exists bl_3nf.ce_payments (
    payment_id BIGINT PRIMARY KEY DEFAULT nextval('payment_id_key_value'),
    store_id INT,
    customer_id INT,
    emp_id INT,
    product_src_id INT,
    discount_id INT,
    Amount INT,
    discount INT, 
    quantity_sold INT,
    sale_date DATE,
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE
);

--DIM tables

CREATE TABLE IF NOT EXISTS bl_cl.dim_products_scd(
    product_surr_id INTEGER PRIMARY KEY DEFAULT nextval('bl_cl.prod_surr_key_value'),
    product_src_id int UNIQUE, 
    product_desc VARCHAR(255),
    category_name VARCHAR(255),
    quantity_sold INT,
    shoe_brand VARCHAR(255),
    colours VARCHAR(255),
    material VARCHAR(255),
    size VARCHAR(255),  
    prices INT,
    start_dt DATE,
    end_dt DATE,
    is_active BOOLEAN,
    insert_dt DATE,
    update_dt DATE,
    CONSTRAINT unique_product_version UNIQUE (product_src_id, start_dt)
);


 CREATE TABLE IF NOT EXISTS bl_cl.dim_customers (
  customer_surr_id INTEGER PRIMARY KEY DEFAULT nextval('bl_cl.cust_surr_key_value'),
  customer_src_id VARCHAR(255) UNIQUE,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  phone VARCHAR(15),
  address VARCHAR(255),
  city VARCHAR(255),
  zipcode VARCHAR(255),
  country VARCHAR(255),
  region VARCHAR(255),
  insert_dt DATE,
  update_dt DATE,
  FOREIGN KEY (customer_surr_id) REFERENCES bl_cl.dim_customers(customer_surr_id)
);



CREATE TABLE IF NOT EXISTS bl_cl.dim_employess (
  employee_surr_id INTEGER PRIMARY KEY DEFAULT nextval('bl_cl.emp_surr_key_value'),
  employee_src_id VARCHAR(255) UNIQUE,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  phone VARCHAR(15),
  address VARCHAR(255),
  city VARCHAR(255),
  zipcode VARCHAR(255),
  country VARCHAR(255),
  region VARCHAR(255),
  insert_dt DATE,
  update_dt DATE,
  FOREIGN KEY (employee_surr_id) REFERENCES bl_cl.dim_employess(employee_surr_id)  
);


CREATE TABLE IF NOT EXISTS bl_cl.dim_stores (
  store_surr_id  INTEGER PRIMARY KEY DEFAULT nextval('bl_cl.store_surr_key_value'),
  store_src_id VARCHAR(255) UNIQUE,
 -- phone VARCHAR(15),
  address VARCHAR(255),
  city VARCHAR(255),
  zipcode VARCHAR(255),
  country VARCHAR(255),
  region VARCHAR(255),
  economic_region VARCHAR(255),
  insert_dt DATE,
  update_dt DATE,
  FOREIGN KEY (store_surr_id) REFERENCES bl_cl.dim_stores(store_surr_id)
);

-- Create the DIM_DATES table for last 2 years 
CREATE TABLE IF NOT EXISTS  bl_cl.DIM_DATES (
    TIME_ID DATE PRIMARY KEY,
    DAY_NAME VARCHAR(50),
    DAY_OF_WEEK INT,
    DAY_OF_MONTH INT,
    DAY_OF_YEAR INT,
    WEEK_OF_YEAR INT,
    CALENDAR_MONTH_NUMBER INT,
    CALENDAR_MONTH_NAME VARCHAR(50),
    DAYS_IN_CALENDAR_MONTH INT,
    DAYS_IN_CALENDAR_YEAR INT,
    "QUARTER" INT ,
    "YEAR" VARCHAR(10), 
    FISCAL_PERIOD INT
);

-- Insert values into the DIM_DATES table


INSERT INTO bl_cl.DIM_DATES (
    TIME_ID,
    DAY_NAME,
    DAY_OF_WEEK,
    DAY_OF_MONTH,
    DAY_OF_YEAR,
    WEEK_OF_YEAR,
    CALENDAR_MONTH_NUMBER,
    CALENDAR_MONTH_NAME,
    DAYS_IN_CALENDAR_MONTH,
    DAYS_IN_CALENDAR_YEAR,
    "QUARTER",
    "YEAR",
    FISCAL_PERIOD  
)
select
    date_id::date AS TIME_ID,
    TO_CHAR(date_id, 'Day') AS DAY_NAME,
    CASE WHEN EXTRACT(DOW FROM date_id) =  0 THEN  7 ELSE EXTRACT(DOW FROM date_id) END AS DAY_OF_WEEK, -- DOW (Day of Week)
    EXTRACT(DAY FROM date_id) AS DAY_OF_MONTH,
    EXTRACT(DOY FROM date_id) AS DAY_OF_YEAR, -- Corrected to DOY (Day of Year)
    EXTRACT(WEEK FROM date_id) AS WEEK_OF_YEAR,
    EXTRACT(MONTH FROM date_id) AS CALENDAR_MONTH_NUMBER,
    TO_CHAR(date_id, 'Month') AS CALENDAR_MONTH_NAME,
    date_part('days', date_trunc('month', date_id + INTERVAL '1 month') - interval '1 day') AS DAYS_IN_CALENDAR_MONTH,
    EXTRACT(DAY FROM DATE_TRUNC('year', date_id + INTERVAL '1 year') - DATE_TRUNC('year', date_id)) AS DAYS_IN_CALENDAR_YEAR,
    EXTRACT(QUARTER FROM date_id) AS "QUARTER",
    EXTRACT(YEAR FROM date_id) AS "YEAR",
    CASE
        WHEN EXTRACT(MONTH FROM date_id) BETWEEN 1 AND 3 THEN 1
        WHEN EXTRACT(MONTH FROM date_id) BETWEEN 4 AND 6 THEN 2
        WHEN EXTRACT(MONTH FROM date_id) BETWEEN 7 AND 9 THEN 3
        WHEN EXTRACT(MONTH FROM date_id) BETWEEN 10 AND 12 THEN 4
    END AS FISCAL_PERIOD
FROM
    generate_series('2020-01-01'::date, '2024-01-05'::date, '1 day') AS date_id
  ; 



CREATE TABLE if not exists bl_cl.dim_payments (
    payment_surr_id BIGINT PRIMARY KEY DEFAULT nextval('payment_id_key_value'),
    store_id INT,
    customer_id INT,
    emp_id INT,
    product_src_id INT,
    discount_id INT,
    Amount INT,
    discount INT, 
    quantity_sold INT,
    sale_date DATE,
    source_system VARCHAR(50),
    source_system_entity VARCHAR(50),
    insert_dt DATE
);
















