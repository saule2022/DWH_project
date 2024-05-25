--SHoe data extraction from external csv file  src_shoe_sales2.csv


CREATE OR REPLACE PROCEDURE bl_cl.load_src_shoe()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_user_info VARCHAR := CURRENT_USER;
	v_inserted_date date := current_date;
	v_text_message VARCHAR;
	v_rows_count int;
	v_error_message VARCHAR;
BEGIN 
	
  
INSERT INTO myschema.scr_shoe_table(
    PRODUCT_ID,
    PRODUCT_SRC_ID,
    SHOE_BRAND_ID,
    Shoe_Brand,
    BRAND_CODE,
    CATEGORY_ID,
    CATEGORY_SCR_ID,
    CATEGORY_NAME,
    CATEGORY_CODE,
    STORE_ID,
    STORE_NAME,
    PRODUCT_DESC,
    IS_ACTIVE,
    END_DT,
    START_DT,
    COLOUR_ID,
    COLOURS,
    MATERIAL_ID,
    MATERIAL,
    SALE_DATE,
    SIZE_ID,
    SIZE,
    QUANTITY_SOLD,
    PRICES,
    AMOUNT,
    CUST_ID,
    cust_FIRST_NAME,
    cust_LAST_NAME,
    cust_PHONE,
    emp_id,
    emp_FIRST_NAME,
    emp_LAST_NAME,
    emp_PHONE,
    emp_country_id,
    emp_COUNTRY,
    cust_country_id,
    cust_COUNTRY,
    store_country_id,
    store_COUNTRY,
    cust_city_id,
    cust_city,
    store_city_id,
    store_city,
    emp_city_id,
    emp_city,
    cust_code ,
    CUST_ADDRESS_ID,
    CUST_ADDRESS,
    emp_code,
    emp_ADDRESS_id,
    emp_ADDRESS,
    store_code,
    store_ADDRESS_ID,
    store_ADDRESS,
    REGION_SRC_ID,
    cust_region_id,
    cust_region,
    store_region_id,
    store_region,
    store_economic_region_id,
    store_economic_region,
    emp_economic_region_id,
    emp_economic_region,
    cust_economic_region,
    cust_economic_region_id,
    emp_region_id,
    emp_region,
    DISCOUNT_ID,
    DISCOUNT_name,
    DISCOUNT_PRC
)
SELECT
    PRODUCT_ID,
    PRODUCT_SRC_ID,
    SHOE_BRAND_ID,
    Shoe_Brand,
    BRAND_CODE,
    CATEGORY_ID,
    CATEGORY_SCR_ID,
    CATEGORY_NAME,
    CATEGORY_CODE,
    STORE_ID,
    STORE_NAME,
    PRODUCT_DESC,
    IS_ACTIVE,
    END_DT,
    START_DT,
    COLOUR_ID,
    COLOURS,
    MATERIAL_ID,
    MATERIAL,
    SALE_DATE,
    SIZE_ID,
    SIZE,
    QUANTITY_SOLD,
    PRICES,
    AMOUNT,
    CUST_ID,
    cust_FIRST_NAME,
    cust_LAST_NAME,
    cust_PHONE,
    emp_id,
    emp_FIRST_NAME,
    emp_LAST_NAME,
    emp_PHONE,
    emp_country_id,
    emp_COUNTRY,
    cust_country_id,
    cust_COUNTRY,
    store_country_id,
    store_COUNTRY,
    cust_city_id,
    cust_city,
    store_city_id,
    store_city,
    emp_city_id,
    emp_city,
    cust_code ,
    CUST_ADDRESS_ID,
    CUST_ADDRESS,
    emp_code,
    emp_ADDRESS_id,
    emp_ADDRESS,
    store_code,
    store_ADDRESS_ID,
    store_ADDRESS,
    REGION_SRC_ID,
    cust_region_id,
    cust_region,
    store_region_id,
    store_region,
    store_economic_region_id,
    store_economic_region,
    emp_economic_region_id,
    emp_economic_region,
    cust_economic_region,
    cust_economic_region_id,
    emp_region_id,
    emp_region,
    DISCOUNT_ID,
    DISCOUNT_name,
    DISCOUNT_PRC
FROM myschema.shoe_table1;

	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, 'src_shoe_sales5', 'scr_shoe_table', v_text_message, v_inserted_date);

		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, 'src_shoe_sales5', 'scr_shoe_table', v_error_message , v_inserted_date);
END ;
$$;

call bl_cl.load_src_shoe();

--drop foreign table myschema.shoe_table3 cascade;
----------------second part data-----------------


CREATE OR REPLACE PROCEDURE bl_cl.load_src_shoe2()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_user_info VARCHAR := CURRENT_USER;
	v_inserted_date date := current_date;
	v_text_message VARCHAR;
	v_rows_count int;
	v_error_message VARCHAR;
BEGIN 
  
INSERT INTO myschema.scr_shoe_table2(
    PRODUCT_ID,
    PRODUCT_SRC_ID,
    SHOE_BRAND_ID,
    Shoe_Brand,
    BRAND_CODE,
    CATEGORY_ID,
    CATEGORY_SCR_ID,
    CATEGORY_NAME,
    CATEGORY_CODE,
    STORE_ID,
    STORE_NAME,
    PRODUCT_DESC,
    IS_ACTIVE,
    END_DT,
    START_DT,
    COLOUR_ID,
    COLOURS,
    MATERIAL_ID,
    MATERIAL,
    SALE_DATE,
    SIZE_ID,
    SIZE,
    QUANTITY_SOLD,
    PRICES,
    AMOUNT,
    CUST_ID,
    cust_FIRST_NAME,
    cust_LAST_NAME,
    cust_PHONE,
    emp_id,
    emp_FIRST_NAME,
    emp_LAST_NAME,
    emp_PHONE,
    emp_country_id,
    emp_COUNTRY,
    cust_code ,
    cust_country_id,
    cust_COUNTRY,
    store_country_id,
    store_COUNTRY,
    cust_city_id,
    cust_city,
    store_city_id,
    store_city,
    emp_city_id,
    emp_city,
    CUST_ADDRESS_ID,
    CUST_ADDRESS,
    emp_code,
    emp_ADDRESS_id,
    emp_ADDRESS,
    store_code,
    store_ADDRESS_ID,
    store_ADDRESS,
    REGION_SRC_ID,
    cust_region_id,
    cust_region,
    store_region_id,
    store_region,
    store_economic_region_id,
    store_economic_region,
    emp_economic_region_id,
    emp_economic_region,
    cust_economic_region,
    cust_economic_region_id,
    emp_region_id,
    emp_region,
    DISCOUNT_ID,
    DISCOUNT_name,
    DISCOUNT_PRC
)
SELECT
    PRODUCT_ID,
    PRODUCT_SRC_ID,
    SHOE_BRAND_ID,
    Shoe_Brand,
    BRAND_CODE,
    CATEGORY_ID,
    CATEGORY_SCR_ID,
    CATEGORY_NAME,
    CATEGORY_CODE,
    STORE_ID,
    STORE_NAME,
    PRODUCT_DESC,
    IS_ACTIVE,
    END_DT,
    START_DT,
    COLOUR_ID,
    COLOURS,
    MATERIAL_ID,
    MATERIAL,
    SALE_DATE,
    SIZE_ID,
    SIZE,
    QUANTITY_SOLD,
    PRICES,
    AMOUNT,
    CUST_ID,
    cust_FIRST_NAME,
    cust_LAST_NAME,
    cust_PHONE,
    emp_id,
    emp_FIRST_NAME,
    emp_LAST_NAME,
    emp_PHONE,
    emp_country_id,
    emp_COUNTRY,
    cust_country_id,
    cust_COUNTRY,
    store_country_id,
    store_COUNTRY,
    cust_city_id,
    cust_city,
    store_city_id,
    store_city,
    emp_city_id,
    emp_city,
    cust_code ,
    CUST_ADDRESS_ID,
    CUST_ADDRESS,
    emp_code,
    emp_ADDRESS_id,
    emp_ADDRESS,
    store_code,
    store_ADDRESS_ID,
    store_ADDRESS,
    REGION_SRC_ID,
    cust_region_id,
    cust_region,
    store_region_id,
    store_region,
    store_economic_region_id,
    store_economic_region,
    emp_economic_region_id,
    emp_economic_region,
    cust_economic_region,
    cust_economic_region_id,
    emp_region_id,
    emp_region,
    DISCOUNT_ID,
    DISCOUNT_name,
    DISCOUNT_PRC
FROM myschema.shoe_table2;


	GET DIAGNOSTICS v_rows_count = ROW_COUNT;
	v_text_message := 'No. of rows inserted: ' || v_rows_count::VARCHAR;
	-- Print total number of insered rows.
	CALL bl_cl.logging_info(v_user_info, 'src_shoe_sales5secpart', 'scr_shoe_table2', v_text_message, v_inserted_date);

		EXCEPTION
	    -- Catch the error and store the error message in the variable
	    WHEN OTHERS THEN
	      GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
	     CALL bl_cl.logging_info(v_user_info, 'src_shoe_sales5secpart', 'scr_shoe_table2', v_error_message , v_inserted_date);
END ;
$$;

call bl_cl.load_src_shoe2()

