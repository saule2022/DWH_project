CREATE SCHEMA IF NOT EXISTS bl_cl;

CREATE OR REPLACE PROCEDURE bl_cl.load_sequences()
LANGUAGE plpgsql
AS $$
BEGIN   
    -- Create sequence for primary key values in product dimension tables.
    CREATE SEQUENCE IF NOT EXISTS bl_cl.prod_surr_key_value
    START WITH   1
    INCREMENT BY   1
    MINVALUE   1
    NO MAXVALUE
    NO CYCLE;

    -- Create sequence for primary key values in customer dimension tables.
    CREATE SEQUENCE IF NOT EXISTS bl_cl.cust_surr_key_value
    START WITH   1
    INCREMENT BY   1
    MINVALUE   1
    NO MAXVALUE
    NO CYCLE;

    -- Create sequence for primary key values in address dimension tables.
    CREATE SEQUENCE IF NOT EXISTS bl_cl.address_surr_key_value
    START WITH   1
    INCREMENT BY   1
    MINVALUE   1
    NO MAXVALUE
    NO CYCLE;

    -- Create sequence for primary key values in employee dimension tables.
    CREATE SEQUENCE IF NOT EXISTS bl_cl.emp_surr_key_value
    START WITH   1
    INCREMENT BY   1
    MINVALUE   1
    NO MAXVALUE
    NO CYCLE;

    -- Create sequence for primary key values in store dimension tables.
    CREATE SEQUENCE IF NOT EXISTS bl_cl.store_surr_key_value
    START WITH   1
    INCREMENT BY   1
    MINVALUE   1
    NO MAXVALUE
    NO CYCLE;
END;
$$;

commit;



CREATE OR REPLACE PROCEDURE bl_cl.insert_dim_products_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_count INT;
    v_inserted_count INT;
    v_table_name VARCHAR := 'dim_products_scd';
    v_inserted_date DATE := CURRENT_DATE;
    v_user_info VARCHAR := CURRENT_USER;
    v_text_message VARCHAR;
    v_error_message VARCHAR;
BEGIN
    -- Update existing records in dim_products_scd
    UPDATE bl_cl.dim_products_scd dimp
    SET   
        update_dt = current_date,   
        end_dt = cp.end_dt,
        is_active = CASE   
            WHEN cp.is_active IS NOT NULL AND cp.is_active IN ('true', 'false', '1', '0') THEN cp.is_active::boolean
            ELSE TRUE -- Default to TRUE if the value is not a valid boolean
        END
    FROM bl_3nf.ce_products cp
    WHERE   
        dimp.product_src_id = cp.product_id::int
        AND dimp.is_active::varchar = 'TRUE' -- Explicit cast to varchar
        AND EXISTS (
            SELECT   1   
            FROM bl_3nf.ce_products cp
            WHERE   
                dimp.product_src_id::int = cp.product_id::int  
                AND dimp.start_dt = cp.start_dt   
                AND cp.is_active::varchar = 'TRUE');
    
    -- Insert new records into dim_products_scd
    INSERT INTO bl_cl.dim_products_scd (
        product_surr_id,
        product_src_id,
        product_desc,
        category_name,
        quantity_sold,
        shoe_brand,
        colours,
        material,
        size,
        prices,
        start_dt,
        end_dt,
        is_active,
        insert_dt,
        update_dt
    )
    SELECT DISTINCT ON (cp.category_name, cp.shoe_brand, cp.colour, cp.material, cp.size)
        nextval('prod_surr_key_value'),
        COALESCE(cp.product_id, -1), 
        COALESCE(cp.product_desc, 'n.a'),
        COALESCE(ca.category_name, 'n.a'),   
        COALESCE(cp.quantity_sold, -1),
        COALESCE(cp.shoe_brand, 'n.a'),   
        COALESCE(cp.colour, 'n.a'),
        COALESCE(cp.material, 'n.a'),
        COALESCE(cp.size, 'n.a'),
        COALESCE(cp.prices, -1),
        COALESCE(cp.start_dt, '1900-01-01'::date),
        COALESCE(cp.end_dt, '2999-01-01'::date),
        CASE   
            WHEN cp.is_active IS NOT NULL AND cp.is_active IN ('true', 'false', '1', '0') THEN cp.is_active::boolean
            ELSE TRUE -- Default to TRUE if the value is not a valid boolean
        END AS is_active,
        CURRENT_DATE as insert_dt,
        CURRENT_DATE as update_dt
    FROM
        bl_3nf.ce_products cp
    LEFT JOIN bl_3nf.ce_categories ca ON cp.category_id = ca.category_id
    WHERE
        NOT EXISTS (
            SELECT   1
            FROM bl_cl.dim_products_scd AS dp
            WHERE
                dp.product_src_id = COALESCE(cp.product_id, -1) 
                AND dp.product_desc = COALESCE(cp.product_desc,'n.a')
                AND dp.category_name = COALESCE(ca.category_name ,'n.a')
                AND dp.shoe_brand = COALESCE(cp.shoe_brand ,'n.a')
                AND dp.quantity_sold = COALESCE(cp.quantity_sold, -1) 
                AND dp.colours = COALESCE(cp.colour,'n.a')
                AND dp.material = COALESCE(cp.material ,'n.a')
                AND dp.size = COALESCE(cp.size,'n.a')   
                AND dp.prices = COALESCE(cp.prices, -1)) 
        ON CONFLICT (product_src_id) DO nothing;
 
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Construct the text message for logging
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
    -- Log the information
    CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_text_message, v_inserted_date);
         
EXCEPTION
    -- Catch the error and store the error message in the variable
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_CL' || 'ce_products_sdc', v_error_message , v_inserted_date);

END;
$$;
commit;


CREATE OR REPLACE PROCEDURE bl_cl.insert_dim_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_count INT;
    v_inserted_count INT;
    v_table_name VARCHAR := 'dim_customers';
    v_inserted_date DATE := CURRENT_DATE;
    v_user_info VARCHAR := CURRENT_USER;
    v_text_message VARCHAR;
    v_error_message VARCHAR;
BEGIN
    -- Insert data into dim_customers from ce_customers, ce_addresses, ce_cities, ce_countries, and ce_regions
    INSERT INTO bl_cl.dim_customers (
        customer_src_id,
        first_name,
        last_name,
        phone,
        address,
        city,
        zipcode,
        country,
        region,
        insert_dt,
        update_dt
    )
    SELECT

        COALESCE(cc.customer_id::VARCHAR, 'n.a'),
        COALESCE(cc.first_name, 'n.a'),
        COALESCE(cc.last_name, 'n.a'),
        COALESCE(cc.cust_phone::VARCHAR, 'n.a'),
        COALESCE(ca.address, 'n.a'),
        COALESCE(c2.city_name, 'n.a') AS city,
        COALESCE(ca.zipcode, 'n.a') AS zipcode,
        COALESCE(c3.country_name::VARCHAR, 'n.a') AS country,
        COALESCE(cr.region_name::VARCHAR, 'n.a') AS region,
        CURRENT_DATE AS insert_dt,
        CURRENT_DATE AS update_dt
    FROM
        bl_3nf.ce_customers cc
    LEFT JOIN bl_3nf.ce_addresses as ca ON cc.address_id  = ca.address_id
    LEFT JOIN bl_3nf.ce_cities AS c2 ON ca.city_id = c2.city_id
    LEFT JOIN bl_3nf.ce_countries as c3 ON c2.country_id = c3.country_id
    LEFT JOIN bl_3nf.ce_regions as cr ON c3.region_id = cr.region_id
    WHERE NOT EXISTS (
        SELECT   1
        FROM bl_cl.dim_customers AS dc
        WHERE
            dc.customer_src_id = COALESCE(cc.customer_id::varchar, 'n.a')
    )
    ON CONFLICT (customer_src_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        phone = EXCLUDED.phone,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        region = EXCLUDED.region,
        update_dt = CURRENT_DATE;
 -- Get the number of rows inserted
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Construct the text message for logging
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
    -- Log the information
    CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_text_message, v_inserted_date);
  
EXCEPTION
    -- Catch the error and store the error message in the variable
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'DIM' || 'ce_customers', v_error_message , v_inserted_date);
END;
$$ ;
commit;


CREATE OR REPLACE PROCEDURE bl_cl.insert_dim_employess()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_count INT;
    v_inserted_count INT;
    v_table_name VARCHAR := 'dim_employess';
    v_inserted_date DATE := CURRENT_DATE;
    v_user_info VARCHAR := CURRENT_USER;
    v_text_message VARCHAR;
    v_error_message VARCHAR;
BEGIN
    -- Insert data into dim_employess from ce_employess, ce_addresses, ce_cities, ce_countries, and ce_regions
    INSERT INTO bl_cl.dim_employess(
        employee_src_id,
        first_name,
        last_name,
        phone,
        address,
        city,
        zipcode,
        country,
        region,
        insert_dt,
        update_dt
    )
    SELECT
        COALESCE(ce.emp_id::VARCHAR, 'n.a'),
        COALESCE(ce.emp_first_name, 'n.a'),
        COALESCE(ce.emp_last_name, 'n.a'),
        COALESCE(ce.emp_phone::VARCHAR, 'n.a'),
        COALESCE(ca2.address, 'n.a'),
        COALESCE(cc.city_name, 'n.a') AS city,
        COALESCE(ca2.zipcode, 'n.a') AS zipcode,
        COALESCE(cc2.country_name::VARCHAR, 'n.a') AS country,
        COALESCE(cr.region_name::VARCHAR, 'n.a') AS region,
        CURRENT_DATE AS insert_dt,
        CURRENT_DATE AS update_dt    
    FROM
        bl_3nf.ce_employess ce
    LEFT JOIN bl_3nf.ce_addresses  AS ca2 ON ce.address_id = ca2.address_id
    LEFT JOIN bl_3nf.ce_cities AS cc ON ca2.city_id = cc.city_id
    LEFT JOIN bl_3nf.ce_countries AS cc2 ON cc.country_id = cc2.country_id
    LEFT JOIN bl_3nf.ce_regions AS cr ON cc2.region_id = cr.region_id
    WHERE NOT EXISTS (
        SELECT  1
        FROM bl_cl.dim_employess AS de
        WHERE
            de.employee_src_id = COALESCE(ce.emp_id::varchar, 'n.a')
    )
    ON CONFLICT (employee_src_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        phone = EXCLUDED.phone,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        zipcode = EXCLUDED.zipcode,
        country = EXCLUDED.country,
        region = EXCLUDED.region,
        update_dt = CURRENT_DATE;

   -- Get the number of rows inserted
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Construct the text message for logging
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
    -- Log the information
    CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_text_message, v_inserted_date);
EXCEPTION
    -- Catch the error and store the error message in the variable
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'DIM' || 'ce_employess', v_error_message , v_inserted_date);
END;
$$ ;
commit;



CREATE OR REPLACE PROCEDURE bl_cl.insert_dim_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_count INT;
    v_inserted_count INT;
    v_table_name VARCHAR := 'dim_stores';
    v_inserted_date DATE := CURRENT_DATE;
    v_user_info VARCHAR := CURRENT_USER;
    v_text_message VARCHAR;
    v_error_message VARCHAR;
BEGIN

    INSERT INTO bl_cl.dim_stores(
        store_src_id,
        --phone,
        address,
        city,
        zipcode,
        country,
        region,
        economic_region,
        insert_dt, 
        update_dt
    )
    SELECT
        COALESCE(cs.store_id::VARCHAR, 'n.a'),
        -- COALESCE(cs.  ::VARCHAR, 'n.a'),
        COALESCE(ca.address, 'n.a'),
        COALESCE(cc.city_name, 'n.a') AS city,
        COALESCE(ca.zipcode, 'n.a') AS zipcode,
        COALESCE(cc2.country_name::VARCHAR, 'n.a') AS country,
        COALESCE(cr.region_name::VARCHAR, 'n.a') AS region,
        COALESCE(cer.economic_region::VARCHAR, 'n.a') AS economic_region,
        CURRENT_DATE AS insert_dt, 
        CURRENT_DATE AS update_dt
    FROM
        bl_3nf.ce_stores  cs
    LEFT JOIN bl_3nf.ce_addresses AS ca ON cs.store_address_id = ca.address_id
    LEFT JOIN bl_3nf.ce_cities AS cc ON ca.city_id = cc.city_id
    LEFT JOIN bl_3nf.ce_countries AS cc2 ON cc.country_id = cc2.country_id
    LEFT JOIN bl_3nf.ce_regions AS cr ON cc2.region_id = cr.region_id
    LEFT JOIN bl_3nf.ce_economic_regions as cer ON cc2.economic_region_id = cer.economic_region_id
    WHERE NOT EXISTS (
        SELECT  1
        FROM bl_cl.dim_stores AS ds
        WHERE
            ds.store_src_id = COALESCE(cs.store_id::varchar, 'n.a')
    )
    ON CONFLICT (store_src_id) DO UPDATE SET
        -- phone = EXCLUDED.phone,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        zipcode = EXCLUDED.zipcode,
        country = EXCLUDED.country,
        region = EXCLUDED.region,
        economic_region = EXCLUDED.economic_region,
        update_dt = CURRENT_DATE;

    -- Get the number of rows inserted
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Construct the text message for logging
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
    -- Log the information
    CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_text_message, v_inserted_date);

EXCEPTION
    -- Catch the error and store the error message in the variable
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'DIM' || 'ce_stores', v_error_message , v_inserted_date);
END;
$$ ;
commit;


CREATE OR REPLACE PROCEDURE bl_cl.load_dim_payments_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'dim_payments'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN

  INSERT INTO bl_cl.dim_payments(
    store_id,
    customer_id,
    emp_id,
    product_src_id,
    discount_id,
    Amount,
    discount,
    quantity_sold,
    sale_date,
    source_system,
    source_system_entity,
    insert_dt
)
SELECT
    COALESCE(ces2.store_id,   -1),
    COALESCE(cec.customer_id,   -1),
    COALESCE(cee.emp_id,   -1),
    COALESCE(cp.product_src_id,   -1),
    COALESCE(cd.discount_id,   -1), 
    COALESCE(ss.amount,   -1),
    cd.discount_prc  AS discount_prc, 
    COALESCE(ss.quantity_sold,   -1),
    COALESCE(ss.sale_date::date, '1900-01-01') AS sale_date,
    'BL_3NF'as source_system,
    'scr_shoe_table' as source_system_entity,
    CURRENT_DATE as insert_dt
FROM
    myschema.scr_shoe_table AS ss
LEFT JOIN bl_3nf.ce_products AS cp ON cp.product_src_id = ss.product_src_id  --AND ss.source_system = 'BL_3NF'
LEFT JOIN bl_3nf.ce_employess AS cee ON cee.employee_src_id::NUMERIC(8,2) = ss.emp_id::NUMERIC(8,2) --AND cee.source_system = 'BL_3NF'
LEFT JOIN bl_3nf.ce_customers AS cec ON cec.customer_src_id::NUMERIC(8,2) = ss.cust_id::NUMERIC(8,2) --AND cec.source_system = 'BL_3NF'
LEFT JOIN bl_3nf.ce_stores AS ces2 ON ces2.store_src_id::NUMERIC(8,2) = ss.store_id::NUMERIC(8,2) --AND ces2.source_system = 'BL_3NF'
LEFT JOIN bl_3nf.ce_discount AS cd ON cd.discount_src_id = ss.discount_id --AND cd.source_system = 'BL_3NF'
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (product_src_id)
        product_src_id
    FROM bl_3nf.ce_products cp
    WHERE source_system = 'BL_3NF'
    ORDER BY product_src_id, insert_dt DESC
) AS unique_cps ON unique_cps.product_src_id = ss.product_src_id
WHERE
    NOT EXISTS (
        SELECT   1
        FROM bl_cl.dim_payments AS cs1
        WHERE cs1.sale_date = COALESCE(ss.sale_date::date, '1900-01-01')
            AND cs1.product_src_id = COALESCE(cp.product_src_id,   -1)
            AND cs1.emp_id = COALESCE(cee.emp_id::NUMERIC(8,2), -1)
            AND cs1.customer_id = COALESCE(cec.customer_id::NUMERIC(8,2),   -1)
            AND cs1.store_id = COALESCE(ces2.store_id::NUMERIC(8,2),   -1)
            AND cs1.source_system = 'BL_3NF'
            AND cs1.source_system_entity = 'scr_shoe_table'
    );
    -- Get the number of rows inserted
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Construct the text message for logging
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
    -- Log the information
    CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_text_message, v_inserted_date);

    EXCEPTION
        -- Catch the error and store the error message in the variable
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
   
            CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_payments', v_error_message, v_inserted_date);
END;
$$;
commit;

