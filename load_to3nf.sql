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


CREATE OR REPLACE PROCEDURE bl_cl.load_3nf_sequences()
 LANGUAGE plpgsql
 AS $$
 BEGIN 
	 
	

CREATE SEQUENCE IF NOT EXISTS category_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;



CREATE SEQUENCE IF NOT EXISTS shoe_brand_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
   
  
    
CREATE SEQUENCE IF NOT EXISTS colour_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;


CREATE SEQUENCE IF NOT EXISTS size_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;

 

CREATE SEQUENCE IF NOT EXISTS material_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
   
 

CREATE SEQUENCE IF NOT EXISTS product_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
   
  

CREATE SEQUENCE IF NOT EXISTS region_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
   
  

CREATE SEQUENCE IF NOT EXISTS economic_region_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
 

CREATE SEQUENCE IF NOT EXISTS country_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
   


CREATE SEQUENCE IF NOT EXISTS city_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
 

CREATE SEQUENCE IF NOT EXISTS address_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;


CREATE SEQUENCE IF NOT EXISTS customer_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;


CREATE SEQUENCE IF NOT EXISTS empr_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS store_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
 

CREATE SEQUENCE IF NOT EXISTS discount_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
 
      

CREATE SEQUENCE IF NOT EXISTS payment_id_key_value
    START WITH  1
    INCREMENT BY  1
    MINVALUE  1
    NO MAXVALUE
    NO CYCLE;
END;
$$;
   
   


--Create function to load data from source tables to caregory table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_category_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_default_row_exists BOOLEAN;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_categories'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct categories from the source tables
    WITH s1 AS (
        SELECT DISTINCT COALESCE(ss.category_name, 'n.a') AS category_name,
            COALESCE(ss.category_id, '-1') AS category_src_id,
            'BL_3NF' AS source_system,
            'ce_categories' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt,
            CONCAT(ss.category_id, '_', 'BL_3NF', '_', 'scr_shoe_table' ) AS category_src_unique_id
        FROM (
            SELECT category_name, category_id 
            FROM myschema.scr_shoe_table
            UNION ALL
            SELECT category_name, category_id
            FROM myschema.scr_shoe_table2
        ) AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_categories AS bc
            WHERE
                bc.category_name = COALESCE(ss.category_name, 'n.a')
                AND bc.category_src_id = COALESCE(ss.category_id::VARCHAR, '-1')
                AND bc.source_system = 'BL_3NF'
                AND bc.source_system_entity = 'ce_categories'
        )
    )
    INSERT INTO bl_3nf.ce_categories (
        category_name,
        category_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        category_src_unique_id
    )
    SELECT
        s1.category_name,
        s1.category_src_id,
        s1.source_system,
        s1.source_system_entity,
        s1.insert_dt,
        s1.update_dt,
        s1.category_src_unique_id
    FROM s1
    ON CONFLICT (category_src_unique_id) DO NOTHING;

    -- Get the number of rows inserted
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Construct the text message for logging
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;
    -- Log the information
    CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_text_message, v_inserted_date);

    -- Check if the default row already exists
    SELECT EXISTS (
        SELECT   1
        FROM bl_3nf.ce_categories
        WHERE category_src_unique_id = 'default_row_unique_id'
    ) INTO v_default_row_exists;

EXCEPTION
    -- Catch the error and store the error message in the variable
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_categories', v_error_message, v_inserted_date);
END;
$$ ;



--Create function to load data from source tables to shoe_brand table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_shoe_brand_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_default_row_exists BOOLEAN;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_shoe_brand'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct shoe brands from the source tables
    WITH s1 AS (
        SELECT DISTINCT COALESCE(ss.shoe_brand, 'n.a') AS shoe_brand,
            COALESCE(ss.shoe_brand_id, '-1') AS shoe_brand_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt,
            CONCAT(ss.shoe_brand_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS shoe_brand_src_unique_id
        FROM (
            SELECT shoe_brand, shoe_brand_id
            FROM myschema.scr_shoe_table
            UNION ALL
            SELECT shoe_brand, shoe_brand_id
            FROM myschema.scr_shoe_table2
        ) AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_shoe_brand AS bc
            WHERE
                bc.shoe_brand = COALESCE(ss.shoe_brand, 'n.a')
                AND bc.shoe_brand_src_id = COALESCE(ss.shoe_brand_id::VARCHAR, '-1')
                AND bc.source_system = 'BL_3NF'
                AND bc.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_shoe_brand (
        shoe_brand,
        shoe_brand_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        shoe_brand_src_unique_id
    )
    SELECT
        s1.shoe_brand,
        s1.shoe_brand_src_id,
        s1.source_system,
        s1.source_system_entity,
        s1.insert_dt,
        s1.update_dt,
        s1.shoe_brand_src_unique_id
    FROM s1
    ON CONFLICT (shoe_brand_src_unique_id) DO NOTHING;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_shoe_brand', v_error_message, v_inserted_date);
END;
$$ ;
commit;

--Create function to load data from source tables to colours table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_colours_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_default_row_exists BOOLEAN;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_colours'; -- Corrected table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN

    WITH s1 AS (
        SELECT DISTINCT COALESCE(ss.colours, 'n.a') AS colour,
            COALESCE(ss.colour_id, '-1') AS colour_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt,
            CONCAT(ss.colour_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS colour_src_unique_id
        FROM (
            SELECT colours, colour_id
            FROM myschema.scr_shoe_table
            UNION  
            SELECT colours, colour_id
            FROM myschema.scr_shoe_table2
        ) AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_colours AS bc
            WHERE
                bc.colour = COALESCE(ss.colours, 'n.a')
                AND bc.colour_src_id = COALESCE(ss.colour_id::VARCHAR, '-1')
                AND bc.source_system = 'BL_3NF'
                AND bc.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_colours (
        colour,
        colour_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        colour_src_unique_id
    )
    SELECT
        s1.colour,
        s1.colour_src_id,
        s1.source_system,
        s1.source_system_entity,
        s1.insert_dt,
        s1.update_dt,
        s1.colour_src_unique_id
    FROM s1
    ON CONFLICT (colour_src_unique_id) DO NOTHING;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_colours', v_error_message, v_inserted_date);
END;
$$ ;
commit;



--Create function to load data from source tables to sizes table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_sizes_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_default_row_exists BOOLEAN;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_sizes'; -- Corrected table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct sizes from the source tables
    WITH s1 AS (
        SELECT DISTINCT COALESCE(ss.size, '-1') AS size,
            COALESCE(ss.size_id, '-1') AS size_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE AS update_dt,
            CONCAT(ss.size_id, '_', 'BL_3NF', '_', 'scr_shoe_table' ) AS size_src_unique_id
        FROM (
            SELECT size, size_id
            FROM myschema.scr_shoe_table
            UNION   
            SELECT size, size_id 
            FROM myschema.scr_shoe_table2
        ) AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_sizes AS bc
            WHERE
                bc.size = COALESCE(ss.size::VARCHAR, '-1')
                AND bc.size_src_id = COALESCE(ss.size_id::VARCHAR, '-1')
                AND bc.source_system = 'BL_3NF'
                AND bc.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_sizes (
        size,
        size_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        size_src_unique_id
    )
    SELECT
        s1.size,
        s1.size_src_id,
        s1.source_system,
        s1.source_system_entity,
        s1.insert_dt,
        s1.update_dt,
        s1.size_src_unique_id
    FROM s1
    ON CONFLICT (size_src_unique_id) DO NOTHING;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_sizes', v_error_message, v_inserted_date);
END;
$$ ;
commit;



--Create function to load data from source tables to material table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_material_data_3nf()   
LANGUAGE plpgsql
AS $$
DECLARE   
    v_inserted_count INT;
    v_table_name VARCHAR := 'ce_materials';
    v_inserted_date DATE := CURRENT_DATE;
    v_user_info VARCHAR := CURRENT_USER;
    v_text_message VARCHAR;
    v_material_id BIGINT := NULL;
    v_new_start_dt DATE;
    v_src_id INTEGER;
    v_src_source VARCHAR;
    v_error_message VARCHAR;
BEGIN   
    -- Implement SCD Type   2 logic
    WITH update_materials AS   
    (
        UPDATE bl_3nf.ce_materials p     
        SET is_active = FALSE,   
            end_dt = CURRENT_DATE,
            update_dt = CURRENT_DATE   
        WHERE EXISTS   
        (
            SELECT   1   
            FROM myschema.scr_shoe_table s1
            WHERE s1.material_id = p.material_src_id   
            AND (s1.material != p.material)
            AND p.is_active = TRUE AND p.source_system = 'BL_3NF'
        )
        RETURNING *
    )
    SELECT material_id, start_dt, material_src_id, source_system   
    INTO v_material_id, v_new_start_dt, v_src_id, v_src_source   
    FROM update_materials;

    -- v_material_id will not be null if update occurs. In that case, need to insert new row.
    IF v_material_id IS NOT NULL THEN   
        RAISE NOTICE 'Row updated';
        -- Insert new row for the updated material with the same material_id
        INSERT INTO bl_3nf.ce_materials   
        (material_id, material, material_src_id, source_system, source_system_entity, is_active, start_dt, end_dt, insert_dt, update_dt)   
        SELECT   
        v_material_id, -- Retain the same material_id
        s1.material as material,   
        s1.material_id::int as material_src_id,   
        'BL_3NF' as source_system, 
        'scr_shoe_table' as source_system_entity,   
        TRUE,   
        CURRENT_DATE,
        '9999-12-31'::DATE,
        CURRENT_DATE,
        CURRENT_DATE   
        FROM myschema.scr_shoe_table s1
        WHERE s1.material_id = v_src_id AND   
              NOT EXISTS (SELECT   1 FROM bl_3nf.ce_materials cps   
                          WHERE cps.material_src_id = s1.material_id::int
                          AND cps.is_active = FALSE   
                          AND cps.material = s1.material
                          AND cps.source_system = 'BL_3NF');
    END IF;

-- Insert new records for initial load and if added new materials
INSERT INTO bl_3nf.ce_materials   
(material_id, material, material_src_id, source_system, source_system_entity, start_dt, end_dt, is_active, insert_dt, update_dt)
SELECT   
    nextval('material_id_key_value'), -- Generate new material_id for new materials
    s1.material, s1.material_src_id,   
    'BL_3NF', 'scr_shoe_table',   
    CURRENT_DATE, -- Always set start_dt to CURRENT_DATE for new materials
    '9999-12-31'::DATE, TRUE, CURRENT_DATE, CURRENT_DATE
FROM (
    SELECT DISTINCT coalesce(material, 'n.a') as material,
    coalesce(material_id::int, -1) as material_src_id,
    'BL_3NF' AS source_system,
    'scr_shoe_table' AS source_system_entity,
    true as is_active,
    CURRENT_DATE AS start_dt,
    '9999-12-31'::DATE AS end_dt ,
    CURRENT_DATE AS insert_dt,
    CURRENT_DATE  AS update_dt
    FROM myschema.scr_shoe_table
) as s1
WHERE NOT EXISTS (
    SELECT   1   
    FROM bl_3nf.ce_materials p1
    WHERE p1.material_src_id = s1.material_src_id
    AND p1.source_system = 'BL_3NF'
    AND p1.source_system_entity = 'ce_materials'
    AND p1.material = s1.material
) OR (
    -- This condition ensures that if a material with the same material_src_id exists but has a different name,
    -- the new material is inserted.
    EXISTS (
        SELECT   1   
        FROM bl_3nf.ce_materials p1
        WHERE p1.material_src_id = s1.material_src_id
        AND p1.source_system = 'BL_3NF'
        AND p1.source_system_entity = 'scr_shoe_table'
        AND p1.material != s1.material
    )
);

    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
    -- Text message for logging table
    v_text_message := 'No. of rows inserted: ' || v_inserted_count::VARCHAR;

EXCEPTION
    -- Catch the error and store the error message in the variable
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF', v_error_message, v_inserted_date);
END;
$$;
commit;




--Create function to load data from source tables to products table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_products_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_products'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct products from the source table
    WITH new_products AS (
    SELECT distinct
        COALESCE(ss.start_dt::DATE, '1900-12-31'::DATE) AS START_DT,
        '9999-12-31'::DATE AS END_DT,
        COALESCE('TRUE'::VARCHAR(40), 'default_is_active') AS IS_ACTIVE,
        COALESCE(ss.product_desc::VARCHAR(255), 'default_desc') AS PRODUCT_DESC,
        COALESCE(c.category_id::BIGINT,   -1) AS CATEGORY_ID,
        COALESCE(c.category_name::VARCHAR(255), 'default_desc') AS CATEGORY_NAME,
       --COALESCE(ss.sale_date::DATE, '9999-12-31'::DATE) AS sale_date,
        'BL_3NF' AS source_system,
        'scr_shoe_table' AS source_system_entity,
        COALESCE(ss.product_id,   -1) AS PRODUCT_SRC_ID,    
        COALESCE(ss.quantity_sold::int,   -1) AS QUANTITY_SOLD,
        COALESCE(csb.shoe_brand_id::BIGINT,   -1) AS SHOE_BRAND_ID,
        COALESCE(csb.shoe_brand::VARCHAR(255), 'default_shoe_brand') AS SHOE_BRAND,
        COALESCE(cm.material_id::BIGINT,   -1) AS MATERIAL_ID,
        COALESCE(cm.material::VARCHAR(255), 'default_material') AS MATERIAL,
        COALESCE(cc.colour_id::BIGINT,   -1) AS COLOUR_ID,
        COALESCE(cc.colour::VARCHAR(255), 'default_colour') AS COLOUR,
        COALESCE(cs.size_id::BIGINT,   -1) AS SIZE_id,
        COALESCE(cs.size::VARCHAR(255), 'default size') AS size,
        COALESCE(ss.prices::NUMERIC,   -1) AS prices
    FROM
        (
            SELECT start_dt, product_desc, category_id, shoe_brand_id, material_id, colour_id, size_id, product_id,  quantity_sold, prices
            FROM myschema.scr_shoe_table
            UNION 
            SELECT start_dt, product_desc, category_id, shoe_brand_id, material_id, colour_id, size_id, product_id,  quantity_sold, prices
            FROM myschema.scr_shoe_table2
        ) AS ss
    LEFT JOIN bl_3nf.ce_shoe_brand as csb ON ss.shoe_brand_id = csb.shoe_brand_id
    LEFT JOIN bl_3nf.ce_materials as cm ON ss.material_id = cm.material_id
    LEFT JOIN bl_3nf.ce_sizes  as cs ON ss.size_id  = cs.size_id   
    LEFT JOIN bl_3nf.ce_colours as cc ON ss.colour_id = cc.colour_id
    LEFT JOIN bl_3nf.ce_categories as c ON ss.category_id = c.category_id
    WHERE NOT EXISTS (
        SELECT   1
        FROM bl_3nf.CE_PRODUCTS cps
        WHERE cps.product_src_id = ss.product_id
            AND cps.is_active = false::VARCHAR(255)
            AND cps.material = cm.material
            AND cps.colour = cc.colour
            AND cps.shoe_brand = csb.shoe_brand
            AND cps.prices = ss.prices
            AND cps.size = cs.size
  )
    )
    INSERT INTO bl_3nf.CE_PRODUCTS (
        START_DT,
        END_DT,
        IS_ACTIVE,
        PRODUCT_DESC,
        CATEGORY_ID,
        category_name,
       -- sale_date,
        source_system,
        source_system_entity,
        PRODUCT_SRC_ID,
        quantity_sold,
        SHOE_BRAND_id,
        SHOE_BRAND,
        colour_id,
        COLOUR,
        MATERIAL_id,
        MATERIAL,
        SIZE_id,
        SIZE,
        PRICES
    )
    select
    --nextval('product_id_key_value'),
        np.START_DT,
        np.END_DT,
        np.IS_ACTIVE,
        np.PRODUCT_DESC,
        np.CATEGORY_ID,
        np.category_name,
       -- np.sale_date,
        np.source_system,
        np.source_system_entity,
        np.PRODUCT_SRC_ID,
        np.quantity_sold,
        np.SHOE_BRAND_id,
        np.SHOE_BRAND,
        np.colour_id,
        np.COLOUR,
        np.MATERIAL_id,
        np.MATERIAL,
        np.SIZE_id,
        np.SIZE,
        np.PRICES
    FROM new_products AS np
    ON CONFLICT (PRODUCT_SRC_ID, START_DT) DO nothing; 
  
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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_products', v_error_message, v_inserted_date);
END;
$$;
commit;


--Create function to load data from source tables to regions table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_regions_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_regions'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct customer regions from the source table
    WITH new_customer_regions AS (
        SELECT DISTINCT
            COALESCE(ss.cust_region, 'n.a') AS region_name,
            COALESCE(ss.cust_region_id, '-1') AS region_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE::DATE AS update_dt,
            CONCAT(ss.cust_region_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS region_src_unique_id
        FROM myschema.scr_shoe_table AS ss
        WHERE NOT EXISTS (
            SELECT  1
            FROM bl_3nf.ce_regions AS cr
            WHERE
                cr.region_name = COALESCE(ss.cust_region, 'n.a')
                AND cr.region_src_id = COALESCE(ss.cust_region_id, '-1')
                AND cr.source_system = 'BL_3NF'
                AND cr.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_regions (
        region_name,
        region_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        region_src_unique_id
    )
    SELECT
        ncr.region_name,
        ncr.region_src_id,
        ncr.source_system,
        ncr.source_system_entity,
        ncr.insert_dt,
        ncr.update_dt,
        ncr.region_src_unique_id
    FROM new_customer_regions AS ncr
    ON CONFLICT (region_src_unique_id) DO UPDATE
        SET region_name = excluded.region_name,
            update_dt = CURRENT_DATE;

    -- Insert distinct store regions from the source table
    WITH new_store_regions AS (
        SELECT DISTINCT
            COALESCE(ss.store_region, 'n.a') AS region_name,
            COALESCE(ss.store_region_id, '-1') AS region_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE::DATE AS update_dt,
            CONCAT(ss.store_region_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS region_src_unique_id
        FROM myschema.scr_shoe_table AS ss
        WHERE NOT EXISTS (
            SELECT  1
            FROM bl_3nf.ce_regions AS cr
            WHERE
                cr.region_name = COALESCE(ss.store_region, 'n.a')
                AND cr.region_src_id = COALESCE(ss.store_region_id, '-1')
                AND cr.source_system = 'BL_3NF'
                AND cr.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_regions (
        region_name,
        region_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        region_src_unique_id
    )
    SELECT
        nsr.region_name,
        nsr.region_src_id,
        nsr.source_system,
        nsr.source_system_entity,
        nsr.insert_dt,
        nsr.update_dt,
        nsr.region_src_unique_id
    FROM new_store_regions AS nsr
    ON CONFLICT (region_src_unique_id) DO UPDATE
        SET region_name = excluded.region_name,
            update_dt = CURRENT_DATE;

    -- Insert distinct employee regions from the source table
    WITH new_employee_regions AS (
        SELECT DISTINCT
            COALESCE(ss.emp_region, 'n.a') AS region_name,
            COALESCE(ss.emp_region_id, '-1') AS region_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE::DATE AS update_dt,
            CONCAT(ss.emp_region_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS region_src_unique_id
        FROM myschema.scr_shoe_table AS ss
        WHERE NOT EXISTS (
            SELECT  1
            FROM bl_3nf.ce_regions AS cr
            WHERE
                cr.region_name = COALESCE(ss.emp_region, 'n.a')
                AND cr.region_src_id = COALESCE(ss.emp_region_id, '-1')
                AND cr.source_system = 'BL_3NF'
                AND cr.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_regions (
        region_name,
        region_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        region_src_unique_id
    )
    SELECT
        ner.region_name,
        ner.region_src_id,
        ner.source_system,
        ner.source_system_entity,
        ner.insert_dt,
        ner.update_dt,
        ner.region_src_unique_id
    FROM new_employee_regions AS ner
    ON CONFLICT (region_src_unique_id) DO nothing;
   -- UPDATE
      --  SET region_name = excluded.region_name,
          --  update_dt = CURRENT_DATE;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_regions', v_error_message, v_inserted_date);
END;
$$;
commit;


--Create function to load data from source tables to economic regions table in 3nf layer
CREATE OR REPLACE PROCEDURE bl_cl.load_economic_regions_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_economic_regions'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct economic regions for employees from the source table
    WITH new_employee_economic_regions AS (
        SELECT DISTINCT
            COALESCE(ss.emp_economic_region_id, '-1') AS economic_region_src_id,
            COALESCE(ss.emp_economic_region, 'n.a') AS economic_region_name,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE::DATE AS update_dt,
            CONCAT(ss.emp_economic_region_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS economic_region_src_unique_id
        FROM myschema.scr_shoe_table AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_economic_regions AS cer
            WHERE
                cer.economic_region = COALESCE(ss.emp_economic_region, 'n.a')
                AND cer.economic_region_src_id = COALESCE(ss.emp_economic_region_id, '-1')
                AND cer.source_system = 'BL_3NF'
                AND cer.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_economic_regions (
        economic_region_src_id,
        economic_region,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        economic_region_src_unique_id
    )
    SELECT
        ner.economic_region_src_id,
        ner.economic_region_name,
        ner.source_system,
        ner.source_system_entity,
        ner.insert_dt,
        ner.update_dt,
        ner.economic_region_src_unique_id
    FROM new_employee_economic_regions AS ner
    ON CONFLICT (economic_region_src_unique_id) DO nothing;
    --UPDATE
      --  SET economic_region = excluded.economic_region,
        --    update_dt = CURRENT_DATE;

    -- Insert distinct economic regions for customers from the source table
    WITH new_customer_economic_regions AS (
        SELECT DISTINCT
            COALESCE(ss.cust_economic_region_id, '-1') AS economic_region_src_id,
            COALESCE(ss.cust_economic_region, 'n.a') AS economic_region_name,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE::DATE AS update_dt,
            CONCAT(ss.cust_economic_region_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS economic_region_src_unique_id
        FROM myschema.scr_shoe_table AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_economic_regions AS cer
            WHERE
                cer.economic_region = COALESCE(ss.cust_economic_region, 'n.a')
                AND cer.economic_region_src_id = COALESCE(ss.cust_economic_region_id, '-1')
                AND cer.source_system = 'BL_3NF'
                AND cer.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_economic_regions (
        economic_region_src_id,
        economic_region,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        economic_region_src_unique_id
    )
    SELECT
        ncr.economic_region_src_id,
        ncr.economic_region_name,
        ncr.source_system,
        ncr.source_system_entity,
        ncr.insert_dt,
        ncr.update_dt,
        ncr.economic_region_src_unique_id
    FROM new_customer_economic_regions AS ncr
    ON CONFLICT (economic_region_src_unique_id) DO nothing; 
    --UPDATE
       -- SET economic_region = excluded.economic_region,
         --   update_dt = CURRENT_DATE;

    -- Insert distinct economic regions for stores from the source table
    WITH new_store_economic_regions AS (
        SELECT DISTINCT
            COALESCE(ss.store_economic_region_id, '-1') AS economic_region_src_id,
            COALESCE(ss.store_economic_region, 'n.a') AS economic_region_name,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            '1900-01-01'::DATE AS update_dt,
            CONCAT(ss.store_economic_region_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS economic_region_src_unique_id
        FROM myschema.scr_shoe_table AS ss
        WHERE NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_economic_regions AS cer
            WHERE
                cer.economic_region = COALESCE(ss.store_economic_region, 'n.a')
                AND cer.economic_region_src_id = COALESCE(ss.store_economic_region_id, '-1')
                AND cer.source_system = 'BL_3NF'
                AND cer.source_system_entity = 'scr_shoe_table'
        )
    )
    INSERT INTO bl_3nf.ce_economic_regions (
        economic_region_src_id,
        economic_region,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        economic_region_src_unique_id
    )
    SELECT
        nsr.economic_region_src_id,
        nsr.economic_region_name,
        nsr.source_system,
        nsr.source_system_entity,
        nsr.insert_dt,
        nsr.update_dt,
        nsr.economic_region_src_unique_id
    FROM new_store_economic_regions AS nsr
    ON CONFLICT (economic_region_src_unique_id) do nothing;
   -- DO UPDATE
      --  SET economic_region = excluded.economic_region,
          --  update_dt = CURRENT_DATE;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_economic_regions', v_error_message, v_inserted_date);
END;
$$;
commit;




CREATE OR REPLACE PROCEDURE bl_cl.load_countries_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_countries'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct countries for employees, customers, and stores from the source table
    WITH new_countries AS (
        SELECT DISTINCT
    COALESCE(ss.emp_country::VARCHAR(50), 'n.a') AS country_name,
    COALESCE(bc.region_id,   -1) AS region_id, -- Ensure this is an integer
    COALESCE(ss.emp_economic_region_id::int, -1) AS economic_region_id, -- Cast to integer
    COALESCE(ss.emp_country_id::VARCHAR(50), '-1') AS country_src_id,
    'BL_3NF' AS source_system,
    'scr_shoe_table' AS source_system_entity,
    current_date AS insert_dt,
    current_date::date AS update_dt,
    concat(ss.emp_country_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS country_src_unique_id
FROM myschema.scr_shoe_table AS ss
LEFT JOIN bl_3nf.ce_regions AS bc ON ss.emp_region_id = bc.region_src_id AND bc.source_system = 'BL_3NF'
WHERE NOT EXISTS (
    SELECT   1
    FROM bl_3nf.ce_countries AS ec
    WHERE
        ec.country_name = COALESCE(ss.emp_country::VARCHAR(50), 'n.a')
        AND ec.country_src_id = COALESCE(ss.emp_country_id::VARCHAR(50), '-1')
        AND ec.region_id = COALESCE(bc.region_id,   -1)
        AND ec.source_system = 'BL_3NF'
        AND ec.source_system_entity = 'scr_shoe_table'
)   
UNION
            -- Customer countries
            SELECT DISTINCT
                COALESCE(ss.cust_country::VARCHAR(50), 'n.a') AS country_name,
                COALESCE(bc.region_id,   -1) AS region_id,
                COALESCE(ss.cust_economic_region_id::int, -1) AS economic_region_id,
                COALESCE(ss.cust_country_id::VARCHAR(50), '-1') AS country_src_id,
                'BL_3NF' AS source_system,
                'scr_shoe_table' AS source_system_entity,
                current_date AS insert_dt,
                current_date::date AS update_dt,
                concat(ss.cust_country_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS country_src_unique_id
            FROM myschema.scr_shoe_table AS ss
            LEFT JOIN bl_3nf.ce_regions AS bc ON ss.cust_region_id = bc.region_src_id AND bc.source_system = 'BL_3NF'
            WHERE NOT EXISTS (
                SELECT   1
                FROM bl_3nf.ce_countries AS ec
                WHERE
                    ec.country_name = COALESCE(ss.cust_country::varchar, 'n.a')
                    AND ec.country_src_id = COALESCE(ss.cust_country_id::varchar, '-1')
                    AND ec.region_id = COALESCE(bc.region_id,   -1)
                    AND ec.source_system = 'BL_3NF'
                    AND ec.source_system_entity = 'scr_shoe_table'
            )
            UNION
            -- Store countries
            SELECT DISTINCT
                COALESCE(ss.store_country::VARCHAR(50), 'n.a') AS country_name,
                COALESCE(bc.region_id,   -1) AS region_id,
                COALESCE(ss.store_economic_region_id::int, -1) AS economic_region_id,
                COALESCE(ss.store_country_id::varchar, '-1') AS country_src_id,
                'BL_3NF' AS source_system,
                'scr_shoe_table' AS source_system_entity,
                current_date AS insert_dt,
                current_date::date AS update_dt,
                concat(ss.store_country_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS country_src_unique_id
            FROM myschema.scr_shoe_table AS ss
            INNER JOIN bl_3nf.ce_regions AS bc ON ss.store_region_id = bc.region_src_id AND bc.source_system = 'BL_3NF'
            LEFT JOIN bl_3nf.ce_economic_regions AS cer ON ss.store_economic_region_id::VARCHAR = cer.economic_region_id::VARCHAR
            WHERE NOT EXISTS (
                SELECT   1
                FROM bl_3nf.ce_countries AS ec
                WHERE
                    ec.country_name = COALESCE(ss.store_country::varchar, 'n.a')
                    AND ec.country_src_id = COALESCE(ss.store_country_id::VARCHAR, '-1')
                    AND ec.region_id = COALESCE(bc.region_id,   -1)
                    AND ec.source_system = 'BL_3NF'
                    AND ec.source_system_entity = 'scr_shoe_table'
            )
            )
      
    INSERT INTO bl_3nf.ce_countries (
        country_name,
        region_id,
        economic_region_id,
        country_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        country_src_unique_id
    )
    SELECT
        nc.country_name,
        nc.region_id,
        nc.economic_region_id,
        nc.country_src_id,
        nc.source_system,
        nc.source_system_entity,
        nc.insert_dt,
        nc.update_dt,
        nc.country_src_unique_id
    FROM new_countries AS nc
    ON CONFLICT (country_src_unique_id) DO nothing;
    --UPDATE
      --  SET country_name = excluded.country_name,
         --   update_dt = CURRENT_DATE;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_countries', v_error_message, v_inserted_date);
END;
$$;
commit;


CREATE OR REPLACE PROCEDURE bl_cl.load_cities_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_cities'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct cities for customers, employees, and stores from the source table
    WITH new_cities AS (
        -- Customer cities
        SELECT DISTINCT
            COALESCE(ss.cust_city, 'n.a') AS city_name,
            COALESCE(cc.country_id,  -1) AS country_id,
            COALESCE(ss.cust_city_id, '-1') AS city_src_id,
            'BL_3NF' AS source_system,
            'ce_cities' AS source_system_entity,
            current_date AS insert_dt,
            current_date::date AS update_dt,
            concat('cust_', ss.cust_city_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS city_src_unique_id
        FROM
            myschema.scr_shoe_table AS ss
        LEFT JOIN bl_3nf.ce_countries AS cc  
            ON ss.cust_country_id = cc.country_src_id
            AND cc.source_system = 'BL_3NF'
        WHERE
            NOT EXISTS (
                SELECT  1
                FROM bl_3nf.ce_cities AS cc2
                WHERE cc2.city_name = COALESCE(ss.cust_city, 'n.a')
                AND cc2.source_system = 'BL_3NF'
                AND cc2.source_system_entity = 'scr_shoe_table'
            )
        UNION  
        -- Employee cities
        SELECT DISTINCT
            COALESCE(ss.emp_city, 'n.a') AS city_name,
            COALESCE(cc.country_id,  -1) AS country_id,
            COALESCE(ss.emp_city_id, '-1') AS city_src_id,
            'BL_3NF' AS source_system,
            'ce_cities' AS source_system_entity,
            current_date AS insert_dt,
            current_date::date AS update_dt,
            concat('emp_', ss.emp_city_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS city_src_unique_id
        FROM
            myschema.scr_shoe_table AS ss
        LEFT JOIN bl_3nf.ce_countries AS cc  
            ON ss.emp_country_id = cc.country_src_id
            AND cc.source_system = 'BL_3NF'
        WHERE
            NOT EXISTS (
                SELECT  1
                FROM bl_3nf.ce_cities AS cc2
                WHERE cc2.city_name = COALESCE(ss.emp_city, 'n.a')
                AND cc2.source_system = 'BL_3NF'
                AND cc2.source_system_entity = 'ce_cities'
            )
        UNION  
        -- Store cities
        SELECT DISTINCT
            COALESCE(ss.store_city, 'n.a') AS city_name,
            COALESCE(cc.country_id,  -1) AS country_id,
            COALESCE(ss.store_city_id, '-1') AS city_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            current_date AS insert_dt,
            current_date::date AS update_dt,
            concat('store_', ss.store_city_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS city_src_unique_id
        FROM
            myschema.scr_shoe_table AS ss
        LEFT JOIN bl_3nf.ce_countries AS cc  
            ON ss.store_country_id = cc.country_src_id
            AND cc.source_system = 'BL_3NF'
        WHERE
            NOT EXISTS (
                SELECT  1
                FROM bl_3nf.ce_cities AS cc2
                WHERE cc2.city_name = COALESCE(ss.store_city, 'n.a')
                AND cc2.source_system = 'BL_3NF'
                AND cc2.source_system_entity = 'scr_shoe_table'
            )
    )
    INSERT INTO bl_3nf.ce_cities (
        city_name,
        country_id,
        city_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        city_src_unique_id
    )
    SELECT
        nc.city_name,
        nc.country_id,
        nc.city_src_id,
        nc.source_system,
        nc.source_system_entity,
        nc.insert_dt,
        nc.update_dt,
        nc.city_src_unique_id
    FROM new_cities AS nc
    ON CONFLICT (city_src_unique_id) DO UPDATE
        SET city_name = excluded.city_name,
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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_cities', v_error_message, v_inserted_date);

END;
$$;
commit;


CREATE OR REPLACE PROCEDURE bl_cl.load_addresses_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_addresses'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct addresses for customers, employees, and stores from the source table
    WITH new_addresses AS (
        -- Employee addresses
        SELECT DISTINCT ON (address_src_unique_id)
            COALESCE(ss.emp_address, 'n.a') AS address,
            COALESCE(ss.emp_code, 'n.a') AS zipcode,
            COALESCE(bcc.city_id,  -1) AS city_id,
            COALESCE(ss.emp_address_id, '-1') AS address_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            current_date AS insert_dt,
            current_date::date AS update_dt,
            concat(ss.emp_address_id, '_', bcc.city_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS address_src_unique_id
       -- FROM
          --  myschema.scr_shoe_table AS ss
          FROM (
            SELECT emp_address_id, emp_address, emp_city_id, emp_code
            FROM myschema.scr_shoe_table
            UNION ALL
            SELECT emp_address_id, emp_address, emp_city_id, emp_code
            FROM myschema.scr_shoe_table2
        ) AS ss    
          LEFT JOIN bl_3nf.ce_cities AS bcc  
            ON ss.emp_city_id = bcc.city_src_id
            AND bcc.source_system = 'BL_3NF'
        WHERE
            NOT EXISTS (
                SELECT  1
                FROM bl_3nf.ce_addresses AS ca
                WHERE
                    ca.address = COALESCE(ss.emp_address, 'n.a')
                    AND ca.address_src_id = COALESCE(ss.emp_address_id, '-1')
                    AND ca.zipcode = COALESCE(ss.emp_code, 'n.a')
                    AND ca.city_id = COALESCE(bcc.city_id,  -1)
                    AND ca.source_system = 'BL_3NF'
                    AND ca.source_system_entity = 'scr_shoe_table'
            )
        UNION
        -- Customer addresses
        SELECT DISTINCT ON (address_src_unique_id)
            COALESCE(ss.cust_address, 'n.a') AS address,
            COALESCE(ss.cust_code, 'n.a') AS zipcode,
            COALESCE(bcc.city_id,  -1) AS city_id,
            COALESCE(ss.cust_address_id, '-1') AS address_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            current_date AS insert_dt,
            current_date::date AS update_dt,
            concat(ss.cust_address_id, '_', bcc.city_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS address_src_unique_id
        --FROM
        --    myschema.scr_shoe_table AS ss
        FROM (
            SELECT cust_address_id, cust_address, cust_city_id, cust_code
            FROM myschema.scr_shoe_table
            UNION ALL
            SELECT cust_address_id, cust_address, cust_city_id, cust_code
            FROM myschema.scr_shoe_table2
        ) AS ss    
          LEFT JOIN bl_3nf.ce_cities AS bcc  
            ON ss.cust_city_id = bcc.city_src_id
            AND bcc.source_system = 'BL_3NF'
        WHERE
            NOT EXISTS (
                SELECT  1
                FROM bl_3nf.ce_addresses AS ca
                WHERE
                    ca.address = COALESCE(ss.cust_address, 'n.a')
                    AND ca.address_src_id = COALESCE(ss.cust_address_id, '-1')
                    AND ca.zipcode = COALESCE(ss.cust_code, 'n.a')
                    AND ca.city_id = COALESCE(bcc.city_id,  -1)
                    AND ca.source_system = 'BL_3NF'
                    AND ca.source_system_entity = 'scr_shoe_table'
            )
        UNION
        -- Store addresses
        SELECT DISTINCT ON (address_src_unique_id)
            COALESCE(ss.store_address, 'n.a') AS address,
            COALESCE(ss.store_code, 'n.a') AS zipcode,
            COALESCE(bcc.city_id,  -1) AS city_id,
            COALESCE(ss.store_address_id, '-1') AS address_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            current_date AS insert_dt,
            current_date::date AS update_dt,
            concat(ss.store_address_id, '_', bcc.city_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS address_src_unique_id
       -- FROM
          --  myschema.scr_shoe_table AS ss
            FROM (
            SELECT store_address_id, store_address, store_city_id, store_code
            FROM myschema.scr_shoe_table
            UNION ALL
            SELECT store_address_id, store_address, store_city_id, store_code
            FROM myschema.scr_shoe_table2
        ) AS ss      
          LEFT JOIN bl_3nf.ce_cities AS bcc  
            ON ss.store_city_id = bcc.city_src_id
            AND bcc.source_system = 'BL_3NF'
        WHERE
            NOT EXISTS (
                SELECT  1
                FROM bl_3nf.ce_addresses AS ca
                WHERE
                    ca.address = COALESCE(ss.store_address, 'n.a')
                    AND ca.address_src_id = COALESCE(ss.store_address_id, '-1')
                    AND ca.zipcode = COALESCE(ss.store_code, 'n.a')
                    AND ca.city_id = COALESCE(bcc.city_id,  -1)
                    AND ca.source_system = 'BL_3NF'
                    AND ca.source_system_entity = 'scr_shoe_table'
            )
    )
    INSERT INTO bl_3nf.ce_addresses (
        address,
        zipcode,
        city_id,
        address_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        address_src_unique_id
    )
    SELECT
        na.address,
        na.zipcode,
        na.city_id,
        na.address_src_id,
        na.source_system,
        na.source_system_entity,
        na.insert_dt,
        na.update_dt,
        na.address_src_unique_id
    FROM new_addresses AS na
    ORDER BY address_src_unique_id, update_dt DESC
    ON CONFLICT (address_src_unique_id) DO nothing;
     
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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_addresses', v_error_message, v_inserted_date);
END;
$$;
commit;




CREATE OR REPLACE PROCEDURE bl_cl.load_customers_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_customers'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct customers from the source table
    INSERT INTO bl_3nf.ce_customers (
        first_name,
        last_name,
        cust_phone,
        address_id,
        customer_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        customer_src_unique_id
    )
    SELECT DISTINCT ON (customer_src_unique_id)
        COALESCE(ss.cust_first_name, 'n.a') AS first_name,
        COALESCE(ss.cust_last_name, 'n.a') AS last_name,
        COALESCE(ss.cust_phone, 'n.a') AS cust_phone,
        COALESCE(ss.cust_address_id, '-1')::INT AS address_id, -- Adjusted column name
        COALESCE(ss.cust_id, '-1') AS customer_src_id,
        'BL_3NF' AS source_system,
        'scr_shoe_table' AS source_system_entity,
        CURRENT_DATE AS insert_dt,
        CURRENT_DATE::date AS update_dt,
        CONCAT(ss.cust_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS customer_src_unique_id
    FROM (
        SELECT cust_first_name, cust_last_name, cust_phone, cust_address_id AS cust_address_id, cust_id
        FROM myschema.scr_shoe_table
        UNION ALL
        SELECT cust_first_name, cust_last_name, cust_phone, cust_address_id AS cust_address_id, cust_id
        FROM myschema.scr_shoe_table2
    ) AS ss      
    LEFT JOIN bl_3nf.ce_addresses AS ca2 ON ss.cust_address_id = ca2.address_src_id AND ca2.source_system = 'BL_3NF'
    WHERE
        NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_customers AS ccu
            WHERE
                ccu.first_name = COALESCE(ss.cust_first_name, 'n.a')
                AND ccu.last_name = COALESCE(ss.cust_last_name, 'n.a')
                AND ccu.address_id = COALESCE(ss.cust_address_id::INT,   -1) -- Adjusted column name
                AND ccu.cust_phone = COALESCE(ss.cust_phone, 'n.a')
                AND ccu.customer_src_id = COALESCE(ss.cust_id::VARCHAR, '-1')
                AND ccu.source_system = 'BL_3NF'
                AND ccu.source_system_entity = 'scr_shoe_table'
        )
    ORDER BY customer_src_unique_id, update_dt DESC;

 
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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_customers', v_error_message, v_inserted_date);
END;
$$;
commit;



CREATE OR REPLACE procedure  bl_cl.load_employess_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_employess'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
  

    -- Insert distinct employees from the source table
    INSERT INTO bl_3nf.ce_employess (
        emp_first_name,
        emp_last_name,
        emp_phone,
        address_id,
        employee_src_id,
        source_system,
        source_system_entity,
        insert_dt,
        update_dt,
        employee_src_unique_id
    )
    SELECT DISTINCT ON (employee_src_unique_id)
        COALESCE(ss.emp_first_name, 'n.a') AS emp_first_name,
        COALESCE(ss.emp_last_name, 'n.a') AS emp_last_name,
        COALESCE(ss.emp_phone, 'n.a') AS emp_phone,
        COALESCE(ss.emp_address_id, '-1')::INT AS address_id,
        COALESCE(ss.emp_id, '-1') AS employee_src_id,
        'BL_3NF' AS source_system,
        'scr_shoe_table' AS source_system_entity,
        CURRENT_DATE AS insert_dt,
       CURRENT_DATE::date AS update_dt,
        CONCAT(ss.emp_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS employee_src_unique_id
  --  FROM
     --   myschema.scr_shoe_table AS ss
        FROM (
        SELECT emp_first_name, emp_last_name, emp_phone, emp_address_id AS emp_address_id, emp_id
        FROM myschema.scr_shoe_table
        UNION ALL
        SELECT emp_first_name, emp_last_name, emp_phone, emp_address_id AS emp_address_id, emp_id
        FROM myschema.scr_shoe_table2
    ) AS ss      
        
    LEFT JOIN bl_3nf.ce_addresses AS ca2 ON ss.emp_address_id = ca2.address_src_id AND ca2.source_system = 'BL_3NF'
    WHERE
        NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_employess AS cce
            WHERE
                cce.emp_first_name = COALESCE(ss.emp_first_name, 'n.a')
                AND cce.emp_last_name = COALESCE(ss.emp_last_name, 'n.a')
                AND cce.emp_phone = COALESCE(ss.emp_phone, 'n.a')
                AND cce.address_id = COALESCE(ss.emp_address_id::INT,   -1)
                AND cce.employee_src_id = COALESCE(ss.emp_id::VARCHAR, '-1')
                AND cce.source_system = 'BL_3NF'
                AND cce.source_system_entity = 'scr_shoe_table'
        )
    ORDER BY employee_src_unique_id, update_dt DESC;

    -- Update existing employees
    UPDATE bl_3nf.ce_employess
    SET emp_first_name = excluded.emp_first_name,
        emp_last_name = excluded.emp_last_name,
        emp_phone = excluded.emp_phone,
        update_dt = current_date
    FROM (
        SELECT DISTINCT ON (employee_src_unique_id)
            COALESCE(ss.emp_first_name, 'n.a') AS emp_first_name,
            COALESCE(ss.emp_last_name, 'n.a') AS emp_last_name,
            COALESCE(ss.emp_phone, 'n.a') AS emp_phone,
            COALESCE(ss.emp_address_id, '-1')::INT AS address_id,
            COALESCE(ss.emp_id, '-1') AS employee_src_id,
            'BL_3NF' AS source_system,
            'scr_shoe_table' AS source_system_entity,
            CURRENT_DATE AS insert_dt,
            CURRENT_DATE::date AS update_dt,
            CONCAT(ss.emp_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS employee_src_unique_id
        FROM
            myschema.scr_shoe_table AS ss
          LEFT JOIN bl_3nf.ce_addresses AS ca2 ON ss.emp_address_id = ca2.address_src_id AND ca2.source_system = 'BL_3NF'
        ORDER BY employee_src_unique_id, update_dt DESC
    ) AS excluded
    WHERE bl_3nf.ce_employess.employee_src_unique_id = excluded.employee_src_unique_id;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_employess', v_error_message, v_inserted_date);
END;
$$;
commit;




CREATE OR REPLACE PROCEDURE bl_cl.load_store_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_stores'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
 
INSERT INTO bl_3nf.ce_stores (
    store_address_id,
    store_src_id,
    source_system,
    source_system_entity,
    store_name,
    emp_id,
    insert_dt,
    update_dt,
    stores_src_unique_id
)
SELECT DISTINCT ON (stores_src_unique_id)
    COALESCE(ss.store_address_id, '-1')::INT AS store_address_id,
    COALESCE(ss.store_id, '-1') AS store_src_id,
    'BL_3NF' AS source_system,
    'ce_stores' AS source_system_entity,
    COALESCE(ss.store_name, 'n.a') AS store_name,
    COALESCE(ss.emp_id::varchar,   '-1') AS emp_id,
    CURRENT_DATE AS insert_dt,
    CURRENT_DATE::date AS update_dt,
    CONCAT(ss.store_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS stores_src_unique_id
--FROM
 --   myschema.scr_shoe_table AS ss
      FROM (
        SELECT store_address_id, store_id, store_name, emp_id 
        FROM myschema.scr_shoe_table
        UNION ALL
        SELECT  store_address_id, store_id, store_name, emp_id 
        FROM myschema.scr_shoe_table2
    ) AS ss      
    
WHERE
    NOT EXISTS (
        SELECT   1
        FROM bl_3nf.ce_stores AS cs
        WHERE
            cs.store_address_id = COALESCE(ss.store_address_id::INT,   -1)
            AND cs.store_src_id = COALESCE(ss.store_id::varchar, '-1')
            AND cs.source_system = 'BL_3NF'
            AND cs.source_system_entity = 'ce_stores'
    )
ORDER BY stores_src_unique_id, update_dt DESC;

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
        CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_stores', v_error_message, v_inserted_date);
END;
$$;
commit;



CREATE OR REPLACE PROCEDURE bl_cl.load_discounts_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_default_row_exists BOOLEAN;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_discount'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert distinct discounts from the source table

INSERT INTO bl_3nf.ce_discount (
    DISCOUNT_name,
    DISCOUNT_PRC,
    discount_src_id,
    source_system,
    source_system_entity,
    insert_dt,
    update_dt,
    discount_src_unique_id
)
SELECT DISTINCT ON (discount_src_unique_id)
    COALESCE(ss.DISCOUNT_name, 'n.a') AS DISCOUNT_name,
    COALESCE(ss.DISCOUNT_PRC,   0) AS DISCOUNT_PRC, -- Assuming   0 is a valid default discount percentage
    COALESCE(ss.discount_id, '-1') AS discount_src_id,
    'BL_3NF' AS source_system,
    'scr_shoe_table' AS source_system_entity,
    CURRENT_DATE AS insert_dt,
    CURRENT_DATE AS update_dt,
    CONCAT(ss.discount_id, '_', 'BL_3NF', '_', 'scr_shoe_table') AS discount_src_unique_id
--FROM myschema.scr_shoe_table AS ss
      FROM (
        SELECT DISCOUNT_name, DISCOUNT_PRC, discount_id, emp_id 
        FROM myschema.scr_shoe_table
        UNION ALL
        SELECT  DISCOUNT_name, DISCOUNT_PRC, discount_id, emp_id 
        FROM myschema.scr_shoe_table2
    ) AS ss      
WHERE
    NOT EXISTS (
        SELECT   1
        FROM bl_3nf.ce_discount AS bd
        WHERE
            bd.discount_src_unique_id = CONCAT(ss.discount_id, '_', 'BL_3NF', '_', 'scr_shoe_table')
    )
ON CONFLICT (discount_src_unique_id) DO UPDATE   
    SET DISCOUNT_name = excluded.DISCOUNT_name,
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
   
            CALL bl_cl.logging_info(v_user_info, v_table_name, 'BL_3NF' || 'ce_discount', v_error_message, v_inserted_date);
END;
$$;
commit;



CREATE OR REPLACE PROCEDURE bl_cl.load_payments_data()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted_count INT;
    v_text_message VARCHAR; -- Variable for constructing the log message
    v_error_message VARCHAR; -- Variable for capturing error messages
    v_user_info VARCHAR := CURRENT_USER; -- Current user information
    v_table_name VARCHAR := 'ce_payments'; -- Table name for logging
    v_inserted_date DATE := CURRENT_DATE; -- Date for logging
BEGIN
    -- Insert payments from the source table
    INSERT INTO bl_3nf.ce_payments (
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
        COALESCE(ces2.store_id,   -1) as store_id,
        COALESCE(cec.customer_id,   -1) as customer_id,
        COALESCE(cee.emp_id,   -1) as emp_id,
        COALESCE(cp.product_src_id,   -1) asproduct_src_id,
        COALESCE(cd.discount_id,   -1) as disocount_id,
        COALESCE(ss.amount,   -1) as amount,
        cd.discount_prc  AS discount_prc,
        COALESCE(ss.quantity_sold,   -1) as quantity_sold,
        COALESCE(ss.sale_date::date, '1900-01-01') AS sale_date,
        'BL_3NF'as source_system,
        'scr_shoe_table' as source_system_entity,
        CURRENT_DATE as insert_dt
     --   from myschema.scr_shoe_table AS ss
    
      FROM (
        SELECT * FROM myschema.scr_shoe_table
        UNION ALL
        SELECT * FROM myschema.scr_shoe_table2

    ) AS ss  
 
    LEFT JOIN bl_3nf.ce_products AS cp ON cp.product_src_id = ss.product_src_id AND cp.source_system = 'BL_3NF'
    LEFT JOIN bl_3nf.ce_employess AS cee ON cee.employee_src_id::NUMERIC(8,2) = ss.emp_id::NUMERIC(8,2) AND cee.source_system = 'BL_3NF'
    LEFT JOIN bl_3nf.ce_customers AS cec ON cec.customer_src_id::NUMERIC(8,2) = ss.cust_id::NUMERIC(8,2) AND cec.source_system = 'BL_3NF'
    LEFT JOIN bl_3nf.ce_stores AS ces2 ON ces2.store_src_id::NUMERIC(8,2) = ss.store_id::NUMERIC(8,2) AND ces2.source_system = 'BL_3NF'
    LEFT JOIN bl_3nf.ce_discount AS cd ON cd.discount_src_id = ss.discount_id AND cd.source_system = 'BL_3NF'
    WHERE
        NOT EXISTS (
            SELECT   1
            FROM bl_3nf.ce_payments AS cs1
            WHERE cs1.sale_date = COALESCE(ss.sale_date::date, '1900-01-01')
                AND cs1.product_src_id = COALESCE(cp.product_src_id,   -1)
                AND cs1.emp_id = COALESCE(cee.emp_id::NUMERIC(8,2), '-1')
                AND cs1.customer_id = COALESCE(cec.customer_id::NUMERIC(8,2),   -1)
                AND cs1.store_id = COALESCE(ces2.store_id::NUMERIC(8,2),   -1)
                AND cs1.source_system = 'BL_3NF'
                AND cs1.source_system_entity = 'scr_shoe_table'
        )
    OR
        ss.product_src_id IS NULL;

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
