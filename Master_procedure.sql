CREATE OR REPLACE
PROCEDURE bl_cl.master_procedure()
LANGUAGE plpgsql
AS $$
BEGIN
	

	

--loading external tables

call bl_cl.load_src_shoe();
call bl_cl.load_src_shoe2();

-- Load to 3NF layer

CALL bl_cl.load_3nf_sequences();
	
CALL bl_cl.load_category_data();
     
CALL bl_cl.load_shoe_brand_data();
  
CALL bl_cl.load_colours_data();
 
CALL bl_cl.load_sizes_data();

CALL bl_cl.load_material_data_3nf();

CALL bl_cl.load_products_data();

CALL bl_cl.load_regions_data();

CALL bl_cl.load_economic_regions_data();

CALL bl_cl.load_countries_data();

CALL bl_cl.load_cities_data();

CALL bl_cl.load_addresses_data();

CALL bl_cl.load_customers_data();

CALL bl_cl.load_employess_data();

CALL bl_cl.load_store_data();

CALL bl_cl.load_discounts_data();

CALL bl_cl.load_payments_data();

---------------------------------------------
   -- Load to dimension Layer
CALL  bl_cl.insert_dim_products_scd();
	
CALL bl_cl.insert_dim_customers();
	
CALL bl_cl.insert_dim_stores();
	
CALL bl_cl.insert_dim_employess();
	
CALL bl_cl.load_dim_payments_data();

END 
$$;

      CALL  bl_cl.master_procedure();

