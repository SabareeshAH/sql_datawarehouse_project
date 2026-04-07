
/*
======================================================================
Performing Bulk insert from the csv file using stored procedure method 
======================================================================
*/

--creating the stored procedure for Bronze layer
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	b_start_time TIMESTAMP;
    b_end_time   TIMESTAMP;
    b_diff_sec   NUMERIC;
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    diff_sec   NUMERIC;
BEGIN
b_start_time := clock_timestamp();
	RAISE NOTICE '===========================================';
	RAISE NOTICE 'Bronze Layer Processing Started!';
	RAISE NOTICE '===========================================';

	RAISE NOTICE '-------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '-------------------------------------------';
	
	-- Table 1: crm_cust_info
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	RAISE NOTICE '>>> Inserting Data into:  crm_cust_info';
	COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_martial_status, cst_gender, cst_create_date)
	FROM 'C:\Users\Lenovo\Documents\Data Project\Data Warehouse\sql_datawarehouse_project\datasets\source_crm\cust_info.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>>> Processing Completed : crm_cust_info';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> crm_cust_info Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 2: crm_prd_info
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	RAISE NOTICE '>>> Inserting Data into:  crm_prd_info';
	COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	FROM 'C:\Users\Lenovo\Documents\Data Project\Data Warehouse\sql_datawarehouse_project\datasets\source_crm\prd_info.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>>> Processing Completed : crm_prd_info';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> crm_prd_info Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 3: crm_sales_details
	-- handling this alone like this to reduce the redudant data
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	
	-- STEP1: creating the temp table with text datatype
	CREATE TABLE temp_table (
	    sls_ord_num     TEXT,
	    sls_prd_key     TEXT,
	    sls_cust_id     TEXT,
	    sls_order_dt    TEXT,
	    sls_ship_dt     TEXT,
	    sls_due_dt      TEXT,
	    sls_sales       TEXT,
	    sls_quantity    TEXT,
	    sls_price       TEXT
	);
	
	-- STEP 2: pushing to the temp table
	COPY temp_table (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	FROM 'C:\Users\Lenovo\Documents\Data Project\Data Warehouse\sql_datawarehouse_project\datasets\source_crm\sales_details.csv'
	DELIMITER ','
	CSV HEADER
	NULL '';
	
	-- STEP 3: Insert into final table with transformations
	RAISE NOTICE '>>> Inserting Data into:  crm_sales_details';
	INSERT INTO bronze.crm_sales_details (
	    sls_ord_num,
	    sls_prd_key,
	    sls_cust_id,
	    sls_order_dt,
	    sls_ship_dt,
	    sls_due_dt,
	    sls_sales,
	    sls_quantity,
	    sls_price
	)
	SELECT
	    sls_ord_num,
	
	    CASE WHEN sls_prd_key ~ '^[0-9]+$' THEN sls_prd_key::BIGINT END,
	    CASE WHEN sls_cust_id ~ '^[0-9]+$' THEN sls_cust_id::BIGINT END,
	
	    -- Safe date parsing
	    CASE WHEN sls_order_dt ~ '^[0-9]{8}$'
	         THEN TO_DATE(sls_order_dt, 'YYYYMMDD') END,
	
	    CASE WHEN sls_ship_dt ~ '^[0-9]{8}$'
	         THEN TO_DATE(sls_ship_dt, 'YYYYMMDD') END,
	
	    CASE WHEN sls_due_dt ~ '^[0-9]{8}$'
	         THEN TO_DATE(sls_due_dt, 'YYYYMMDD') END,
	
	    NULLIF(sls_sales, '')::NUMERIC,
	    NULLIF(sls_quantity, '')::INT,
	    NULLIF(sls_price, '')::NUMERIC
	
	FROM temp_table;
	
	-- STEP 4: Dropping the Temporary Table
	DROP TABLE temp_table;

	RAISE NOTICE '>>> Processing Completed : crm_sales_details';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> crm_sales_details Table Load Duration (in Seconds) : %', diff_sec;

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'CRM Table Processing Completed Successfully!';
	RAISE NOTICE '----------------------------------------------';

	RAISE NOTICE '-------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '-------------------------------------------';
	
	-- Table4: erp_cust_az12
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	RAISE NOTICE '>>> Inserting Data into:  erp_cust_az12';
	COPY bronze.erp_cust_az12 (cid, bdate, gender)
	FROM 'C:\Users\Lenovo\Documents\Data Project\Data Warehouse\sql_datawarehouse_project\datasets\source_erp\CUST_AZ12.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>>> Processing Completed : erp_cust_az12';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> erp_cust_az12 Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 5: erp_loc_a101
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	RAISE NOTICE '>>> Inserting Data into:  erp_loc_a101';
	COPY bronze.erp_loc_a101 (cid, cntry)
	FROM 'C:\Users\Lenovo\Documents\Data Project\Data Warehouse\sql_datawarehouse_project\datasets\source_erp\LOC_A101.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>>> Processing Completed : erp_loc_a101';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> erp_loc_a101 Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 6: erp_px_cat_g1v2
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	RAISE NOTICE '>>> Inserting Data into:  erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	FROM 'C:\Users\Lenovo\Documents\Data Project\Data Warehouse\sql_datawarehouse_project\datasets\source_erp\PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>>> Processing Completed : erp_px_cat_g1v2';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> erp_px_cat_g1v2 Table Load Duration (in Seconds) : %', diff_sec;

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'ERP Table Processing Completed Successfully!';
	RAISE NOTICE '----------------------------------------------';
	
	RAISE NOTICE '===========================================';
	RAISE NOTICE 'Bronze Layer Table Processing Completed!!!';
	RAISE NOTICE '===========================================';
	
b_end_time := clock_timestamp();
b_diff_sec := EXTRACT(EPOCH FROM (b_end_time-b_start_time));
RAISE NOTICE '>>> Total Bronze Layer Loading and Processing Time(In Seconds) : %', b_diff_sec;
EXCEPTION
    WHEN OTHERS THEN
		RAISE NOTICE 'Error in Loading Bronze Layer!!!';
        RAISE NOTICE 'Error: %, SQLSTATE: %', SQLERRM, SQLSTATE;
END;
$$;

call bronze.load_bronze();

--checking
select count(*) from bronze.crm_cust_info;
select count(*) from bronze.crm_prd_info;
select count(*) from bronze.crm_sales_details;
select count(*) from bronze.erp_cust_az12;
select count(*) from bronze.erp_loc_a101;
select count(*) from bronze.erp_px_cat_g1v2;
