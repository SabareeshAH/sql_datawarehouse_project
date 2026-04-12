
/*
======================================================================
Performing Bulk insert for Silver Layet from the Bronze Layer Tables 
using stored procedure method 
======================================================================
*/

--creating the stored procedure for silver layer
CREATE OR REPLACE PROCEDURE silver.load_silver()
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
	RAISE NOTICE 'Silver Layer Processing Started!';
	RAISE NOTICE '===========================================';

	RAISE NOTICE '-------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '-------------------------------------------';
	
	-- Table 1: crm_cust_info
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	RAISE NOTICE '>>> Inserting Data into silver.crm_cust_info from bronze.crm_cust_info';
	INSERT INTO silver.crm_cust_info(cst_id, 
	cst_key, 
	cst_firstname, 
	cst_lastname, 
	cst_martial_status, 
	cst_gender, 
	cst_create_date)
	SELECT cst_id, 
	cst_key, 
	TRIM(cst_firstname) as cst_firstname, --TRIM WILL BE USED TO REMOVE THE LEADING AND TRAILING WHITE SPACES
	TRIM(cst_lastname) as cst_lastname, 
	CASE 
		WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single' -- USING THE CASE CHECK TO CONVERT TO MARRIED OR SINGLE BASED ON M / S
		WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
		ELSE 'NA'
	END AS cst_martial_status,
	CASE 
		WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male' -- USING THE CASE CHECK TO CONVERT TO MALE OR FEMALE BASED ON M / F 
		WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		ELSE 'NA'
	END AS cst_gender,
	cst_create_date
	FROM (
	SELECT *, 
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag -- TAKING THE 1ST RANK AS THE VALID BASED ON CREATE_DATE TO IGNORE THE NULL VALUES 
	FROM bronze.crm_cust_info) AS T1
	WHERE Flag=1; -- SELECT MOST RECENT RECORD AS PER CUSTOMER RECORD

	RAISE NOTICE '>>> Insertion Completed : silver.crm_cust_info';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> silver.crm_cust_info Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 2: crm_prd_info
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	RAISE NOTICE '>>> Inserting Data into silver.crm_prd_info from bronze.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, --Extracting the first 5 characters from prd_key, those will be the category id. Also replacing the - with _
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, --Extracting the Second Part which is prd_key
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost, --Replacing NULL with value 0 using COALESCE function
	CASE UPPER(TRIM(prd_line)) --Replacing with the required Abbreviation
		WHEN 'M' THEN 'Mountain'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'R' THEN 'Road'
		WHEN 'T' THEN 'Touring'
		ELSE 'NA'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt --Using the (next row date - 1) as end date for the current row 
	FROM bronze.crm_prd_info;

	RAISE NOTICE '>>> Insertion Completed : silver.crm_prd_info';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> silver.crm_prd_info Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 3: crm_sales_details
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	
	RAISE NOTICE '>>> Inserting Data into silver.crm_sales_details from bronze.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
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
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	--Transformation Rule for Numerical Columns
	--When Sales is Negative, Zero, Null derive it using Quantity and Price
	--When Price is Negative, Zero, Null derive it using Quantity and Sales
	--If Price is Negative convert to postive
	CASE WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity,0) -- NULLIF is used to reduce the divide by zero risk
		 ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	
	RAISE NOTICE '>>> Insertion Completed silver.crm_sales_details';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> silver.crm_sales_details Table Load Duration (in Seconds) : %', diff_sec;

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'CRM Table Processing Completed Successfully!';
	RAISE NOTICE '----------------------------------------------';

	RAISE NOTICE '-------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '-------------------------------------------';
	
	-- Table4: erp_cust_az12
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	RAISE NOTICE '>>> Inserting Data into silver.erp_cust_az12 from bronze.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gender
	)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) --Extracting the cst_key from cid column, it will be useful to connect to cust_info table. Removig NAS Prefix
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > NOW() THEN NULL --If birthdate is greater than curret date, it is considered as NULL Because Future values will not be applicable
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male' --Handling the Transformation for gender column with M for Male, F for Female, other values for NA
		 WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
		 ELSE 'NA'
	END AS gender
	FROM bronze.erp_cust_az12;

	RAISE NOTICE '>>> Insertion Completed silver.erp_cust_az12';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> silver.erp_cust_az12 Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 5: erp_loc_a101
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	RAISE NOTICE '>>> Inserting Data into silver.erp_loc_a101 from bronze.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
	cid, cntry
	)
	SELECT 
	REPLACE(cid,'-',''), --Removing the '-' from the column so that we can join it with cst_key from cust_info table
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'NA'
		 ELSE TRIM(cntry)
	END AS cntry 
	FROM bronze.erp_loc_a101;

	RAISE NOTICE '>>> Insertion Completed : silver.erp_loc_a101';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> silver.erp_loc_a101 Table Load Duration (in Seconds) : %', diff_sec;
	
	-- Table 6: erp_px_cat_g1v2
	start_time := clock_timestamp();
	RAISE NOTICE '>>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	RAISE NOTICE '>>> Inserting Data into silver.erp_px_cat_g1v2 from bronze.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
	id, cat, subcat, maintenance
	)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;

	RAISE NOTICE '>>> Insertion Completed : silver.erp_px_cat_g1v2';
	end_time := clock_timestamp();
	diff_sec := EXTRACT(EPOCH FROM (end_time - start_time));
	RAISE NOTICE '>>> silver.erp_px_cat_g1v2 Table Load Duration (in Seconds) : %', diff_sec;

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'ERP Table Processing Completed Successfully!';
	RAISE NOTICE '----------------------------------------------';
	
	RAISE NOTICE '===========================================';
	RAISE NOTICE 'Silver Layer Table Processing Completed!!!';
	RAISE NOTICE '===========================================';
	
b_end_time := clock_timestamp();
b_diff_sec := EXTRACT(EPOCH FROM (b_end_time-b_start_time));
RAISE NOTICE '>>> Total Silver Layer Loading and Processing Time(In Seconds) : %', b_diff_sec;
EXCEPTION
    WHEN OTHERS THEN
		RAISE NOTICE 'Error in Loading Silver Layer!!!';
        RAISE NOTICE 'Error: %, SQLSTATE: %', SQLERRM, SQLSTATE;
END;
$$;

call silver.load_silver();

--checking
select count(*) from silver.crm_cust_info;
select count(*) from silver.crm_prd_info;
select count(*) from silver.crm_sales_details;
select count(*) from silver.erp_cust_az12;
select count(*) from silver.erp_loc_a101;
select count(*) from silver.erp_px_cat_g1v2;
