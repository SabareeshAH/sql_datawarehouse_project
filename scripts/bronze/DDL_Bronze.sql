/*
======================================================================
Creating DDL for the Source system
======================================================================

Note: Follow the naming conventions while creating the table and 
column names.
*/

-- creating the table for dataset cust_info (Customer Information) under crm source system
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(20),
	cst_firstname VARCHAR(25),
	cst_lastname VARCHAR(25),
	cst_martial_status VARCHAR(3),
	cst_gender VARCHAR(3),
	cst_create_date DATE
);

-- creating the table for dataset prd_info (Product Information) under crm source system
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(35),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(3),
	prd_start_dt DATE,
	prd_end_dt DATE
);

-- creating the table for dataset sales_details under crm source system
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(30),
	sls_prd_key VARCHAR(35),
	sls_cust_id BIGINT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales BIGINT,
	sls_quantity INT,
	sls_price INT
);

-- creating the table for dataset CUST_AZ12 (Related To Customer) under erp source system
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE,
	gender VARCHAR(15)
);

-- creating the table for dataset LOC_A101 (Related to location) under erp source system
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

-- creating the table for dataset PX_CAT_G1V2 (Related to Products Info) under erp source system
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(10),
	cat VARCHAR(30),
	subcat VARCHAR(50),
	maintenance VARCHAR(5)
);