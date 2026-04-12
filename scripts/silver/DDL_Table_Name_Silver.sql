/*
=======================================================================
DDL Script for Silver Layer.

Table schemas will be same as of Bronze layer. Along with that Metadata 
will be added by data engineer which consists of 
create_date: record load timestamp, 
update_date: record's last update timestamp, 
source_system: origin system of record, 
file_location: file source of record.

=======================================================================
*/

-- creating the table for dataset cust_info (Customer Information) under crm source system
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(20),
	cst_firstname VARCHAR(25),
	cst_lastname VARCHAR(25),
	cst_martial_status VARCHAR(15),
	cst_gender VARCHAR(15),
	cst_create_date DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

-- creating the table for dataset prd_info (Product Information) under crm source system
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id VARCHAR(10),
	prd_key VARCHAR(20),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(15),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

-- creating the table for dataset sales_details under crm source system
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num VARCHAR(30),
	sls_prd_key VARCHAR(35),
	sls_cust_id BIGINT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales BIGINT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

-- creating the table for dataset CUST_AZ12 (Related To Customer) under erp source system
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE,
	gender VARCHAR(15),
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

-- creating the table for dataset LOC_A101 (Related to location) under erp source system
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

-- creating the table for dataset PX_CAT_G1V2 (Related to Products Info) under erp source system
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id VARCHAR(10),
	cat VARCHAR(30),
	subcat VARCHAR(50),
	maintenance VARCHAR(5),
	dwh_create_date DATE DEFAULT CURRENT_DATE
);