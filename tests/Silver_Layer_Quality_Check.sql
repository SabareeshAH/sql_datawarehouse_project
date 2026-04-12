/*
======================================================================
Quality Check: Silver Layer
NULL Values, DISTINCT Values, Missing Data, Redudant Data are checked
after the insertion from Bronze layer
======================================================================
*/

/*
======================================
Table 1:silver.crm_cust_info
======================================
*/

--Quality Check for Data Inserted in Silver Layer

--Checking for NULL and Dupilcate values in primary key of  crm_cust_info table from bronze layer 
--Expectation: No NULL or Duplicate values should be present
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL;

--Checking for unwanted spaces in text(string) columns
--Expectation: No unwanted spacces should present (No result)
SELECT * FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname) OR 
cst_lastname != TRIM(cst_lastname) OR
cst_martial_status != TRIM(cst_martial_status) OR 
cst_gender != TRIM(cst_gender);

--checking the Data Consistency from the gender and Martial_status columns
--Expectation: Only Valid Values should come
SELECT DISTINCT(cst_martial_status) FROM silver.crm_cust_info;

SELECT DISTINCT(cst_gender) FROM silver.crm_cust_info;

--Full Check
SELECT * FROM silver.crm_cust_info;

/*
======================================
Table 2:silver.crm_prd_info
======================================
*/

--Quality Check for Data Inserted in Silver Layer

--Check for NULL or DUPLICATES in Primary Key
--Expectation: No result should come
SELECT prd_id, COUNT(*) FROM
silver.crm_prd_info 
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--Check for Unwanted Spaces in Text Columns
--Expectation: No Result should come
SELECT prd_nm FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Check for NULLS or Negative Numbers in cost column
--Expectation: No Result should come
SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--Check the Date Columns
--End Date should not be less than start date
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--Checking for Data Consistency in prd_line column
--Expectaion: VAlid Values should only come
SELECT DISTINCT(prd_line) FROM silver.crm_prd_info;

--Final Check
SELECT * FROM silver.crm_prd_info;

/*
======================================
Table 3:silver.crm_sales_details
======================================
*/

--Quality Check for Data Inserted in Silver Layer

--Checking fpor Unwanted spaces from text columns
--Expectation: No Result should come
SELECT * FROM silver.crm_sales_details 
WHERE sls_ord_num != TRIM(sls_ord_num) OR 
sls_prd_key != TRIM(sls_prd_key);

--Numerical Column Checks
--Rule: Sales = Quantity * Price, this should be followed
--No Negative, NULL values are allowed
SELECT DISTINCT sls_sales, sls_quantity, sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--Final Check
SELECT * FROM silver.crm_sales_details;

/*
======================================
Table 4:silver.erp_cust_az12
======================================
*/

--Quality Check for Data Inserted in Silver Layer

--Checking NULL Values
--Expectation:No Result
SELECT cid FROM silver.erp_cust_az12
WHERE cid IS NULL;

--Checking Out of Range Dates in bdate column
--Expectation:No Result
SELECT bdate FROM silver.erp_cust_az12
WHERE bdate > NOW();

--Checking Distinct value for Gender Column
--Expectation:Male or Female or NA
SELECT DISTINCT gender FROM silver.erp_cust_az12;

--Final Check
SELECT * FROM silver.erp_cust_az12;

/*
======================================
Table 5:silver.erp_loc_a101
======================================
*/

--Quality Check for Data Inserted in Silver Layer

--Checking for Unwanted Spaces in Text Strings
--Expectation : No Result
SELECT cntry FROM silver.erp_loc_a101 
WHERE cntry != TRIM(cntry);

--Data Consistency Check
--Expectation Distinct Values without NULL
SELECT DISTINCT cntry FROM silver.erp_loc_a101;

--Final Check 
SELECT * FROM silver.erp_loc_a101;

/*
======================================
Table 6:silver.erp_px_cat_g1v2
======================================
*/

--Quality Check for Data Inserted in Silver Layer

--Checking for NULL Values or Missing values
--Expectation : No Result
SELECT * FROM silver.erp_px_cat_g1v2 
WHERE id IS NULL OR id = ' ' OR id = '' OR
cat IS NULL OR cat = ' ' OR cat = '' OR
subcat IS NULL OR subcat = ' ' OR subcat = ''OR 
maintenance IS NULL OR maintenance = ' ' or maintenance = '';

--Checking for Unwanted Spaces in String Columns
--Expectation : No Result
SELECT * FROM silver.erp_px_cat_g1v2 
WHERE id != TRIM(id) OR cat != TRIM(cat) 
OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

--Data Consistency Check
--Expectation : Unique Values should come
SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;

--Final Check
SELECT * FROM silver.erp_px_cat_g1v2;