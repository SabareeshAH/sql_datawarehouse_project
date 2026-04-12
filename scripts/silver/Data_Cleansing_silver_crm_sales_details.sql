/* 
=====================================================================
Data Cleansing: crm_sales_details table from bronze layer 
=====================================================================
*/

--Checking the Table crm_sales_details from bronze layer

--Checking fpor Unwanted spaces from text columns
--Expectation: No Result should come
SELECT sls_ord_num FROM 
bronze.crm_sales_details WHERE sls_ord_num != TRIM(sls_ord_num);

SELECT sls_prd_key FROM 
bronze.crm_sales_details WHERE sls_prd_key != TRIM(sls_prd_key);

--Checking for Invalid Dates
SELECT sls_order_dt from bronze.crm_sales_details
WHERE sls_order_dt IS NULL OR sls_order_dt <= DATE '1900-01-01' OR sls_order_dt >= DATE '2050-12-31';

SELECT sls_ship_dt from bronze.crm_sales_details
WHERE sls_ship_dt IS NULL OR sls_ship_dt <= DATE '1900-01-01' OR sls_ship_dt >= DATE '2050-12-31';

SELECT sls_due_dt from bronze.crm_sales_details
WHERE sls_due_dt IS NULL OR sls_due_dt <= DATE '1900-01-01' OR sls_due_dt >= DATE '2050-12-31';

--Order should not be greater than ship or due date
--Expectaion: Order date shoul be lower than ship/fue date (No Result)
SELECT sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--Numerical Column Checks
--Rule: Sales = Quantity * Price, this should be followed
--No Negative, NULL values are allowed
SELECT DISTINCT sls_sales, sls_quantity, sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--Final Query
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

--Inserted in Silver Layer Data Quality Chcek 
--Checking fpor Unwanted spaces from text columns
--Expectation: No Result should come
SELECT sls_ord_num FROM 
silver.crm_sales_details WHERE sls_ord_num != TRIM(sls_ord_num);

SELECT sls_prd_key FROM 
silver.crm_sales_details WHERE sls_prd_key != TRIM(sls_prd_key);


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