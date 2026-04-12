/* 
=====================================================================
Data Cleansing: erp_loc_a101 table from bronze layer 
=====================================================================
*/

--Checking the Bronze Layer Table
--Checking for Unwanted Spaces in Text Strings
SELECT cntry FROM bronze.erp_loc_a101 
WHERE cntry != TRIM(cntry);

--Data Consistency Check
SELECT DISTINCT cntry FROM bronze.erp_loc_a101;

--Final Query
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