/* 
=====================================================================
Data Cleansing: erp_px_cat_g1v2 table from bronze layer 
=====================================================================
*/

--Checking the Bronze Layer Table
--Checking for NULL Values or Missing values
SELECT id FROM bronze.erp_px_cat_g1v2 
WHERE id IS NULL OR id = ' ' or id = '';

SELECT cat FROM bronze.erp_px_cat_g1v2 
WHERE cat IS NULL OR cat = ' ' or cat = '';

SELECT subcat FROM bronze.erp_px_cat_g1v2 
WHERE subcat IS NULL OR subcat = ' ' or subcat = '';

SELECT maintenance FROM bronze.erp_px_cat_g1v2 
WHERE maintenance IS NULL OR maintenance = ' ' or maintenance = '';

--Checking for Unwanted Spaces in String Columns
SELECT id FROM bronze.erp_px_cat_g1v2 
WHERE id != TRIM(id);

SELECT cat FROM bronze.erp_px_cat_g1v2 
WHERE cat != TRIM(cat);

SELECT subcat FROM bronze.erp_px_cat_g1v2 
WHERE subcat != TRIM(subcat);

SELECT maintenance FROM bronze.erp_px_cat_g1v2 
WHERE maintenance != TRIM(maintenance);

--Data Consistency Check
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

--Final Query
INSERT INTO silver.erp_px_cat_g1v2(
id, cat, subcat, maintenance
)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

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