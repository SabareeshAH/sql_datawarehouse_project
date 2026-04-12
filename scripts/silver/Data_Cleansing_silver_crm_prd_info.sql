/* 
=====================================================================
Data Cleansing: crm_prd_info table from bronze layer 
=====================================================================
*/

--Checking the table from bronze layer

--Check for NULL or DUPLICATES in Primary Key
--Expectation: No result should come
SELECT prd_id, COUNT(*) FROM
bronze.crm_prd_info 
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--Check for Unwanted Spaces in Text Columns
--Expectation: No Result should come
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Check for NULLS or Negative Numbers in cost column
--Expectation: No Result should come
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--Check the Date Columns
--End Date should not be less than start date
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--Checking for Data Consistency in prd_line column
SELECT DISTINCT(prd_line) FROM bronze.crm_prd_info;


--Final Query
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

--Inserted Silver Table Quality Check

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
SELECT DISTINCT(prd_line) FROM silver.crm_prd_info;

--Final Check
SELECT * FROM silver.crm_prd_info;