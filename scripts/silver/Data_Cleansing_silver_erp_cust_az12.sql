/* 
=====================================================================
Data Cleansing: erp_cust_az12 table from bronze layer 
=====================================================================
*/

--Checking the Bronze Layer Table

--Checking NULL Values
SELECT cid FROM bronze.erp_cust_az12
WHERE cid IS NULL;

--Checking Out of Range Dates in bdate column
SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate < DATE '1924-01-01' OR bdate > NOW();

--Checking Distinct value for Gender Column
SELECT DISTINCT gender FROM bronze.erp_cust_az12;


--Final Query
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

--Inserted Data Quality Check in Silver Layer

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