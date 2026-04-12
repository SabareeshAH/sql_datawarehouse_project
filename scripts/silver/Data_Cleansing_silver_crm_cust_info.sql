--Checking from Broze Layer

--Checking for NULL and Dupilcate values in primary key of  crm_cust_info table from bronze layer 
--Expectation: No NULL or Duplicate values should be present
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL;

--Ranking the rows based rank 1 on the create_date updated recently
SELECT * FROM (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag
from bronze.crm_cust_info) AS T1
WHERE Flag=1;

--Checking for unwanted spaces in text columns
--Expectation: No unwanted spacces should present (No result)
SELECT cst_firstname FROM bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname FROM bronze.crm_cust_info 
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_martial_status FROM bronze.crm_cust_info 
WHERE cst_martial_status != TRIM(cst_martial_status);

SELECT cst_gender FROM bronze.crm_cust_info 
WHERE cst_gender != TRIM(cst_gender);

--checking the Data Consistency from the gender and Martial_status columns
--Expectation: Only 2 Values should come
--For Martial Status: M / S
--For Gender: M / F

SELECT DISTINCT(cst_martial_status) FROM bronze.crm_cust_info;

SELECT DISTINCT(cst_gender) FROM bronze.crm_cust_info;

--Final Query
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

--Deleting the unwanted NULL VALUE Column
DELETE FROM silver.crm_cust_info WHERE cst_id IS NULL;

--Checking the qaulity of data inserted in silver layer

--Checking for NULL and Dupilcate values in primary key of  crm_cust_info table from bronze layer 
--Expectation: No NULL or Duplicate values should be present
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info 
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL;

--Checking for unwanted spaces in text(string) columns
--Expectation: No unwanted spacces should present (No result)
SELECT cst_firstname FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname FROM silver.crm_cust_info 
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_martial_status FROM silver.crm_cust_info 
WHERE cst_martial_status != TRIM(cst_martial_status);

SELECT cst_gender FROM silver.crm_cust_info 
WHERE cst_gender != TRIM(cst_gender);

--checking the Data Consistency from the gender and Martial_status columns
--Expectation: Only 2 Values should come
SELECT DISTINCT(cst_martial_status) FROM silver.crm_cust_info;

SELECT DISTINCT(cst_gender) FROM silver.crm_cust_info;

--Full Check
select * from silver.crm_cust_info;