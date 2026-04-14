/*
======================================================================
Gold Layer : Building the Data based on relationship between 
CRM nad ERP Tables
======================================================================
*/

/*
=====================================================================
Customer Table Relationship

Table A: crm_cust_info
Table B: erp_cust_az12
Table C: erp_loc_a101

Table Category: Dimension (Only Descriptive Info Available)
=====================================================================
*/

/*VIEW will be created in gold layer instead of TABLES like in bronze, silver layers*/

--Customer Table has only Descriptive info, which belongs to dimension table category
CREATE VIEW gold.dim_customers AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY A.cst_id) AS customer_key, --Surrogate key is created which will act as primary key for connecting with data models 
		A.cst_id AS customer_id,
		A.cst_key AS customer_number,
		A.cst_firstname AS first_name,
		A.cst_lastname AS last_name,
		C.cntry AS country,
		A.cst_martial_status AS martial_status,
		CASE WHEN A.cst_gender != 'NA' THEN A.cst_gender --So, we Gender columns from 2 tables crm, erp. In, which CRM will be the MASTER data we will consider that as First Priority
			 ELSE COALESCE(B.gender, 'NA')               --In case of the NA from CRM we will take the data from ERP. We are using COALESCE to avoid NULL value insertion because of no data availability from LEFT JOIN        
		END AS gender,
		B.bdate AS birthdate,
		A.cst_create_date AS create_date 
	FROM silver.crm_cust_info A
	LEFT JOIN silver.erp_cust_az12 B ON A.cst_key = B.cid 
	LEFT JOIN silver.erp_loc_a101 C ON A.cst_key = C.cid; --LEFT JOIN is used because during inner join if data is not available it will be skipped so LEFT JOIN with the source table crm_cust_info is performed

--Gold Layer : Quality Check

--Dupilcates Check for Customer Id
--Expectation: No Result
SELECT COUNT(customer_id) FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(customer_id) > 1;

--Dupilcates Check for Customer number
--Expectation: No Result
SELECT COUNT(customer_number) FROM gold.dim_customers
GROUP BY customer_number
HAVING COUNT(customer_number) > 1;

--Data Consistency Check for gender column
--Expectation: Unique Values should come
SELECT DISTINCT gender FROM gold.dim_customers;

--Data Consistency Check for martial_status column
--Expectation: Unique Values should come
SELECT DISTINCT martial_status FROM gold.dim_customers;

--Unwanted Space Checks were already performed in silver layer transformation itself
--For a safer side if needed we can perform that too

--Final Check
SELECT * FROM gold.dim_customers;
