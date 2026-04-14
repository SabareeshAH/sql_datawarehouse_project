/*
======================================================================
Gold Layer : Building the Data based on relationship between 
CRM nad ERP Tables
======================================================================
*/

/*
=====================================================================
Product Table Relationship

Table A: crm_prd_info
Table B: erp_px_cat_g1v2

Table Category: Dimension (Only Descriptive Info Available)
=====================================================================
*/

/*VIEW will be created in gold layer instead of TABLES like in bronze, silver layers*/

--Product Table has only Descriptive info, which belongs to dimension table category
CREATE VIEW gold.dim_products AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY A.prd_start_dt, A.prd_key) AS product_key, --Surrogate Key
		A.prd_id AS product_id,
		A.prd_key AS product_number,
		A.prd_nm AS product_name,
		A.cat_id AS category_id, 
		B.cat AS category,
		B.subcat AS subcategory,
		B.maintenance AS maintenance,
		A.prd_cost AS cost,
		A.prd_line AS product_line,
		A.prd_start_dt AS start_date
	FROM silver.crm_prd_info A
	LEFT JOIN silver.erp_px_cat_g1v2 B  --LEFT JOIN is used so that we will not loose any data unlike inner join when no common info is found
	ON A.cat_id = B.id
	WHERE prd_end_dt IS NULL; --Here we are filtering out the Historical Data, considering only the Latest Data which has END_DATE has NULL

--Quality Check: Gold Layer

--Duplicate Value check for product_id
--Expectation: No Result
SELECT COUNT(product_id) FROM gold.dim_products
GROUP BY product_id 
HAVING COUNT(product_id) > 1;

--Duplicate Value check for product_number
--Expectation: No Result
SELECT COUNT(product_number) FROM gold.dim_products
GROUP BY product_number 
HAVING COUNT(product_number) > 1;

--Data Consistency Check for maintenance column
--Expectation: Unique Values should come
SELECT DISTINCT maintenance FROM gold.dim_products;

--Final Check
SELECT * FROM gold.dim_products;
