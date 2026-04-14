/*
======================================================================
Gold Layer : Building the Data based on relationship between 
CRM nad ERP Tables
======================================================================
*/

/*
=====================================================================
Sales Table Relationship

Table: crm_sales_details

Table Category: Fact (Quantative Info Available about the Sales)
=====================================================================
*/

/*VIEW will be created in gold layer instead of TABLES like in bronze, silver layers*/

--Sales Table has info about keys, dates, measures(quantities) which belongs to fact table category
CREATE VIEW gold.fact_sales AS
	SELECT 
		sls_ord_num AS order_number,
		B.product_key, --Surrogate Key from dim_products
		C.customer_key, --Surrogate Key from dim_customers
		sls_order_dt AS order_date,
		sls_ship_dt AS shipping_date,
		sls_due_dt AS due_date,
		sls_sales AS sales_amount,
		sls_quantity AS quantity,
		sls_price AS price
	FROM silver.crm_sales_details A
	LEFT JOIN gold.dim_products B ON A.sls_prd_key = B.product_number
	LEFT JOIN gold.dim_customers C ON A.sls_cust_id = C.customer_id; --So, Here we are joining the silver layer sales table with the processed gold layer dimension tables products and customer using the matching key. By this method Fact and Dimension Tables are now joined. 


--Quality Check: Gold Layer

--Foriegn Key Integrity Check between Fact and Dimesion Table in Gold Layer
--NULL Check for Surogate Key: customer_key
--Expectation: No Result
SELECT * FROM gold.fact_sales A
LEFT JOIN gold.dim_customers B 
ON A.customer_key = B.customer_key
WHERE B.customer_key IS NULL;

--NULL Check for Surogate Key: product_key
--Expectation: No Result
SELECT * FROM gold.fact_sales A
LEFT JOIN gold.dim_products C 
ON A.product_key = C.product_key
WHERE C.product_key IS NULL;

--Final Check
SELECT * FROM gold.fact_sales;
