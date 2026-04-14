/*
======================================================================
Quality Check: Gold Layer
Value Uniquness, Redudant Data are checked
after the insertion from Silver layer
======================================================================
*/

/*
======================================
Table 1: gold.dim_products
======================================
*/

--Quality Check for Data Inserted in Gold Layer

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


/*
======================================
Table 2: gold.dim_customers
======================================
*/

--Quality Check for Data Inserted in Gold Layer

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


/*
======================================
Table 3: gold.fact_sales
======================================
*/

--Quality Check for Data Inserted in Gold Layer

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