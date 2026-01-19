/*
========================================================
DDL Scripts: Create Gold Views
========================================================
Script Purpose:
  This script creates views in the Gold layer in the data warehouse.
  The Gold Layer represents the final dimension and fact tabels (Star Schema).

Each view has been created fromt the silver layer with clean and transformed data, ready for any business analysis.
  /*

=========================================================
-- CREATE Dimensions : gold.dim_customers
=========================================================

USE DataWarehouseDB;
GO
IF OBJECT_ID('gold.dim_customers', 'V' ) IS NOT NULL 
DROP VIEW gold.dim_customers;
GO
CREATE OR ALTER VIEW gold.dim_customers AS 
SELECT

	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_skey,
	ci.cst_id				AS customer_id,
	ci.cst_key				AS customer_number  ,
	ci.cst_firstname		AS first_name,
	ci.cst_lastname			AS last_name,
	ci.cst_marital_status	AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the Master for Gender Info
	ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	--ci.cst_gndr				AS gender,
	ci.cst_create_date		AS create_date,
	ca.BDATE				AS birth_date,
	--ca.GEN					AS gender2,
	la.CNTRY				AS country

FROM Silver.crm_cust_info AS ci

LEFT JOIN silver.erp_cust_az12	AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101	AS la ON ci.cst_key = la.cid

/*
-- Gender Validation
SELECT
	DISTINCT
	ci.cst_gndr				AS gender,
	ca.GEN					AS gender2,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the Master for Gender Info
	ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
	
FROM Silver.crm_cust_info AS ci

LEFT JOIN silver.erp_cust_az12	AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101	AS la ON ci.cst_key = la.cid
ORDER BY 1,2

---- duplicate check
select customer_id, count(*) from 
(
SELECT
	ci.cst_id				AS customer_id,
	ci.cst_key				AS customer_key,
	ci.cst_firstname		AS first_name,
	ci.cst_lastname			AS last_name,
	ci.cst_marital_status	AS marital_status,
	ci.cst_gndr				AS gender,
	ci.cst_create_date		AS create_date,
	ca.BDATE				AS birth_date,
	ca.GEN					AS gender2,
	la.CNTRY				AS country
FROM Silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12	AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101	AS la ON ci.cst_key = la.cid	
) t
group by customer_id
having count(*)>1
*/

=========================================================
-- CREATE Dimensions : gold.dim_products
=========================================================

USE DataWarehouseDB;
GO
IF OBJECT_ID('gold.dim_products', 'V' ) IS NOT NULL 
DROP VIEW gold.dim_products;
GO
CREATE OR ALTER VIEW gold.dim_products AS 

SELECT 
	
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_skey,
	pn.prd_id			AS product_id,
	pn.prd_key			AS product_number,
	pn.prd_nm			AS product_name,
	pn.cat_id			AS category_id,
	pc.CAT				AS product_category,
	pc.SUBCAT			AS product_sub_category,
	pc.MAINTENANCE		AS product_maintenance_status,
	pn.prd_cost			AS product_cost,
	pn.prd_line			AS product_line,
	pn.prd_start_dt		AS product_start_date,
	pn.prd_end_dt		AS product_end_date,
	pn.sales_prd_key	AS sales_product_key 
		
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data


/*
Checking uniqueness of product key
select prd_key, count(*) from 
(
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.sales_prd_key,
	pn.prd_nm,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
	
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data
) t
group by prd_key
having count(*)>1
*/

=========================================================
-- CREATE Facts : gold.fact_sales
=========================================================
USE DataWarehouseDB;
GO
IF OBJECT_ID('gold.fact_sales', 'V' ) IS NOT NULL 
DROP VIEW gold.fact_sales;
GO
CREATE OR ALTER VIEW gold.fact_sales AS

SELECT 

    sls_ord_num     AS sales_order_number,
    sls_prd_key     AS sales_product_key,
    sls_cust_id     AS sales_customer_id,
    sls_order_dt    AS sales_order_date,
    sls_ship_dt     AS sales_shipment_date,
    sls_due_dt      AS sales_due_date,
    sls_sales       AS sales,
    sls_quantity    AS sales_quantity ,
    sls_price       AS sales_price,

    pr.product_skey,
    cu.customer_skey
    
FROM silver.crm_sales_details  AS sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.sales_product_key
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
--We have 2 dimension keys from the dimension table and this will help us to connect the 
-- data model to connect the facts with the dimensions.

/*
-- Pulling all the columns names
SELECT 
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver'
  AND TABLE_NAME = 'crm_sales_details'
ORDER BY ORDINAL_POSITION;

Validation checks on the fact table
select
*
from gold.fact_sales f
left join gold.dim_customers c on f.customer_skey = c.customer_skey
where c.customer_skey is NULL

--left join gold.dim_products p on f.product_skey = p.product_skey

select
*
from gold.fact_sales f
left join gold.dim_products p on f.product_skey = p.product_skey
where p.product_skey is NULL
*/
