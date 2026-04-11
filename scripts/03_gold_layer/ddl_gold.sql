/*
===============================================================================
DDL Script: Create Gold Layer Views (Star Schema)
===============================================================================

Purpose:
    This script creates views in the 'gold' schema representing the final
    business-ready data model (Star Schema).

    - Dimension tables: dim_customers, dim_products
    - Fact table: fact_sales

    These views transform and combine data from the Silver layer to provide
    clean, enriched, and analytics-ready datasets.

Usage:
    These views can be queried directly for reporting, dashboards,
    and analytical workloads.

===============================================================================
*/


-- ============================================================================
-- View: gold.dim_customers
-- Description: Customer dimension with enriched attributes
-- ============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,   -- surrogate key
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,

    ci.cst_material_status AS marital_status,

    -- Gender logic: CRM is primary source, fallback to ERP
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO



-- ============================================================================
-- View: gold.dim_products
-- Description: Product dimension with category hierarchy
-- ============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (
        ORDER BY pn.prd_start_date, pn.prd_key
    ) AS product_key,   -- surrogate key

    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_name AS product_name,

    pn.category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,

    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_date AS start_date

FROM silver.crm_product_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.category_id = pc.id

-- Only keep active/latest products
WHERE pn.prd_end_date IS NULL;
GO



-- ============================================================================
-- View: gold.fact_sales
-- Description: Fact table containing sales transactions
-- ============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,

    pr.product_key,
    cu.customer_key,

    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,

    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price

FROM silver.crm_sales_details sd

-- Join with product dimension
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

-- Join with customer dimension
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
