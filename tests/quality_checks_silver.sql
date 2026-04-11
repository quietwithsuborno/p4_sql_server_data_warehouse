/*
===============================================================================
Quality Checks: Silver Layer
===============================================================================
Purpose:
    Perform data quality checks on the 'silver' schema to ensure consistency,
    accuracy, and standardization.

Checks included:
    - Nulls or duplicates in primary keys
    - Unwanted spaces in string fields
    - Data standardization and consistency
    - Invalid date ranges and orders
    - Data consistency between related fields (e.g., sales = quantity * price)

Usage:
    Run after loading the Silver layer.
    Investigate and resolve any discrepancies found.
===============================================================================
*/

-------------------------------------------------
-- Table: silver.crm_cust_info
-------------------------------------------------

-- 1. Check for nulls or duplicates in primary key
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2. Check for unwanted leading/trailing spaces
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- 3. Check data standardization and consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


-------------------------------------------------
-- Table: silver.crm_product_info
-------------------------------------------------

-- 1. Check for nulls or duplicates in primary key
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_product_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2. Check for unwanted spaces in product name
SELECT prd_name
FROM silver.crm_product_info
WHERE prd_name != TRIM(prd_name);

-- 3. Check for nulls or negative product cost
SELECT prd_cost
FROM silver.crm_product_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- 4. Check data standardization for product line
SELECT DISTINCT prd_line
FROM silver.crm_product_info;

-- 5. Check for invalid date orders
SELECT *
FROM silver.crm_product_info
WHERE prd_end_date < prd_start_date;


-------------------------------------------------
-- Table: silver.crm_sales_details
-------------------------------------------------

-- 1. Check for invalid order dates
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;

-- 2. Check for invalid date orders (order > ship/due)
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- 3. Check sales consistency: sales = quantity * price, no nulls/zeros
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-------------------------------------------------
-- Table: silver.erp_cust_az12
-------------------------------------------------

-- 1. Identify out-of-range birth dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- 2. Check data standardization for gender
SELECT DISTINCT
    gen,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen_standardized
FROM silver.erp_cust_az12;


-------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-------------------------------------------------

-- 1. Check for unwanted spaces in string fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- 2. Check data standardization for subcategory
SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2;


-------------------------------------------------
-- Table: silver.erp_loc_a101
-------------------------------------------------

-- 1. Check data standardization for country
SELECT DISTINCT
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry_standardized
FROM silver.erp_loc_a101
ORDER BY cntry_standardized;
