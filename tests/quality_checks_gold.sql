/*
===============================================================================
Quality Checks: Gold Layer
===============================================================================

Purpose:
    This script validates the integrity, consistency, and accuracy of the
    Gold Layer (Star Schema).

Checks Included:
    - Uniqueness of surrogate keys in dimension tables
    - Referential integrity between fact and dimension tables
    - Validation of relationships for analytical correctness

Usage:
    Run after loading the Gold Layer.
    Investigate and resolve any discrepancies found.

===============================================================================
*/


-- ============================================================================
-- Check: gold.dim_customers
-- ============================================================================

-- 1. Check for duplicate customer_key (should be unique)
-- Expectation: No results

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;



-- ============================================================================
-- Check: gold.dim_products
-- ============================================================================

-- 2. Check for duplicate product_key (should be unique)
-- Expectation: No results

SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;



-- ============================================================================
-- Check: gold.fact_sales
-- ============================================================================

-- 3. Referential Integrity Check
-- Ensure all fact records properly map to dimension tables
-- Expectation: No results

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL 
   OR p.product_key IS NULL;
