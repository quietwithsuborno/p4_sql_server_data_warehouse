/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source → Bronze)
===============================================================================

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs a full refresh of the Bronze layer by truncating existing tables
before loading new data.

The procedure uses the BULK INSERT command to efficiently ingest data from
source CSV files into the corresponding Bronze tables.

-------------------------------------------------------------------------------

Parameters:
None.
This stored procedure does not accept any input parameters and does not return
any values.

-------------------------------------------------------------------------------

Usage Example:
EXEC bronze.load_bronze;

===============================================================================
*/

/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================

Purpose:
This stored procedure performs a full load of the Bronze layer by ingesting
data from external CSV files into Bronze tables.

Process Overview:
1. Truncate existing Bronze tables (full refresh approach)
2. Load fresh data using BULK INSERT
3. Capture and print load duration for each table
4. Track total batch execution time
5. Handle errors using TRY...CATCH

Notes:
- Assumes source CSV files are accessible to SQL Server
- Designed for initial/raw data ingestion (no transformations applied)

===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Declare variables for tracking execution time
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        -- Start overall batch timer
        SET @batch_start_time = GETDATE();

        PRINT '===========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '===========================================';

        -- =========================================
        -- CRM TABLES
        -- =========================================
        PRINT '-----------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------';

        -- Load: bronze.crm_cust_info
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\SUBORNA\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT '>> Inserted Data Into: bronze.crm_cust_info';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------';

        -- Load: bronze.crm_product_info
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_product_info';
        TRUNCATE TABLE bronze.crm_product_info;

        BULK INSERT bronze.crm_product_info
        FROM 'C:\Users\SUBORNA\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT '>> Inserted Data Into: bronze.crm_product_info';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------';

        -- Load: bronze.crm_sales_details
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\SUBORNA\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT '>> Inserted Data Into: bronze.crm_sales_details';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------';

        -- =========================================
        -- ERP TABLES
        -- =========================================
        PRINT '-----------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------';

        -- Load: bronze.erp_loc_a101
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\SUBORNA\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT '>> Inserted Data Into: bronze.erp_loc_a101';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------';

        -- Load: bronze.erp_cust_az12
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\SUBORNA\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT '>> Inserted Data Into: bronze.erp_cust_az12';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------';

        -- Load: bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\SUBORNA\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT '>> Inserted Data Into: bronze.erp_px_cat_g1v2';

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------';

        -- End of batch execution
        SET @batch_end_time = GETDATE();

        PRINT '=============================================';
        PRINT 'Total Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=============================================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';

        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);

        PRINT '=========================================';
    END CATCH
END;
GO
