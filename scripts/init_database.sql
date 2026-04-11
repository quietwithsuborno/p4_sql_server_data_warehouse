/*
===============================================================================
Create Database and Schemas
===============================================================================

Script Purpose:
This script initializes the 'DataWarehouse' database environment.
It first checks for the existence of the database and, if found, drops and
recreates it to ensure a clean setup.

Additionally, the script creates three schemas within the database:
- bronze : stores raw, unprocessed source data
- silver : stores cleaned and transformed data
- gold   : stores business-ready, aggregated data for analytics

-------------------------------------------------------------------------------

Warning:
Executing this script will permanently drop the existing 'DataWarehouse'
database (if it exists), resulting in complete data loss.

Ensure that:
- All necessary backups have been taken
- No critical data will be affected

Proceed with caution.
===============================================================================
*/

-- Switch context to the master database
-- Required to manage (create/drop) other databases
USE master;
GO

-- Check if the 'DataWarehouse' database already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Set database to SINGLE_USER mode to terminate active connections
    -- ROLLBACK IMMEDIATE ensures any open transactions are rolled back
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database to allow a fresh setup
    DROP DATABASE DataWarehouse;
END;
GO

-- Create a new DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created database
USE DataWarehouse;
GO

-- Create Bronze schema
-- Used for raw, unprocessed data ingestion (source-level data)
CREATE SCHEMA bronze;
GO

-- Create Silver schema
-- Used for cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Create Gold schema
-- Used for business-ready, aggregated, and analytical data models
CREATE SCHEMA gold;
GO
