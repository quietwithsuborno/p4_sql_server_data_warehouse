# Data Warehouse and Analytics Project 🚀

Welcome to the **Data Warehouse and Analytics Project** repository!

This project demonstrates the design and implementation of a modern data warehouse and analytics solution using SQL Server. It follows an end-to-end approach from raw data ingestion to delivering business-ready datasets for analytical reporting.

The project integrates data from CRM and ERP systems and implements the Medallion Architecture (Bronze, Silver, and Gold layers) to transform raw data into meaningful business insights.

---

## Objectives

### Data Engineering Goals
- Build a modern data warehouse using SQL Server
- Ingest and consolidate data from CRM and ERP source systems
- Implement Medallion Architecture (Bronze → Silver → Gold)
- Develop robust ETL pipelines for data cleansing and transformation
- Ensure high data quality, consistency, and integrity

---

### Project Scope
- Focus on the latest available dataset (no historization)
- Batch processing with full refresh approach
- Deliver analysis-ready datasets for downstream reporting and dashboards

---

## Data Warehouse Architecture

This project follows the Medallion Architecture, a modern layered design pattern widely used in data engineering.

<p align="center">
  <img src="images/data_architecture_diagram.png" alt="Data Architecture Diagram" width="800">
</p>

### Bronze Layer - Raw Data
- Ingests raw data from CRM and ERP systems in its original format
- Uses BULK INSERT for efficient loading
- Acts as the single source of truth for raw data
- No transformations applied

### Silver Layer - Cleaned & Transformed Data
Responsible for data quality and standardization. Key transformations include:
- Handling null values and duplicates
- Standardizing categorical values
- Cleaning text fields and fixing inconsistent data
- Applying business rules
- Data type casting and validation

Data in this layer is clean, reliable, and ready for modeling.

### Gold Layer - Business-Ready Data (Star Schema)
The final analytics layer structured using a Star Schema model, consisting of fact and dimension tables.

**Optimized for:**
- Fast querying and aggregations
- Business reporting and dashboards
- Supporting analytical use cases such as customer insights and sales analysis

## ETL (Extract, Transform, Load) Approach

**Extraction:**
- Source data is extracted from ERP and CRM systems in CSV format  
- Full data extraction is performed for initial data loading  

**Transformation:**
- Data cleaning and standardization (handling nulls, invalid values, and formatting issues)  
- Removal of duplicates and unwanted spaces  
- Data type conversions and validation  
- Business logic implementation (e.g., product categorization, derived fields)  
- Integration of ERP and CRM datasets into a unified structure  

**Loading:**
- Batch processing approach is used  
- Data is loaded into Bronze, Silver, and Gold layers  
- Full load strategy (truncate and insert) is applied for initial stages 

---
