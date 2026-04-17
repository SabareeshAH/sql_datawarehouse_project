# Data Warehouse Project (SQL-Based)

## Overview

This project centers on building a robust and scalable data warehouse using SQL, designed to integrate heterogeneous data sources and adhere to modern data engineering best practices. By applying a structured architectural approach, the solution transforms raw, unprocessed data into curated, analytics-ready datasets that enable accurate reporting and data-driven decision-making.

## High Level Architecture

<img width="914" height="553" alt="image" src="https://github.com/user-attachments/assets/67338f15-0635-4a02-9925-08d3c4cacc14" />

## Data Sources

Two primary datasets were used in this project:

### 1. CRM Dataset (CSV)

* Contains customer-related information
* Includes attributes such as:

  * Customer details
  * Contact information
  * Customer segmentation data

### 2. ERP Dataset (CSV)

* Focuses on operational and transactional data
* Includes:

  * Product information
  * Sales transactions
  * Order details

These datasets were ingested from CSV files and processed within the data warehouse.

## Architecture: Medallion Framework

This project follows the **Medallion Architecture**, which organizes data into layered stages to ensure quality, reliability, and performance.

### Bronze Layer (Raw Data)

* Stores raw ingested data from CRM and ERP sources
* Data is loaded as-is with minimal transformation
* Acts as a historical archive for traceability

### Silver Layer (Cleaned & Transformed Data)

* Data is cleaned, standardized, and validated
* Handles:

  * Missing values
  * Data type corrections
  * Deduplication
* Joins and relationships between CRM and ERP datasets are established here

### Gold Layer (Business-Ready Data)

* Final curated datasets and fact, dimensions tables are decided. View is created based on that
* Aggregations and business logic applied
* Structured for reporting and dashboard consumption

## Tools & Technologies

### PostgreSQL (pgAdmin 4)

* Core database system used for building the data warehouse
* SQL used for:

  * Data ingestion
  * Transformation pipelines
  * Query optimization

### Notion

* Used for project management and documentation
* Tracks:

  * Task progress
  * Milestones
  * Development updates

### draw.io

* Used to design and visualize:

  * Data flow diagrams
  * Architecture layers
  * Entity relationships

## Database Relationship

<img width="1108" height="515" alt="image" src="https://github.com/user-attachments/assets/92da9483-d7a7-4615-9ad9-cb8b17cab2d6" />

## Data Pipeline Workflow

1. **Data Ingestion**

   * Load CRM and ERP CSV files into the Bronze layer

2. **Data Transformation**

   * Clean and normalize data in the Silver layer
   * Apply joins across datasets (customers, products, sales)

3. **Data Modeling**

   * Design analytical tables in the Gold layer
   * Apply aggregations and business rules

4. **Data Consumption**

   * Provide structured datasets for reporting and analytics

## Final Data Flow Layer

<img width="893" height="470" alt="image" src="https://github.com/user-attachments/assets/d4f8d572-7a92-42ff-82d7-45fdbbece85c" />

## Key Features

* End-to-end SQL-based data pipeline
* Integration of multiple data sources (CRM & ERP)
* Structured layered architecture (Medallion)
* Clean and analytics-ready data models
* Documentation and workflow tracking

## Project Links

Notion Link - https://www.notion.so/Data-Ware-House-Project-3237dcd1cef680e08269f210aa4ecf72?source=copy_link

## Project Repo Structure 

```
/sql_datawarehouse_project
│
├── datasets/
│   ├── crm/                     # Raw CRM CSV datasets (customers, etc.)
│   └── erp/                     # Raw ERP CSV datasets (products, sales, etc.)
│
├── docs/
│   └── flows/
│       ├── bronze/              # Architecture flows for Bronze layer
│       ├── silver/              # Architecture flows for Silver layer
│       └── gold/                # Architecture flows for Gold layer
│
├── scripts/
│   ├── bronze/                  # SQL scripts for raw data ingestion
│   ├── silver/                  # SQL scripts for data cleaning & transformation
│   └── gold/                    # SQL scripts for business-level transformations
│
└── tests/                       # Data validation and testing scripts
```

## Visual Flow Summary
```
[ CSV Datasets ]
       │
       ▼
Bronze Layer (Raw Data)
       │
       ▼
Silver Layer (Cleaned & Transformed)
       │
       ▼
Gold Layer (Business-Ready Data)
       │
       ▼
Analytics / Reporting
```

## Conclusion

This project demonstrates a structured approach to building a modern data warehouse using SQL and PostgreSQL. By leveraging the Medallion Architecture and integrating CRM and ERP datasets, it ensures scalable, maintainable, and analytics-ready data processing.

