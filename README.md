# Metadata-Driven ETL Framework using Databricks

A metadata-driven ETL framework built using **Databricks, PySpark, Delta Lake, Unity Catalog, and YAML**. The framework follows the **Medallion Architecture (Bronze, Silver, and Gold)** and enables dynamic data processing without modifying the code when onboarding new datasets.

---

# Features

- Metadata-driven ETL pipeline
- YAML-based configuration
- Dynamic dataset onboarding
- Medallion Architecture (Bronze, Silver, Gold)
- Delta Lake support
- Unity Catalog integration
- Common and custom transformations
- Automatic metadata generation
- Scalable and reusable framework

---

# Architecture

```
                Source Tables
                      │
                      ▼
           source_metadata.yml
                      │
                      ▼
              Bronze Pipeline
                      │
                      ▼
           bronze_metadata.yml
                      │
                      ▼
              Silver Pipeline
                      │
                      ▼
           silver_metadata.yml
                      │
                      ▼
               Gold Pipeline
                      │
                      ▼
            gold_metadata.yml
```

---

# Project Structure

```
Metadata-Driven-ETL/
│
├── Bronze/
│   ├── bronze_pipeline.py
│   └── source_metadata.yml
│
├── Silver/
│   ├── silver_pipeline.py
│   ├── silver_rules.yml
│   └── bronze_metadata.yml
│
├── Gold/
│   ├── gold_pipeline.py
│   ├── gold_rules.yml
│   └── silver_metadata.yml
│
├── Generated/
│   ├── bronze_metadata.yml
│   ├── silver_metadata.yml
│   └── gold_metadata.yml
│
└── README.md
```

---

# Technologies Used

- Python
- PySpark
- Spark SQL
- Databricks
- Delta Lake
- Unity Catalog
- YAML

---

# How It Works

## Step 1: Source Layer

Create a `source_metadata.yml` file containing the source tables.

Example:

```yaml
catalog: source_catalog
schema: raw

datasets:
  - table: customers
  - table: products
  - table: sales
```

The Bronze pipeline reads this metadata file.

---

# Bronze Layer

## Purpose

The Bronze layer ingests raw data from the source and stores it without applying business transformations.

### Input

- `source_metadata.yml`

### Process

- Reads source tables
- Creates Bronze Delta tables
- Generates metadata for the next layer

### Output

- `bronze_metadata.yml`

Example:

```yaml
catalog: ETL_Framework_metadata
schema: bronze

datasets:
  - table: customers
    path: ETL_Framework_metadata.bronze.customers

  - table: products
    path: ETL_Framework_metadata.bronze.products

  - table: sales
    path: ETL_Framework_metadata.bronze.sales
```

---

# Silver Layer

## Purpose

The Silver layer cleans, standardizes, and transforms the Bronze data.

### Inputs

- `bronze_metadata.yml`
- `silver_rules.yml`

### Supported Transformations

- Trim
- Remove Nulls
- Remove Duplicates
- Rename Columns
- Change Data Types
- Uppercase
- Lowercase
- Add Columns
- Drop Columns

### Output

- `silver_metadata.yml`

Example:

```yaml
catalog: ETL_Framework_metadata
schema: silver

datasets:
  - table: customers
    path: ETL_Framework_metadata.silver.customers

  - table: products
    path: ETL_Framework_metadata.silver.products

  - table: sales
    path: ETL_Framework_metadata.silver.sales
```

---

# Gold Layer

## Purpose

The Gold layer creates business-ready analytical tables such as Fact and Dimension tables.

### Inputs

- `silver_metadata.yml`
- `gold_rules.yml`

### Process

- Reads Silver tables
- Creates Dimension tables
- Joins Dimension tables with Fact table
- Stores results in Gold layer
- Generates `gold_metadata.yml`

### Output

- `gold_metadata.yml`

Example:

```yaml
catalog: ETL_Framework_metadata
schema: gold

datasets:
  - table: dim_customers
    path: ETL_Framework_metadata.gold.dim_customers

  - table: dim_products
    path: ETL_Framework_metadata.gold.dim_products

  - table: fact_sales
    path: ETL_Framework_metadata.gold.fact_sales
```

---

# Pipeline Execution Flow

```
Source Tables
      │
      ▼
Bronze Pipeline
      │
      ▼
bronze_metadata.yml
      │
      ▼
Silver Pipeline
      │
      ▼
silver_metadata.yml
      │
      ▼
Gold Pipeline
      │
      ▼
gold_metadata.yml
```

---

# Installation

## Prerequisites

- Databricks Workspace
- Unity Catalog
- Delta Lake
- Python 3.x
- PySpark
- PyYAML

Install PyYAML if required:

```bash
pip install pyyaml
```

---

# Clone the Repository

```bash
git clone https://github.com/<your-username>/Metadata-Driven-ETL-Framework.git

cd Metadata-Driven-ETL-Framework
```

---

# Setup

1. Upload the project files to your Databricks workspace.
2. Create the required Unity Catalog catalog and schemas.
3. Upload all YAML files to a Databricks Volume.
4. Update the YAML file paths inside the notebooks.
5. Execute the pipelines in the following order:

```
Bronze Pipeline
        ↓
Silver Pipeline
        ↓
Gold Pipeline
```

---

# Adding a New Dataset

One of the key advantages of this framework is that **no code changes are required** when onboarding a new dataset.

Follow these steps:

1. Add the dataset information to `source_metadata.yml`.
2. Add transformation rules in `silver_rules.yml`.
3. If needed, update `gold_rules.yml` to define new Fact or Dimension tables.
4. Run the Bronze, Silver, and Gold pipelines.

The framework automatically processes the new dataset.

---

# Benefits

- Metadata-driven architecture
- Dynamic ETL processing
- Reusable framework
- Easy maintenance
- Minimal code changes
- Scalable design
- Enterprise-ready solution
- Follows Databricks Medallion Architecture

---

# Future Enhancements

- Incremental data loading
- Slowly Changing Dimensions (SCD)
- Audit logging
- Data Quality Validation
- Error handling and notifications
- Workflow orchestration using Databricks Workflows

---

**Technologies:** Databricks | PySpark | Delta Lake | Unity Catalog | YAML | Data Engineering

---
