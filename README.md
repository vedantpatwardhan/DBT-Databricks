# dbt Sales Transformation Pipeline (Databricks)

This project implements a modular Data Engineering pipeline using dbt (data build tool) and Databricks. It transforms raw retail data into a structured "Gold" layer ready for analytics, utilizing a Medallion Architecture (Bronze, Silver, Gold).

### 🚀 Project Overview
The pipeline processes raw sales, customer, and product data to provide insights into total gross amounts and customer demographics.

Key Features:
Medallion Architecture: Clear separation between Raw (Bronze), Transformed (Silver), and Business-Ready (Gold) data.

Custom Macros: Implemented modular Jinja macros (e.g., multiply) to handle reusable business logic.

Data Quality: Integrated generic and custom tests to ensure non-negative values and unique identifiers.

Snapshots: Implemented SCD Type 2 tracking for gold-level items to monitor historical changes.

### 🏗️ Architecture
The project follows these transformation stages:

Bronze (Staging): Casts raw data types and cleanses field names from the source Databricks catalog.

Silver (Intermediate): Joins disparate sources (Sales, Products, Customers) into flattened, enriched tables.

Gold (Mart): Aggregates data into high-level business views, such as total_gross_amount by category and gender.

### 🛠️ Tech Stack
Data Warehouse: Databricks (SQL Warehouse)

Transformation: dbt Core (v1.11.8)

Language: SQL & Jinja

Version Control: Git & GitHub

### 📖 How to Run This Project
#### 1. Prerequisites
Python 3.9+ installed.

A Databricks workspace with a SQL Warehouse.

The dbt-databricks adapter installed.

#### 2. Setup
Clone the repository and install dependencies:

Bash
git clone https://github.com/vedantpatwardhan/DBT-Databricks.git
cd dbt_project
pip install -r requirements.txt  # Or manually: pip install dbt-databricks
#### 3. Profile Configuration
Since the profiles.yml is ignored for security, you must create one in your ~/.dbt/ folder:

#### YAML
dbt_project:
  outputs:
    dev:
      type: databricks
      host: [your-databricks-host]
      http_path: [your-sql-warehouse-path]
      token: [your-access-token]
      catalog: dbt_tutorial_dev
      schema: default
      threads: 1
  target: dev
#### 4. Execution
Run the following commands to build the pipeline:

Bash
dbt seed      # Load lookup data
dbt run       # Run all models
dbt test      # Run data quality tests
dbt snapshot  # Capture historical changes
### 📁 Project Structure
/models: Contains the SQL transformations organized by layer.

/macros: Reusable Jinja logic for calculations.

/seeds: Static lookup data.

/tests: Custom data validation logic.