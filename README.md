# Pharma-sales-manufacturing
Architecture
This project implements an end-to-end Azure Medallion Architecture to process pharmaceutical sales, inventory, hospital, and quality data.

Data Flow:
CSV Data Sources → Azure Data Factory → ADLS Gen2 Bronze → Azure Databricks → ADLS Gen2 Silver → Gold Layer → Azure Synapse Analytics → Power BI

Data Sources: Sales, Inventory, Hospital, and Quality Reports CSV files.
Azure Data Factory: Ingests source data into the data lake using automated pipelines.
Bronze Layer: Stores raw data in its original form.
Silver Layer: Azure Databricks and PySpark perform data cleaning, validation, deduplication, standardization, and transformation.
Gold Layer: Contains curated, business-ready datasets optimized for analytics.
Azure Synapse Analytics: Provides the serving layer using external tables and views.
Power BI: Connects to the Synapse layer to deliver interactive dashboards covering executive KPIs, sales & product performance, and inventory & quality analysis.
