# Databricks ETL Pipeline and Dashboard Project
<img width="1650" alt="image" src="https://github.com/user-attachments/assets/ae7e680d-4ae4-48fc-b2b6-9e997ccc3098" />


This project demonstrates a complete ETL workflow in Databricks, ideal for first-time users. It walks through the steps of uploading data, creating and updating Delta Tables, using Spark and pandas, and visualizing the data through an interactive dashboard. 
Throughout the notebook I try simplify as much as possible the steps and share some common errors you might come across in your first databricks ETL implementation, such as schema enforcement (Struct type errors when trying to append data) or  the differences between volumes and DFS type folders file paths.

The example uses UFO sighting records to show how data can be ingested, transformed, stored, and monitored. Please note that the dataset was used only as a funny example and should not be assumed that the author believes in the subject.

## Project Objectives

- Upload and manage data using Databricks Volumes
- Use pandas and PySpark to process CSV and Excel data
- Create and append to Delta Tables using Spark
- Build a dashboard that reflects new data changes automatically

## Workflow Steps

### Step 1: Upload Files to a Volume
- Create or access a Unity Catalog volume
- Upload data files (CSV, Excel) to the volume

<img width="1500" alt="image" src="https://github.com/user-attachments/assets/07f916da-5e2a-460a-b1a5-aaca8cdb7951" />

<img width="1500" alt="image" src="https://github.com/user-attachments/assets/fa157bf5-9fa4-487d-b11f-7dfcfa4b7e8b" />

### Step 2: Create Workspace and Load Data
- Create a new notebook in Databricks
- Load your data using pandas and convert it to Spark DataFrames

Example:
```python
import pandas as pd
import pyspark.pandas as ps

df = pd.read_csv('/Volumes/schema_name/volume_name/ufo_data.csv')
sdf = ps.from_pandas(df)
```

### Step 3: Create a Delta Table
- Save the Spark DataFrame as a Delta Table in the catalog

```python
sdf.to_table("catalog.schema.table_name", mode="overwrite")
```

### Step 4: Append New Data
- Read new data files (e.g., Excel or CSV)
- Clean or transform as needed
- Append the data to the existing Delta Table

```python
sdf_new.to_table("catalog.schema.table_name", mode="append")
```

### Step 5: Build a Dashboard
- Use Databricks SQL dashboard features to create visualizations:
  - Sightings by country
  - Encounter duration by city
  - Trends over time

- The dashboard reflects changes as new data is appended to the Delta Table.

## Dashboard Overview

<img width="1471" alt="image" src="https://github.com/user-attachments/assets/8cd321e9-9ff7-4c1f-aa8d-335ae4961b7b" />


The final dashboard includes:
- UFO sightings by country
- Average encounter length by country and city
- Number of sightings over time

View the dashboard here:  
[Databricks Dashboard Example]([https://dbc-f4254ae7-a57c.cloud.databricks.com/?o=2808389061517388](https://dbc-f4254ae7-a57c.cloud.databricks.com/dashboardsv3/01f059da5b111add9c7b7b9a557e3609/published?o=2808389061517388))

## Tools and Technologies

- Databricks Workspace and Unity Catalog
- pandas and pandas-on-Spark (pyspark.pandas)
- PySpark
- Delta Lake
- Databricks SQL Dashboards

## Project Structure

```
notebooks/
   Load_and_Save_Data_in_Databricks.ipynb

volumes/
   ufo_data.csv
   ufo_new_data.xlsx

ufo_sighting_data_test_dashboard.pdf
README.md
```

## Outcomes

By completing this project, users will learn how to:
- Ingest data into Databricks using volumes
- Perform ETL with pandas and Spark
- Maintain and update Delta Tables
- Create live dashboards for monitoring and analysis
