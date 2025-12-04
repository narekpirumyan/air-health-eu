"""
Power BI Python Script - Get All Tables and Views from Warehouse

Copy this entire code into Power BI Desktop Python script dialog to import
all tables and views from the SQLite warehouse database.

Usage in Power BI:
1. Get Data → More → Python script
2. Paste this code
3. Click OK
"""

import sqlite3
import pandas as pd

DB_PATH = r"C:\Users\narek.pirumyan\Desktop\IAE\2025\Big Data\Capstone Project\air-health-eu\prod\data\warehouse\air_health_eu.db"

conn = sqlite3.connect(DB_PATH)

# Get all dimension tables (keeping original table names)
dim_geography = pd.read_sql_query("SELECT * FROM dim_geography", conn)
dim_time = pd.read_sql_query("SELECT * FROM dim_time", conn)
dim_sector = pd.read_sql_query("SELECT * FROM dim_sector", conn)
dim_gas = pd.read_sql_query("SELECT * FROM dim_gas", conn)
dim_icd10_cod = pd.read_sql_query("SELECT * FROM dim_icd10_cod", conn)
dim_discharge_type = pd.read_sql_query("SELECT * FROM dim_discharge_type", conn)

# Get all fact tables (keeping original table names)
fact_emissions = pd.read_sql_query("SELECT * FROM fact_emissions", conn)
fact_causes_of_death = pd.read_sql_query("SELECT * FROM fact_causes_of_death", conn)
fact_hospital_discharges = pd.read_sql_query("SELECT * FROM fact_hospital_discharges", conn)
fact_population = pd.read_sql_query("SELECT * FROM fact_population", conn)

# Get all views (keeping original view names)
vw_emissions = pd.read_sql_query("SELECT * FROM vw_emissions", conn)
vw_health_metrics = pd.read_sql_query("SELECT * FROM vw_health_metrics", conn)
vw_hospital_discharges = pd.read_sql_query("SELECT * FROM vw_hospital_discharges", conn)

# Combine everything (original column names preserved)
all_dataframes = [
    dim_geography, dim_time, dim_sector, dim_gas, dim_icd10_cod, dim_discharge_type,
    fact_emissions, fact_causes_of_death, fact_hospital_discharges, fact_population,
    vw_emissions, vw_health_metrics, vw_hospital_discharges
]

df_all = pd.concat(all_dataframes, ignore_index=True, sort=False)

conn.close()

# Return DataFrame (Power BI will import this)
df_all

