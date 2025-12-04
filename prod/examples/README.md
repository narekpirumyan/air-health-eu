# Power BI Python Scripts

This folder contains ready-to-use Python scripts for connecting Power BI Desktop to the SQLite data warehouse.

## Quick Start

1. Open **Power BI Desktop**
2. Click **Get Data** → **More...** → **Python script**
3. Copy and paste one of the scripts below
4. Click **OK**

## Available Scripts

### `power_bi_get_all_tables.py`
**Get all tables and views from the warehouse**

This script imports all dimension tables, fact tables, and views in one go. Perfect for getting the complete dataset.

**Location:** `prod/examples/power_bi_get_all_tables.py`

## What's Included

The script imports:
- **6 Dimension Tables:** dim_geography, dim_time, dim_sector, dim_gas, dim_icd10_cod, dim_discharge_type
- **4 Fact Tables:** fact_emissions, fact_causes_of_death, fact_hospital_discharges, fact_population
- **3 Views:** vw_emissions, vw_health_metrics, vw_hospital_discharges

All original table and column names are preserved.

## Notes

- Make sure Python is installed and configured in Power BI: **File → Options → Python scripting**
- The database path in the script is already set to your location
- For large datasets, you may want to add a LIMIT clause for testing first

