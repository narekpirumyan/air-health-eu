# Power BI Examples and Guides

This folder contains ready-to-use scripts and comprehensive guides for connecting Power BI Desktop to the SQLite data warehouse and creating dashboards.

## 📊 Dashboard Guide (Start Here!)

### `power-bi-dashboard-guide.md`
**Complete guide for creating an EU Air & Health Analysis dashboard**

This comprehensive guide walks you through:
- Connecting to the database
- Setting up the data model
- Creating emissions visualizations
- Creating health visualizations
- Analyzing correlations between emissions and health outcomes
- Best practices and troubleshooting

**👉 [Read the Dashboard Guide](power-bi-dashboard-guide.md)**

## 🐍 Python Scripts

### `power_bi_get_all_tables.py`
**Get all tables and views from the warehouse**

This script imports all dimension tables, fact tables, and views in one go. Perfect for getting the complete dataset.

**Usage:**
1. Open **Power BI Desktop**
2. Click **Get Data** → **More...** → **Python script**
3. Copy and paste the script
4. Update the `DB_PATH` variable with your database location
5. Click **OK**

**What's Included:**

The script imports:
- **6 Dimension Tables:** dim_geography, dim_time, dim_sector, dim_gas, dim_icd10_cod, dim_discharge_type
- **4 Fact Tables:** fact_emissions, fact_causes_of_death, fact_hospital_discharges, fact_population
- **3 Views:** vw_emissions, vw_health_metrics, vw_hospital_discharges

All original table and column names are preserved.

## Quick Start Options

### Option 1: Direct SQLite Connection (Recommended)
1. Open Power BI Desktop
2. **Get Data** → **More...** → **Database** → **SQLite database**
3. Browse to: `prod/data/warehouse/air_health_eu.db`
4. Select tables/views to import
5. See [Dashboard Guide](power-bi-dashboard-guide.md) for detailed instructions

### Option 2: Python Script
1. Open Power BI Desktop
2. **Get Data** → **More...** → **Python script**
3. Use `power_bi_get_all_tables.py`
4. Follow [Dashboard Guide](power-bi-dashboard-guide.md) for visualization setup

## Notes

- Make sure Python is installed and configured in Power BI: **File → Options → Python scripting**
- The database path in the script needs to be updated to your location
- For large datasets, you may want to add a LIMIT clause for testing first
- **Recommended**: Use views (`vw_emissions`, `vw_health_metrics`, `vw_hospital_discharges`) to avoid relationship issues

