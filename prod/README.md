# Production Data Warehouse Setup

This folder contains the production-like data warehouse setup using SQLite and Power BI.

## Structure

```
prod/
├── sql/              # SQL DDL scripts for star schema
├── etl/              # ETL scripts to load data from parquet to SQLite
├── config/           # Configuration files
├── docs/             # Documentation for production setup
├── examples/         # Power BI connection examples
├── data/
│   └── warehouse/    # SQLite database files (air_health_eu.db is included)
└── README.md         # This file
```

## Overview

This production setup:
- Uses SQLite as the data warehouse (file-based, no server needed)
- Implements an enhanced star schema design with multiple fact tables
- **The database (`air_health_eu.db`) is included in the repository and ready to use**
- Provides ETL scripts to load processed parquet data into SQLite (if rebuilding)
- Supports Power BI connections for visualization
- Preserves the existing MVP (no changes to existing code)
- Supports all NUTS levels (country, NUTS1, NUTS2, NUTS3+) for maximum flexibility

**Note**: The processed parquet files used by the ETL pipeline are created by the root-level `notebooks/` ETL pipeline (see main README.md).

## Prerequisites

- Python 3.8+
- Required packages (see `requirements-prod.txt`)
- Power BI Desktop (for visualization)

## Quick Start

The database is **already created and included** in the repository. You can start using it immediately:

1. **Install dependencies:**
   ```bash
   pip install -r prod/requirements-prod.txt
   ```

2. **Connect Power BI:**
   - Open Power BI Desktop
   - Get Data → More → SQLite database
   - Browse to `prod/data/warehouse/air_health_eu.db`
   - Select tables and build visualizations
   - See `examples/README.md` for ready-to-use Python scripts

## Rebuilding the Database (Optional)

If you need to rebuild the database from scratch:

1. **Ensure processed data files exist:**
   - Run the ETL pipeline notebooks from the project root:
     - `notebooks/01_ingest_emissions.ipynb` → creates `data/processed/emissions_nuts2.parquet`
     - `notebooks/01_ingest_health.ipynb` → creates `data/processed/health_*.parquet`
     - `notebooks/02_load_population.ipynb` → creates `data/processed/population_nuts2.parquet`

2. **Create the database schema:**
   ```bash
   python prod/etl/create_database.py
   ```

3. **Load data:**
   ```bash
   python -m prod.etl.load_data
   ```
   
   Or with NUTS2 filtering:
   ```bash
   python -m prod.etl.load_data --filter-nuts2
   ```

## Database Schema

The enhanced star schema consists of:

- **Fact Tables** (multiple fact tables preserve full timelines):
  - `fact_emissions` - GHG emissions by region, time, sector, gas (1990-2022)
  - `fact_health_metrics` - Causes of death rates by region, time, ICD-10 code (2000-2021)
  - `fact_hospital_discharges` - Hospital discharge counts/rates by region, time, discharge type (2000-2021)
  - `fact_population` - Population counts by region, time (varies by region)
  
- **Dimension Tables**:
  - `dim_geography` - Geographic regions (all NUTS levels: country=0, NUTS1=1, NUTS2=2, NUTS3+=3)
  - `dim_time` - Time/date attributes with availability flags
  - `dim_sector` - Emissions sectors
  - `dim_gas` - Greenhouse gas types
  - `dim_icd10_cod` - ICD-10 codes for causes of death
  - `dim_discharge_type` - ICD-10 codes for hospital discharges

See `sql/schema.sql` for the complete schema definition and `docs/schema-design.md` for detailed documentation.

## ETL Process

1. Read processed parquet files from `data/processed/`:
   - `emissions_nuts2.parquet` - EDGAR emissions data
   - `health_causes_of_death.parquet` - Eurostat causes of death data
   - `health_hospital_discharges.parquet` - Eurostat hospital discharge data
   - `population_nuts2.parquet` - Eurostat population data
2. Extract and load dimension tables (geography, time, sector, gas, ICD-10 codes, discharge types)
3. Load fact tables separately:
   - `fact_population` - from population data
   - `fact_emissions` - from emissions data
   - `fact_health_metrics` - from causes of death data
   - `fact_hospital_discharges` - from hospital discharge data (calculates `discharge_rate_per_100k`)
4. Update `dim_time` availability flags based on loaded fact tables
5. Verify data integrity

**Note**: The ETL preserves all NUTS levels by default. Use `--filter-nuts2` flag to filter to NUTS2 level only.

## Power BI Connection

The database is ready to use. See `examples/README.md` for ready-to-use Python scripts that connect Power BI to all tables and views.

Quick connection:
1. Open Power BI Desktop
2. Get Data → More → SQLite database
3. Browse to: `prod/data/warehouse/air_health_eu.db`
4. Select tables to import
5. View Model to see the star schema visualization
6. Build dashboards and visualizations

## Notes

- **The database file (`air_health_eu.db`) is included in the repository** and ready to use
- The processed parquet files are created by the root-level `notebooks/` ETL pipeline
- All ETL scripts are designed to be idempotent (safe to run multiple times)
- To customize the schema, edit `sql/schema.sql` before rebuilding the database

