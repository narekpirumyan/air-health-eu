# EU Air & Health Dashboard

This repository explores links between greenhouse gas emissions and respiratory health outcomes across EU NUTS2 regions. It provides reproducible ingestion pipelines, curated datasets, and two implementation approaches: an MVP Streamlit dashboard and a Production SQLite data warehouse.

## Project Structure

```
air-health-eu/
├── data/
│   ├── raw/              # Raw source datasets (EDGAR, Eurostat, geo assets)
│   └── processed/        # Processed parquet files (shared by MVP and prod)
├── notebooks/            # ETL pipeline notebooks (common)
├── mvp/                  # MVP: Streamlit dashboard (see mvp/README.md)
└── prod/                 # Production: SQLite + Power BI (see prod/README.md)
```

## Common ETL Pipeline

The repository includes a shared ETL pipeline that processes raw data into standardized parquet files used by both MVP and Production implementations.

### ETL Notebooks

Run these notebooks in order to process raw data:

1. **`notebooks/01_ingest_emissions.ipynb`**
   - Processes EDGAR v8.0 emissions data
   - Output: `data/processed/emissions_nuts2.parquet`

2. **`notebooks/01_ingest_health.ipynb`**
   - Processes Eurostat health data (causes of death, hospital discharges)
   - Output: `data/processed/health_causes_of_death.parquet`, `health_hospital_discharges.parquet`

3. **`notebooks/02_load_population.ipynb`**
   - Processes Eurostat population data
   - Output: `data/processed/population_nuts2.parquet`

**Note**: The processed parquet files are included in the repository and shared between MVP and Production.

## Included Data

This repository includes:
- **Raw data files**: EDGAR emissions, Eurostat health TSVs, population data, and geo JSON files (in `data/raw/`)
- **Processed datasets**: All parquet files in `data/processed/` (emissions, health, population)
- **MVP curated dataset**: `mvp/data/curated/eu_climate_health.parquet`
- **Production database**: `prod/data/warehouse/air_health_eu.db` (SQLite star schema)

**Note**: Climate TRACE data is excluded from the repository (see `.gitignore`) due to its large size. It is also not used in the current implementation, which relies on EDGAR emissions data instead.

## Getting Started

### For MVP (Streamlit Dashboard)
See [mvp/README.md](mvp/README.md) for setup and usage instructions.

### For Production (SQLite + Power BI)
See [prod/README.md](prod/README.md) for setup and usage instructions.