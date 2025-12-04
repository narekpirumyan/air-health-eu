# MVP - Streamlit Dashboard

This folder contains the MVP implementation with an interactive Streamlit dashboard for exploring the relationship between greenhouse gas emissions and health outcomes across EU regions.

## Structure

- `app/` – Streamlit dashboard application
- `src/pipeline/harmonize.py` – MVP-specific harmonization module (creates curated dataset)
- `notebooks/` – Jupyter notebooks for MVP-specific processing
- `data/curated/` – Final harmonized dataset (`eu_climate_health.parquet`)
- `docs/` – MVP documentation
- `tests/` – Data quality tests
- `requirements.txt` – Python dependencies

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r mvp/requirements.txt
   ```

2. **Run the dashboard:**
   ```bash
   streamlit run mvp/app/main.py
   ```

The curated dataset (`mvp/data/curated/eu_climate_health.parquet`) is already included in the repository and ready to use.

## Rebuilding the Curated Dataset

If you need to rebuild the curated dataset from scratch:

1. **Run the common ETL pipeline** (from project root):
   ```bash
   # Run notebooks at root level:
   # notebooks/01_ingest_emissions.ipynb → creates data/processed/emissions_nuts2.parquet
   # notebooks/01_ingest_health.ipynb → creates data/processed/health_*.parquet
   # notebooks/02_load_population.ipynb → creates data/processed/population_nuts2.parquet
   ```

2. **Run MVP-specific harmonization:**
   ```bash
   # Run: mvp/notebooks/04_harmonize_mvp.ipynb
   # Or use the Python module:
   python -m mvp.src.pipeline.harmonize
   ```
   
   This creates `mvp/data/curated/eu_climate_health.parquet`

## Data Flow

1. **Raw data** → Common ETL pipeline (`notebooks/` at project root) → `data/processed/*.parquet`
2. **Processed data** → MVP harmonization (`mvp/notebooks/04_harmonize_mvp.ipynb` or `mvp/src/pipeline/harmonize.py`) → `mvp/data/curated/eu_climate_health.parquet`
3. **Dashboard** reads from `mvp/data/curated/eu_climate_health.parquet`

## Dashboard Features

The Streamlit dashboard provides:
- Interactive choropleth maps of EU regions
- Filtering by country, region, sector, and gas type
- Time series visualization of emissions and health metrics
- Side-by-side comparison of emissions and health outcomes
- Sector breakdowns and hospital discharge analysis

## Notebooks

- `mvp/notebooks/04_harmonize_mvp.ipynb` – MVP-specific data harmonization (creates curated dataset)

For common ETL pipeline notebooks, see the main [README.md](../README.md).

