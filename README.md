# EU Air & Health Dashboard

This repository explores links between greenhouse gas emissions and respiratory health outcomes across EU NUTS2 regions. It ships reproducible ingestion pipelines, curated datasets, and (eventually) interactive visualizations.

## Project structure

- `DataSources/` – upstream datasets already downloaded (EDGAR, Climate TRACE, Eurostat health TSVs).
- `docs/data-audit.md` – current data inventory and transformation notes.
- `src/pipeline/` – ingestion & harmonization helpers written in Python.
- `notebooks/` – Jupyter front-ends that call the reusable pipeline functions.
- `data/processed/` – tidy intermediate datasets (git-tracked for now).
- `data/curated/` – merged climate-health dataset ready for visualization.

## Setup

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## Rebuild the datasets

Run the CLI modules (or execute the companion notebooks) from the project root:

```bash
# EDGAR emissions (creates data/processed/emissions_nuts2.parquet)
python -m src.pipeline.ingest_emissions

# Eurostat health metrics (creates data/processed/health_*.parquet)
python -m src.pipeline.ingest_health

# Merge emissions + health + Eurostat population into data/curated/eu_climate_health.parquet
python -m src.pipeline.harmonize --refresh-population
```

The harmonization step automatically downloads the latest Eurostat NUTS2 population figures (dataset `demo_r_pjangrp3`) via the `eurostat` Python package and caches them under `data/processed/population_nuts2.parquet`.

## Run the interactive dashboard

```bash
streamlit run app/main.py
```

Use the sidebar filters to focus on specific countries or regions, switch between emission/health metrics for the choropleth, and review sector or hospital-discharge breakouts.

## Testing & QA

Basic data-quality checks live under `tests/`. Run them with:

```bash
pytest
```

The suite verifies curated column coverage, region-year uniqueness, positive population counts, and alignment between sector totals and the aggregated emissions metric.

## Next steps

1. Perform exploratory analysis (`notebooks/02_eda.ipynb`) using the curated dataset.
2. Prototype the interactive dashboard (Streamlit/Dash) with shared filters for region, sector, gas, and health metrics.
3. Add automated data validation and deployment workflows.

