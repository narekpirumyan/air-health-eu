# EU Air & Health Dashboard

This repository explores links between greenhouse gas emissions and respiratory health outcomes across EU NUTS2 regions. It ships reproducible ingestion pipelines, curated datasets, and (eventually) interactive visualizations.

## Project structure

- `data/raw/` – upstream datasets (EDGAR, Climate TRACE, Eurostat health TSVs, geo assets).
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

## Bring your own raw data

The repo ignores bulky upstream datasets. Collaborators must download the sources below and drop them into the indicated folders before running any pipelines:

| Dataset | Description | Download link | Expected location |
| --- | --- | --- | --- |
| Climate TRACE emissions (v4.8.0) | Country-level sector GHG emissions | https://climatetrace.org/downloads | `data/raw/emissions/Climattrace/` (preserve the `DATA/` tree) |
| EDGAR v8.0 GHG by substance (GWP100, AR5) | NUTS2 emissions by gas & sector | https://edgar.jrc.ec.europa.eu/dataset_ghg80 | `data/raw/emissions/EDGARv8.0_GHG_by substance_GWP100_AR5_NUTS2_1990_2022.xlsx` |
| Eurostat `hlth_cd_asdr2` | Age-standardised death rates | https://ec.europa.eu/eurostat/api/discover/datasets/hlth_cd_asdr2 | `data/raw/health/hlth_cd_asdr2.tsv` |
| Eurostat `hlth_co_disch1t` | Hospital discharges by diagnosis | https://ec.europa.eu/eurostat/api/discover/datasets/hlth_co_disch1t | `data/raw/health/hlth_co_disch1t.tsv` |
| NUTS 2021 GeoJSON (20 m, EPSG:4326) | Regional boundaries for the map | https://gisco-services.ec.europa.eu/distribution/v2/nuts/ | `data/raw/geo/NUTS_RG_20M_2021_4326.geojson` |

> Eurostat tip: pick the “TSV (raw)” option so the ingestion scripts can stream the files without additional conversions.

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

If Streamlit complains about missing parquet files, revisit the “Bring your own raw data” section and rerun the ingestion + harmonization steps. Use the sidebar filters to focus on specific countries or regions, switch between emission/health metrics for the choropleth, and review sector or hospital-discharge breakouts.

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

