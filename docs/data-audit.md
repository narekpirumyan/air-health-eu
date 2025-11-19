# Data Audit – EU Climate & Health Dashboard

## 1. Snapshot
- Primary emissions data already available locally (EDGAR NUTS2 + Climate TRACE facility-level) covering 1990–2022.
- Eurostat health datasets (causes of death, hospital discharges) downloaded but still in raw TSV with wide year format and metadata codes.
- Geo-boundaries and population denominators for per-capita metrics are missing and must be sourced (e.g., Eurostat GISCO, Eurostat demo_r_pjan). 
- Common join key will be NUTS2 code; all datasets use ISO country codes but require alignment of naming conventions and reference years.

## 2. Emissions Data Sources

### 2.1 EDGAR v8.0 GHG (NUTS2)
- **Location:** DataSources/Emmissions/EDGARv8.0_GHG_by substance_GWP100_AR5_NUTS2_1990_2022.xlsx
- **Sheets:** Fossil CO2 AR5, CH4_AR5, N2O_AR5, F-gas AR5 (+ info).
- **Granularity:** NUTS2 x Sector x Gas, annual 1990–2022, units = kt CO₂-eq (AR5 GWP100).
- **Schema (after skipping first 5 metadata rows):** Substance, ISO, Country, NUTS 2, NUTS 2 desc, Sector, Y_1990 ... Y_2022.
- **Transform needs:**
  - Melt yearly columns into tidy format (year, value).
  - Normalize sector taxonomy (map to transport/industry/energy/agriculture/buildings/other for UI filters).
  - Derive per-capita metrics after population join; optional log-scaled metrics for visualization stability.
  - Filter to EU27 + EFTA as needed; confirm small territories present.

### 2.2 Climate TRACE Facility-Level Emissions
- **Location:** DataSources/Emmissions/Climattrace/
  - Schema file: ABOUT_THE_DATA/detailed_data_schema_v4_8_0.csv (47 columns describing metadata such as sector, subsector, source_id, lat, lon, gas, emissions_quantity).
  - Sector folders under DATA/ (agriculture, power, transportation, etc.) contain country-level emissions + confidence tables.
- **Usage intent:** enrich dashboard with point sources / sector deep dives (e.g., specific plants or operations) or cross-check EDGAR totals.
- **Transform needs:** standardize coordinate system (WGS84 already), convert reporting windows to calendar year, aggregate to NUTS2 by spatial join using facility coordinates, track confidence intervals where available.

## 3. Health Data Sources (Eurostat)

### 3.1 Causes of Death – Respiratory (hlth_cd_asdr2)
- **Location:** DataSources/Diseases/hlth_cd_asdr2.tsv
- **Fields (wide format):** freq, unit, sex, age, icd10, geo\TIME_PERIOD, 2011 ... 2022.
- **Content:** Age-standardised death rates per 100k for ICD10 respiratory groupings (e.g., A-R_V-Y).
- **Transform needs:**
  - Split geo\TIME_PERIOD into geo (NUTS2/NUTS3/country) + year values.
  - Pivot longer on year columns; convert : placeholders to NaN.
  - Filter ICD10 codes relevant to asthma, COPD, bronchitis, pneumonia; map to friendly labels.
  - Validate geographic coverage (dataset includes AT, AT1, AT11, etc.); confirm all EU NUTS2 present.

### 3.2 Hospital Discharges – Respiratory (hlth_co_disch1t)
- **Location:** DataSources/Diseases/hlth_co_disch1t.tsv
- **Fields:** freq, age, indic_he, unit, sex, icd10, geo\TIME_PERIOD, 2000 ... 2021.
- **Content:** Number of in-patient discharges by diagnosis (ICD10). Values currently : for many rows; first non-null entries observed around 2015 for Austrian regions.
- **Transform needs:** similar pivot as above, convert counts to per-100k if population available, map indicator codes (e.g., INPAT, NR), filter respiratory ICD10 groups.

## 4. Supporting Data Requirements
- **Geo boundaries:** Need EU NUTS2 GeoJSON (from Eurostat GISCO NUTS_RG_01M_2021_4326.geojson). Required for Plotly choropleths + spatial joins.
- **Population:** Annual NUTS2 population (Eurostat demo_r_pjanaggr3 or demo_r_pjangrp3) to compute per-capita emissions and health metrics.
- **Lookup tables:** NUTS2 ↔ country mapping, sector category mapping, ICD10 group dictionary for readable labels.

## 5. Gaps & Next Steps
1. Acquire and version-control geo boundaries + population tables under data/raw/geo and data/raw/population.
2. Document provenance + license details for each dataset in docs/data-audit.md (this file) and future README.
3. Define unified calendar coverage (likely 2010–2021 intersection between emissions and health) and flag missing years per dataset.
4. Decide on storage format for processed layers (Parquet recommended) and directory layout (data/processed, data/curated).
5. Establish data quality checks: NUTS2 completeness, outlier detection, consistent units.

This audit will guide the upcoming ingestion notebooks and ensures the dashboard filters (region/sector/gas/per-capita) are backed by harmonized, well-documented sources.
