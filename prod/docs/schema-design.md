# Database Schema Design - Enhanced Star Schema

## Overview

This document describes an enhanced star schema design for the EU Climate & Health data warehouse. The schema uses **multiple fact tables** to preserve the integrity of each data source's timeline and granularity, enabling flexible analysis across emissions, health metrics, and hospital discharge data.

## Design Principles

1. **Separate Fact Tables**: Each data source (emissions, health metrics, hospital discharges) has its own fact table with its complete timeline
2. **No Data Loss**: Fact tables are not cut to match merged data - each preserves its full temporal coverage
3. **Normalized Dimensions**: Dimension tables capture all descriptive attributes to avoid redundancy
4. **Flexible Analysis**: Schema supports both individual dataset analysis and cross-dataset correlation analysis
5. **Scalability**: Structure allows easy addition of new fact tables or dimension attributes

---

## Dimension Tables

### `dim_geography`
**Purpose**: Geographic regions at all NUTS levels (country, NUTS1, NUTS2, NUTS3+)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `geography_id` | INTEGER | PRIMARY KEY | Surrogate key |
| `nuts_id` | TEXT | UNIQUE, NOT NULL | NUTS code (2-5+ characters, e.g., "AT", "AT1", "AT11", "AT111") |
| `nuts_label` | TEXT | | Full NUTS region name (when available) |
| `country_iso` | TEXT | NOT NULL | ISO 3166-1 alpha-2 country code (e.g., "AT") |
| `country_name` | TEXT | | Full country name (e.g., "Austria") |
| `nuts_level` | INTEGER | NOT NULL | NUTS hierarchy level: 0=country (2 chars), 1=NUTS1 (3 chars), 2=NUTS2 (4 chars), 3=NUTS3+ (5+ chars) |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |
| `updated_date` | TEXT | DEFAULT (datetime('now')) | Record last update timestamp |

**Indexes**:
- `idx_dim_geography_nuts_id` on `nuts_id`
- `idx_dim_geography_country_iso` on `country_iso`
- `idx_dim_geography_nuts_level` on `nuts_level`

**Notes**:
- Supports all NUTS levels for maximum flexibility in analysis
- Country information is denormalized for query performance
- NUTS level is calculated from code length: 2 chars = country, 3 = NUTS1, 4 = NUTS2, 5+ = NUTS3+

---

### `dim_time`
**Purpose**: Time dimension with calendar attributes

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `time_id` | INTEGER | PRIMARY KEY | Surrogate key (year value) |
| `year` | INTEGER | UNIQUE, NOT NULL | Calendar year (1990-2022+) |
| `decade` | INTEGER | | Decade start year (e.g., 1990, 2000, 2010) |
| `year_label` | TEXT | | Formatted year string (e.g., "2020") |
| `is_leap_year` | INTEGER | DEFAULT 0 | 1 if leap year, 0 otherwise |
| `quarter` | INTEGER | | Quarter number (1-4) - always 4 for annual data |
| `is_emissions_available` | INTEGER | DEFAULT 0 | 1 if emissions data exists for this year |
| `is_health_available` | INTEGER | DEFAULT 0 | 1 if health data exists for this year |
| `is_population_available` | INTEGER | DEFAULT 0 | 1 if population data exists for this year |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |

**Indexes**:
- `idx_dim_time_year` on `year`
- `idx_dim_time_decade` on `decade`

**Notes**:
- `time_id` equals `year` for simplicity (no need for separate surrogate)
- Flags indicate data availability per source for query optimization

---

### `dim_sector`
**Purpose**: Emissions sectors and their groupings

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `sector_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `sector_code` | TEXT | UNIQUE, NOT NULL | Original sector code/name (e.g., "Agriculture", "Buildings") |
| `sector_name` | TEXT | NOT NULL | Human-readable sector name |
| `sector_group` | TEXT | NOT NULL | Grouped sector category (agriculture, buildings, energy, industry, transport, waste) |
| `sector_description` | TEXT | | Optional detailed description |
| `is_active` | INTEGER | DEFAULT 1 | 1 if currently used, 0 if deprecated |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |

**Indexes**:
- `idx_dim_sector_code` on `sector_code`
- `idx_dim_sector_group` on `sector_group`

**Sample Data**:
- `sector_code`: "Agriculture" → `sector_group`: "agriculture"
- `sector_code`: "Buildings" → `sector_group`: "buildings"
- `sector_code`: "Energy" → `sector_group`: "energy"
- `sector_code`: "Industry" → `sector_group`: "industry"
- `sector_code`: "Transport" → `sector_group`: "transport"
- `sector_code`: "Waste" → `sector_group`: "waste"

---

### `dim_gas`
**Purpose**: Greenhouse gas types

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `gas_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `gas_code` | TEXT | UNIQUE, NOT NULL | Gas code (e.g., "CO2", "CH4", "N2O", "F-gas") |
| `gas_name` | TEXT | NOT NULL | Full gas name |
| `gas_formula` | TEXT | | Chemical formula (e.g., "CO₂", "CH₄", "N₂O") |
| `gwp_ar5` | REAL | | Global Warming Potential (AR5, 100-year) |
| `is_active` | INTEGER | DEFAULT 1 | 1 if currently used, 0 if deprecated |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |

**Indexes**:
- `idx_dim_gas_code` on `gas_code`

**Sample Data**:
- `gas_code`: "CO2" → `gas_name`: "Carbon Dioxide", `gwp_ar5`: 1.0
- `gas_code`: "GWP_100_AR5_CH4" → `gas_name`: "Methane", `gwp_ar5`: 28.0
- `gas_code`: "GWP_100_AR5_N2O" → `gas_name`: "Nitrous Oxide", `gwp_ar5`: 265.0
- `gas_code`: "GWP_100_AR5_F-gases" → `gas_name`: "Fluorinated Gases", `gwp_ar5`: varies

---

### `dim_icd10_cod`
**Purpose**: ICD-10 codes for Causes of Death (age-standardized rates)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `icd10_cod_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `icd10_code` | TEXT | UNIQUE, NOT NULL | ICD-10 code (e.g., "J", "J09-J11", "J12-J18") |
| `icd10_name` | TEXT | NOT NULL | Human-readable name |
| `icd10_category` | TEXT | | Category grouping (e.g., "respiratory", "all_respiratory") |
| `icd10_description` | TEXT | | Detailed description of the condition |
| `is_respiratory` | INTEGER | DEFAULT 0 | 1 if respiratory-related, 0 otherwise |
| `is_active` | INTEGER | DEFAULT 1 | 1 if currently used, 0 if deprecated |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |

**Indexes**:
- `idx_dim_icd10_cod_code` on `icd10_code`
- `idx_dim_icd10_cod_category` on `icd10_category`

**Sample Data** (Respiratory-related):
- `icd10_code`: "J" → `icd10_name`: "All Respiratory Diseases", `icd10_category`: "all_respiratory"
- `icd10_code`: "J09-J11" → `icd10_name`: "Influenza", `icd10_category`: "respiratory"
- `icd10_code`: "J12-J18" → `icd10_name`: "Pneumonia", `icd10_category`: "respiratory"
- `icd10_code`: "J40-J44_J47" → `icd10_name`: "COPD", `icd10_category`: "respiratory"
- `icd10_code`: "J45_J46" → `icd10_name`: "Asthma", `icd10_category`: "respiratory"

---

### `dim_discharge_type`
**Purpose**: ICD-10 codes for Hospital Discharges

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `discharge_type_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `discharge_code` | TEXT | UNIQUE, NOT NULL | ICD-10 code (e.g., "J", "J00-J11", "J12-J18") |
| `discharge_name` | TEXT | NOT NULL | Human-readable name |
| `discharge_category` | TEXT | | Category grouping (e.g., "respiratory", "all_respiratory") |
| `icd10_codes` | TEXT | | Full ICD-10 code range (same as discharge_code for consistency) |
| `discharge_description` | TEXT | | Detailed description |
| `is_respiratory` | INTEGER | DEFAULT 0 | 1 if respiratory-related, 0 otherwise |
| `is_active` | INTEGER | DEFAULT 1 | 1 if currently used, 0 if deprecated |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |

**Indexes**:
- `idx_dim_discharge_type_code` on `discharge_code`
- `idx_dim_discharge_type_category` on `discharge_category`

**Sample Data** (Respiratory-related):
- `discharge_code`: "J" → `discharge_name`: "All Respiratory Diseases", `discharge_category`: "all_respiratory"
- `discharge_code`: "J00-J11" → `discharge_name`: "Upper Respiratory Infections", `discharge_category`: "respiratory"
- `discharge_code`: "J12-J18" → `discharge_name`: "Pneumonia", `discharge_category`: "respiratory"
- `discharge_code`: "J20-J22" → `discharge_name`: "Bronchitis", `discharge_category`: "respiratory"
- `discharge_code`: "J40-J44_J47" → `discharge_name`: "COPD", `discharge_category`: "respiratory"
- `discharge_code`: "J45_J46" → `discharge_name`: "Asthma", `discharge_category`: "respiratory"
- `discharge_code`: "J60-J99" → `discharge_name`: "Other Respiratory", `discharge_category`: "respiratory"

---

## Fact Tables

### `fact_emissions`
**Purpose**: Greenhouse gas emissions by region, time, sector, and gas type

**Grain**: One row per `(geography_id, time_id, sector_id, gas_id)`

**Timeline**: 1990-2022 (full EDGAR coverage, not cut)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `fact_key` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `geography_id` | INTEGER | NOT NULL, FK → dim_geography | Region identifier |
| `time_id` | INTEGER | NOT NULL, FK → dim_time | Year identifier |
| `sector_id` | INTEGER | NOT NULL, FK → dim_sector | Sector identifier |
| `gas_id` | INTEGER | NOT NULL, FK → dim_gas | Gas type identifier |
| `emissions_kt_co2e` | REAL | NOT NULL | Emissions in kilotons CO₂-equivalent |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |
| `updated_date` | TEXT | DEFAULT (datetime('now')) | Record last update timestamp |

**Constraints**:
- `UNIQUE(geography_id, time_id, sector_id, gas_id)` - ensures no duplicates
- Foreign keys to `dim_geography`, `dim_time`, `dim_sector`, `dim_gas`

**Indexes**:
- `idx_fact_emissions_geo_time` on `(geography_id, time_id)`
- `idx_fact_emissions_time` on `time_id`
- `idx_fact_emissions_sector` on `sector_id`
- `idx_fact_emissions_gas` on `gas_id`
- `idx_fact_emissions_geo_sector` on `(geography_id, sector_id)`

**Notes**:
- Preserves full EDGAR timeline (1990-2022)
- Granularity: region × year × sector × gas
- Supports aggregation by any dimension combination
- All emissions are already in CO₂-equivalent (GWP-100 AR5)

**Source Data Columns** (from `emissions_nuts2.parquet`):
- `nuts_id`, `nuts_label`, `country_iso`, `country_name` → mapped to `dim_geography`
- `year` → mapped to `dim_time`
- `gas` → mapped to `dim_gas`
- `sector` → mapped to `dim_sector`
- `sector_group` → stored in `dim_sector.sector_group` (agriculture, buildings, energy, industry, transport, waste)
- `emissions_kt_co2e` → mapped to fact table measure

---

### `fact_health_metrics`
**Purpose**: Age-standardized causes of death rates by region and time

**Grain**: One row per `(geography_id, time_id, icd10_cod_id)`

**Timeline**: 2000-2021 (full health data coverage, not cut)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `fact_key` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `geography_id` | INTEGER | NOT NULL, FK → dim_geography | Region identifier |
| `time_id` | INTEGER | NOT NULL, FK → dim_time | Year identifier |
| `icd10_cod_id` | INTEGER | NOT NULL, FK → dim_icd10_cod | ICD-10 code identifier |
| `age_standardised_rate_per_100k` | REAL | NOT NULL | Age-standardized death rate per 100,000 population |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |
| `updated_date` | TEXT | DEFAULT (datetime('now')) | Record last update timestamp |

**Constraints**:
- `UNIQUE(geography_id, time_id, icd10_cod_id)` - ensures no duplicates
- Foreign keys to `dim_geography`, `dim_time`, `dim_icd10_cod`

**Indexes**:
- `idx_fact_health_geo_time` on `(geography_id, time_id)`
- `idx_fact_health_time` on `time_id`
- `idx_fact_health_icd10` on `icd10_cod_id`
- `idx_fact_health_geo_icd10` on `(geography_id, icd10_cod_id)`

**Notes**:
- Preserves full health data timeline (2000-2021)
- Granularity: region × year × ICD-10 code
- Rates are age-standardized (already adjusted for population age structure)
- Focus on respiratory diseases (J codes) but schema supports all ICD-10 codes

**Source Data Columns** (from `health_causes_of_death.parquet`):
- `nuts_id`, `year`, `icd10_group` → mapped to fact table
- `age_standardised_rate_per_100k` → mapped to fact table
- **Metadata fields** (not in fact table, available in processed data):
  - `frequency`: Data frequency (typically "A" for annual)
  - `unit_code`: Unit code (typically "R_HTHAB" for rate per 100,000)
  - `sex`: Sex/gender dimension (typically "T" for total, or "M"/"F" for male/female)
  - `age_group`: Age group (typically "TOTAL" for all ages, or specific age ranges)
  
  *Note: These metadata fields are currently not stored in the fact table but are available in the processed parquet files. They can be added to dimension tables if needed for filtering/analysis.*

---

### `fact_hospital_discharges`
**Purpose**: Hospital discharge counts and rates by region, time, and discharge type

**Grain**: One row per `(geography_id, time_id, discharge_type_id)`

**Timeline**: 2000-2021 (full hospital data coverage, not cut)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `fact_key` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `geography_id` | INTEGER | NOT NULL, FK → dim_geography | Region identifier |
| `time_id` | INTEGER | NOT NULL, FK → dim_time | Year identifier |
| `discharge_type_id` | INTEGER | NOT NULL, FK → dim_discharge_type | Discharge type identifier |
| `discharge_count` | INTEGER | | Raw discharge count (absolute number) |
| `discharge_rate_per_100k` | REAL | | Discharge rate per 100,000 population (calculated) |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |
| `updated_date` | TEXT | DEFAULT (datetime('now')) | Record last update timestamp |

**Constraints**:
- `UNIQUE(geography_id, time_id, discharge_type_id)` - ensures no duplicates
- Foreign keys to `dim_geography`, `dim_time`, `dim_discharge_type`

**Indexes**:
- `idx_fact_discharges_geo_time` on `(geography_id, time_id)`
- `idx_fact_discharges_time` on `time_id`
- `idx_fact_discharges_type` on `discharge_type_id`
- `idx_fact_discharges_geo_type` on `(geography_id, discharge_type_id)`

**Notes**:
- Preserves full hospital data timeline (2000-2021)
- Granularity: region × year × discharge type
- Both raw counts and rates stored for flexibility
- **`discharge_rate_per_100k` is calculated** during ETL: `(discharge_count / population) * 100,000`
- Raw data contains `discharges` (absolute count), which maps to `discharge_count` in the fact table

**Source Data Columns** (from `health_hospital_discharges.parquet`):
- `nuts_id`, `year`, `icd10_group` → mapped to fact table
- `discharges` → mapped to `discharge_count` in fact table
- **Metadata fields** (not in fact table, available in processed data):
  - `frequency`: Data frequency (typically "A" for annual)
  - `indicator`: Indicator code (e.g., "DISCH" for discharges)
  - `unit_code`: Unit code (typically "NR" for number, or "R_HTHAB" for rate per 100,000)
  - `sex`: Sex/gender dimension (typically "T" for total, or "M"/"F" for male/female)
  - `age_group`: Age group (typically "TOTAL" for all ages, or specific age ranges)
  
  *Note: These metadata fields are currently not stored in the fact table but are available in the processed parquet files. They can be added to dimension tables if needed for filtering/analysis.*

---

### `fact_population`
**Purpose**: Population counts by region and time

**Grain**: One row per `(geography_id, time_id)`

**Timeline**: Varies by region (typically 2011-2021+, full population data coverage)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `fact_key` | INTEGER | PRIMARY KEY AUTOINCREMENT | Surrogate key |
| `geography_id` | INTEGER | NOT NULL, FK → dim_geography | Region identifier |
| `time_id` | INTEGER | NOT NULL, FK → dim_time | Year identifier |
| `population` | INTEGER | NOT NULL | Total population count |
| `created_date` | TEXT | DEFAULT (datetime('now')) | Record creation timestamp |
| `updated_date` | TEXT | DEFAULT (datetime('now')) | Record last update timestamp |

**Constraints**:
- `UNIQUE(geography_id, time_id)` - ensures one population value per region-year
- Foreign keys to `dim_geography`, `dim_time`

**Indexes**:
- `idx_fact_population_geo_time` on `(geography_id, time_id)`
- `idx_fact_population_time` on `time_id`

**Notes**:
- Population is a fact (measure) that can be joined to other fact tables
- Used for calculating per-capita metrics (e.g., emissions per capita, discharge rates)
- Timeline varies by region but typically covers 2011-2021+

---

## Schema Relationships

```
dim_geography (1) ────< (many) fact_emissions
dim_time (1) ─────────< (many) fact_emissions
dim_sector (1) ───────< (many) fact_emissions
dim_gas (1) ──────────< (many) fact_emissions

dim_geography (1) ────< (many) fact_health_metrics
dim_time (1) ─────────< (many) fact_health_metrics
dim_icd10_cod (1) ────< (many) fact_health_metrics

dim_geography (1) ────< (many) fact_hospital_discharges
dim_time (1) ─────────< (many) fact_hospital_discharges
dim_discharge_type (1) ─< (many) fact_hospital_discharges

dim_geography (1) ────< (many) fact_population
dim_time (1) ─────────< (many) fact_population
```

---

## Key Advantages of This Design

### 1. **Preserved Timelines**
- **Emissions**: Full 1990-2022 coverage (33 years)
- **Health Metrics**: Full 2000-2021 coverage (22 years)
- **Hospital Discharges**: Full 2000-2021 coverage (22 years)
- **Population**: Varies by region (typically 2011-2021+)

No data is cut to match merged datasets - each fact table maintains its complete temporal coverage.

### 2. **Flexible Analysis**
- **Individual Analysis**: Query each fact table independently
- **Cross-Dataset Analysis**: Join fact tables on `geography_id` and `time_id` for correlation analysis
- **Aggregation**: Aggregate by any dimension combination (region, time, sector, gas, ICD-10 code, etc.)

### 3. **Normalized Dimensions**
- All descriptive attributes stored in dimension tables
- No redundancy across fact tables
- Easy to update dimension attributes without touching fact data

### 4. **Scalability**
- Easy to add new fact tables (e.g., `fact_air_quality`, `fact_economic_indicators`)
- Easy to add new dimension attributes (e.g., `dim_geography.latitude`, `dim_geography.longitude`)
- Supports future data sources without schema changes

### 5. **Query Performance**
- Indexes on all foreign keys and common filter columns
- Composite indexes for common join patterns
- Dimension flags (e.g., `is_emissions_available`) enable query optimization

---

## Example Queries

### 1. Total Emissions by Country and Year
```sql
SELECT 
    g.country_name,
    t.year,
    SUM(e.emissions_kt_co2e) AS total_emissions_kt
FROM fact_emissions e
JOIN dim_geography g ON e.geography_id = g.geography_id
JOIN dim_time t ON e.time_id = t.time_id
GROUP BY g.country_name, t.year
ORDER BY g.country_name, t.year;
```

### 2. Respiratory Death Rates by Region (2020)
```sql
SELECT 
    g.nuts_label,
    g.country_name,
    icd.icd10_name,
    h.age_standardised_rate_per_100k
FROM fact_health_metrics h
JOIN dim_geography g ON h.geography_id = g.geography_id
JOIN dim_time t ON h.time_id = t.time_id
JOIN dim_icd10_cod icd ON h.icd10_cod_id = icd.icd10_cod_id
WHERE t.year = 2020
  AND icd.is_respiratory = 1
ORDER BY h.age_standardised_rate_per_100k DESC;
```

### 3. Correlation: Emissions vs. Health Metrics (2011-2021)
```sql
SELECT 
    g.nuts_id,
    g.nuts_label,
    t.year,
    SUM(e.emissions_kt_co2e) AS total_emissions_kt,
    h.age_standardised_rate_per_100k AS cod_all_resp_rate
FROM fact_emissions e
JOIN dim_geography g ON e.geography_id = g.geography_id
JOIN dim_time t ON e.time_id = t.time_id
LEFT JOIN fact_health_metrics h ON 
    h.geography_id = g.geography_id 
    AND h.time_id = t.time_id
    AND h.icd10_cod_id = (SELECT icd10_cod_id FROM dim_icd10_cod WHERE icd10_code = 'J')
WHERE t.year BETWEEN 2011 AND 2021
GROUP BY g.nuts_id, g.nuts_label, t.year, h.age_standardised_rate_per_100k
ORDER BY g.nuts_id, t.year;
```

### 4. Emissions per Capita by Sector (2020)
```sql
SELECT 
    s.sector_name,
    s.sector_group,
    SUM(e.emissions_kt_co2e) AS total_emissions_kt,
    SUM(p.population) AS total_population,
    (SUM(e.emissions_kt_co2e) * 1000.0 / SUM(p.population)) AS emissions_per_capita_tonnes
FROM fact_emissions e
JOIN dim_sector s ON e.sector_id = s.sector_id
JOIN dim_time t ON e.time_id = t.time_id
JOIN fact_population p ON 
    p.geography_id = e.geography_id 
    AND p.time_id = t.time_id
WHERE t.year = 2020
GROUP BY s.sector_name, s.sector_group
ORDER BY emissions_per_capita_tonnes DESC;
```

### 5. Hospital Discharge Rates by Type (2021)
```sql
SELECT 
    g.country_name,
    dt.discharge_name,
    SUM(d.discharge_count) AS total_discharges,
    AVG(d.discharge_rate_per_100k) AS avg_rate_per_100k
FROM fact_hospital_discharges d
JOIN dim_geography g ON d.geography_id = g.geography_id
JOIN dim_time t ON d.time_id = t.time_id
JOIN dim_discharge_type dt ON d.discharge_type_id = dt.discharge_type_id
WHERE t.year = 2021
  AND dt.is_respiratory = 1
GROUP BY g.country_name, dt.discharge_name
ORDER BY g.country_name, avg_rate_per_100k DESC;
```

---

## Data Loading Strategy

### 1. **Load Dimensions First**
- `dim_geography`: Extract unique NUTS codes from all sources (all levels: country, NUTS1, NUTS2, NUTS3+)
  - Calculate `nuts_level` from code length (2=country, 3=NUTS1, 4=NUTS2, 5+=NUTS3+)
  - Merge geography info from emissions, health, and population sources
- `dim_time`: Generate all years from 1990 to 2022+
- `dim_sector`: Extract unique sectors from emissions data (`sector` column)
  - Map `sector_group` from processed data (agriculture, buildings, energy, industry, transport, waste)
- `dim_gas`: Extract unique gases from emissions data (`gas` column)
  - Includes: CO2, CH4, N2O, F-gas (and their GWP-100 AR5 equivalents)
- `dim_icd10_cod`: Extract unique ICD-10 codes from causes of death data (`icd10_group` column)
- `dim_discharge_type`: Extract unique discharge types from hospital data (`icd10_group` column)

### 2. **Load Fact Tables**
- `fact_population`: Load from `population_nuts2.parquet` (columns: `geo`, `year`, `population`)
  - Map `geo` → `geography_id` via `dim_geography`
- `fact_emissions`: Load from `emissions_nuts2.parquet` (columns: `nuts_id`, `year`, `gas`, `sector`, `emissions_kt_co2e`)
  - Map `nuts_id` → `geography_id`, `gas` → `gas_id`, `sector` → `sector_id`
  - Preserve all years 1990-2022 (full EDGAR coverage)
- `fact_health_metrics`: Load from `health_causes_of_death.parquet` (columns: `nuts_id`, `year`, `icd10_group`, `age_standardised_rate_per_100k`)
  - Map `nuts_id` → `geography_id`, `icd10_group` → `icd10_cod_id`
  - Preserve all years 2000-2021 (full health data coverage)
  - *Note: Metadata fields (`frequency`, `unit_code`, `sex`, `age_group`) are not loaded into fact table*
- `fact_hospital_discharges`: Load from `health_hospital_discharges.parquet` (columns: `nuts_id`, `year`, `icd10_group`, `discharges`)
  - Map `nuts_id` → `geography_id`, `icd10_group` → `discharge_type_id`
  - Map `discharges` → `discharge_count`
  - **Calculate** `discharge_rate_per_100k` = `(discharge_count / population) * 100,000` (join with `fact_population`)
  - Preserve all years 2000-2021 (full hospital data coverage)
  - *Note: Metadata fields (`frequency`, `indicator`, `unit_code`, `sex`, `age_group`) are not loaded into fact table*

### 3. **Update Dimension Flags**
- Update `dim_time.is_emissions_available` based on `fact_emissions`
- Update `dim_time.is_health_available` based on `fact_health_metrics`
- Update `dim_time.is_population_available` based on `fact_population`

---

## Future Enhancements

### Potential Additional Fact Tables
- `fact_air_quality`: Air quality metrics (PM2.5, PM10, NO₂, O₃) by region and time
- `fact_economic_indicators`: GDP, employment, income by region and time
- `fact_weather`: Temperature, precipitation, extreme events by region and time

### Potential Additional Dimensions
- `dim_icd10_detail`: More granular ICD-10 codes (if needed)
- `dim_age_group`: Age groups for population demographics
- `dim_sex`: Sex/gender dimension (if needed for detailed health analysis)

### Potential Bridge Tables
- `bridge_geography_hierarchy`: NUTS hierarchy (NUTS0 → NUTS1 → NUTS2 → NUTS3)
- `bridge_sector_hierarchy`: Sector hierarchy (if sectors have sub-categories)

---

## Data Source Mapping

### Processed Data Files → Schema Tables

| Processed File | Columns | Mapped To |
|----------------|---------|-----------|
| `emissions_nuts2.parquet` | `nuts_id`, `nuts_label`, `country_iso`, `country_name`, `year`, `gas`, `sector`, `sector_group`, `emissions_kt_co2e` | `dim_geography`, `dim_sector`, `dim_gas`, `fact_emissions` |
| `health_causes_of_death.parquet` | `nuts_id`, `year`, `frequency`, `unit_code`, `sex`, `age_group`, `icd10_group`, `age_standardised_rate_per_100k` | `dim_geography`, `dim_icd10_cod`, `fact_health_metrics` |
| `health_hospital_discharges.parquet` | `nuts_id`, `year`, `frequency`, `indicator`, `unit_code`, `sex`, `age_group`, `icd10_group`, `discharges` | `dim_geography`, `dim_discharge_type`, `fact_hospital_discharges` |
| `population_nuts2.parquet` | `geo`, `year`, `population` | `dim_geography`, `fact_population` |

**Note**: Metadata fields (`frequency`, `unit_code`, `sex`, `age_group`, `indicator`) are available in processed files but not currently stored in fact tables. They can be added to dimension tables if needed for filtering or analysis.

---

## Migration Notes

### From Current Schema
The current `fact_climate_health` table is a **merged/denormalized** fact table. Migration to this new schema involves:

1. **Splitting** `fact_climate_health` into separate fact tables
2. **Extracting** dimension data from the merged table
3. **Preserving** all historical data (no data loss)
4. **Loading from processed files** instead of curated file (for production)
5. **Maintaining** backward compatibility through views (if needed)

### View for Backward Compatibility
```sql
CREATE VIEW vw_climate_health_merged AS
SELECT 
    g.nuts_id,
    g.nuts_label,
    g.country_iso,
    g.country_name,
    t.year,
    p.population,
    -- Aggregated emissions
    SUM(CASE WHEN s.sector_group = 'agriculture' THEN e.emissions_kt_co2e ELSE 0 END) AS emissions_agriculture_kt,
    SUM(CASE WHEN s.sector_group = 'buildings' THEN e.emissions_kt_co2e ELSE 0 END) AS emissions_buildings_kt,
    -- ... other sectors
    SUM(e.emissions_kt_co2e) AS total_emissions_kt,
    -- Health metrics
    MAX(CASE WHEN icd.icd10_code = 'J' THEN h.age_standardised_rate_per_100k END) AS cod_all_resp_rate,
    -- ... other health metrics
FROM dim_geography g
JOIN dim_time t ON 1=1
LEFT JOIN fact_population p ON p.geography_id = g.geography_id AND p.time_id = t.time_id
LEFT JOIN fact_emissions e ON e.geography_id = g.geography_id AND e.time_id = t.time_id
LEFT JOIN dim_sector s ON e.sector_id = s.sector_id
LEFT JOIN fact_health_metrics h ON h.geography_id = g.geography_id AND h.time_id = t.time_id
LEFT JOIN dim_icd10_cod icd ON h.icd10_cod_id = icd.icd10_cod_id
GROUP BY g.nuts_id, g.nuts_label, g.country_iso, g.country_name, t.year, p.population;
```

---

## Summary

This enhanced star schema design provides:

✅ **Multiple fact tables** preserving full timelines  
✅ **Normalized dimensions** for efficient storage  
✅ **Flexible analysis** across datasets  
✅ **Scalable structure** for future enhancements  
✅ **Query performance** through strategic indexing  
✅ **No data loss** - all historical data preserved  

The schema is optimized for both **individual dataset analysis** and **cross-dataset correlation analysis**, making it ideal for exploring relationships between emissions, health metrics, and hospital discharges across EU regions.

