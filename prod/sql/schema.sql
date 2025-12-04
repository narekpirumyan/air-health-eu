-- ============================================
-- STAR SCHEMA DDL FOR EU CLIMATE & HEALTH DATA
-- SQLite Version - Multi-Fact Table Design
-- ============================================
-- 
-- This schema implements an enhanced star schema design with multiple fact tables:
-- - fact_emissions: GHG emissions by region, time, sector, gas (1990-2022)
-- - fact_causes_of_death: Causes of death rates by region, time, ICD-10 code (2000-2021)
-- - fact_hospital_discharges: Hospital discharge counts/rates by region, time, discharge type (2000-2021)
-- - fact_population: Population counts by region, time (varies by region)
--
-- Dimension tables:
-- - dim_geography: All NUTS levels (country, NUTS1, NUTS2, NUTS3+)
-- - dim_time: Time dimension with availability flags
-- - dim_sector: Emissions sectors
-- - dim_gas: Greenhouse gas types
-- - dim_icd10_cod: ICD-10 codes for causes of death
-- - dim_discharge_type: ICD-10 codes for hospital discharges
-- ============================================

-- Enable foreign key constraints (SQLite requires explicit enablement)
PRAGMA foreign_keys = ON;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Dimension: Geography (all NUTS levels)
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nuts_id TEXT NOT NULL UNIQUE,
    nuts_label TEXT,
    nuts_level INTEGER NOT NULL,  -- 0=country (2 chars), 1=NUTS1 (3 chars), 2=NUTS2 (4 chars), 3=NUTS3+ (5+ chars)
    country_iso TEXT NOT NULL,
    country_name TEXT,
    created_date TEXT DEFAULT (datetime('now')),
    updated_date TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_geography_nuts_id ON dim_geography(nuts_id);
CREATE INDEX IF NOT EXISTS idx_dim_geography_country_iso ON dim_geography(country_iso);
CREATE INDEX IF NOT EXISTS idx_dim_geography_nuts_level ON dim_geography(nuts_level);

-- Dimension: Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY,  -- Same as year for simplicity
    year INTEGER NOT NULL UNIQUE,
    decade INTEGER,  -- e.g., 1990, 2000, 2010
    year_label TEXT,  -- e.g., "2020"
    is_leap_year INTEGER DEFAULT 0,  -- 1 if leap year, 0 otherwise
    quarter INTEGER DEFAULT 4,  -- Always 4 for annual data
    is_emissions_available INTEGER DEFAULT 0,  -- 1 if emissions data exists for this year
    is_health_available INTEGER DEFAULT 0,  -- 1 if health data exists for this year
    is_population_available INTEGER DEFAULT 0,  -- 1 if population data exists for this year
    created_date TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_time_year ON dim_time(year);
CREATE INDEX IF NOT EXISTS idx_dim_time_decade ON dim_time(decade);

-- Dimension: Sector (Emissions)
CREATE TABLE IF NOT EXISTS dim_sector (
    sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_code TEXT NOT NULL UNIQUE,  -- Original sector name (e.g., "Agriculture", "Buildings")
    sector_name TEXT NOT NULL,  -- Human-readable name
    sector_group TEXT NOT NULL,  -- Grouped category (agriculture, buildings, energy, industry, transport, waste)
    sector_description TEXT,
    is_active INTEGER DEFAULT 1,  -- 1 if currently used, 0 if deprecated
    created_date TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_sector_code ON dim_sector(sector_code);
CREATE INDEX IF NOT EXISTS idx_dim_sector_group ON dim_sector(sector_group);

-- Dimension: Gas (Greenhouse Gases)
CREATE TABLE IF NOT EXISTS dim_gas (
    gas_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gas_code TEXT NOT NULL UNIQUE,  -- Gas code (e.g., "CO2", "CH4", "N2O", "F-gas")
    gas_name TEXT NOT NULL,  -- Full gas name
    gas_formula TEXT,  -- Chemical formula (e.g., "CO₂", "CH₄", "N₂O")
    gwp_ar5 REAL,  -- Global Warming Potential (AR5, 100-year)
    is_active INTEGER DEFAULT 1,  -- 1 if currently used, 0 if deprecated
    created_date TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_gas_code ON dim_gas(gas_code);

-- Dimension: ICD-10 Codes for Causes of Death
CREATE TABLE IF NOT EXISTS dim_icd10_cod (
    icd10_cod_id INTEGER PRIMARY KEY AUTOINCREMENT,
    icd10_code TEXT NOT NULL UNIQUE,  -- ICD-10 code (e.g., "J", "J09-J11", "J12-J18")
    icd10_name TEXT NOT NULL,  -- Human-readable name
    icd10_category TEXT,  -- Category grouping (e.g., "respiratory", "all_respiratory")
    icd10_description TEXT,  -- Detailed description
    is_respiratory INTEGER DEFAULT 0,  -- 1 if respiratory-related, 0 otherwise
    is_active INTEGER DEFAULT 1,  -- 1 if currently used, 0 if deprecated
    created_date TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_icd10_cod_code ON dim_icd10_cod(icd10_code);
CREATE INDEX IF NOT EXISTS idx_dim_icd10_cod_category ON dim_icd10_cod(icd10_category);

-- Dimension: Discharge Type (Hospital Discharges)
CREATE TABLE IF NOT EXISTS dim_discharge_type (
    discharge_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    discharge_code TEXT NOT NULL UNIQUE,  -- ICD-10 code (e.g., "J", "J00-J11", "J12-J18")
    discharge_name TEXT NOT NULL,  -- Human-readable name
    discharge_category TEXT,  -- Category grouping (e.g., "respiratory", "all_respiratory")
    icd10_codes TEXT,  -- Full ICD-10 code range (same as discharge_code for consistency)
    discharge_description TEXT,  -- Detailed description
    is_respiratory INTEGER DEFAULT 0,  -- 1 if respiratory-related, 0 otherwise
    is_active INTEGER DEFAULT 1,  -- 1 if currently used, 0 if deprecated
    created_date TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_discharge_type_code ON dim_discharge_type(discharge_code);
CREATE INDEX IF NOT EXISTS idx_dim_discharge_type_category ON dim_discharge_type(discharge_category);

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact Table: Emissions
-- Grain: (geography_id, time_id, sector_id, gas_id)
-- Timeline: 1990-2022 (full EDGAR coverage)
CREATE TABLE IF NOT EXISTS fact_emissions (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    geography_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    sector_id INTEGER NOT NULL,
    gas_id INTEGER NOT NULL,
    emissions_kt_co2e REAL NOT NULL,  -- Emissions in kilotons CO₂-equivalent
    created_date TEXT DEFAULT (datetime('now')),
    updated_date TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (sector_id) REFERENCES dim_sector(sector_id),
    FOREIGN KEY (gas_id) REFERENCES dim_gas(gas_id),
    UNIQUE(geography_id, time_id, sector_id, gas_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_emissions_geo_time ON fact_emissions(geography_id, time_id);
CREATE INDEX IF NOT EXISTS idx_fact_emissions_time ON fact_emissions(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_emissions_sector ON fact_emissions(sector_id);
CREATE INDEX IF NOT EXISTS idx_fact_emissions_gas ON fact_emissions(gas_id);
CREATE INDEX IF NOT EXISTS idx_fact_emissions_geo_sector ON fact_emissions(geography_id, sector_id);

-- Fact Table: Causes of Death
-- Grain: (geography_id, time_id, icd10_cod_id)
-- Timeline: 2000-2021 (full health data coverage)
CREATE TABLE IF NOT EXISTS fact_causes_of_death (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    geography_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    icd10_cod_id INTEGER NOT NULL,
    age_standardised_rate_per_100k REAL NOT NULL,  -- Age-standardized death rate per 100,000 population
    created_date TEXT DEFAULT (datetime('now')),
    updated_date TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (icd10_cod_id) REFERENCES dim_icd10_cod(icd10_cod_id),
    UNIQUE(geography_id, time_id, icd10_cod_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_causes_geo_time ON fact_causes_of_death(geography_id, time_id);
CREATE INDEX IF NOT EXISTS idx_fact_causes_time ON fact_causes_of_death(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_causes_icd10 ON fact_causes_of_death(icd10_cod_id);
CREATE INDEX IF NOT EXISTS idx_fact_causes_geo_icd10 ON fact_causes_of_death(geography_id, icd10_cod_id);

-- Fact Table: Hospital Discharges
-- Grain: (geography_id, time_id, discharge_type_id)
-- Timeline: 2000-2021 (full hospital data coverage)
CREATE TABLE IF NOT EXISTS fact_hospital_discharges (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    geography_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    discharge_type_id INTEGER NOT NULL,
    discharge_count INTEGER,  -- Raw discharge count (absolute number)
    discharge_rate_per_100k REAL,  -- Discharge rate per 100,000 population (calculated)
    created_date TEXT DEFAULT (datetime('now')),
    updated_date TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (discharge_type_id) REFERENCES dim_discharge_type(discharge_type_id),
    UNIQUE(geography_id, time_id, discharge_type_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_discharges_geo_time ON fact_hospital_discharges(geography_id, time_id);
CREATE INDEX IF NOT EXISTS idx_fact_discharges_time ON fact_hospital_discharges(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_discharges_type ON fact_hospital_discharges(discharge_type_id);
CREATE INDEX IF NOT EXISTS idx_fact_discharges_geo_type ON fact_hospital_discharges(geography_id, discharge_type_id);

-- Fact Table: Population
-- Grain: (geography_id, time_id)
-- Timeline: Varies by region (typically 2011-2021+)
CREATE TABLE IF NOT EXISTS fact_population (
    fact_key INTEGER PRIMARY KEY AUTOINCREMENT,
    geography_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,
    population INTEGER NOT NULL,  -- Total population count
    created_date TEXT DEFAULT (datetime('now')),
    updated_date TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (geography_id) REFERENCES dim_geography(geography_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    UNIQUE(geography_id, time_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_population_geo_time ON fact_population(geography_id, time_id);
CREATE INDEX IF NOT EXISTS idx_fact_population_time ON fact_population(time_id);

-- ============================================
-- VIEWS (Optional - for easier querying)
-- ============================================

-- View: Emissions summary with dimensions
CREATE VIEW IF NOT EXISTS vw_emissions AS
SELECT 
    g.nuts_id,
    g.nuts_label,
    g.nuts_level,
    g.country_iso,
    g.country_name,
    t.year,
    t.decade,
    s.sector_code,
    s.sector_name,
    s.sector_group,
    gas.gas_code,
    gas.gas_name,
    e.emissions_kt_co2e
FROM fact_emissions e
INNER JOIN dim_geography g ON e.geography_id = g.geography_id
INNER JOIN dim_time t ON e.time_id = t.time_id
INNER JOIN dim_sector s ON e.sector_id = s.sector_id
INNER JOIN dim_gas gas ON e.gas_id = gas.gas_id;

-- View: Causes of death summary with dimensions
CREATE VIEW IF NOT EXISTS vw_health_metrics AS
SELECT 
    g.nuts_id,
    g.nuts_label,
    g.nuts_level,
    g.country_iso,
    g.country_name,
    t.year,
    t.decade,
    icd.icd10_code,
    icd.icd10_name,
    icd.icd10_category,
    icd.is_respiratory,
    h.age_standardised_rate_per_100k
FROM fact_causes_of_death h
INNER JOIN dim_geography g ON h.geography_id = g.geography_id
INNER JOIN dim_time t ON h.time_id = t.time_id
INNER JOIN dim_icd10_cod icd ON h.icd10_cod_id = icd.icd10_cod_id;

-- View: Hospital discharges summary with dimensions
CREATE VIEW IF NOT EXISTS vw_hospital_discharges AS
SELECT 
    g.nuts_id,
    g.nuts_label,
    g.nuts_level,
    g.country_iso,
    g.country_name,
    t.year,
    t.decade,
    dt.discharge_code,
    dt.discharge_name,
    dt.discharge_category,
    dt.is_respiratory,
    d.discharge_count,
    d.discharge_rate_per_100k
FROM fact_hospital_discharges d
INNER JOIN dim_geography g ON d.geography_id = g.geography_id
INNER JOIN dim_time t ON d.time_id = t.time_id
INNER JOIN dim_discharge_type dt ON d.discharge_type_id = dt.discharge_type_id;
