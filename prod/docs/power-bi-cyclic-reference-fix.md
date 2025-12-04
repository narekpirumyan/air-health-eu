# Power BI Cyclic Reference Fix

## Problem

When loading the SQLite database into Power BI, you may encounter cyclic reference errors for:
- `dim_gas`
- `dim_icd10_cod`
- `dim_sector`
- `fact_causes_of_death`
- `fact_emissions`
- `vw_emissions`

## Root Cause

The cyclic references are caused by **loading both fact tables AND views** that join the same dimension tables. Power BI auto-detects relationships from:
- Fact tables → Dimension tables (via foreign keys)
- Views → Dimension tables (via joined columns)

This creates a cycle because views already contain the joined data from fact and dimension tables.

## Solution 1: Load Only Views (Recommended)

**Option A: Load only views (simplest)**
1. In Power BI, connect to SQLite database
2. **Only select these tables:**
   - `vw_emissions`
   - `vw_health_metrics`
   - `vw_hospital_discharges`
   - `dim_geography` (if needed for additional filtering)
   - `dim_time` (if needed for additional filtering)
3. **Do NOT load:**
   - `fact_emissions`
   - `fact_causes_of_death`
   - `fact_hospital_discharges`
   - `fact_population`
   - Individual dimension tables (already in views)

**Why this works:** Views already contain all the joined data, so you don't need the separate fact/dimension tables.

## Solution 2: Load Only Fact Tables (More Control)

**Option B: Load only fact and dimension tables**
1. In Power BI, connect to SQLite database
2. **Only select these tables:**
   - `fact_emissions`
   - `fact_causes_of_death`
   - `fact_hospital_discharges`
   - `fact_population`
   - `dim_geography`
   - `dim_time`
   - `dim_sector`
   - `dim_gas`
   - `dim_icd10_cod`
   - `dim_discharge_type`
3. **Do NOT load any views** (vw_emissions, vw_health_metrics, etc.)

**Why this works:** You manually control relationships between fact and dimension tables.

## Solution 3: Fix Relationships in Power BI

If you've already loaded both fact tables and views:

1. **Open Model View** in Power BI (bottom left icon)
2. **Delete relationships** that connect views to dimension tables:
   - Delete: `vw_emissions` → `dim_gas`
   - Delete: `vw_emissions` → `dim_sector`
   - Delete: `vw_health_metrics` → `dim_icd10_cod`
   - Delete any other view → dimension relationships
3. **Keep only fact → dimension relationships:**
   - Keep: `fact_emissions` → `dim_gas` (via `gas_id`)
   - Keep: `fact_emissions` → `dim_sector` (via `sector_id`)
   - Keep: `fact_emissions` → `dim_geography` (via `geography_id`)
   - Keep: `fact_emissions` → `dim_time` (via `time_id`)
   - Keep: `fact_causes_of_death` → `dim_icd10_cod` (via `icd10_cod_id`)
   - etc.
4. **Ensure relationships are one-way** (single arrow, not bidirectional):
   - Right-click relationship → Properties
   - Set "Cross filter direction" to "Single"
   - Uncheck "Make this relationship active" if you have multiple paths

## Solution 4: Use Python Script (Alternative)

Use the provided Python script (`prod/examples/power_bi_get_all_tables.py`) but modify it to load only views OR only fact tables (not both).

## Quick Fix Steps

1. **Close Power BI** (if open)
2. **Reconnect to database:**
   - Get Data → SQLite database
   - Browse to your database file
3. **Select tables carefully:**
   - ✅ **Recommended:** Select only `vw_emissions`, `vw_health_metrics`, `vw_hospital_discharges`
   - ❌ **Do NOT select:** `fact_emissions`, `fact_causes_of_death`, or individual dimension tables
4. **Load data**
5. **Verify:** No cyclic reference errors should appear

## Why Views Are Better for Power BI

Views are pre-joined and contain:
- All dimension attributes (names, labels, categories)
- All fact measures
- Human-readable column names

This makes them ideal for Power BI because:
- No need to set up relationships manually
- Faster queries (pre-joined)
- Simpler data model
- No risk of cyclic references

## Verification

After applying the fix:
1. Check for errors in Power BI (View → Errors)
2. Verify data loads correctly
3. Test a simple visualization (e.g., bar chart with emissions by country)

If errors persist, use Solution 3 to manually fix relationships in Model View.

