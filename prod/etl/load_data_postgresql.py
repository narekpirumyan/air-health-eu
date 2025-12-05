"""
ETL script to load processed parquet data into PostgreSQL star schema.

This script:
1. Reads processed parquet files (emissions, health, population)
2. Extracts and loads dimension tables
3. Loads fact tables separately (emissions, health_metrics, hospital_discharges, population)
4. Calculates discharge_rate_per_100k
5. Updates dimension flags
6. Verifies data integrity

This is a PostgreSQL version of load_data.py, adapted for Aiven PostgreSQL.
"""

from pathlib import Path
import sys
import psycopg2
from sqlalchemy import create_engine

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from prod.config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DBNAME,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_SSLMODE,
    POSTGRES_CONNECTION_STRING,
    EMISSIONS_PATH,
    CAUSES_PATH,
    DISCHARGES_PATH,
    POPULATION_PATH,
)

# Import helper functions from SQLite version
from prod.etl.load_data import (
    _calculate_nuts_level,
    _is_leap_year,
    _get_icd10_name,
    _get_icd10_category,
    _get_icd10_description,
)


def load_dimensions(conn, emissions_df, causes_df, discharges_df, population_df, filter_nuts2: bool = False):
    """Load dimension tables from processed data."""
    cursor = conn.cursor()
    # Create SQLAlchemy engine for pandas operations (avoids warnings)
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    
    print("\n=== Loading Dimension Tables ===")
    
    # Dim Geography
    print("Loading dim_geography...")
    all_geo = pd.concat([
        emissions_df[['nuts_id', 'nuts_label', 'country_iso', 'country_name']].drop_duplicates(),
        causes_df[['nuts_id']].drop_duplicates().assign(nuts_label=None, country_iso=None, country_name=None),
        discharges_df[['nuts_id']].drop_duplicates().assign(nuts_label=None, country_iso=None, country_name=None),
        population_df[['geo']].rename(columns={'geo': 'nuts_id'}).drop_duplicates().assign(nuts_label=None, country_iso=None, country_name=None),
    ]).drop_duplicates(subset=['nuts_id'])
    
    # Fill missing country info for health/population data using emissions data
    geo_with_country = emissions_df[['nuts_id', 'country_iso', 'country_name']].drop_duplicates()
    all_geo = all_geo.merge(geo_with_country, on='nuts_id', how='left', suffixes=('_x', ''))
    all_geo['country_iso'] = all_geo['country_iso'].fillna(all_geo['country_iso_x'])
    all_geo['country_name'] = all_geo['country_name'].fillna(all_geo['country_name_x'])
    all_geo = all_geo.drop(columns=['country_iso_x', 'country_name_x'], errors='ignore')
    
    # Calculate NUTS level
    all_geo['nuts_level'] = all_geo['nuts_id'].apply(_calculate_nuts_level)
    
    # Optional: Filter to NUTS2 only if requested
    if filter_nuts2:
        all_geo = all_geo[all_geo['nuts_level'] == 2].copy()
        print(f"  Filtered to NUTS2 level only")
    
    # Drop any rows with invalid NUTS codes
    all_geo = all_geo.dropna(subset=['nuts_level', 'country_iso'])
    all_geo = all_geo.sort_values('nuts_id').reset_index(drop=True)
    
    # Clear existing data
    cursor.execute("DELETE FROM dim_geography")
    conn.commit()
    
    # Use SQLAlchemy engine for pandas to_sql (better compatibility)
    all_geo[['nuts_id', 'nuts_label', 'nuts_level', 'country_iso', 'country_name']].to_sql(
        'dim_geography', engine, if_exists='append', index=False, method='multi'
    )
    
    # Post-process: Populate empty nuts_label values
    cursor.execute("""
        UPDATE dim_geography 
        SET nuts_label = country_name 
        WHERE nuts_level = 0 AND (nuts_label IS NULL OR nuts_label = '')
    """)
    
    cursor.execute("""
        UPDATE dim_geography 
        SET nuts_label = nuts_id 
        WHERE (nuts_label IS NULL OR nuts_label = '')
    """)
    
    conn.commit()
    
    print(f"  ✓ Loaded {len(all_geo):,} geographic regions")
    print(f"    NUTS level distribution: {all_geo['nuts_level'].value_counts().sort_index().to_dict()}")
    
    # Get geography lookup (use engine instead of conn to avoid warnings)
    geography_lookup = pd.read_sql("SELECT geography_id, nuts_id FROM dim_geography", engine)
    geography_map = dict(zip(geography_lookup['nuts_id'], geography_lookup['geography_id']))
    
    # Dim Time
    print("Loading dim_time...")
    all_years = sorted(set(
        list(emissions_df['year'].unique()) +
        list(causes_df['year'].unique()) +
        list(discharges_df['year'].unique()) +
        list(population_df['year'].unique())
    ))
    
    dim_time = pd.DataFrame({
        'time_id': all_years,
        'year': all_years,
        'decade': [y // 10 * 10 for y in all_years],
        'year_label': [str(y) for y in all_years],
        'is_leap_year': [_is_leap_year(y) for y in all_years],
        'quarter': [4] * len(all_years),  # Always 4 for annual data
        'is_emissions_available': [0] * len(all_years),
        'is_health_available': [0] * len(all_years),
        'is_population_available': [0] * len(all_years),
    })
    
    cursor.execute("DELETE FROM dim_time")
    conn.commit()
    dim_time.to_sql('dim_time', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(dim_time):,} time periods")
    
    # Dim Sector
    print("Loading dim_sector...")
    dim_sector = emissions_df[['sector', 'sector_group']].drop_duplicates()
    dim_sector = dim_sector.rename(columns={'sector': 'sector_code'})
    dim_sector['sector_name'] = dim_sector['sector_code']  # Use code as name for now
    dim_sector['sector_description'] = None
    dim_sector['is_active'] = 1
    dim_sector = dim_sector[['sector_code', 'sector_name', 'sector_group', 'sector_description', 'is_active']]
    
    cursor.execute("DELETE FROM dim_sector")
    conn.commit()
    dim_sector.to_sql('dim_sector', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(dim_sector):,} sectors")
    
    sector_lookup = pd.read_sql("SELECT sector_id, sector_code FROM dim_sector", engine)
    sector_map = dict(zip(sector_lookup['sector_code'], sector_lookup['sector_id']))
    
    # Dim Gas
    print("Loading dim_gas...")
    dim_gas = emissions_df[['gas']].drop_duplicates()
    dim_gas = dim_gas.rename(columns={'gas': 'gas_code'})
    
    # Map gas codes to names and formulas
    gas_mapping = {
        'CO2': ('Carbon Dioxide', 'CO₂', 1.0),
        'CH4': ('Methane', 'CH₄', 28.0),
        'N2O': ('Nitrous Oxide', 'N₂O', 265.0),
        'F-gas': ('Fluorinated Gases', 'F-gases', 1000.0),
        'fossil_co2': ('Fossil CO2', 'CO₂', 1.0),
        'ch4': ('Methane', 'CH₄', 28.0),
        'n2o': ('Nitrous Oxide', 'N₂O', 265.0),
        'f_gas': ('Fluorinated Gases', 'F-gases', 1000.0),
    }
    
    def get_gas_info(code):
        """Extract gas information from code, handling various formats including GWP_100_AR5_*."""
        code_upper = str(code).upper()
        
        # Handle GWP_100_AR5_* format and other variations
        if 'CH4' in code_upper or 'METHANE' in code_upper:
            return ('Methane', 'CH₄', 28.0)
        elif 'N2O' in code_upper or 'NITROUS' in code_upper:
            return ('Nitrous Oxide', 'N₂O', 265.0)
        elif 'F-GASES' in code_upper or 'F-GAS' in code_upper or 'F_GAS' in code_upper or 'FLUORINATED' in code_upper:
            return ('Fluorinated Gases', 'F-gases', 1000.0)
        elif 'CO2' in code_upper or 'CARBON' in code_upper:
            return ('Carbon Dioxide', 'CO₂', 1.0)
        
        # Fallback to original mapping dictionary
        for key, (name, formula, gwp) in gas_mapping.items():
            if key.upper() == code_upper:
                return name, formula, gwp
        
        # Final fallback: return code as name
        return str(code), None, None
    
    dim_gas['gas_name'], dim_gas['gas_formula'], dim_gas['gwp_ar5'] = zip(*dim_gas['gas_code'].apply(get_gas_info))
    dim_gas['is_active'] = 1
    dim_gas = dim_gas[['gas_code', 'gas_name', 'gas_formula', 'gwp_ar5', 'is_active']]
    
    cursor.execute("DELETE FROM dim_gas")
    conn.commit()
    dim_gas.to_sql('dim_gas', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(dim_gas):,} gas types")
    
    gas_lookup = pd.read_sql("SELECT gas_id, gas_code FROM dim_gas", engine)
    gas_map = dict(zip(gas_lookup['gas_code'], gas_lookup['gas_id']))
    
    # Dim ICD-10 COD
    print("Loading dim_icd10_cod...")
    dim_icd10_cod = causes_df[['icd10_group']].drop_duplicates()
    dim_icd10_cod = dim_icd10_cod.rename(columns={'icd10_group': 'icd10_code'})
    dim_icd10_cod['icd10_name'] = dim_icd10_cod['icd10_code'].apply(_get_icd10_name)
    dim_icd10_cod['icd10_category'] = dim_icd10_cod['icd10_code'].apply(_get_icd10_category)
    dim_icd10_cod['icd10_description'] = dim_icd10_cod['icd10_code'].apply(_get_icd10_description)
    dim_icd10_cod['is_respiratory'] = dim_icd10_cod['icd10_code'].apply(lambda x: 1 if x.startswith('J') else 0)
    dim_icd10_cod['is_active'] = 1
    dim_icd10_cod = dim_icd10_cod[['icd10_code', 'icd10_name', 'icd10_category', 'icd10_description', 'is_respiratory', 'is_active']]
    
    cursor.execute("DELETE FROM dim_icd10_cod")
    conn.commit()
    dim_icd10_cod.to_sql('dim_icd10_cod', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(dim_icd10_cod):,} ICD-10 codes")
    
    icd10_lookup = pd.read_sql("SELECT icd10_cod_id, icd10_code FROM dim_icd10_cod", engine)
    icd10_map = dict(zip(icd10_lookup['icd10_code'], icd10_lookup['icd10_cod_id']))
    
    # Dim Discharge Type
    print("Loading dim_discharge_type...")
    dim_discharge_type = discharges_df[['icd10_group']].drop_duplicates()
    dim_discharge_type = dim_discharge_type.rename(columns={'icd10_group': 'discharge_code'})
    dim_discharge_type['discharge_name'] = dim_discharge_type['discharge_code'].apply(_get_icd10_name)
    dim_discharge_type['discharge_category'] = dim_discharge_type['discharge_code'].apply(_get_icd10_category)
    dim_discharge_type['icd10_codes'] = dim_discharge_type['discharge_code']
    dim_discharge_type['discharge_description'] = dim_discharge_type['discharge_code'].apply(_get_icd10_description)
    dim_discharge_type['is_respiratory'] = dim_discharge_type['discharge_code'].apply(lambda x: 1 if x.startswith('J') else 0)
    dim_discharge_type['is_active'] = 1
    dim_discharge_type = dim_discharge_type[['discharge_code', 'discharge_name', 'discharge_category', 'icd10_codes', 'discharge_description', 'is_respiratory', 'is_active']]
    
    cursor.execute("DELETE FROM dim_discharge_type")
    conn.commit()
    dim_discharge_type.to_sql('dim_discharge_type', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(dim_discharge_type):,} discharge types")
    
    discharge_lookup = pd.read_sql("SELECT discharge_type_id, discharge_code FROM dim_discharge_type", engine)
    discharge_map = dict(zip(discharge_lookup['discharge_code'], discharge_lookup['discharge_type_id']))
    
    engine.dispose()
    
    return {
        'geography': geography_map,
        'sector': sector_map,
        'gas': gas_map,
        'icd10_cod': icd10_map,
        'discharge_type': discharge_map,
    }


def load_fact_tables(conn, emissions_df, causes_df, discharges_df, population_df, lookup_maps, filter_nuts2: bool = False):
    """Load fact tables from processed data."""
    cursor = conn.cursor()
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    
    print("\n=== Loading Fact Tables ===")
    
    # Optional: Filter to NUTS2 only if requested
    if filter_nuts2:
        emissions_df = emissions_df[emissions_df['nuts_id'].str.len() == 4].copy()
        causes_df = causes_df[causes_df['nuts_id'].str.len() == 4].copy()
        discharges_df = discharges_df[discharges_df['nuts_id'].str.len() == 4].copy()
        population_df = population_df[population_df['geo'].str.len() == 4].copy()
        print(f"  Filtered all data to NUTS2 level only")
    
    # Fact Population
    print("Loading fact_population...")
    fact_population = population_df[['geo', 'year', 'population']].copy()
    fact_population = fact_population.rename(columns={'geo': 'nuts_id'})
    fact_population['geography_id'] = fact_population['nuts_id'].map(lookup_maps['geography'])
    fact_population['time_id'] = fact_population['year']
    fact_population = fact_population.dropna(subset=['geography_id', 'time_id'])
    fact_population = fact_population[['geography_id', 'time_id', 'population']].drop_duplicates(subset=['geography_id', 'time_id'])
    
    cursor.execute("DELETE FROM fact_population")
    conn.commit()
    fact_population.to_sql('fact_population', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(fact_population):,} population records")
    
    # Fact Emissions
    print("Loading fact_emissions...")
    fact_emissions = emissions_df[['nuts_id', 'year', 'gas', 'sector', 'emissions_kt_co2e']].copy()
    fact_emissions['geography_id'] = fact_emissions['nuts_id'].map(lookup_maps['geography'])
    fact_emissions['time_id'] = fact_emissions['year']
    fact_emissions['sector_id'] = fact_emissions['sector'].map(lookup_maps['sector'])
    fact_emissions['gas_id'] = fact_emissions['gas'].map(lookup_maps['gas'])
    fact_emissions = fact_emissions.dropna(subset=['geography_id', 'time_id', 'sector_id', 'gas_id', 'emissions_kt_co2e'])
    fact_emissions = fact_emissions[['geography_id', 'time_id', 'sector_id', 'gas_id', 'emissions_kt_co2e']].drop_duplicates(
        subset=['geography_id', 'time_id', 'sector_id', 'gas_id']
    )
    
    cursor.execute("DELETE FROM fact_emissions")
    conn.commit()
    fact_emissions.to_sql('fact_emissions', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(fact_emissions):,} emissions records")
    
    # Fact Causes of Death
    print("Loading fact_causes_of_death...")
    fact_health = causes_df[['nuts_id', 'year', 'icd10_group', 'age_standardised_rate_per_100k']].copy()
    fact_health['geography_id'] = fact_health['nuts_id'].map(lookup_maps['geography'])
    fact_health['time_id'] = fact_health['year']
    fact_health['icd10_cod_id'] = fact_health['icd10_group'].map(lookup_maps['icd10_cod'])
    fact_health = fact_health.dropna(subset=['geography_id', 'time_id', 'icd10_cod_id', 'age_standardised_rate_per_100k'])
    fact_health = fact_health[['geography_id', 'time_id', 'icd10_cod_id', 'age_standardised_rate_per_100k']].drop_duplicates(
        subset=['geography_id', 'time_id', 'icd10_cod_id']
    )
    
    cursor.execute("DELETE FROM fact_causes_of_death")
    conn.commit()
    fact_health.to_sql('fact_causes_of_death', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(fact_health):,} causes of death records")
    
    # Fact Hospital Discharges
    print("Loading fact_hospital_discharges...")
    fact_discharges = discharges_df[['nuts_id', 'year', 'icd10_group', 'discharges']].copy()
    fact_discharges['geography_id'] = fact_discharges['nuts_id'].map(lookup_maps['geography'])
    fact_discharges['time_id'] = fact_discharges['year']
    fact_discharges['discharge_type_id'] = fact_discharges['icd10_group'].map(lookup_maps['discharge_type'])
    fact_discharges = fact_discharges.rename(columns={'discharges': 'discharge_count'})
    fact_discharges = fact_discharges.dropna(subset=['geography_id', 'time_id', 'discharge_type_id', 'discharge_count'])
    
    # Calculate discharge_rate_per_100k by joining with population
    fact_discharges = fact_discharges.merge(
        fact_population[['geography_id', 'time_id', 'population']],
        on=['geography_id', 'time_id'],
        how='left'
    )
    fact_discharges['discharge_rate_per_100k'] = (
        fact_discharges['discharge_count'] / fact_discharges['population'] * 100000
    ).round(2)
    fact_discharges = fact_discharges[['geography_id', 'time_id', 'discharge_type_id', 'discharge_count', 'discharge_rate_per_100k']].drop_duplicates(
        subset=['geography_id', 'time_id', 'discharge_type_id']
    )
    
    cursor.execute("DELETE FROM fact_hospital_discharges")
    conn.commit()
    fact_discharges.to_sql('fact_hospital_discharges', engine, if_exists='append', index=False, method='multi')
    print(f"  ✓ Loaded {len(fact_discharges):,} hospital discharge records")
    
    # Update dim_time availability flags
    print("Updating dim_time availability flags...")
    cursor.execute("""
        UPDATE dim_time
        SET is_emissions_available = 1
        WHERE time_id IN (SELECT DISTINCT time_id FROM fact_emissions)
    """)
    cursor.execute("""
        UPDATE dim_time
        SET is_health_available = 1
        WHERE time_id IN (SELECT DISTINCT time_id FROM fact_causes_of_death)
    """)
    cursor.execute("""
        UPDATE dim_time
        SET is_population_available = 1
        WHERE time_id IN (SELECT DISTINCT time_id FROM fact_population)
    """)
    conn.commit()
    print("  ✓ Updated time availability flags")
    
    engine.dispose()


def verify_data(conn):
    """Verify data integrity and provide summary statistics."""
    cursor = conn.cursor()
    
    print("\n=== Data Verification ===")
    
    # Count rows
    cursor.execute("SELECT COUNT(*) FROM dim_geography")
    geography_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_time")
    time_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_sector")
    sector_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_gas")
    gas_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_icd10_cod")
    icd10_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_discharge_type")
    discharge_type_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_emissions")
    emissions_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_causes_of_death")
    health_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_hospital_discharges")
    discharges_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM fact_population")
    population_count = cursor.fetchone()[0]
    
    print(f"Dimensions:")
    print(f"  - Geographic regions: {geography_count:,}")
    print(f"  - Time periods: {time_count:,}")
    print(f"  - Sectors: {sector_count:,}")
    print(f"  - Gas types: {gas_count:,}")
    print(f"  - ICD-10 codes: {icd10_count:,}")
    print(f"  - Discharge types: {discharge_type_count:,}")
    
    print(f"\nFact Tables:")
    print(f"  - Emissions: {emissions_count:,} records")
    print(f"  - Causes of death: {health_count:,} records")
    print(f"  - Hospital discharges: {discharges_count:,} records")
    print(f"  - Population: {population_count:,} records")
    
    # Show NUTS level distribution
    cursor.execute("""
        SELECT nuts_level, COUNT(*) as count
        FROM dim_geography
        GROUP BY nuts_level
        ORDER BY nuts_level
    """)
    nuts_dist = cursor.fetchall()
    if nuts_dist:
        print(f"\nNUTS level distribution:")
        level_names = {0: "Country", 1: "NUTS1", 2: "NUTS2", 3: "NUTS3+"}
        for level, count in nuts_dist:
            print(f"  - {level_names.get(level, f'Level {level}')}: {count:,}")
    
    # Year ranges
    cursor.execute("SELECT MIN(year), MAX(year) FROM dim_time")
    min_year, max_year = cursor.fetchone()
    print(f"\nTime coverage: {min_year} - {max_year}")
    
    # Check for orphaned records
    cursor.execute("""
        SELECT COUNT(*) FROM fact_emissions e
        LEFT JOIN dim_geography g ON e.geography_id = g.geography_id
        WHERE g.geography_id IS NULL
    """)
    orphaned_emissions = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM fact_causes_of_death h
        LEFT JOIN dim_geography g ON h.geography_id = g.geography_id
        WHERE g.geography_id IS NULL
    """)
    orphaned_health = cursor.fetchone()[0]
    
    if orphaned_emissions > 0 or orphaned_health > 0:
        print(f"\n⚠ Warnings:")
        if orphaned_emissions > 0:
            print(f"  - {orphaned_emissions} emissions records with invalid geography_id")
        if orphaned_health > 0:
            print(f"  - {orphaned_health} causes of death records with invalid geography_id")
    else:
        print("\n✓ All foreign key relationships valid")


def load_data(filter_nuts2: bool = False):
    """
    Main ETL function for PostgreSQL.
    
    Parameters
    ----------
    filter_nuts2 : bool, default False
        If True, filter to NUTS2 level only (4-character codes).
        If False, preserve all geographic levels.
    """
    # Check if processed data files exist
    missing_files = []
    if not EMISSIONS_PATH.exists():
        missing_files.append(str(EMISSIONS_PATH))
    if not CAUSES_PATH.exists():
        missing_files.append(str(CAUSES_PATH))
    if not DISCHARGES_PATH.exists():
        missing_files.append(str(DISCHARGES_PATH))
    if not POPULATION_PATH.exists():
        missing_files.append(str(POPULATION_PATH))
    
    if missing_files:
        print("Error: Missing processed data files:")
        for f in missing_files:
            print(f"  - {f}")
        print("\nPlease run the ingestion notebooks first:")
        print("  - notebooks/01_ingest_emissions.ipynb")
        print("  - notebooks/01_ingest_health.ipynb")
        print("  - notebooks/02_load_population.ipynb")
        return
    
    print(f"Loading data from processed files into PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}")
    if filter_nuts2:
        print("⚠ Filtering to NUTS2 level only")
    else:
        print("✓ Preserving all geographic levels")
    
    # Load processed data
    print("\nReading processed parquet files...")
    emissions_df = pd.read_parquet(EMISSIONS_PATH)
    print(f"  ✓ Emissions: {len(emissions_df):,} rows")
    
    causes_df = pd.read_parquet(CAUSES_PATH)
    print(f"  ✓ Causes of death: {len(causes_df):,} rows")
    
    discharges_df = pd.read_parquet(DISCHARGES_PATH)
    print(f"  ✓ Hospital discharges: {len(discharges_df):,} rows")
    
    population_df = pd.read_parquet(POPULATION_PATH)
    print(f"  ✓ Population: {len(population_df):,} rows")
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DBNAME,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            sslmode=POSTGRES_SSLMODE
        )
        print(f"\n✓ Connected to PostgreSQL database")
    except psycopg2.OperationalError as e:
        print(f"\n✗ Error connecting to database: {e}")
        print("\nPlease check your Aiven connection settings:")
        print("  - Set AIVEN_HOST environment variable")
        print("  - Set AIVEN_PORT environment variable (default: 5432)")
        print("  - Set AIVEN_DBNAME environment variable")
        print("  - Set AIVEN_USER environment variable")
        print("  - Set AIVEN_PASSWORD environment variable")
        print("\nOr update prod/config/settings.py directly")
        raise
    
    try:
        # Load dimensions
        lookup_maps = load_dimensions(
            conn, emissions_df, causes_df, discharges_df, population_df, filter_nuts2=filter_nuts2
        )
        
        # Load fact tables
        load_fact_tables(
            conn, emissions_df, causes_df, discharges_df, population_df, lookup_maps, filter_nuts2=filter_nuts2
        )
        
        # Verify data
        verify_data(conn)
        
        print("\n✓ Data loading completed successfully!")
        print(f"\nDatabase ready at: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}")
        print("You can now connect Tableau or Power BI to this database.")
        print("See prod/docs/aiven-postgresql-setup.md for connection instructions.")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error loading data: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load processed data into PostgreSQL star schema")
    parser.add_argument(
        "--filter-nuts2",
        action="store_true",
        help="Filter to NUTS2 level only (4-character codes)"
    )
    args = parser.parse_args()
    load_data(filter_nuts2=args.filter_nuts2)

