"""
ETL script to load processed parquet data into SQLite star schema.

This script:
1. Reads processed parquet files (emissions, health, population)
2. Extracts and loads dimension tables
3. Loads fact tables separately (emissions, health_metrics, hospital_discharges, population)
4. Calculates discharge_rate_per_100k
5. Updates dimension flags
6. Verifies data integrity
"""

from pathlib import Path
import sys
import sqlite3

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from prod.config.settings import (
    DB_PATH,
    EMISSIONS_PATH,
    CAUSES_PATH,
    DISCHARGES_PATH,
    POPULATION_PATH,
)


def _calculate_nuts_level(nuts_id: str) -> int:
    """Calculate NUTS level from code length: 0=country, 1=NUTS1, 2=NUTS2, 3=NUTS3+."""
    if not isinstance(nuts_id, str):
        nuts_id = str(nuts_id)
    length = len(nuts_id.strip())
    if length == 2:
        return 0  # Country level
    elif length == 3:
        return 1  # NUTS1
    elif length == 4:
        return 2  # NUTS2
    else:
        return 3  # NUTS3 or higher


def _is_leap_year(year: int) -> int:
    """Return 1 if leap year, 0 otherwise."""
    if year % 4 != 0:
        return 0
    elif year % 100 != 0:
        return 1
    elif year % 400 != 0:
        return 0
    else:
        return 1


def _get_icd10_name(code: str) -> str:
    """Map ICD-10 code to human-readable name."""
    if not isinstance(code, str):
        code = str(code)
    
    code_upper = code.upper()
    
    # Special aggregate cases (must be checked first)
    if code == 'A-R_V-Y':
        return 'All causes of death'
    elif code == 'A-T_Z' or code.upper() == 'A-T_Z':
        return 'All diseases and health factors (A-T, Z)'
    elif code.startswith('A-T_Z'):
        # Handle variations like A-T_Z_XNB (excludes certain categories)
        return 'All diseases and health factors (A-T, Z)' + (f' - {code[6:]}' if len(code) > 5 else '')
    elif code == 'J':
        return 'All respiratory diseases'
    
    # Handle codes ending with "_OTH" (other)
    base_code = code
    is_other = False
    if code.endswith('_OTH'):
        base_code = code[:-4]  # Remove "_OTH" suffix
        is_other = True
    
    # Respiratory codes (J00-J99)
    if base_code.startswith('J') or code.startswith('J'):
        if code == 'J00-J11' or base_code == 'J00-J11':
            return 'Acute upper respiratory infections' + (' (other)' if is_other else '')
        elif code == 'J12-J18' or base_code == 'J12-J18':
            return 'Pneumonia' + (' (other)' if is_other else '')
        elif code == 'J20-J22' or base_code == 'J20-J22':
            return 'Other acute lower respiratory infections' + (' (other)' if is_other else '')
        elif code == 'J40-J44' or code == 'J40-J44_J47' or base_code == 'J40-J44' or base_code == 'J40-J44_J47':
            return 'Chronic lower respiratory diseases (COPD)' + (' (other)' if is_other else '')
        elif code == 'J45_J46' or base_code == 'J45_J46':
            return 'Asthma' + (' (other)' if is_other else '')
        elif code == 'J60-J99' or base_code == 'J60-J99':
            return 'Other respiratory diseases' + (' (other)' if is_other else '')
        elif code == 'J_OTH' or code == 'UPRESPIR_OTH':
            return 'Other respiratory conditions'
        else:
            return f'Respiratory disease ({code})'
    
    # Infectious diseases (A00-B99)
    if base_code.startswith('A') or base_code.startswith('B') or code.startswith('A') or code.startswith('B'):
        if base_code.startswith('A00-A08') or code.startswith('A00-A08'):
            return 'Intestinal infectious diseases' + (' (other)' if is_other else '')
        elif base_code.startswith('A09') or code.startswith('A09'):
            return 'Other gastroenteritis and colitis' + (' (other)' if is_other else '')
        elif base_code.startswith('A15-A19') or 'A15-A19' in base_code or code.startswith('A15-A19') or 'A15-A19' in code:
            return 'Tuberculosis' + (' (other)' if is_other else '')
        elif base_code.startswith('A40_A41') or base_code == 'A40_A41' or code.startswith('A40_A41') or code == 'A40_A41':
            return 'Sepsis' + (' (other)' if is_other else '')
        elif base_code.startswith('B15-B19') or 'B15-B19' in base_code or code.startswith('B15-B19') or 'B15-B19' in code:
            return 'Viral hepatitis' + (' (other)' if is_other else '')
        elif base_code.startswith('B20-B24') or code.startswith('B20-B24'):
            return 'HIV disease' + (' (other)' if is_other else '')
        else:
            return f'Infectious disease ({code})'
    
    # Neoplasms (C00-D49)
    if base_code.startswith('C') or base_code.startswith('D') or code.startswith('C') or code.startswith('D'):
        if code == 'C' or base_code == 'C':
            return 'All neoplasms'
        else:
            return f'Neoplasm ({code})'
    
    # Eye and adnexa (H00-H59)
    if base_code.startswith('H00-H59') or code.startswith('H00-H59'):
        return 'Diseases of eye and adnexa' + (' (other)' if is_other else '')
    elif (base_code.startswith('H') and len(base_code) > 0 and base_code[1:].isdigit() and int(base_code[1:3] if len(base_code) >= 3 else base_code[1:]) < 60) or \
         (code.startswith('H') and len(code) > 1 and code[1:3].isdigit() and int(code[1:3]) < 60):
        return 'Diseases of eye and adnexa' + (' (other)' if is_other else '')
    
    # Ear and mastoid process (H60-H95)
    if base_code.startswith('H60-H95') or code.startswith('H60-H95'):
        return 'Diseases of ear and mastoid process' + (' (other)' if is_other else '')
    elif (base_code.startswith('H') and len(base_code) > 1 and base_code[1:3].isdigit() and 60 <= int(base_code[1:3]) <= 95) or \
         (code.startswith('H') and len(code) > 1 and code[1:3].isdigit() and 60 <= int(code[1:3]) <= 95):
        return 'Diseases of ear and mastoid process' + (' (other)' if is_other else '')
    
    # Circulatory (I00-I99)
    if base_code.startswith('I') or code.startswith('I'):
        if code == 'I' or base_code == 'I':
            return 'All circulatory diseases'
        elif base_code.startswith('I20-I25') or code.startswith('I20-I25'):
            return 'Ischaemic heart diseases' + (' (other)' if is_other else '')
        elif base_code.startswith('I60-I69') or code.startswith('I60-I69'):
            return 'Cerebrovascular diseases' + (' (other)' if is_other else '')
        else:
            return f'Circulatory disease ({code})'
    
    # Digestive (K00-K95)
    if base_code.startswith('K') or code.startswith('K'):
        return f'Digestive disease ({code})'
    
    # Skin and subcutaneous tissue (L00-L99)
    if base_code.startswith('L') or code.startswith('L'):
        return f'Diseases of skin and subcutaneous tissue ({code})'
    
    # Musculoskeletal (M00-M99)
    if base_code.startswith('M') or code.startswith('M'):
        return f'Diseases of musculoskeletal system ({code})'
    
    # Genitourinary (N00-N99)
    if base_code.startswith('N') or code.startswith('N'):
        return f'Diseases of genitourinary system ({code})'
    
    # Pregnancy, childbirth (O00-O99)
    if base_code.startswith('O') or code.startswith('O'):
        return f'Pregnancy, childbirth and puerperium ({code})'
    
    # Perinatal period (P00-P96)
    if base_code.startswith('P') or code.startswith('P'):
        return f'Conditions originating in perinatal period ({code})'
    
    # Congenital malformations (Q00-Q99)
    if base_code.startswith('Q') or code.startswith('Q'):
        return f'Congenital malformations ({code})'
    
    # Symptoms, signs, abnormal findings (R00-R99)
    if base_code.startswith('R') or code.startswith('R'):
        return f'Symptoms, signs and abnormal findings ({code})'
    
    # Injury, poisoning (S00-T98)
    if base_code.startswith('S') or base_code.startswith('T') or code.startswith('S') or code.startswith('T'):
        return f'Injury, poisoning and external causes ({code})'
    
    # Health factors (Z00-Z99)
    if base_code.startswith('Z') or code.startswith('Z'):
        return f'Factors influencing health status ({code})'
    
    # Other common patterns
    if code.startswith('ACC') or code == 'ACC':
        return 'Accidents'
    if code.startswith('ABORT'):
        return 'Abortion and related conditions'
    if code.startswith('ARTHROPAT'):
        return 'Arthropathies'
    
    # Fallback: return code as name
    return code


def _get_icd10_category(code: str) -> str:
    """Map ICD-10 code to category."""
    if not isinstance(code, str):
        code = str(code)
    
    code_upper = code.upper()
    
    # Special aggregate cases (must be checked first)
    if code == 'A-R_V-Y':
        return 'all_causes'
    elif code == 'A-T_Z' or code.upper() == 'A-T_Z' or code.startswith('A-T_Z'):
        return 'all_conditions'  # Broad category covering all diseases and health factors
    
    # Handle codes ending with "_OTH" (other)
    base_code = code
    if code.endswith('_OTH'):
        base_code = code[:-4]  # Remove "_OTH" suffix
    
    # Respiratory (J00-J99)
    if base_code.startswith('J') or code.startswith('J'):
        if code == 'J' or base_code == 'J':
            return 'all_respiratory'
        else:
            return 'respiratory'
    
    # Infectious and parasitic (A00-B99)
    if base_code.startswith('A') or base_code.startswith('B') or code.startswith('A') or code.startswith('B'):
        return 'infectious'
    
    # Neoplasms (C00-D49)
    if base_code.startswith('C') or (base_code.startswith('D') and len(base_code) <= 3) or \
       code.startswith('C') or (code.startswith('D') and len(code) <= 3):
        return 'neoplasms'
    
    # Diseases of blood and immune system (D50-D89)
    if (base_code.startswith('D') and len(base_code) > 3) or (code.startswith('D') and len(code) > 3):
        return 'blood_immune'
    
    # Endocrine, nutritional and metabolic (E00-E90)
    if base_code.startswith('E') or code.startswith('E'):
        return 'endocrine_metabolic'
    
    # Mental and behavioural (F00-F99)
    if base_code.startswith('F') or code.startswith('F'):
        return 'mental_behavioural'
    
    # Nervous system (G00-G99)
    if base_code.startswith('G') or code.startswith('G'):
        return 'nervous_system'
    
    # Eye and adnexa (H00-H59)
    if base_code.startswith('H00-H59') or code.startswith('H00-H59'):
        return 'eye_adnexa'
    elif (base_code.startswith('H') and len(base_code) > 1 and base_code[1:3].isdigit() and int(base_code[1:3]) < 60) or \
         (code.startswith('H') and len(code) > 1 and code[1:3].isdigit() and int(code[1:3]) < 60):
        return 'eye_adnexa'
    
    # Ear and mastoid process (H60-H95)
    if base_code.startswith('H60-H95') or code.startswith('H60-H95'):
        return 'ear_mastoid'
    elif (base_code.startswith('H') and len(base_code) > 1 and base_code[1:3].isdigit() and 60 <= int(base_code[1:3]) <= 95) or \
         (code.startswith('H') and len(code) > 1 and code[1:3].isdigit() and 60 <= int(code[1:3]) <= 95):
        return 'ear_mastoid'
    
    # Circulatory (I00-I99)
    if base_code.startswith('I') or code.startswith('I'):
        return 'circulatory'
    
    # Digestive (K00-K95)
    if base_code.startswith('K') or code.startswith('K'):
        return 'digestive'
    
    # Skin and subcutaneous tissue (L00-L99)
    if base_code.startswith('L') or code.startswith('L'):
        return 'skin_subcutaneous'
    
    # Musculoskeletal (M00-M99)
    if base_code.startswith('M') or code.startswith('M'):
        return 'musculoskeletal'
    
    # Genitourinary (N00-N99)
    if base_code.startswith('N') or code.startswith('N'):
        return 'genitourinary'
    
    # Pregnancy, childbirth (O00-O99)
    if base_code.startswith('O') or code.startswith('O'):
        return 'pregnancy_childbirth'
    
    # Perinatal period (P00-P96)
    if base_code.startswith('P') or code.startswith('P'):
        return 'perinatal'
    
    # Congenital malformations (Q00-Q99)
    if base_code.startswith('Q') or code.startswith('Q'):
        return 'congenital'
    
    # Symptoms, signs, abnormal findings (R00-R99)
    if base_code.startswith('R') or code.startswith('R'):
        return 'symptoms_signs'
    
    # Injury, poisoning (S00-T98)
    if base_code.startswith('S') or base_code.startswith('T') or code.startswith('S') or code.startswith('T'):
        return 'injury_poisoning'
    
    # Health factors (Z00-Z99)
    if base_code.startswith('Z') or code.startswith('Z'):
        return 'health_factors'
    
    # External causes (V00-Y99)
    if base_code.startswith('V') or base_code.startswith('W') or base_code.startswith('X') or base_code.startswith('Y') or \
       code.startswith('V') or code.startswith('W') or code.startswith('X') or code.startswith('Y'):
        return 'external_causes'
    
    # Accidents and injuries
    if base_code.startswith('ACC') or code.startswith('ACC'):
        return 'external_causes'
    
    # Default
    return None


def _get_icd10_description(code: str) -> str:
    """Get description for ICD-10 code."""
    if not isinstance(code, str):
        code = str(code)
    
    # Basic descriptions - can be enhanced with full ICD-10 classification later
    name = _get_icd10_name(code)
    category = _get_icd10_category(code)
    
    if category:
        return f"{name} ({category.replace('_', ' ')})"
    else:
        return name


def load_dimensions(conn, emissions_df, causes_df, discharges_df, population_df, filter_nuts2: bool = False):
    """Load dimension tables from processed data."""
    cursor = conn.cursor()
    
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
    all_geo[['nuts_id', 'nuts_label', 'nuts_level', 'country_iso', 'country_name']].to_sql(
        'dim_geography', conn, if_exists='append', index=False
    )
    
    # Post-process: Populate empty nuts_label values
    # For country level (nuts_level=0): use country_name
    cursor.execute("""
        UPDATE dim_geography 
        SET nuts_label = country_name 
        WHERE nuts_level = 0 AND (nuts_label IS NULL OR nuts_label = '')
    """)
    
    # For other levels: use NUTS code as fallback if still NULL
    cursor.execute("""
        UPDATE dim_geography 
        SET nuts_label = nuts_id 
        WHERE (nuts_label IS NULL OR nuts_label = '')
    """)
    
    conn.commit()
    
    print(f"  ✓ Loaded {len(all_geo):,} geographic regions")
    print(f"    NUTS level distribution: {all_geo['nuts_level'].value_counts().sort_index().to_dict()}")
    
    # Get geography lookup
    geography_lookup = pd.read_sql("SELECT geography_id, nuts_id FROM dim_geography", conn)
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
    dim_time.to_sql('dim_time', conn, if_exists='append', index=False)
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
    dim_sector.to_sql('dim_sector', conn, if_exists='append', index=False)
    print(f"  ✓ Loaded {len(dim_sector):,} sectors")
    
    sector_lookup = pd.read_sql("SELECT sector_id, sector_code FROM dim_sector", conn)
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
            # F-gases are a group with varying GWPs (1,000-23,000). 
            # Using a representative value for aggregate F-gases data.
            # Note: Individual F-gas compounds have specific GWPs, but aggregate data uses this representative value.
            return ('Fluorinated Gases', 'F-gases', 1000.0)  # Representative GWP for F-gases aggregate
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
    dim_gas.to_sql('dim_gas', conn, if_exists='append', index=False)
    print(f"  ✓ Loaded {len(dim_gas):,} gas types")
    
    gas_lookup = pd.read_sql("SELECT gas_id, gas_code FROM dim_gas", conn)
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
    dim_icd10_cod.to_sql('dim_icd10_cod', conn, if_exists='append', index=False)
    print(f"  ✓ Loaded {len(dim_icd10_cod):,} ICD-10 codes")
    
    icd10_lookup = pd.read_sql("SELECT icd10_cod_id, icd10_code FROM dim_icd10_cod", conn)
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
    dim_discharge_type.to_sql('dim_discharge_type', conn, if_exists='append', index=False)
    print(f"  ✓ Loaded {len(dim_discharge_type):,} discharge types")
    
    discharge_lookup = pd.read_sql("SELECT discharge_type_id, discharge_code FROM dim_discharge_type", conn)
    discharge_map = dict(zip(discharge_lookup['discharge_code'], discharge_lookup['discharge_type_id']))
    
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
    fact_population.to_sql('fact_population', conn, if_exists='append', index=False)
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
    fact_emissions.to_sql('fact_emissions', conn, if_exists='append', index=False)
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
    fact_health.to_sql('fact_causes_of_death', conn, if_exists='append', index=False)
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
    fact_discharges.to_sql('fact_hospital_discharges', conn, if_exists='append', index=False)
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
    print("  ✓ Updated time availability flags")


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
    Main ETL function.
    
    Parameters
    ----------
    filter_nuts2 : bool, default False
        If True, filter to NUTS2 level only (4-character codes).
        If False, preserve all geographic levels.
    """
    # Check if database exists
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        print("Please run 'python -m prod.etl.create_database' first")
        return
    
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
    
    print(f"Loading data from processed files into: {DB_PATH}")
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
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    
    try:
        # Load dimensions
        lookup_maps = load_dimensions(
            conn, emissions_df, causes_df, discharges_df, population_df, filter_nuts2=filter_nuts2
        )
        
        # Load fact tables
        load_fact_tables(
            conn, emissions_df, causes_df, discharges_df, population_df, lookup_maps, filter_nuts2=filter_nuts2
        )
        
        # Commit all changes
        conn.commit()
        
        # Verify data
        verify_data(conn)
        
        print("\n✓ Data loading completed successfully!")
        print(f"\nDatabase ready at: {DB_PATH}")
        print("You can now connect Power BI to this database.")
        
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
    parser = argparse.ArgumentParser(description="Load processed data into SQLite star schema")
    parser.add_argument(
        "--filter-nuts2",
        action="store_true",
        help="Filter to NUTS2 level only (4-character codes)"
    )
    args = parser.parse_args()
    load_data(filter_nuts2=args.filter_nuts2)




