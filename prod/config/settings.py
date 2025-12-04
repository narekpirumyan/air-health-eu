"""
Configuration settings for production data warehouse.

Centralized configuration for database paths, file locations, etc.
"""

from pathlib import Path

# Project root (two levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Database configuration
DB_NAME = "air_health_eu.db"
DB_PATH = PROJECT_ROOT / "prod" / "data" / "warehouse" / DB_NAME

# Schema file
SCHEMA_PATH = PROJECT_ROOT / "prod" / "sql" / "schema.sql"

# Data paths
# Processed data (shared between MVP and prod)
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
EMISSIONS_PATH = PROCESSED_DATA_DIR / "emissions_nuts2.parquet"
CAUSES_PATH = PROCESSED_DATA_DIR / "health_causes_of_death.parquet"
DISCHARGES_PATH = PROCESSED_DATA_DIR / "health_hospital_discharges.parquet"
POPULATION_PATH = PROCESSED_DATA_DIR / "population_nuts2.parquet"

# Curated data (MVP-specific)
CURATED_DATA_PATH = PROJECT_ROOT / "mvp" / "data" / "curated" / "eu_climate_health.parquet"

# SQLite connection string (for SQLAlchemy if needed)
SQLITE_CONNECTION_STRING = f"sqlite:///{DB_PATH}"

