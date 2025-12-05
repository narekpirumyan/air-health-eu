"""
Configuration settings for production data warehouse.

Centralized configuration for database paths, file locations, etc.
"""

import os
from pathlib import Path

# Project root (two levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Database configuration
# SQLite (default)
DB_NAME = "air_health_eu.db"
DB_PATH = PROJECT_ROOT / "prod" / "data" / "warehouse" / DB_NAME
SCHEMA_PATH = PROJECT_ROOT / "prod" / "sql" / "schema.sql"
SQLITE_CONNECTION_STRING = f"sqlite:///{DB_PATH}"

# PostgreSQL configuration (for Aiven or other PostgreSQL databases)
# Option 1: Use connection string directly (recommended for Aiven)
# Set AIVEN_CONNECTION_STRING environment variable with full connection string from Aiven console
# Example: postgres://avnadmin:password@hostname:port/dbname?sslmode=require
AIVEN_CONNECTION_STRING = os.getenv("AIVEN_CONNECTION_STRING", "")

# Option 2: Set individual components
# Set these environment variables or update directly:
# AIVEN_HOST, AIVEN_PORT, AIVEN_DBNAME, AIVEN_USER, AIVEN_PASSWORD
POSTGRES_HOST = os.getenv("AIVEN_HOST", "localhost")
POSTGRES_PORT = os.getenv("AIVEN_PORT", "5432")
POSTGRES_DBNAME = os.getenv("AIVEN_DBNAME", "defaultdb")
POSTGRES_USER = os.getenv("AIVEN_USER", "avnadmin")
POSTGRES_PASSWORD = os.getenv("AIVEN_PASSWORD", "")
POSTGRES_SSLMODE = os.getenv("AIVEN_SSLMODE", "require")

# Parse connection string if provided, otherwise use individual components
if AIVEN_CONNECTION_STRING:
    # Parse the connection string
    import urllib.parse
    # Handle both postgres:// and postgresql://
    conn_str = AIVEN_CONNECTION_STRING.replace("postgres://", "postgresql://")
    parsed = urllib.parse.urlparse(conn_str)
    
    POSTGRES_USER = parsed.username or POSTGRES_USER
    POSTGRES_PASSWORD = parsed.password or POSTGRES_PASSWORD
    POSTGRES_HOST = parsed.hostname or POSTGRES_HOST
    POSTGRES_PORT = str(parsed.port) if parsed.port else POSTGRES_PORT
    POSTGRES_DBNAME = parsed.path.lstrip('/').split('?')[0] if parsed.path else POSTGRES_DBNAME
    
    # Parse query parameters for sslmode
    if parsed.query:
        query_params = urllib.parse.parse_qs(parsed.query)
        if 'sslmode' in query_params:
            POSTGRES_SSLMODE = query_params['sslmode'][0]

# PostgreSQL connection string (for SQLAlchemy)
POSTGRES_CONNECTION_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}?sslmode={POSTGRES_SSLMODE}"
)

# PostgreSQL schema file
POSTGRES_SCHEMA_PATH = PROJECT_ROOT / "prod" / "sql" / "schema_postgresql.sql"

# Database type selection
# Set USE_POSTGRESQL=True to use PostgreSQL, otherwise uses SQLite
USE_POSTGRESQL = os.getenv("USE_POSTGRESQL", "False").lower() == "true"

# Data paths
# Processed data (shared between MVP and prod)
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
EMISSIONS_PATH = PROCESSED_DATA_DIR / "emissions_nuts2.parquet"
CAUSES_PATH = PROCESSED_DATA_DIR / "health_causes_of_death.parquet"
DISCHARGES_PATH = PROCESSED_DATA_DIR / "health_hospital_discharges.parquet"
POPULATION_PATH = PROCESSED_DATA_DIR / "population_nuts2.parquet"

# Curated data (MVP-specific)
CURATED_DATA_PATH = PROJECT_ROOT / "mvp" / "data" / "curated" / "eu_climate_health.parquet"

