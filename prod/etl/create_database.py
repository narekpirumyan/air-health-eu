"""
Create SQLite database and schema.

This script creates the database file and applies the schema.
Run this after you've reviewed and enhanced the schema in sql/schema.sql.
"""

from pathlib import Path
import sqlite3
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from prod.config.settings import DB_PATH, SCHEMA_PATH


def create_database():
    """
    Create SQLite database and apply schema.
    
    This will:
    1. Create the database file if it doesn't exist
    2. Enable foreign key constraints
    3. Execute the schema SQL file
    4. Verify the schema was created correctly
    """
    # Ensure directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database if it exists (for clean rebuild)
    if DB_PATH.exists():
        response = input(f"Database {DB_PATH} already exists. Delete and recreate? (y/N): ")
        if response.lower() == 'y':
            DB_PATH.unlink()
            print(f"✓ Deleted existing database: {DB_PATH}")
        else:
            print("Database creation cancelled.")
            return
    
    # Connect to database (creates file if doesn't exist)
    print(f"Creating database: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    print("✓ Foreign keys enabled")
    
    # Read and execute schema
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}")
    
    print(f"Reading schema from: {SCHEMA_PATH}")
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    # Execute schema (SQLite executes multiple statements)
    cursor.executescript(schema_sql)
    conn.commit()
    print("✓ Schema applied successfully")
    
    # Verify tables were created
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n✓ Created {len(tables)} tables:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  - {table} ({count} rows)")
    
    # Verify views
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='view'
        ORDER BY name
    """)
    views = [row[0] for row in cursor.fetchall()]
    
    if views:
        print(f"\n✓ Created {len(views)} views:")
        for view in views:
            print(f"  - {view}")
    
    conn.close()
    print(f"\n✓ Database created successfully: {DB_PATH}")
    print("\nNext step: Run 'python etl/load_data.py' to load data")


if __name__ == "__main__":
    create_database()

