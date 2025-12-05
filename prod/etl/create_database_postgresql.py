"""
Create PostgreSQL database and schema for Aiven or other PostgreSQL instances.

This script creates the database schema in PostgreSQL.
Run this after you've set up your Aiven PostgreSQL database and configured connection settings.
"""

from pathlib import Path
import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
    POSTGRES_SCHEMA_PATH,
)


def create_database():
    """
    Create PostgreSQL database schema.
    
    This will:
    1. Connect to the PostgreSQL database
    2. Execute the schema SQL file
    3. Verify the schema was created correctly
    """
    # Read schema file
    if not POSTGRES_SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {POSTGRES_SCHEMA_PATH}")
    
    print(f"Reading schema from: {POSTGRES_SCHEMA_PATH}")
    with open(POSTGRES_SCHEMA_PATH, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    # Connect to database
    print(f"Connecting to PostgreSQL database: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DBNAME,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            sslmode=POSTGRES_SSLMODE
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        print("✓ Connected to PostgreSQL database")
    except psycopg2.OperationalError as e:
        print(f"✗ Error connecting to database: {e}")
        print("\nPlease check your Aiven connection settings:")
        print("  - Set AIVEN_HOST environment variable")
        print("  - Set AIVEN_PORT environment variable (default: 5432)")
        print("  - Set AIVEN_DBNAME environment variable")
        print("  - Set AIVEN_USER environment variable")
        print("  - Set AIVEN_PASSWORD environment variable")
        print("\nOr update prod/config/settings.py directly")
        raise
    
    try:
        # Execute schema - PostgreSQL requires executing statements one at a time
        print("Applying schema...")
        
        # Simple approach: execute the entire SQL file
        # psycopg2 can handle multiple statements if we execute them properly
        # But we need to split them correctly
        
        # Remove comment-only lines and clean up
        lines = []
        for line in schema_sql.split('\n'):
            stripped = line.strip()
            # Skip empty lines and comment-only lines
            if stripped and not stripped.startswith('--'):
                lines.append(line)
        
        # Join and split by semicolon (simple approach)
        full_sql = '\n'.join(lines)
        
        # Split by semicolon, but be smart about it
        # Use a simple state machine to track when we're in a string or view
        statements = []
        current_stmt = []
        in_string = False
        string_char = None
        
        i = 0
        while i < len(full_sql):
            char = full_sql[i]
            current_stmt.append(char)
            
            # Track string boundaries
            if char in ("'", '"') and (i == 0 or full_sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            # Split on semicolon if not in string
            if char == ';' and not in_string:
                stmt = ''.join(current_stmt).strip()
                if stmt:
                    statements.append(stmt)
                current_stmt = []
            
            i += 1
        
        # Add any remaining statement
        if current_stmt:
            stmt = ''.join(current_stmt).strip()
            if stmt:
                statements.append(stmt)
        
        # Execute each statement
        executed = 0
        for i, stmt in enumerate(statements, 1):
            stmt = stmt.strip()
            if not stmt or stmt.startswith('--'):
                continue
                
            try:
                cursor.execute(stmt)
                executed += 1
                if executed % 5 == 0 or 'CREATE TABLE' in stmt.upper() or 'CREATE VIEW' in stmt.upper():
                    print(f"  ✓ Executed statement {executed} ({i}/{len(statements)})")
            except psycopg2.Error as e:
                # Some statements might fail if objects already exist
                error_msg = str(e).lower()
                if "already exists" in error_msg or "duplicate" in error_msg:
                    print(f"  ⚠ Statement {i} skipped (already exists)")
                else:
                    print(f"  ✗ Error in statement {i}: {e}")
                    # Show first 200 chars of the statement for debugging
                    print(f"     Statement preview: {stmt[:200]}...")
                    raise
        
        print("✓ Schema applied successfully")
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"\n✓ Created {len(tables)} tables:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  - {table} ({count} rows)")
        
        # Verify views
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        views = [row[0] for row in cursor.fetchall()]
        
        if views:
            print(f"\n✓ Created {len(views)} views:")
            for view in views:
                print(f"  - {view}")
        
        print(f"\n✓ Database schema created successfully!")
        print(f"Database: {POSTGRES_DBNAME} on {POSTGRES_HOST}:{POSTGRES_PORT}")
        print("\nNext step: Run 'python -m prod.etl.load_data_postgresql' to load data")
        
    except Exception as e:
        print(f"\n✗ Error creating schema: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_database()

