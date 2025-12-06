import sqlite3
from pathlib import Path

BASE_DIR = Path(r"C:\Users\dell\Desktop\uni\VS CODE\CST1510---Week-8\week 8\DATA")
DATA_DIR = BASE_DIR / "DATA"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "intelligence_platform.db"

def connect_database(db_path=DB_PATH):
    """Connect to SQLite database."""
    return sqlite3.connect(str(db_path))


# LOADING CSV FILES
from pathlib import Path
import pandas as pd
from app.data.db import DB_PATH
from app.data.incidents import connect_database
from app.data.schema import create_all_tables
from app.services.user_service import migrate_users_from_file


def load_csv_to_table(csv_path, table_name):
    """Load csv to table."""
    conn = connect_database()
    if not Path(csv_path).exists():
        print(f"File not found: {csv_path}")
        return False

    # Read CSV into DataFrame
    df = pd.read_csv(csv_path)
    # if the table exists, append otherwise pandas creates. DataFrame's index will not be added as a separate column
    df.to_sql(table_name, con=conn, if_exists='append', index=False)
    print(f"✅ Loaded {len(df)} rows from {csv_path} into table '{table_name}'.")
    conn.close() # closes the database. Frees resources and ensures no further operations are made using this connection.
    return len(df)

def load_all_csv_data():
    PATH_DATA = Path(r"C:\Users\dell\Desktop\uni\VS CODE\CST1510---Week-8\week 8\DATA")

    csv_table_map = {
        "cyber-operations-incidents.csv": "cyber_incidents",
        "datasets_metadata.csv": "datasets_metadata",
        "it_tickets.csv": "it_tickets"
    }

    for csv_file, table_name in csv_table_map.items():
        csv_path = str(PATH_DATA / csv_file)
        load_csv_to_table(csv_path, table_name)


# COMPLETE SETUP FUNCTION
def setup_database_complete():
    """
    Complete database setup:
    1. Connect to database
    2. Create all tables
    3. Migrate users from users.txt
    4. Load CSV data for all domains
    5. Verify setup
    """
    print("\n" + "="*60)
    print("STARTING COMPLETE DATABASE SETUP")
    print("="*60)
    
    # Step 1: Connect
    print("\n[1/5] Connecting to database...")
    conn = connect_database()
    print("       Connected")
    
    # Step 2: Create tables
    print("\n[2/5] Creating database tables...")
    create_all_tables(conn)
    
    # Step 3: Migrate users
    print("\n[3/5] Migrating users from users.txt...")
    user_count = migrate_users_from_file(conn)
    print(f"       Migrated {user_count} users")
    
    # Step 4: Load CSV data
    print("\n[4/5] Loading CSV data...")
    total_rows = load_all_csv_data(conn)
    
    # Step 5: Verify
    print("\n[5/5] Verifying database setup...")
    cursor = conn.cursor()
    
    # Count rows in each table
    tables = ['users', 'cyber_incidents', 'datasets_metadata', 'it_tickets']
    print("\n Database Summary:")
    print(f"{'Table':<25} {'Row Count':<15}")
    print("-" * 40)
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:<25} {count:<15}")
    
    conn.close()
    
    print("\n" + "="*60)
    print(" DATABASE SETUP COMPLETE!")
    print("="*60)
    print(f"\n Database location: {DB_PATH.resolve()}")
    print("\nYou're ready for Week 9 (Streamlit web interface)!")

# Run the complete setup
setup_database_complete()