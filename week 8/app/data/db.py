import sqlite3
from pathlib import Path
import pandas as pd

# ============================
# FIXED PATHS
# ============================
# BASE_DIR = project root (week 8)
BASE_DIR = Path(__file__).resolve().parents[2]

# DATA folder
DATA_DIR = BASE_DIR / "DATA"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Database path
DB_PATH = DATA_DIR / "intelligence_platform.db"


# ============================
# DATABASE CONNECTION
# ============================
def connect_database(db_path=DB_PATH):
    return sqlite3.connect(str(db_path))


# ============================
# LOAD CSV INTO TABLE
# ============================
def load_csv_to_table(conn, csv_path, table_name):

    if not Path(csv_path).exists():
        print(f"File not found: {csv_path}")
        return False

    df = pd.read_csv(csv_path)
    df.to_sql(table_name, con=conn, if_exists="append", index=False)
    print(f"✅ Loaded {len(df)} rows from {csv_path} into table '{table_name}'.")
    return len(df)


def load_all_csv_data(conn):
    csv_map = {
        "cyber-operations-incidents.csv": "cyber_incidents",
        "datasets_metadata.csv": "datasets_metadata",
        "it_tickets.csv": "it_tickets"
    }

    total_rows = 0

    for file, table in csv_map.items():
        csv_path = DATA_DIR / file
        rows = load_csv_to_table(conn, csv_path, table)
        total_rows += rows if rows else 0

    return total_rows
