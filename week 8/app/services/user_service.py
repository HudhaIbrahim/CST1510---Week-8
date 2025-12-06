import bcrypt
from pathlib import Path
from app.data.db import connect_database
from app.data.users import get_user_by_username, insert_user
from app.data.schema import create_users_table
import sqlite3

def register_user(username, password, role='user'):
    """Register new user with password hashing."""
    conn = connect_database()
    cursor = conn.cursor()

    # Check if user already exists
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    if cursor.fetchone():
        conn.close()
        return False, "Username already exists."
    
    # Hash password
    password_hash = bcrypt.hashpw(
        password.encode('utf-8'),
        bcrypt.gensalt()
    ).decode('utf-8')
    
    # Insert into database
    insert_user(username, password_hash, role)
    return True, f"User '{username}' registered successfully."

def login_user(username, password):
    """Authenticate user."""
    user = get_user_by_username(username)
    if not user:
        return False, "User not found."
    
    # Verify password
    stored_hash = user[2]  # password_hash column
    if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
        return True, f"Login successful!"
    return False, "Incorrect password."

def migrate_users_from_file(conn, filepath='DATA_DIR/users.txt'):
    """Migrate users from text file to database."""
    if not Path(filepath).exists():
        print(f"File {filepath} does not exist.")
        print("No users migrated.")
        return
    
    cursor = conn.cursor()
    migrated_count = 0

    with open(filepath, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            # Parse line
            parts = line.split(sep = ',', maxsplit=2)
            if len(parts) == 3:
                username = parts[0]
                password = parts[1]
                role = parts[2]

            # Insert user, ignore if exists
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO users (username, password_hash, role) VALUES (?, ?, ?)"""
                    , (username, password, role)
                )

                if cursor.rowcount > 0:
                    migrated_count += 1
                
            except sqlite3.Error as e:
                print(f"Error inserting user {username}: {e}")
            
    conn.commit()
    print(f"Migrated {migrated_count} users from {filepath}.")

# Verify users were migrated
conn = connect_database()
cursor = conn.cursor()

# Query all users
cursor.execute("SELECT id, username, role FROM users")
users = cursor.fetchall()

print(" Users in database:")
print(f"{'ID':<5} {'Username':<15} {'Role':<10}")
print("-" * 35)
for user in users:
    print(f"{user[0]:<5} {user[1]:<15} {user[2]:<10}")

print(f"\nTotal users: {len(users)}")
conn.close()





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
    PATH_DATA = Path(r"C:\Users\dell\Desktop\uni\VS CODE\CW2_CST1510_M01088116\DATA")

    csv_table_map = {
        "cyber-operations-incidents.csv": "cyber_incidents",
        "datasets_metadata.csv": "datasets_metadata",
        "it_tickets.csv": "it_tickets"
    }

    for csv_file, table_name in csv_table_map.items():
        csv_path = str(PATH_DATA / csv_file)
        load_csv_to_table(csv_path, table_name)