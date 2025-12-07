import bcrypt
from pathlib import Path
from app.data.db import connect_database
from app.data.users import get_user_by_username, insert_user
from app.data.schema import create_users_table
import sqlite3

def register_user(conn, username, password, role='user'):
    """Register new user with password hashing."""
    cursor = conn.cursor()

    # Check if user already exists
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    if cursor.fetchone():
        return False, f"Username '{username}' already exists."
    
    # Hash password
    password_hash = bcrypt.hashpw(
        password.encode('utf-8'),
        bcrypt.gensalt()
    ).decode('utf-8')
    
    # Insert new user into database
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

