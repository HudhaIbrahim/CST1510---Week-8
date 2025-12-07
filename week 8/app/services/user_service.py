import bcrypt
from pathlib import Path
from app.data.db import connect_database, DATA_DIR
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

def migrate_users_from_file(conn, filename="users.txt"):
    """
    Migrate users from DATA/users.txt into the users table.
    Expected file format:
        username,password_hash,role
    """
    users_file = DATA_DIR / filename

    if not users_file.exists():
        print(f"⚠️ File not found: {users_file}")
        print("No users migrated.")
        return 0

    cursor = conn.cursor()
    migrated = 0

    with open(users_file, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            try:
                username, password_hash, role = line.split(",")

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO users (username, password_hash, role)
                    VALUES (?, ?, ?)
                    """,
                    (username.strip(), password_hash.strip(), role.strip())
                )

                if cursor.rowcount > 0:
                    migrated += 1

            except Exception as e:
                print(f"Error processing line '{line}': {e}")

    conn.commit()
    print(f"✅ Migrated {migrated} users from {users_file}")
    return migrated
