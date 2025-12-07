from app.data.db import connect_database
from app.data.schema import create_all_tables
from app.services.user_service import register_user, login_user, migrate_users_from_file
from app.data.incidents import insert_incident, get_all_incidents, get_incidents_by_type_count, \
    get_high_severity_by_status, get_incident_types_with_many_cases

def main():
    print("=" * 60)
    print("Week 8: Database Demo")
    print("=" * 60)
    
    # 1. Setup database
    conn = connect_database()
    create_all_tables(conn)

    
    # 2. Migrate users
    migrate_users_from_file(conn)
    
    # 3. Test authentication
    success, msg = register_user(conn, "alice", "SecurePass123!", "analyst")
    print(msg)
    
    success, msg = login_user("alice", "SecurePass123!")
    print(msg)

        # 4. Test CRUD
    incident_id = insert_incident(
        "2024-11-05",
        "Phishing",
        "High",
        "Open",
        "Suspicious email detected",
        "alice"
    )
    print(f"Created incident #{incident_id}")
    
    # 5. Query data
    df = get_all_incidents()
    print(f"Total incidents: {len(df)}")

    # Test: Run analytical queries

    print("\n Incidents by Type:")
    df_by_type = get_incidents_by_type_count(conn)
    print(df_by_type)

    print("\n High Severity Incidents by Status:")
    df_high_severity = get_high_severity_by_status(conn)
    print(df_high_severity)

    print("\n Incident Types with Many Cases (>5):")
    df_many_cases = get_incident_types_with_many_cases(conn, min_count=5)
    print(df_many_cases)

    conn.close()

if __name__ == "__main__":
    main()