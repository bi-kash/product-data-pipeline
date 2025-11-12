"""
Kill the blocking transaction and clean up.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT', 5432)
)

cursor = conn.cursor()

try:
    print("Finding blocking transactions...")
    cursor.execute("""
        SELECT 
            pid,
            usename,
            state,
            query_start,
            age(clock_timestamp(), query_start) AS duration
        FROM pg_stat_activity
        WHERE datname = current_database()
        AND state LIKE '%idle in transaction%'
        AND query LIKE '%filtered_products%';
    """)
    
    blocking_transactions = cursor.fetchall()
    
    if blocking_transactions:
        print(f"\nFound {len(blocking_transactions)} blocking transaction(s):")
        for pid, user, state, query_start, duration in blocking_transactions:
            print(f"\n  PID: {pid}")
            print(f"  User: {user}")
            print(f"  State: {state}")
            print(f"  Started: {query_start}")
            print(f"  Duration: {duration}")
            
            # Kill the blocking transaction
            print(f"\n  Terminating PID {pid}...")
            cursor.execute(f"SELECT pg_terminate_backend({pid});")
            result = cursor.fetchone()
            if result[0]:
                print(f"  ✓ Successfully terminated PID {pid}")
            else:
                print(f"  ⚠️  Failed to terminate PID {pid}")
        
        conn.commit()
        print("\n✓ All blocking transactions terminated")
    else:
        print("No blocking transactions found")
    
    print("\n" + "="*80)
    print("You can now retry your INSERT operation")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    cursor.close()
    conn.close()
