"""
Simple database maintenance and test.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT', 5432)
)

conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

try:
    # Kill any idle transactions
    print("Killing idle transactions...")
    cursor.execute("""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database()
        AND pid != pg_backend_pid()
        AND state LIKE '%idle in transaction%';
    """)
    print("✓ Done")
    
    # Simple VACUUM (not FULL)
    print("\nRunning VACUUM ANALYZE...")
    cursor.execute("VACUUM ANALYZE filtered_products;")
    print("✓ Done")
    
    # Check row count
    cursor.execute("SELECT COUNT(*) FROM filtered_products;")
    count = cursor.fetchone()[0]
    print(f"\nCurrent rows in filtered_products: {count}")
    
    # Check if the problematic product already exists
    cursor.execute("SELECT product_id FROM filtered_products WHERE product_id = '1005007368909530';")
    exists = cursor.fetchone()
    if exists:
        print(f"\n⚠️  Product 1005007368909530 already exists in filtered_products!")
        print("This might be causing the timeout. Consider deleting it first:")
        print("  DELETE FROM filtered_products WHERE product_id = '1005007368909530';")
    else:
        print(f"\n✓ Product 1005007368909530 does not exist yet")
    
    print("\n" + "="*80)
    print("Database is ready. Try your command again.")
    
except Exception as e:
    print(f"Error: {e}")
    
finally:
    cursor.close()
    conn.close()
