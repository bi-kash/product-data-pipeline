"""
Check for locks and increase statement timeout for filtered_products operations.
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
    print("Checking for locks on filtered_products table...")
    cursor.execute("""
        SELECT 
            pid,
            usename,
            state,
            query,
            age(clock_timestamp(), query_start) AS query_duration
        FROM pg_stat_activity
        WHERE datname = current_database()
        AND query LIKE '%filtered_products%'
        AND state != 'idle';
    """)
    
    locks = cursor.fetchall()
    
    if locks:
        print(f"\nFound {len(locks)} active query(ies) on filtered_products:")
        for pid, user, state, query, duration in locks:
            print(f"\n  PID: {pid}")
            print(f"  User: {user}")
            print(f"  State: {state}")
            print(f"  Duration: {duration}")
            print(f"  Query: {query[:200]}...")
    else:
        print("No active locks found")
    
    print("\n" + "="*80)
    print("Checking table statistics...")
    cursor.execute("""
        SELECT 
            schemaname,
            tablename,
            n_live_tup,
            n_dead_tup,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE tablename = 'filtered_products';
    """)
    
    stats = cursor.fetchone()
    if stats:
        schema, table, live_tuples, dead_tuples, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze = stats
        print(f"\nTable: {schema}.{table}")
        print(f"  Live rows: {live_tuples}")
        print(f"  Dead rows: {dead_tuples}")
        print(f"  Last vacuum: {last_vacuum}")
        print(f"  Last autovacuum: {last_autovacuum}")
        print(f"  Last analyze: {last_analyze}")
        print(f"  Last autoanalyze: {last_autoanalyze}")
        
        if dead_tuples and live_tuples and dead_tuples > live_tuples * 0.2:
            print(f"\n⚠️  WARNING: Many dead tuples detected ({dead_tuples} dead vs {live_tuples} live)")
            print("Recommend running VACUUM ANALYZE on filtered_products table")
            
            print("\nRunning VACUUM ANALYZE now...")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor.execute("VACUUM ANALYZE filtered_products;")
            print("✓ VACUUM ANALYZE completed")
    
    print("\n" + "="*80)
    print("Recommendation: Increase statement_timeout for long operations")
    print("\nYou can add this to your connection string or run before inserts:")
    print("  SET statement_timeout = '10min';")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    cursor.close()
    conn.close()
