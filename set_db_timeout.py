"""
Set statement timeout at the database level for the current user.
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
    username = os.getenv('DB_USER')
    
    print(f"Setting statement_timeout for user: {username}")
    
    # Set statement timeout to 10 minutes for this user
    cursor.execute(f"ALTER ROLE {username} SET statement_timeout = '10min';")
    print(f"✓ Statement timeout set to 10 minutes for user {username}")
    
    # Also set for current session
    cursor.execute("SET statement_timeout = '10min';")
    print("✓ Statement timeout set for current session")
    
    # Verify
    cursor.execute("SHOW statement_timeout;")
    timeout = cursor.fetchone()[0]
    print(f"\nCurrent statement_timeout: {timeout}")
    
    print("\n" + "="*80)
    print("✓ Configuration complete!")
    print("\nNote: Restart your Python application to pick up the new settings.")
    print("The timeout will now be 10 minutes instead of 2 minutes.")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    cursor.close()
    conn.close()
