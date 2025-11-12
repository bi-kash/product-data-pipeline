"""
Simple script to rename price and currency columns in filtered_products table.
"""
import sqlite3

# Connect to the database
conn = sqlite3.connect('test.db')
cursor = conn.cursor()

try:
    # Rename price column to target_sale_price
    cursor.execute('ALTER TABLE filtered_products RENAME COLUMN price TO target_sale_price')
    print("✓ Renamed 'price' to 'target_sale_price'")
    
    # Rename currency column to target_sale_price_currency
    cursor.execute('ALTER TABLE filtered_products RENAME COLUMN currency TO target_sale_price_currency')
    print("✓ Renamed 'currency' to 'target_sale_price_currency'")
    
    # Commit the changes
    conn.commit()
    print("\n✓ All column renames completed successfully!")
    
except sqlite3.OperationalError as e:
    print(f"Error: {e}")
    print("Note: Columns may already be renamed or don't exist")
    conn.rollback()
    
finally:
    conn.close()
