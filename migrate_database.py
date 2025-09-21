#!/usr/bin/env python3
"""
Database migration script to add sku_id and variant_key columns to product_images table.
"""

import sqlite3
import os
import sys

def migrate_database():
    """Add sku_id and variant_key columns to the product_images table."""
    
    # Connect to the database
    db_path = "test.db"
    if not os.path.exists(db_path):
        print("❌ Database file not found. Creating new database with updated schema.")
        # Import and create tables with the new schema
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from src.common.database import create_tables_if_not_exist
        create_tables_if_not_exist()
        print("✅ New database created with updated schema.")
        return
    
    print(f"🔧 Migrating database: {db_path}")
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if the new columns already exist
        cursor.execute("PRAGMA table_info(product_images)")
        columns = [column[1] for column in cursor.fetchall()]
        
        print(f"📋 Current columns in product_images: {columns}")
        
        # Add sku_id column if it doesn't exist
        if 'sku_id' not in columns:
            print("➕ Adding sku_id column...")
            cursor.execute("ALTER TABLE product_images ADD COLUMN sku_id TEXT")
            print("✅ sku_id column added.")
        else:
            print("ℹ️  sku_id column already exists.")
        
        # Add variant_key column if it doesn't exist
        if 'variant_key' not in columns:
            print("➕ Adding variant_key column...")
            cursor.execute("ALTER TABLE product_images ADD COLUMN variant_key TEXT")
            print("✅ variant_key column added.")
        else:
            print("ℹ️  variant_key column already exists.")
        
        # Commit the changes
        conn.commit()
        print("✅ Database migration completed successfully!")
        
        # Show updated schema
        cursor.execute("PRAGMA table_info(product_images)")
        updated_columns = [column[1] for column in cursor.fetchall()]
        print(f"📋 Updated columns in product_images: {updated_columns}")
        
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()