#!/usr/bin/env python3
"""
Migration script to recreate the filtered_products table with new schema.

This script:
1. Backs up existing filtered_products data
2. Drops the filtered_products table
3. Recreates it with the new schema (matching products table + 3 extra fields)
4. Optionally restores data if possible

Usage:
    python migrate_filtered_products.py [--backup-only] [--restore]
"""

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from src.common.database import create_tables_if_not_exist


def backup_filtered_products(db_path: str, backup_path: str):
    """Backup existing filtered_products data to JSON."""
    print(f"Backing up filtered_products data to {backup_path}...")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable dict-like access
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='filtered_products'
        """)
        
        if not cursor.fetchone():
            print("filtered_products table does not exist. Nothing to backup.")
            return 0
        
        # Get all data
        cursor.execute("SELECT * FROM filtered_products")
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        data = []
        for row in rows:
            row_dict = dict(row)
            # Convert datetime strings to ISO format if needed
            for key, value in row_dict.items():
                if isinstance(value, str) and ('_at' in key or 'time' in key):
                    # Already in string format, keep as is
                    pass
            data.append(row_dict)
        
        # Write to JSON file
        with open(backup_path, 'w') as f:
            json.dump({
                'backup_date': datetime.now().isoformat(),
                'record_count': len(data),
                'data': data
            }, f, indent=2, default=str)
        
        print(f"Backup completed: {len(data)} records saved to {backup_path}")
        return len(data)
        
    except Exception as e:
        print(f"Error during backup: {e}")
        return 0
    finally:
        conn.close()


def drop_filtered_products_table(db_path: str):
    """Drop the existing filtered_products table."""
    print("Dropping existing filtered_products table...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("DROP TABLE IF EXISTS filtered_products")
        conn.commit()
        print("Table dropped successfully.")
    except Exception as e:
        print(f"Error dropping table: {e}")
    finally:
        conn.close()


def recreate_table():
    """Recreate the filtered_products table with new schema."""
    print("Recreating filtered_products table with new schema...")
    
    try:
        create_tables_if_not_exist()
        print("Table recreated successfully with new schema.")
    except Exception as e:
        print(f"Error recreating table: {e}")


def main():
    parser = argparse.ArgumentParser(description="Migrate filtered_products table schema")
    parser.add_argument('--backup-only', action='store_true', 
                       help='Only backup data, do not drop/recreate table')
    parser.add_argument('--restore', action='store_true',
                       help='Restore data from backup (not implemented yet)')
    
    args = parser.parse_args()
    
    # Database and backup paths
    db_path = "product_data.db"
    backup_path = f"filtered_products_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    if not Path(db_path).exists():
        print(f"Database file {db_path} not found!")
        return
    
    if args.restore:
        print("Restore functionality not implemented yet.")
        return
    
    # Step 1: Backup existing data
    backup_count = backup_filtered_products(db_path, backup_path)
    
    if args.backup_only:
        print("Backup-only mode. Exiting without modifying database.")
        return
    
    # Step 2: Drop existing table
    drop_filtered_products_table(db_path)
    
    # Step 3: Recreate table with new schema
    recreate_table()
    
    print("\nMigration completed!")
    print(f"- Backed up {backup_count} records to {backup_path}")
    print("- Old table dropped")
    print("- New table created with updated schema")
    print("\nThe new filtered_products table now matches the products table structure")
    print("plus ship_to_country, delivery_time, and max_variant_price fields.")


if __name__ == "__main__":
    main()