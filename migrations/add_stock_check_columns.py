#!/usr/bin/env python3
"""
Migration script to add stock check columns to filtered_products and product_variants tables.

This migration adds:
- status column to filtered_products (for Online/Todo/Offline states)
- stock_status column to product_variants (for stock check results)
"""

import os
import sys
from sqlalchemy import text

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.database import get_db_session, USE_SQLITE

def run_migration():
    """Run the migration to add stock check columns."""
    print("Starting migration: Adding stock check columns...")
    
    with get_db_session() as db:
        try:
            # Add status column to filtered_products
            print("Adding 'status' column to filtered_products table...")
            if USE_SQLITE:
                db.execute(text("""
                    ALTER TABLE filtered_products 
                    ADD COLUMN status VARCHAR(20)
                """))
            else:
                db.execute(text("""
                    ALTER TABLE filtered_products 
                    ADD COLUMN IF NOT EXISTS status VARCHAR(20)
                """))
            
            # Add stock_status column to product_variants
            print("Adding 'stock_status' column to product_variants table...")
            if USE_SQLITE:
                db.execute(text("""
                    ALTER TABLE product_variants 
                    ADD COLUMN stock_status VARCHAR(20)
                """))
            else:
                db.execute(text("""
                    ALTER TABLE product_variants 
                    ADD COLUMN IF NOT EXISTS stock_status VARCHAR(20)
                """))
            
            db.commit()
            print("✅ Migration completed successfully!")
            print("   - Added 'status' column to filtered_products")
            print("   - Added 'stock_status' column to product_variants")
            
        except Exception as e:
            db.rollback()
            error_msg = str(e).lower()
            # Check for various database-specific messages indicating column already exists
            already_exists_indicators = [
                "already exists",
                "duplicate column",
                "column of relation",  # PostgreSQL
                "duplicate column name"  # MySQL
            ]
            
            if any(indicator in error_msg for indicator in already_exists_indicators):
                print("ℹ️  Columns may already exist. Migration may have been run previously.")
            else:
                print(f"❌ Migration failed: {e}")
                raise

def rollback_migration():
    """Rollback the migration (remove the columns)."""
    print("Rolling back migration: Removing stock check columns...")
    
    with get_db_session() as db:
        try:
            # Note: SQLite doesn't support DROP COLUMN easily, so we skip rollback for SQLite
            if USE_SQLITE:
                print("⚠️  SQLite doesn't support DROP COLUMN. Manual rollback required if needed.")
                return
            
            print("Removing 'stock_status' column from product_variants table...")
            db.execute(text("""
                ALTER TABLE product_variants 
                DROP COLUMN IF EXISTS stock_status
            """))
            
            print("Removing 'status' column from filtered_products table...")
            db.execute(text("""
                ALTER TABLE filtered_products 
                DROP COLUMN IF EXISTS status
            """))
            
            db.commit()
            print("✅ Rollback completed successfully!")
            
        except Exception as e:
            db.rollback()
            print(f"❌ Rollback failed: {e}")
            raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate database to add stock check columns")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()
    
    if args.rollback:
        rollback_migration()
    else:
        run_migration()
