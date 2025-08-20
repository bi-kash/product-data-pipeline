#!/usr/bin/env python3
"""
Utility script to export all database tables to separate CSV files.
This script will generate CSV files for each table in the database and save them
in a specified output directory (default: data/exports).
"""

import os
import pandas as pd
import logging
import argparse
from datetime import datetime
from sqlalchemy import inspect
from src.common.database import engine, Base, get_db_session, Seller, Product, JobRun

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def ensure_export_directory(export_dir):
    """
    Ensure the export directory exists.
    
    Args:
        export_dir: Directory path for CSV exports
    
    Returns:
        Path to the export directory
    """
    if not os.path.exists(export_dir):
        os.makedirs(export_dir, exist_ok=True)
        logger.info(f"Created export directory: {export_dir}")
    return export_dir


def export_table_to_csv(table_name, export_dir):
    """
    Export a specific table to a CSV file.
    
    Args:
        table_name: Name of the table to export
        export_dir: Directory to save the CSV file
        
    Returns:
        Path to the exported CSV file
    """
    session = get_db_session()
    
    try:
        # Use pandas to read the table and export to CSV
        logger.info(f"Exporting table: {table_name}")
        
        # Execute raw SQL to get all data from the table
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        
        # Define the output file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = os.path.join(export_dir, f"{table_name}.csv")
        
        # Export to CSV
        df.to_csv(csv_file, index=False)
        logger.info(f"Exported {len(df)} rows to {csv_file}")
        
        return csv_file
    except Exception as e:
        logger.error(f"Error exporting table {table_name}: {e}")
        return None
    finally:
        session.close()


def get_all_table_names():
    """
    Get all table names from the database.
    
    Returns:
        List of table names
    """
    inspector = inspect(engine)
    return inspector.get_table_names()


def main():
    """Main function to export all tables to CSV files."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Export database tables to CSV files.")
    parser.add_argument(
        "--output-dir", 
        default="data/exports", 
        help="Directory to save CSV exports (default: data/exports)"
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Specific tables to export (default: all tables)"
    )
    args = parser.parse_args()
    
    # Ensure the export directory exists
    export_dir = ensure_export_directory(args.output_dir)
    logger.info(f"Exporting tables to {export_dir}")
    
    # Get table names
    all_tables = get_all_table_names()
    logger.info(f"Found {len(all_tables)} tables in the database: {', '.join(all_tables)}")
    
    # Filter tables if specific ones were requested
    tables_to_export = args.tables if args.tables else all_tables
    
    # Export each table
    exported_files = []
    for table_name in tables_to_export:
        if table_name in all_tables:
            csv_file = export_table_to_csv(table_name, export_dir)
            if csv_file:
                exported_files.append(csv_file)
        else:
            logger.warning(f"Table '{table_name}' not found in the database")
    
    # Summary
    logger.info(f"Export complete. {len(exported_files)} files created.")
    for file_path in exported_files:
        logger.info(f"- {os.path.basename(file_path)}")


if __name__ == "__main__":
    main()
