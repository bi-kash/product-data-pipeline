"""
Script to load data from CSV files into the database.

This script loads data from:
1. sellers.csv -> sellers table
2. scraped_products.csv -> scraped_products table

Column names in CSV files should match the database column names.
"""

import csv
import logging
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from src.common.database import get_db_session, Seller, ScrapedProduct
from src.common.logging_config import setup_logging

# Setup logging
setup_logging('load_csv_data')
logger = logging.getLogger(__name__)


def parse_datetime(date_str):
    """
    Parse datetime string from CSV.
    Handles multiple formats.
    """
    if not date_str or date_str.strip() == '':
        return datetime.now()
    
    # Try different datetime formats
    formats = [
        '%Y-%m-%d %H:%M:%S.%f%z',  # With timezone and microseconds
        '%Y-%m-%d %H:%M:%S%z',      # With timezone
        '%Y-%m-%d %H:%M:%S.%f',     # Without timezone, with microseconds
        '%Y-%m-%d %H:%M:%S',        # Without timezone
        '%Y-%m-%d',                 # Date only
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse datetime: {date_str}, using current time")
    return datetime.now()


def parse_boolean(value):
    """Parse boolean values from CSV."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 't')
    return bool(value)


def load_sellers(csv_file_path):
    """
    Load sellers from CSV file into sellers table.
    
    Args:
        csv_file_path: Path to sellers.csv file
        
    Returns:
        Dict with statistics
    """
    logger.info(f"Loading sellers from {csv_file_path}")
    
    stats = {
        'total_rows': 0,
        'inserted': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            with get_db_session() as db:
                for row in reader:
                    stats['total_rows'] += 1
                    
                    try:
                        # Check if seller already exists
                        existing_seller = db.query(Seller).filter(
                            Seller.shop_id == row['shop_id']
                        ).first()
                        
                        if existing_seller:
                            # Update existing seller
                            existing_seller.shop_url = row.get('shop_url', existing_seller.shop_url)
                            existing_seller.shop_name = row.get('shop_name', existing_seller.shop_name)
                            existing_seller.approval_status = row.get('approval_status', existing_seller.approval_status)
                            existing_seller.last_seen_at = parse_datetime(row.get('last_seen_at'))
                            existing_seller.note = row.get('note', existing_seller.note)
                            
                            stats['updated'] += 1
                            logger.debug(f"Updated seller: {row['shop_id']}")
                        else:
                            # Create new seller
                            seller = Seller(
                                shop_id=row['shop_id'],
                                shop_url=row['shop_url'],
                                shop_name=row.get('shop_name'),
                                approval_status=row.get('approval_status', 'PENDING'),
                                first_seen_at=parse_datetime(row.get('first_seen_at')),
                                last_seen_at=parse_datetime(row.get('last_seen_at')),
                                note=row.get('note')
                            )
                            db.add(seller)
                            stats['inserted'] += 1
                            logger.debug(f"Inserted seller: {row['shop_id']}")
                        
                        # Commit after each row to handle potential duplicates
                        db.commit()
                        
                    except IntegrityError as e:
                        db.rollback()
                        stats['errors'] += 1
                        logger.error(f"Integrity error for seller {row.get('shop_id')}: {e}")
                    except Exception as e:
                        db.rollback()
                        stats['errors'] += 1
                        logger.error(f"Error processing seller row {stats['total_rows']}: {e}")
        
        logger.info(f"Sellers loading completed: {stats}")
        return stats
        
    except FileNotFoundError:
        logger.error(f"File not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading sellers: {e}")
        raise


def load_scraped_products(csv_file_path):
    """
    Load scraped products from CSV file into scraped_products table.
    
    Args:
        csv_file_path: Path to scraped_products.csv file
        
    Returns:
        Dict with statistics
    """
    logger.info(f"Loading scraped products from {csv_file_path}")
    
    stats = {
        'total_rows': 0,
        'inserted': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            with get_db_session() as db:
                for row in reader:
                    stats['total_rows'] += 1
                    
                    try:
                        # Check if product already exists (by id if present, or by product_id + seller_id)
                        if 'id' in row and row['id']:
                            existing_product = db.query(ScrapedProduct).filter(
                                ScrapedProduct.id == int(row['id'])
                            ).first()
                        else:
                            existing_product = db.query(ScrapedProduct).filter(
                                ScrapedProduct.product_id == row['product_id'],
                                ScrapedProduct.seller_id == row['seller_id']
                            ).first()
                        
                        if existing_product:
                            # Update existing product
                            existing_product.is_extracted = parse_boolean(row.get('is_extracted', False))
                            existing_product.is_failed = parse_boolean(row.get('is_failed', False))
                            
                            if row.get('extracted_at'):
                                existing_product.extracted_at = parse_datetime(row['extracted_at'])
                            if row.get('failed_at'):
                                existing_product.failed_at = parse_datetime(row['failed_at'])
                            
                            stats['updated'] += 1
                            logger.debug(f"Updated scraped product: {row['product_id']}")
                        else:
                            # Create new scraped product
                            scraped_product = ScrapedProduct(
                                product_id=row['product_id'],
                                seller_id=row['seller_id'],
                                is_extracted=parse_boolean(row.get('is_extracted', False)),
                                is_failed=parse_boolean(row.get('is_failed', False)),
                                scraped_at=parse_datetime(row.get('scraped_at')) if row.get('scraped_at') else datetime.now(),
                                extracted_at=parse_datetime(row['extracted_at']) if row.get('extracted_at') else None,
                                failed_at=parse_datetime(row['failed_at']) if row.get('failed_at') else None
                            )
                            db.add(scraped_product)
                            stats['inserted'] += 1
                            logger.debug(f"Inserted scraped product: {row['product_id']}")
                        
                        # Commit after each row
                        db.commit()
                        
                    except IntegrityError as e:
                        db.rollback()
                        stats['errors'] += 1
                        logger.error(f"Integrity error for product {row.get('product_id')}: {e}")
                    except Exception as e:
                        db.rollback()
                        stats['errors'] += 1
                        logger.error(f"Error processing scraped product row {stats['total_rows']}: {e}")
        
        logger.info(f"Scraped products loading completed: {stats}")
        return stats
        
    except FileNotFoundError:
        logger.error(f"File not found: {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading scraped products: {e}")
        raise


def main():
    """Main function to load all CSV data."""
    logger.info("Starting CSV data load process")
    
    try:
        # Load sellers first (since scraped_products has foreign key to sellers)
        logger.info("=" * 60)
        logger.info("STEP 1: Loading sellers")
        logger.info("=" * 60)
        sellers_stats = load_sellers('data/sellers.csv')
        
        # Load scraped products
        logger.info("=" * 60)
        logger.info("STEP 2: Loading scraped products")
        logger.info("=" * 60)
        products_stats = load_scraped_products('data/scraped_products.csv')
        
        # Summary
        logger.info("=" * 60)
        logger.info("DATA LOAD SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Sellers: {sellers_stats}")
        logger.info(f"Scraped Products: {products_stats}")
        logger.info("=" * 60)
        logger.info("CSV data load completed successfully!")
        
    except Exception as e:
        logger.error(f"CSV data load failed: {e}")
        raise


if __name__ == "__main__":
    main()
