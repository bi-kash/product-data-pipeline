"""
Scraper-based product filter workflow.

This module implements the scraping workflow that:
1. Scrapes product IDs from seller store pages using Selenium
2. Stores product IDs in scraped_products table for later processing

The actual API fetching and filtering is handled by the filter:products command.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime

from src.common.database import (
    Seller,
    ScraperProgress,
    ScrapedProduct,
    get_db_session,
    get_utc_now,
)
from src.common.config import get_env
from src.scraper.seller_scraper import SellerStoreScraper

logger = logging.getLogger(__name__)


class ScraperBasedFilter:
    """
    Main class for the scraper-based filtering workflow.
    
    This class orchestrates scraping product IDs from seller stores
    and saving them to scraped_products table for later processing.
    """

    def __init__(self):
        """Initialize the scraper-based filter."""
        # Get configuration from environment
        self.headless = get_env("SELENIUM_HEADLESS", "true").lower() == "true"
        self.timeout = int(get_env("SELENIUM_TIMEOUT", "10"))
        self.country = get_env("TARGET_COUNTRY", "Germany")
        self.currency = get_env("TARGET_CURRENCY", "EUR")
        
        logger.info(f"ScraperBasedFilter initialized: headless={self.headless}, country={self.country}, currency={self.currency}")

    def process_sellers(self, seller_ids: Optional[List[str]] = None, limit: int = None) -> Dict:
        """
        Process sellers - scrape product IDs and save to scraped_products.
        
        Args:
            seller_ids: List of specific seller IDs to process (if None, processes all whitelisted)
            limit: Maximum number of sellers to process
            
        Returns:
            Dict with overall statistics
        """
        overall_stats = {
            'sellers_processed': 0,
            'sellers_completed': 0,
            'sellers_failed': 0,
            'total_products_scraped': 0,
            'errors': []
        }
        
        db = get_db_session()
        
        try:
            # Get sellers to process
            if seller_ids:
                sellers = db.query(Seller).filter(Seller.shop_id.in_(seller_ids)).all()
            else:
                # Process all whitelisted sellers
                query = db.query(Seller).filter(Seller.approval_status == 'WHITELIST')
                if limit:
                    query = query.limit(limit)
                sellers = query.all()
            
            logger.info(f"Found {len(sellers)} sellers to process")
            
            # Process each seller sequentially
            for seller in sellers:
                logger.info(f"\n{'='*80}")
                logger.info(f"Processing seller: {seller.shop_id} ({seller.shop_name or 'Unknown'})")
                logger.info(f"{'='*80}\n")
                
                try:
                    seller_stats = self.process_single_seller(seller.shop_id)
                    
                    overall_stats['sellers_processed'] += 1
                    if seller_stats['status'] == 'completed':
                        overall_stats['sellers_completed'] += 1
                    else:
                        overall_stats['sellers_failed'] += 1
                    
                    overall_stats['total_products_scraped'] += seller_stats.get('products_scraped', 0)
                    
                except Exception as e:
                    logger.error(f"Error processing seller {seller.shop_id}: {e}")
                    overall_stats['sellers_failed'] += 1
                    overall_stats['errors'].append(f"Seller {seller.shop_id}: {str(e)}")
            
            logger.info(f"\n{'='*80}")
            logger.info("Overall Scraping Complete")
            logger.info(f"{'='*80}")
            logger.info(f"Sellers processed: {overall_stats['sellers_processed']}")
            logger.info(f"Sellers completed: {overall_stats['sellers_completed']}")
            logger.info(f"Sellers failed: {overall_stats['sellers_failed']}")
            logger.info(f"Total products scraped: {overall_stats['total_products_scraped']}")
            logger.info(f"\n💡 Next step: Run 'python main.py filter:products' to fetch and filter these products")
            
            return overall_stats
            
        finally:
            db.close()

    def process_single_seller(self, seller_id: str) -> Dict:
        """
        Process a single seller - scrape product IDs and save to scraped_products.
        
        Workflow:
        1. Create/update progress entry (status: in_progress)
        2. Scrape product IDs from seller store
        3. Save to scraped_products table
        4. Update progress entry (status: completed)
        
        Note: API fetching and filtering is handled separately by filter:products command
        
        Args:
            seller_id: The seller ID to process
            
        Returns:
            Dict with processing statistics for this seller
        """
        stats = {
            'seller_id': seller_id,
            'status': 'not_started',
            'products_scraped': 0,
            'products_fetched': 0,
            'products_filtered': 0,
            'error_message': None
        }
        
        db = get_db_session()
        progress = None
        
        try:
            # Step 1: Create/update progress tracking
            logger.info(f"Step 1: Initializing progress tracking for seller {seller_id}")
            progress = self._init_progress(seller_id, db)
            stats['status'] = 'in_progress'
            
            # Step 2: Scrape product IDs
            logger.info(f"Step 2: Scraping product IDs from seller {seller_id} store page")
            product_ids = self._scrape_product_ids(seller_id)
            stats['products_scraped'] = len(product_ids)
            logger.info(f"✓ Scraped {len(product_ids)} product IDs")
            
            # Step 3: Save scraped products to scraped_products table
            logger.info(f"Step 3: Saving product IDs to scraped_products table")
            new_count = self._save_scraped_products(seller_id, product_ids, db)
            logger.info(f"✓ Saved {new_count} new products to scraped_products")
            
            # Update progress
            progress.products_scraped = len(product_ids)
            progress.total_products_found = len(product_ids)
            db.commit()
            
            # Step 4: Mark as completed
            logger.info(f"Step 4: Marking seller {seller_id} as completed")
            progress.status = 'completed'
            progress.completed_at = get_utc_now()
            stats['status'] = 'completed'
            db.commit()
            
            logger.info(f"✅ Successfully completed scraping seller {seller_id}")
            logger.info(f"   Scraped: {stats['products_scraped']} products")
            logger.info(f"   Use 'filter:products' command to fetch and filter these products")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error processing seller {seller_id}: {e}")
            stats['status'] = 'failed'
            stats['error_message'] = str(e)
            
            # Update progress with error
            if progress:
                progress.status = 'failed'
                progress.error_message = str(e)
                progress.completed_at = get_utc_now()
                try:
                    db.commit()
                except Exception as commit_error:
                    logger.error(f"Failed to update progress with error: {commit_error}")
            
            raise
            
        finally:
            db.close()

    def _init_progress(self, seller_id: str, db) -> ScraperProgress:
        """
        Initialize or reset progress tracking for a seller.
        
        Args:
            seller_id: The seller ID
            db: Database session
            
        Returns:
            ScraperProgress object
        """
        # Check if progress entry exists
        progress = db.query(ScraperProgress).filter(
            ScraperProgress.seller_id == seller_id
        ).first()
        
        if progress:
            # Reset existing progress entry
            logger.debug(f"Resetting existing progress entry for seller {seller_id}")
            progress.status = 'in_progress'
            progress.started_at = get_utc_now()
            progress.completed_at = None
            progress.total_products_found = None
            progress.products_scraped = None
            progress.products_fetched = None
            progress.products_filtered = None
            progress.error_message = None
        else:
            # Create new progress entry
            logger.debug(f"Creating new progress entry for seller {seller_id}")
            progress = ScraperProgress(
                seller_id=seller_id,
                status='in_progress',
                started_at=get_utc_now()
            )
            db.add(progress)
        
        db.commit()
        return progress

    def _scrape_product_ids(self, seller_id: str) -> List[str]:
        """
        Scrape product IDs from seller store page.
        
        Args:
            seller_id: The seller ID
            
        Returns:
            List of product IDs
        """
        with SellerStoreScraper(
            headless=self.headless,
            timeout=self.timeout,
            country=self.country,
            currency=self.currency
        ) as scraper:
            product_ids = scraper.scrape_seller_products(seller_id)
            return product_ids

    def _save_scraped_products(self, seller_id: str, product_ids: List[str], db) -> int:
        """
        Save scraped product IDs to scraped_products table.
        
        Args:
            seller_id: The seller ID
            product_ids: List of product IDs scraped
            db: Database session
            
        Returns:
            Number of new products saved
        """
        new_count = 0
        
        for product_id in product_ids:
            try:
                # Check if this product is already in scraped_products
                existing = db.query(ScrapedProduct).filter(
                    ScrapedProduct.product_id == product_id,
                    ScrapedProduct.seller_id == seller_id
                ).first()
                
                if not existing:
                    # Create new scraped product entry
                    scraped_product = ScrapedProduct(
                        product_id=product_id,
                        seller_id=seller_id,
                        is_extracted=False
                    )
                    db.add(scraped_product)
                    new_count += 1
                    logger.debug(f"Added product {product_id} to scraped_products")
                else:
                    logger.debug(f"Product {product_id} already in scraped_products")
                    
            except Exception as e:
                logger.error(f"Error saving scraped product {product_id}: {e}")
                continue
        
        db.commit()
        logger.info(f"Saved {new_count} new products to scraped_products table")
        return new_count


def run_scraper_based_filtering(seller_ids: Optional[List[str]] = None, limit: int = None) -> Dict:
    """
    Main entry point for scraping product IDs from seller stores.
    
    This command only scrapes product IDs and saves them to scraped_products table.
    Use 'filter:products' command afterwards to fetch and filter the products.
    
    Args:
        seller_ids: List of specific seller IDs to process (if None, processes all whitelisted)
        limit: Maximum number of sellers to process
        
    Returns:
        Processing statistics
        
    Example:
        >>> stats = run_scraper_based_filtering(seller_ids=["2663214"])
        >>> print(f"Scraped {stats['total_products_scraped']} products")
    """
    logger.info("Starting product ID scraping workflow")
    
    scraper_filter = ScraperBasedFilter()
    stats = scraper_filter.process_sellers(seller_ids, limit)
    
    logger.info("Product ID scraping completed")
    
    return stats
