"""
Scraper-based product filter workflow.

This module implements the new filtering workflow that:
1. Scrapes product IDs from seller store pages using Selenium
2. Fetches product details via AliExpress API
3. Applies filtering rules (price, delivery time)
4. Stores qualifying products in filtered_products table
5. Tracks progress in scraper_progress table
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
    upsert_product,
    upsert_seller,
)
from src.common.config import get_env
from src.common.official_aliexpress_client import OfficialAliExpressClient
from src.scraper.seller_scraper import SellerStoreScraper
from src.filter.product_filter import ProductFilterEngine

logger = logging.getLogger(__name__)


class ScraperBasedFilter:
    """
    Main class for the scraper-based filtering workflow.
    
    This class orchestrates:
    - Scraping product IDs from seller stores
    - Fetching product details from API
    - Applying filtering rules
    - Tracking progress in database
    """

    def __init__(self):
        """Initialize the scraper-based filter."""
        self.api_client = OfficialAliExpressClient()
        self.filter_engine = ProductFilterEngine()
        
        # Get configuration from environment
        self.headless = get_env("SELENIUM_HEADLESS", "true").lower() == "true"
        self.timeout = int(get_env("SELENIUM_TIMEOUT", "10"))
        self.country = get_env("TARGET_COUNTRY", "Germany")
        self.currency = get_env("TARGET_CURRENCY", "EUR")
        
        logger.info(f"ScraperBasedFilter initialized: headless={self.headless}, country={self.country}, currency={self.currency}")

    def process_sellers(self, seller_ids: Optional[List[str]] = None, limit: int = None) -> Dict:
        """
        Process sellers one at a time through the complete workflow.
        
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
            'total_products_fetched': 0,
            'total_products_filtered': 0,
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
                    overall_stats['total_products_fetched'] += seller_stats.get('products_fetched', 0)
                    overall_stats['total_products_filtered'] += seller_stats.get('products_filtered', 0)
                    
                except Exception as e:
                    logger.error(f"Error processing seller {seller.shop_id}: {e}")
                    overall_stats['sellers_failed'] += 1
                    overall_stats['errors'].append(f"Seller {seller.shop_id}: {str(e)}")
            
            logger.info(f"\n{'='*80}")
            logger.info("Overall Processing Complete")
            logger.info(f"{'='*80}")
            logger.info(f"Sellers processed: {overall_stats['sellers_processed']}")
            logger.info(f"Sellers completed: {overall_stats['sellers_completed']}")
            logger.info(f"Sellers failed: {overall_stats['sellers_failed']}")
            logger.info(f"Total products scraped: {overall_stats['total_products_scraped']}")
            logger.info(f"Total products fetched: {overall_stats['total_products_fetched']}")
            logger.info(f"Total products filtered: {overall_stats['total_products_filtered']}")
            
            return overall_stats
            
        finally:
            db.close()

    def process_single_seller(self, seller_id: str) -> Dict:
        """
        Process a single seller through the complete workflow.
        
        Workflow:
        1. Create/update progress entry (status: in_progress)
        2. Scrape product IDs from seller store
        3. Fetch product details from API
        4. Filter products and save to filtered_products
        5. Update progress entry (status: completed)
        
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
            
            # Save scraped products to scraped_products table
            self._save_scraped_products(seller_id, product_ids, db)
            
            # Update progress
            progress.products_scraped = len(product_ids)
            progress.total_products_found = len(product_ids)
            db.commit()
            
            # Step 3: Fetch product details from API
            logger.info(f"Step 3: Fetching product details from API")
            fetched_products = self._fetch_products(seller_id, product_ids, db)
            stats['products_fetched'] = len(fetched_products)
            logger.info(f"✓ Fetched {len(fetched_products)} products from API")
            
            # Update progress
            progress.products_fetched = len(fetched_products)
            db.commit()
            
            # Step 4: Apply filters and save to filtered_products
            logger.info(f"Step 4: Applying filters to products")
            filtered_count = self._filter_products(fetched_products, db)
            stats['products_filtered'] = filtered_count
            logger.info(f"✓ {filtered_count} products passed filters and saved to filtered_products")
            
            # Update progress
            progress.products_filtered = filtered_count
            db.commit()
            
            # Step 5: Mark as completed
            logger.info(f"Step 5: Marking seller {seller_id} as completed")
            progress.status = 'completed'
            progress.completed_at = get_utc_now()
            stats['status'] = 'completed'
            db.commit()
            
            logger.info(f"✅ Successfully completed processing seller {seller_id}")
            logger.info(f"   Scraped: {stats['products_scraped']}, Fetched: {stats['products_fetched']}, Filtered: {stats['products_filtered']}")
            
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

    def _fetch_products(self, seller_id: str, product_ids: List[str], db) -> List[str]:
        """
        Fetch product details from API and save to products table.
        
        Args:
            seller_id: The seller ID
            product_ids: List of product IDs to fetch
            db: Database session
            
        Returns:
            List of successfully fetched product IDs
        """
        fetched_products = []
        
        for i, product_id in enumerate(product_ids, 1):
            try:
                logger.debug(f"Fetching product {i}/{len(product_ids)}: {product_id}")
                
                # Fetch product details from API
                product_data = self.api_client.get_product_details(product_id)
                
                if not product_data:
                    logger.warning(f"No data returned for product {product_id}")
                    continue
                
                # Extract product information
                result = product_data.get('aliexpress_ds_product_get_response', {}).get('result', {})
                
                # Save product to database
                upsert_product(
                    product_id=product_id,
                    shop_id=seller_id,
                    product_title=result.get('subject'),
                    product_detail_url=result.get('product_detail_url'),
                    product_main_image_url=result.get('product_main_image_url'),
                    product_video_url=result.get('product_video_url'),
                    original_price=self._parse_float(result.get('original_price')),
                    target_sale_price=self._parse_float(result.get('target_sale_price')),
                    original_price_currency=result.get('original_price_currency'),
                    target_sale_price_currency=result.get('target_sale_price_currency'),
                    discount=result.get('discount'),
                    evaluate_rate=result.get('evaluate_rate'),
                    category_id=result.get('category_id'),
                    raw_json_detail=product_data
                )
                
                fetched_products.append(product_id)
                logger.debug(f"✓ Successfully fetched and saved product {product_id}")
                
            except Exception as e:
                logger.error(f"Error fetching product {product_id}: {e}")
                continue
        
        return fetched_products

    def _filter_products(self, product_ids: List[str], db) -> int:
        """
        Apply filtering rules to products and save qualifying ones to filtered_products.
        Also marks products as extracted in scraped_products table.
        
        Args:
            product_ids: List of product IDs to filter
            db: Database session
            
        Returns:
            Number of products that passed filters
        """
        from src.common.database import Product
        
        filtered_count = 0
        
        for product_id in product_ids:
            try:
                # Get product from database
                product = db.query(Product).filter(Product.product_id == product_id).first()
                
                if not product:
                    logger.warning(f"Product {product_id} not found in database")
                    # Still mark as extracted even if not found
                    self._mark_as_extracted(product_id, product.shop_id if product else None, db)
                    continue
                
                # Apply filtering rules using ProductFilterEngine
                filter_result = self.filter_engine._apply_filtering_rules(product, db)
                
                # If product passes all rules, create filtered product entry
                if filter_result['passed_price_rule'] and filter_result['passed_shipping_rule']:
                    self.filter_engine._create_filtered_product(product, filter_result, db)
                    filtered_count += 1
                    logger.debug(f"✓ Product {product_id} passed filters")
                else:
                    logger.debug(f"✗ Product {product_id} failed filters: price={filter_result['passed_price_rule']}, shipping={filter_result['passed_shipping_rule']}")
                
                # Mark product as extracted in scraped_products
                self._mark_as_extracted(product_id, product.shop_id, db)
                
            except Exception as e:
                logger.error(f"Error filtering product {product_id}: {e}")
                continue
        
        # Commit all changes
        db.commit()
        
        return filtered_count

    def _mark_as_extracted(self, product_id: str, seller_id: str, db) -> None:
        """
        Mark a product as extracted in the scraped_products table.
        
        Args:
            product_id: The product ID
            seller_id: The seller ID
            db: Database session
        """
        try:
            scraped_product = db.query(ScrapedProduct).filter(
                ScrapedProduct.product_id == product_id,
                ScrapedProduct.seller_id == seller_id
            ).first()
            
            if scraped_product:
                scraped_product.is_extracted = True
                scraped_product.extracted_at = get_utc_now()
                logger.debug(f"Marked product {product_id} as extracted")
            else:
                logger.warning(f"Product {product_id} not found in scraped_products table")
                
        except Exception as e:
            logger.error(f"Error marking product {product_id} as extracted: {e}")

    def _parse_float(self, value) -> Optional[float]:
        """Safely parse float value."""
        if value is None or value == "null":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


def run_scraper_based_filtering(seller_ids: Optional[List[str]] = None, limit: int = None) -> Dict:
    """
    Main entry point for scraper-based filtering.
    
    Args:
        seller_ids: List of specific seller IDs to process (if None, processes all whitelisted)
        limit: Maximum number of sellers to process
        
    Returns:
        Processing statistics
        
    Example:
        >>> stats = run_scraper_based_filtering(seller_ids=["2663214"])
        >>> print(f"Filtered {stats['total_products_filtered']} products")
    """
    logger.info("Starting scraper-based filtering workflow")
    
    scraper_filter = ScraperBasedFilter()
    stats = scraper_filter.process_sellers(seller_ids, limit)
    
    logger.info("Scraper-based filtering completed")
    
    return stats
