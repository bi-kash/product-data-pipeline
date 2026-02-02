"""
Stock Checker Module

This module handles stock checking functionality for products and their variants.
It processes only products marked as "Online" and updates stock status based on
availability data from the AliExpress API.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.common.database import (
    get_db_session,
    FilteredProduct,
    ProductVariant,
)
from src.common.official_aliexpress_client import OfficialAliExpressClient
from src.airtable.sync import sync_to_airtable

logger = logging.getLogger(__name__)


class StockChecker:
    """Handles stock checking for online products and their variants."""
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize the stock checker.
        
        Args:
            dry_run: If True, performs checks without updating the database
        """
        self.dry_run = dry_run
        self.api_client = OfficialAliExpressClient()
        self.stats = {
            'products_checked': 0,
            'products_updated': 0,
            'variants_checked': 0,
            'variants_updated': 0,
            'variants_available': 0,
            'variants_out_of_stock': 0,
            'errors': 0,
        }
    
    def check_stock(self, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Check stock status for all products marked as "Online".
        
        This method:
        1. Queries all products with status="Online"
        2. For each product, calls the API to refresh variant data
        3. Updates local database with latest stock information
        4. Updates Airtable with the new data
        
        Args:
            limit: Maximum number of products to check (None for all)
        
        Returns:
            Dictionary with statistics about the stock check operation
        """
        logger.info(f"Starting stock check (dry_run={self.dry_run}, limit={limit})")
        
        with get_db_session() as db:
            # Get all products with status="Online"
            query = db.query(FilteredProduct).filter(
                FilteredProduct.status == 'Online'
            )
            
            if limit:
                query = query.limit(limit)
            
            online_products = query.all()
            total_products = len(online_products)
            
            logger.info(f"Found {total_products} products with status='Online'")
            
            if total_products == 0:
                logger.warning("No products found with status='Online'. Please set products to 'Online' status first.")
                return self.stats
            
            # Process each product
            for idx, product in enumerate(online_products, 1):
                logger.info(f"Processing product {idx}/{total_products}: {product.product_id}")
                try:
                    self._check_product_stock(db, product)
                    self.stats['products_checked'] += 1
                except Exception as e:
                    logger.error(f"Error checking stock for product {product.product_id}: {e}")
                    self.stats['errors'] += 1
                    continue
            
            # Commit all changes if not dry run
            if not self.dry_run:
                db.commit()
                logger.info("Database updated successfully")
            else:
                logger.info("Dry run mode - changes not committed")
        
        # Sync to Airtable if not dry run
        if not self.dry_run and self.stats['products_updated'] > 0:
            logger.info("Syncing updated data to Airtable...")
            try:
                sync_to_airtable()
                logger.info("Airtable sync completed")
            except Exception as e:
                logger.error(f"Error syncing to Airtable: {e}")
                self.stats['errors'] += 1
        
        self._log_summary()
        return self.stats
    
    def _check_product_stock(self, db: Session, product: FilteredProduct) -> None:
        """
        Check and update stock for a single product and its variants.
        
        Args:
            db: Database session
            product: FilteredProduct instance to check
        """
        try:
            # Call API to get fresh product data
            logger.debug(f"Calling API for product {product.product_id}")
            product_data = self.api_client.get_product_details(product.product_id)
            
            if not product_data or 'aliexpress_ds_product_get_response' not in product_data:
                logger.warning(f"No product data returned for {product.product_id}")
                return
            
            response = product_data['aliexpress_ds_product_get_response']
            result = response.get('result', {})
            
            if not result:
                logger.warning(f"Empty result in API response for {product.product_id}")
                return
            
            # Update product's raw_json_detail with fresh data
            if not self.dry_run:
                product.raw_json_detail = product_data
                product.last_seen_at = datetime.now(timezone.utc)
            
            # Extract and process variant data
            sku_info = result.get('ae_item_sku_info_dtos', {})
            sku_list = sku_info.get('ae_item_sku_info_d_t_o', [])
            
            if not sku_list:
                logger.warning(f"No SKU information found for product {product.product_id}")
                return
            
            logger.debug(f"Found {len(sku_list)} variants for product {product.product_id}")
            
            # Process each variant
            variants_updated = 0
            for sku_data in sku_list:
                if self._update_variant_stock(db, product.product_id, sku_data):
                    variants_updated += 1
            
            if variants_updated > 0:
                self.stats['products_updated'] += 1
            
            logger.info(f"Updated {variants_updated} variants for product {product.product_id}")
            
        except Exception as e:
            logger.error(f"Error in _check_product_stock for {product.product_id}: {e}")
            raise
    
    def _update_variant_stock(self, db: Session, product_id: str, sku_data: Dict) -> bool:
        """
        Update stock information for a single variant.
        
        Args:
            db: Database session
            product_id: Product ID
            sku_data: SKU data from API response
        
        Returns:
            True if variant was updated, False otherwise
        """
        try:
            sku_id = sku_data.get('sku_id')
            if not sku_id:
                logger.warning(f"SKU data missing sku_id: {sku_data}")
                return False
            
            sku_available_stock = sku_data.get('sku_available_stock')
            
            # Determine stock status
            stock_status = self._determine_stock_status(sku_available_stock)
            
            # Find or create variant
            variant = db.query(ProductVariant).filter(
                ProductVariant.sku_id == sku_id
            ).first()
            
            if variant:
                # Update existing variant
                old_stock = variant.sku_available_stock
                old_status = variant.stock_status
                
                if not self.dry_run:
                    variant.sku_available_stock = sku_available_stock
                    variant.stock_status = stock_status
                    variant.updated_at = datetime.now(timezone.utc)
                    variant.raw_variant_data = sku_data
                
                self.stats['variants_checked'] += 1
                
                # Track if there was a change
                if old_stock != sku_available_stock or old_status != stock_status:
                    self.stats['variants_updated'] += 1
                    logger.debug(
                        f"Variant {sku_id}: stock {old_stock} -> {sku_available_stock}, "
                        f"status {old_status} -> {stock_status}"
                    )
                
                # Update status counters
                if stock_status == 'available':
                    self.stats['variants_available'] += 1
                elif stock_status == 'out_of_stock':
                    self.stats['variants_out_of_stock'] += 1
                
                return True
            else:
                logger.debug(f"Variant {sku_id} not found in database - may need to run filter first")
                return False
                
        except Exception as e:
            logger.error(f"Error updating variant stock for sku_id {sku_data.get('sku_id')}: {e}")
            return False
    
    def _determine_stock_status(self, sku_available_stock: Optional[int]) -> str:
        """
        Determine stock status based on available stock quantity.
        
        Args:
            sku_available_stock: Available stock quantity from API
        
        Returns:
            Stock status string: 'available', 'out_of_stock', or 'unknown'
        """
        if sku_available_stock is None:
            return 'unknown'
        
        if sku_available_stock > 0:
            return 'available'
        else:
            return 'out_of_stock'
    
    def _log_summary(self) -> None:
        """Log summary statistics of the stock check operation."""
        logger.info("=" * 60)
        logger.info("Stock Check Summary")
        logger.info("=" * 60)
        logger.info(f"Products checked: {self.stats['products_checked']}")
        logger.info(f"Products updated: {self.stats['products_updated']}")
        logger.info(f"Variants checked: {self.stats['variants_checked']}")
        logger.info(f"Variants updated: {self.stats['variants_updated']}")
        logger.info(f"Variants available: {self.stats['variants_available']}")
        logger.info(f"Variants out of stock: {self.stats['variants_out_of_stock']}")
        if self.stats['errors'] > 0:
            logger.warning(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)


def run_stock_check(limit: Optional[int] = None, dry_run: bool = False) -> Dict[str, int]:
    """
    Run stock check for all Online products.
    
    This is the main entry point for the stock check functionality.
    
    Args:
        limit: Maximum number of products to check (None for all)
        dry_run: If True, performs checks without updating the database
    
    Returns:
        Dictionary with statistics about the operation
    """
    checker = StockChecker(dry_run=dry_run)
    return checker.check_stock(limit=limit)
