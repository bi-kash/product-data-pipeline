"""
Stock Checker Module

This module handles stock checking functionality for products and their variants.
It processes only products marked as "Online" and updates stock status based on
availability data from the AliExpress API.
Also supports CSV-based availability checking for products from automatic.csv."""

import csv
import re
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
from src.airtable.sync import sync_to_airtable, sync_status_from_airtable

logger = logging.getLogger(__name__)


class StockChecker:
    """Handles stock checking for online products and their variants."""
    
    # Regex pattern to extract product ID from AliExpress URL
    PRODUCT_ID_PATTERN = re.compile(r'/item/(\d+)\.html')
    
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
            'products_available': 0,
            'products_unavailable': 0,
            'products_delisted': 0,
            'variants_checked': 0,
            'variants_updated': 0,
            'variants_available': 0,
            'variants_out_of_stock': 0,
            'errors': 0,
        }
        self.unavailable_products = []  # Track products that became unavailable
        self.checked_product_ids = []  # Track product IDs that were checked
        self.airtable_client = None  # Lazy init for Airtable
    
    def _extract_product_id(self, aliexpress_link: str) -> Optional[str]:
        """Extract product ID from AliExpress URL."""
        if not aliexpress_link:
            return None
        match = self.PRODUCT_ID_PATTERN.search(aliexpress_link)
        return match.group(1) if match else None
    
    def _load_product_ids_from_csv(self, csv_path: str) -> List[str]:
        """
        Load product IDs from CSV file, filtering to only those in filtered_products.
        
        Args:
            csv_path: Path to CSV file with aliexpress_link column
            
        Returns:
            List of product IDs that exist in filtered_products table
        """
        csv_product_ids = set()
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    aliexpress_link = row.get('aliexpress_link', '')
                    product_id = self._extract_product_id(aliexpress_link)
                    if product_id:
                        csv_product_ids.add(product_id)
            
            logger.info(f"Found {len(csv_product_ids)} product IDs in CSV")
            
            # Filter to only products that exist in filtered_products
            with get_db_session() as db:
                existing_products = db.query(FilteredProduct.product_id).filter(
                    FilteredProduct.product_id.in_(list(csv_product_ids))
                ).all()
                existing_ids = [p.product_id for p in existing_products]
            
            logger.info(f"Found {len(existing_ids)} products in filtered_products table")
            return existing_ids
            
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_path}")
            return []
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return []
    
    def _check_availability_only(self, product_id: str) -> str:
        """
        Check availability for a single product (product-level only, no variants).
        
        Args:
            product_id: AliExpress product ID
            
        Returns:
            Status string: 'available', 'unavailable', or 'delisted'
        """
        try:
            product_data = self.api_client.get_product_details(product_id)
            
            if not product_data or 'aliexpress_ds_product_get_response' not in product_data:
                return 'unavailable'
            
            response = product_data['aliexpress_ds_product_get_response']
            result = response.get('result', {})
            
            if not result:
                rsp_code = response.get('rsp_code')
                rsp_msg = response.get('rsp_msg', '')
                
                if rsp_code:
                    logger.debug(f"API Error for {product_id}: Code {rsp_code} - {rsp_msg}")
                    if rsp_code == 604 or 'unsaleable' in rsp_msg.lower():
                        return 'delisted'
                return 'unavailable'
            
            return 'available'
            
        except Exception as e:
            logger.error(f"Error checking availability for {product_id}: {e}")
            return 'unavailable'
    
    def _batch_update_airtable_status(self, status_updates: List[Dict]) -> Dict:
        """
        Batch update product statuses in Airtable.
        
        Args:
            status_updates: List of {'product_id': str, 'status': str}
            
        Returns:
            Dict with update statistics
        """
        if self.dry_run or not status_updates:
            return {'updated': 0}
        
        try:
            from src.airtable.client import AirtableClient
            if not self.airtable_client:
                self.airtable_client = AirtableClient()
            
            # Generate anonymous IDs and prepare mapping
            anon_to_status = {}
            for update in status_updates:
                anon_id = self.airtable_client.generate_anonymous_id(update['product_id'])
                anon_to_status[anon_id] = update['status']
            
            # Get all existing records from Airtable
            all_products = self.airtable_client.products_table.all()
            
            # Find records to update
            records_to_update = []
            for record in all_products:
                anon_id = record['fields'].get('anon_product_id')
                if anon_id in anon_to_status:
                    records_to_update.append({
                        'id': record['id'],
                        'fields': {'status': anon_to_status[anon_id]}
                    })
            
            if records_to_update:
                self.airtable_client.products_table.batch_update(records_to_update)
                logger.info(f"Batch updated {len(records_to_update)} products in Airtable")
            
            return {'updated': len(records_to_update)}
            
        except Exception as e:
            logger.error(f"Error in batch Airtable update: {e}")
            return {'updated': 0, 'error': str(e)}
    
    def check_availability_from_csv(self, csv_path: str, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Check availability for products from CSV file (product-level only).
        
        Only checks products that exist in filtered_products table.
        No variant-level checks or price updates.
        
        Args:
            csv_path: Path to CSV file with aliexpress_link column
            limit: Maximum number of products to check
            
        Returns:
            Dictionary with statistics
        """
        logger.info(f"Starting CSV-based availability check from {csv_path} (dry_run={self.dry_run})")
        
        # Load product IDs that exist in filtered_products
        product_ids = self._load_product_ids_from_csv(csv_path)
        
        if not product_ids:
            logger.warning("No matching products found in filtered_products table")
            return self.stats
        
        if limit:
            product_ids = product_ids[:limit]
        
        total = len(product_ids)
        logger.info(f"Checking availability for {total} products")
        
        status_updates = []
        
        with get_db_session() as db:
            for idx, product_id in enumerate(product_ids, 1):
                product = db.query(FilteredProduct).filter(
                    FilteredProduct.product_id == product_id
                ).first()
                
                if not product:
                    continue
                
                title = (product.product_title or product_id)[:50]
                logger.info(f"[{idx}/{total}] Checking {product_id}: {title}...")
                
                try:
                    status = self._check_availability_only(product_id)
                    self.stats['products_checked'] += 1
                    self.checked_product_ids.append(product_id)
                    
                    # Update stats
                    if status == 'available':
                        self.stats['products_available'] += 1
                    elif status == 'delisted':
                        self.stats['products_delisted'] += 1
                    else:
                        self.stats['products_unavailable'] += 1
                    
                    # Update local database
                    if not self.dry_run:
                        old_status = product.status
                        product.status = status
                        product.updated_at = datetime.now(timezone.utc)
                        if old_status != status:
                            self.stats['products_updated'] += 1
                            logger.info(f"  -> Status: {old_status} -> {status}")
                        else:
                            logger.info(f"  -> Status: {status} (unchanged)")
                    else:
                        logger.info(f"  -> Status: {status}")
                    
                    status_updates.append({'product_id': product_id, 'status': status})
                    
                except Exception as e:
                    logger.error(f"Error processing product {product_id}: {e}")
                    self.stats['errors'] += 1
            
            if not self.dry_run:
                db.commit()
                logger.info("Database updated successfully")
        
        # Batch update Airtable
        if status_updates and not self.dry_run:
            logger.info(f"Batch updating {len(status_updates)} products in Airtable...")
            airtable_result = self._batch_update_airtable_status(status_updates)
            logger.info(f"Airtable update: {airtable_result.get('updated', 0)} updated")
        
        self._log_csv_summary()
        return self.stats
    
    def _log_csv_summary(self):
        """Log summary for CSV-based availability check."""
        logger.info("=" * 60)
        logger.info("CSV Availability Check Summary")
        logger.info("=" * 60)
        logger.info(f"Products checked:   {self.stats['products_checked']}")
        logger.info(f"  Available:        {self.stats['products_available']}")
        logger.info(f"  Unavailable:      {self.stats['products_unavailable']}")
        logger.info(f"  Delisted:         {self.stats['products_delisted']}")
        logger.info(f"Products updated:   {self.stats['products_updated']}")
        if self.stats['errors'] > 0:
            logger.warning(f"Errors:             {self.stats['errors']}")
        logger.info("=" * 60)
    
    def check_stock(self, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Check stock status for all products marked as "Online".
        
        This method:
        1. Syncs status from Airtable to database
        2. Queries all products with status="Online"
        3. For each product, calls the API to refresh variant data
        4. Updates local database with latest stock information
        5. Updates Airtable with the new data
        
        Args:
            limit: Maximum number of products to check (None for all)
        
        Returns:
            Dictionary with statistics about the stock check operation
        """
        logger.info(f"Starting stock check (dry_run={self.dry_run}, limit={limit})")
        
        # Step 1: Sync status from Airtable to database
        logger.info("Syncing product status from Airtable to database...")
        try:
            status_sync_result = sync_status_from_airtable(dry_run=self.dry_run)
            logger.info(f"Status sync completed: {status_sync_result['products_updated']} products updated")
        except Exception as e:
            logger.error(f"Error syncing status from Airtable: {e}")
            logger.warning("Continuing with stock check despite status sync error...")
        
        with get_db_session() as db:
            # Get all products with status="Online" (case-insensitive)
            from sqlalchemy import func
            query = db.query(FilteredProduct).filter(
                func.lower(FilteredProduct.status) == 'online'
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
                    self.checked_product_ids.append(product.product_id)
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
        
        # Sync to Airtable if not dry run and there were any changes
        if not self.dry_run and (self.stats['products_updated'] > 0 or self.stats['products_delisted'] > 0):
            logger.info(f"Syncing {len(self.checked_product_ids)} checked products to Airtable...")
            try:
                # Only sync products that were actually checked, not all products in DB
                from src.airtable.sync import AirtableDataSync
                sync_engine = AirtableDataSync(dry_run=False)
                
                # Sync only the checked products
                products_result = sync_engine.sync_products_by_ids(self.checked_product_ids)
                
                # Sync variants for those products
                synced_product_ids = products_result.get('synced_product_ids', [])
                variants_result = sync_engine.sync_variants(synced_product_ids=synced_product_ids)
                
                logger.info(f"Airtable sync completed: {products_result['updated']} products, {variants_result['updated']} variants updated")
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
                # Check if it's an error response
                rsp_code = response.get('rsp_code')
                rsp_msg = response.get('rsp_msg', '')
                
                logger.warning(f"Empty result in API response for {product.product_id}")
                
                if rsp_code:
                    logger.warning(f"API Error - Code: {rsp_code}, Message: {rsp_msg}")
                    
                    # Track this as unavailable
                    self.stats['products_unavailable'] += 1
                    self.unavailable_products.append({
                        'product_id': product.product_id,
                        'title': product.product_title,
                        'error_code': rsp_code,
                        'error_message': rsp_msg
                    })
                    
                    # Mark all variants as unavailable and update product status to delisted if not dry run
                    if not self.dry_run:
                        # Update product status to delisted in local database
                        old_status = product.status
                        product.status = 'delisted'
                        product.updated_at = datetime.now(timezone.utc)
                        self.stats['products_delisted'] += 1
                        logger.info(f"Updated product {product.product_id} status from '{old_status}' to 'delisted'")
                        
                        # Mark all variants as unavailable
                        variants = db.query(ProductVariant).filter(
                            ProductVariant.product_id == product.product_id
                        ).all()
                        
                        for variant in variants:
                            variant.stock_status = 'unavailable'
                            variant.sku_available_stock = 0
                            variant.updated_at = datetime.now(timezone.utc)
                        
                        logger.info(f"Marked {len(variants)} variants as unavailable for product {product.product_id}")
                
                return
            
            # Update product's raw_json_detail and price information with fresh data
            if not self.dry_run:
                product.raw_json_detail = product_data
                product.last_seen_at = datetime.now(timezone.utc)
                
                # Extract and update price information at product level
                old_price = product.target_sale_price
                new_price = result.get('target_sale_price')
                
                if new_price is not None:
                    product.target_sale_price = float(new_price)
                    
                    # Also update currency if available
                    currency = result.get('target_sale_price_currency')
                    if currency:
                        product.target_sale_price_currency = currency
                    
                    # Log price change if detected
                    if old_price != new_price:
                        logger.info(
                            f"Product {product.product_id} price changed: "
                            f"{old_price} -> {new_price}"
                        )
            
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
        Update stock and price information for a single variant.
        
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
            offer_sale_price = sku_data.get('offer_sale_price')
            
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
                old_price = variant.offer_sale_price
                
                if not self.dry_run:
                    variant.sku_available_stock = sku_available_stock
                    variant.stock_status = stock_status
                    
                    # Update price if available in API response
                    if offer_sale_price is not None:
                        variant.offer_sale_price = float(offer_sale_price)
                    
                    variant.updated_at = datetime.now(timezone.utc)
                    variant.raw_variant_data = sku_data
                
                self.stats['variants_checked'] += 1
                
                # Track if there was a change
                price_changed = old_price != offer_sale_price if offer_sale_price is not None else False
                if old_stock != sku_available_stock or old_status != stock_status or price_changed:
                    self.stats['variants_updated'] += 1
                    logger.debug(
                        f"Variant {sku_id}: stock {old_stock} -> {sku_available_stock}, "
                        f"status {old_status} -> {stock_status}"
                        f"{f', price {old_price} -> {offer_sale_price}' if price_changed else ''}"
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
        
        if sku_available_stock < 0:
            # Negative stock indicates a data error - log it but treat as out of stock
            logger.warning(f"Negative stock value detected: {sku_available_stock}. Treating as out_of_stock.")
            return 'out_of_stock'
        
        if sku_available_stock > 0:
            return 'available'
        else:  # sku_available_stock == 0
            return 'out_of_stock'
    
    def _log_summary(self) -> None:
        """Log summary statistics of the stock check operation."""
        logger.info("=" * 60)
        logger.info("Stock Check Summary")
        logger.info("=" * 60)
        logger.info(f"Products checked: {self.stats['products_checked']}")
        logger.info(f"Products updated: {self.stats['products_updated']}")
        logger.info(f"Products unavailable: {self.stats['products_unavailable']}")
        logger.info(f"Products delisted: {self.stats['products_delisted']}")
        logger.info(f"Variants checked: {self.stats['variants_checked']}")
        logger.info(f"Variants updated: {self.stats['variants_updated']}")
        logger.info(f"Variants available: {self.stats['variants_available']}")
        logger.info(f"Variants out of stock: {self.stats['variants_out_of_stock']}")
        if self.stats['errors'] > 0:
            logger.warning(f"Errors encountered: {self.stats['errors']}")
        
        # Log unavailable products
        if self.unavailable_products:
            logger.warning("=" * 60)
            logger.warning("Unavailable Products (status updated to 'delisted'):")
            logger.warning("=" * 60)
            for prod in self.unavailable_products:
                logger.warning(f"  • {prod['product_id']}: {prod['title'][:50]}...")
                logger.warning(f"    Error: {prod['error_code']} - {prod['error_message']}")
        
        logger.info("=" * 60)
        logger.info("Stock Check Summary")
        logger.info("=" * 60)
        logger.info(f"Products checked: {self.stats['products_checked']}")
        logger.info(f"Products updated: {self.stats['products_updated']}")
        logger.info(f"Products delisted: {self.stats['products_delisted']}")
        logger.info(f"Variants checked: {self.stats['variants_checked']}")
        logger.info(f"Variants updated: {self.stats['variants_updated']}")
        logger.info(f"Variants available: {self.stats['variants_available']}")
        logger.info(f"Variants out of stock: {self.stats['variants_out_of_stock']}")
        if self.stats['errors'] > 0:
            logger.warning(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)


def run_stock_check(limit: Optional[int] = None, dry_run: bool = False, csv_path: Optional[str] = None) -> Dict[str, int]:
    """
    Run stock check for products.
    
    This is the main entry point for the stock check functionality.
    
    Args:
        limit: Maximum number of products to check (None for all)
        dry_run: If True, performs checks without updating the database
        csv_path: If provided, check availability for products from CSV file
                  (product-level only, no variant/price updates).
                  If None, check stock for all Online products (full check).
    
    Returns:
        Dictionary with statistics about the operation
    """
    checker = StockChecker(dry_run=dry_run)
    
    if csv_path:
        # CSV mode: availability check only (no variant/price updates)
        return checker.check_availability_from_csv(csv_path=csv_path, limit=limit)
    else:
        # Normal mode: full stock check for Online products
        return checker.check_stock(limit=limit)
