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
    
    def _load_csv_rows(self, csv_path: str) -> Tuple[List[str], List[Dict]]:
        """
        Load all rows from CSV file with their extracted product IDs.
        
        Args:
            csv_path: Path to CSV file with aliexpress_link column
            
        Returns:
            Tuple of (fieldnames, rows) where rows are list of dicts
        """
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = list(reader.fieldnames) if reader.fieldnames else []
                rows = list(reader)
            return fieldnames, rows
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_path}")
            return [], []
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return [], []
    
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
        Check availability for products from CSV file (standalone, CSV-only).
        
        This method does NOT interact with the database or Airtable.
        It reads the CSV, checks each product via the API, and writes
        the status back to the CSV file as an 'available'/'not_available' column.
        
        Args:
            csv_path: Path to CSV file with aliexpress_link column
            limit: Maximum number of products to check
            
        Returns:
            Dictionary with statistics
        """
        logger.info(f"Starting CSV-based availability check from {csv_path} (dry_run={self.dry_run})")
        
        fieldnames, rows = self._load_csv_rows(csv_path)
        
        if not rows:
            logger.warning("No rows found in CSV file")
            return self.stats
        
        # Ensure 'status' is in fieldnames for writing back
        if 'status' not in fieldnames:
            fieldnames.append('status')
        
        # Build list of (index, product_id) for rows with valid links
        check_list = []
        for idx, row in enumerate(rows):
            product_id = self._extract_product_id(row.get('aliexpress_link', ''))
            if product_id:
                check_list.append((idx, product_id))
        
        logger.info(f"Found {len(check_list)} products with valid links in CSV")
        
        if limit:
            check_list = check_list[:limit]
        
        total = len(check_list)
        logger.info(f"Checking availability for {total} products")
        
        for i, (row_idx, product_id) in enumerate(check_list, 1):
            title = (rows[row_idx].get('title', '') or product_id)[:50]
            logger.info(f"[{i}/{total}] Checking {product_id}: {title}...")
            
            try:
                api_status = self._check_availability_only(product_id)
                self.stats['products_checked'] += 1
                
                # Map to CSV column values
                if api_status == 'available':
                    csv_status = 'available'
                    self.stats['products_available'] += 1
                else:
                    csv_status = 'not_available'
                    if api_status == 'delisted':
                        self.stats['products_delisted'] += 1
                    else:
                        self.stats['products_unavailable'] += 1
                
                old_status = rows[row_idx].get('status', '')
                rows[row_idx]['status'] = csv_status
                
                if old_status and old_status != csv_status:
                    self.stats['products_updated'] += 1
                    logger.info(f"  -> Status: {old_status} -> {csv_status}")
                elif not old_status:
                    self.stats['products_updated'] += 1
                    logger.info(f"  -> Status: {csv_status} (new)")
                else:
                    logger.info(f"  -> Status: {csv_status} (unchanged)")
                    
            except Exception as e:
                logger.error(f"Error processing product {product_id}: {e}")
                self.stats['errors'] += 1
        
        # Write updated rows back to CSV
        if not self.dry_run:
            try:
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                logger.info(f"CSV file updated: {csv_path}")
            except Exception as e:
                logger.error(f"Error writing CSV file: {e}")
                self.stats['errors'] += 1
        
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
                
                # Sync only the checked products — price/stock/status fields only
                products_result = sync_engine.sync_products_by_ids(self.checked_product_ids, stock_update_only=True)
                
                # Sync variants for those products — price/stock fields only
                synced_product_ids = products_result.get('synced_product_ids', [])
                variants_result = sync_engine.sync_variants(synced_product_ids=synced_product_ids, stock_update_only=True)
                
                logger.info(f"Airtable sync completed: {products_result['updated']} products, {variants_result['updated']} variants updated")
            except Exception as e:
                logger.error(f"Error syncing to Airtable: {e}")
                self.stats['errors'] += 1
        
        self._log_summary()
        return dict(self.stats)
    
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
                logger.warning(f"No SKU information found for product {product.product_id}; marking existing variants unavailable")
                # Mark all existing DB variants as unavailable
                if not self.dry_run:
                    variants = db.query(ProductVariant).filter(
                        ProductVariant.product_id == product.product_id
                    ).all()
                    changed = 0
                    for variant in variants:
                        if variant.stock_status != 'unavailable':
                            variant.stock_status = 'unavailable'
                            variant.sku_available_stock = 0
                            variant.updated_at = datetime.now(timezone.utc)
                            changed += 1
                            self.stats['variants_checked'] += 1
                            self.stats['variants_updated'] += 1
                            self.stats['variants_out_of_stock'] += 1
                    if changed:
                        logger.info(f"Marked {changed} variants as unavailable for product {product.product_id} (no SKU info)")
                return

            logger.debug(f"Found {len(sku_list)} variants for product {product.product_id}")

            # Process each variant and collect API SKU IDs
            api_sku_ids = set()
            variants_updated = 0
            for sku_data in sku_list:
                sku_id = sku_data.get('sku_id')
                if not sku_id:
                    logger.debug(f"SKU entry without sku_id for product {product.product_id}: {sku_data}")
                    continue
                api_sku_ids.add(str(sku_id))
                if self._update_variant_stock(db, product.product_id, sku_data):
                    variants_updated += 1

            # Mark stale DB variants (not returned by API) as unavailable
            if not self.dry_run and api_sku_ids:
                stale_variants = db.query(ProductVariant).filter(
                    ProductVariant.product_id == product.product_id,
                    ~ProductVariant.sku_id.in_(api_sku_ids)
                ).all()
                stale_count = 0
                for sv in stale_variants:
                    if sv.stock_status != 'unavailable':
                        sv.stock_status = 'unavailable'
                        sv.sku_available_stock = 0
                        sv.updated_at = datetime.now(timezone.utc)
                        stale_count += 1
                        self.stats['variants_checked'] += 1
                        self.stats['variants_updated'] += 1
                        self.stats['variants_out_of_stock'] += 1
                if stale_count:
                    logger.info(f"Marked {stale_count} stale variants as unavailable for product {product.product_id} (SKUs changed)")

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
    import os

    if csv_path:
        # Explicit CSV mode: availability check only (no variant/price updates)
        checker = StockChecker(dry_run=dry_run)
        return checker.check_availability_from_csv(csv_path=csv_path, limit=limit)

    # Default behavior: perform original full Airtable-based stock check first
    checker = StockChecker(dry_run=dry_run)
    stats_full = checker.check_stock(limit=limit)

    # After full check, optionally run automatic CSV availability check
    run_csv = os.getenv('RUN_AUTOMATIC_CSV', 'false').lower() == 'true'
    if run_csv:
        csv_file = os.getenv('AUTOMATIC_CSV_FILE', 'automatic.csv')
        logger.info(f"Running automatic CSV availability check ({csv_file})...")
        try:
            csv_checker = StockChecker(dry_run=dry_run)
            csv_checker.check_availability_from_csv(csv_path=csv_file, limit=limit)
        except Exception as e:
            logger.error(f"Error in automatic CSV check: {e}")

    return stats_full
