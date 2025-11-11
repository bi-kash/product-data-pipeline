"""
Product Filter Engine - Business Rules Implementation
====================================================

This module implements the core business rules for filtering products from whitelisted sellers.

Business Rules:
1. Price Rule: (Most expensive variant + cheapest shipping) <= TOTAL_MAX_PRICE
2. Shipping Rule: delivery_time <= MAX_DELIVERY_DAYS
3. Shipping Badge Detection: Identifies shipping types from API data

The module processes products from whitelisted sellers and stores qualifying
products in the filtered_products table.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from src.common.database import (
    Product, 
    Seller, 
    FilteredProduct,
    ProductVariant,
    ShippingInfo,
    ScrapedProduct,
    get_db_session,
    get_utc_now
)
from src.common.config import get_env
from src.common.official_aliexpress_client import OfficialAliExpressClient

# Configure logging
logger = logging.getLogger(__name__)


class ProductFilterEngine:
    """Main engine for filtering products based on business rules."""
    
    def __init__(self):
        """
        Initialize the filter engine.
        
        Configuration values are read from environment variables:
        - TOTAL_MAX_PRICE: Maximum total cost in EUR (variant + shipping)
        - MAX_DELIVERY_DAYS: Maximum delivery time in days
        """
        self.max_price_eur = float(get_env("TOTAL_MAX_PRICE", "100"))
        self.max_delivery_days = int(get_env("MAX_DELIVERY_DAYS", "8"))
        
        # Initialize API client for enrichment
        try:
            self.api_client = OfficialAliExpressClient()
            logger.info("API client initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize API client: {e}")
            self.api_client = None
        
        logger.info(f"Filter engine initialized: max_price_eur={self.max_price_eur}, max_delivery_days={self.max_delivery_days}")

    def process_whitelisted_products(self, limit: int = None, dry_run: bool = False) -> Dict[str, int]:
        """
        Process products from scraped_products table through complete filtering workflow.
        Each product is processed completely (fetch → filter → images → shipping → videos) before moving to next.
        
        Args:
            limit: Maximum number of SUCCESSFUL products to process (failed products don't count)
            dry_run: If True, don't save to database
            
        Returns:
            Dict with processing statistics
        """
        stats = {
            'products_processed': 0,
            'products_passed_filter': 0,
            'products_failed_price_rule': 0,
            'products_failed_shipping_rule': 0,
            'products_api_enriched': 0,
            'images_extracted': 0,
            'videos_extracted': 0,
            'shipping_info_fetched': 0,
            'errors': 0
        }
        
        db = get_db_session()
        
        try:
            # Process products in batches until we reach the desired number of successful products
            # Failed products don't count toward the limit
            batch_size = 50  # Fetch products in batches
            offset = 0
            
            while limit is None or stats['products_passed_filter'] < limit:
                # Get next batch of products to process
                batch = self._get_unextracted_scraped_products_ids(db, batch_size, offset)
                if not batch:
                    logger.info(f"No more unextracted products available")
                    break
                
                if offset == 0:  # Only log on first batch
                    logger.info(f"Processing unextracted products from scraped_products table (target: {limit if limit else 'all'})")
                
                for scraped_product in batch:
                    product_id = scraped_product['product_id']
                    seller_id = scraped_product['seller_id']
                    
                    try:
                        stats['products_processed'] += 1
                        logger.info(f"\n{'='*80}")
                        logger.info(f"Processing product {product_id} from seller {seller_id} (Processed: {stats['products_processed']}, Passed: {stats['products_passed_filter']}/{limit if limit else '∞'})")
                        logger.info(f"{'='*80}")
                        
                        # Process this product completely through all stages
                        product_result = self._process_single_product(product_id, seller_id, db, dry_run)
                        
                        # Update stats
                        if product_result['passed_filter']:
                            stats['products_passed_filter'] += 1
                        if product_result.get('failed_price_rule'):
                            stats['products_failed_price_rule'] += 1
                        if product_result.get('failed_shipping_rule'):
                            stats['products_failed_shipping_rule'] += 1
                        if product_result.get('api_enriched'):
                            stats['products_api_enriched'] += 1
                        if product_result.get('images_extracted'):
                            stats['images_extracted'] += product_result['images_extracted']
                        if product_result.get('videos_extracted'):
                            stats['videos_extracted'] += product_result['videos_extracted']
                        if product_result.get('shipping_info_fetched'):
                            stats['shipping_info_fetched'] += 1
                        
                        # Mark products as extracted or failed (unless dry run)
                        if not dry_run:
                            if product_result['passed_filter']:
                                self._mark_product_as_extracted(product_id, seller_id, db)
                                db.commit()
                                logger.info(f"✅ Product {product_id} marked as extracted")
                            else:
                                # Mark as failed if it didn't pass filters
                                self._mark_product_as_failed(product_id, seller_id, db)
                                db.commit()
                                logger.info(f"❌ Product {product_id} marked as failed")
                        
                        # Check if we've reached our target of successful products
                        if limit and stats['products_passed_filter'] >= limit:
                            logger.info(f"✅ Reached target of {limit} successful products")
                            break
                        
                    except Exception as e:
                        logger.error(f"❌ Error processing product {product_id}: {e}")
                        stats['errors'] += 1
                        # Don't mark as extracted if there was an error
                        continue
                
                # If we've reached our limit, exit the outer loop
                if limit and stats['products_passed_filter'] >= limit:
                    break
                
                # Move to next batch
                offset += batch_size
            
        except Exception as e:
            logger.error(f"Error in filter processing: {e}")
            raise
        finally:
            db.close()
        
        return stats

    def _get_unextracted_scraped_products_ids(self, db, limit: int = None, offset: int = 0) -> List[Dict]:
        """
        Get product IDs from scraped_products table where is_extracted=False and is_failed=False.
        Only includes products from whitelisted sellers that haven't been processed yet.
        """
        query = db.query(
            ScrapedProduct.product_id,
            ScrapedProduct.seller_id
        ).join(
            Seller, ScrapedProduct.seller_id == Seller.shop_id
        ).outerjoin(
            FilteredProduct, ScrapedProduct.product_id == FilteredProduct.product_id
        ).filter(
            ScrapedProduct.is_extracted == False,
            ScrapedProduct.is_failed == False,  # Skip products that already failed
            Seller.approval_status == 'WHITELIST',
            FilteredProduct.product_id.is_(None)
        )
        
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        
        results = query.all()
        return [{'product_id': r.product_id, 'seller_id': r.seller_id} for r in results]

    def _process_single_product(self, product_id: str, seller_id: str, db, dry_run: bool) -> Dict:
        """
        Process a single product through complete workflow:
        1. Fetch from API
        2. Apply filters
        3. If passes: populate filtered_products + variants + images + shipping + videos
        
        Args:
            product_id: Product ID to process
            seller_id: Seller ID
            db: Database session
            dry_run: If True, don't save to database
            
        Returns:
            Dict with processing results
        """
        result = {
            'passed_filter': False,
            'failed_price_rule': False,
            'failed_shipping_rule': False,
            'api_enriched': False,
            'images_extracted': 0,
            'videos_extracted': 0,
            'shipping_info_fetched': False
        }
        
        try:
            # Step 1: Fetch product details from API
            logger.info(f"Step 1/5: Fetching product details from API")
            product_data = self.api_client.get_product_details(product_id)
            
            if not product_data:
                logger.warning(f"No data returned for product {product_id}")
                return result
            
            result['api_enriched'] = True
            
            # Extract product information from API response
            response = product_data.get('aliexpress_ds_product_get_response', {})
            api_result = response.get('result', {})
            
            # Step 2: Apply filtering rules using API data
            logger.info(f"Step 2/5: Applying filtering rules")
            filter_result = self._apply_filtering_rules_from_api(product_id, seller_id, product_data, db)
            
            result['failed_price_rule'] = not filter_result['passed_price_rule']
            result['failed_shipping_rule'] = not filter_result['passed_shipping_rule']
            
            # If product doesn't pass filters, stop here
            if not (filter_result['passed_price_rule'] and filter_result['passed_shipping_rule']):
                logger.info(f"❌ Product {product_id} failed filters: price={filter_result['passed_price_rule']}, shipping={filter_result['passed_shipping_rule']}")
                return result
            
            logger.info(f"✅ Product {product_id} passed all filters")
            
            if dry_run:
                result['passed_filter'] = True
                return result
            
            # Step 3: Create filtered_product entry with all fields populated
            logger.info(f"Step 3/5: Creating filtered_product entry")
            filtered_product = self._create_filtered_product_from_api(
                product_id, seller_id, api_result, product_data, filter_result, db
            )
            
            # Step 4: Extract and save product images
            logger.info(f"Step 4/5: Extracting product images")
            images_count = self._extract_and_save_images(product_id, product_data, db)
            result['images_extracted'] = images_count
            logger.info(f"✓ Extracted {images_count} images")
            
            # Step 5: Extract and save product videos
            logger.info(f"Step 5/5: Extracting product videos")
            videos_count = self._extract_and_save_videos(product_id, product_data, db)
            result['videos_extracted'] = videos_count
            logger.info(f"✓ Extracted {videos_count} videos")
            
            # Step 6: Fetch and save shipping information
            logger.info(f"Step 6/5: Fetching shipping information")
            shipping_saved = self._fetch_and_save_shipping_info_single(product_id, product_data, filtered_product, db)
            result['shipping_info_fetched'] = shipping_saved
            
            # Commit all changes for this product
            db.commit()
            
            result['passed_filter'] = True
            logger.info(f"✅ Successfully completed all steps for product {product_id}")
            
        except Exception as e:
            logger.error(f"Error in _process_single_product for {product_id}: {e}")
            db.rollback()
            raise
        
        return result

    def _mark_product_as_extracted(self, product_id: str, seller_id: str, db) -> None:
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
                logger.debug(f"Product {product_id} not found in scraped_products table (may not have been scraped)")
                
        except Exception as e:
            logger.error(f"Error marking product {product_id} as extracted: {e}")

    def _mark_product_as_failed(self, product_id: str, seller_id: str, db) -> None:
        """
        Mark a product as failed in the scraped_products table.
        
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
                scraped_product.is_failed = True
                scraped_product.failed_at = get_utc_now()
                logger.debug(f"Marked product {product_id} as failed")
            else:
                logger.debug(f"Product {product_id} not found in scraped_products table (may not have been scraped)")
                
        except Exception as e:
            logger.error(f"Error marking product {product_id} as failed: {e}")

    def _apply_filtering_rules_from_api(self, product_id: str, seller_id: str, product_data: Dict, db) -> Dict:
        """
        Apply filtering rules directly from API data (without needing Product object).
        
        Returns:
            Dict with filtering results
        """
        result = {
            'passed_price_rule': False,
            'passed_shipping_rule': False,
            'max_variant_price': None,
            'min_shipping_cost': 0.0,
            'total_cost': None,
            'delivery_time': None,
            'ship_to_country': None,
        }
        
        try:
            # Extract max variant price from API data
            max_variant_price = self._extract_max_variant_price(None, product_data, product_id)
            
            if max_variant_price is not None:
                # For now, assume 0 shipping cost (will be updated when we fetch shipping info)
                total_cost = max_variant_price
                
                result.update({
                    'max_variant_price': max_variant_price,
                    'min_shipping_cost': 0.0,
                    'total_cost': total_cost,
                    'passed_price_rule': total_cost <= self.max_price_eur
                })
                
                logger.info(f"💰 Price check for {product_id}: max_variant=€{max_variant_price:.2f}, total=€{total_cost:.2f}, limit=€{self.max_price_eur:.2f}, passed={result['passed_price_rule']}")
            else:
                logger.warning(f"⚠️  Could not extract variant price for {product_id} - no SKU pricing found")
            
            # Extract delivery time and apply shipping rule
            delivery_time = self._extract_delivery_time(product_data)
            ship_to_country = self._extract_ship_to_country(product_data)
            
            result.update({
                'delivery_time': delivery_time,
                'ship_to_country': ship_to_country
            })
            
            if delivery_time is not None:
                result['passed_shipping_rule'] = delivery_time <= self.max_delivery_days
                logger.info(f"📦 Shipping check for {product_id}: delivery_time={delivery_time} days, limit={self.max_delivery_days} days, passed={result['passed_shipping_rule']}")
            else:
                # If no delivery time data, pass the rule
                result['passed_shipping_rule'] = True
                logger.info(f"📦 Shipping check for {product_id}: no delivery time data, passing by default")
                
        except Exception as e:
            logger.error(f"Error applying filtering rules for {product_id}: {e}")
        
        return result

    def _create_filtered_product_from_api(self, product_id: str, seller_id: str, api_result: Dict, 
                                         product_data: Dict, filter_result: Dict, db) -> FilteredProduct:
        """
        Create filtered_product entry with ALL fields populated from API response.
        
        Args:
            product_id: Product ID
            seller_id: Seller ID
            api_result: Result object from API response
            product_data: Full API response
            filter_result: Filtering results
            db: Database session
            
        Returns:
            FilteredProduct object
        """
        from src.common.database import ProductVariant
        
        now = get_utc_now()
        
        # Extract nested DTOs
        base_info = api_result.get('ae_item_base_info_dto', {})
        multimedia = api_result.get('ae_multimedia_info_dto', {})
        sku_info = api_result.get('ae_item_sku_info_dtos', {})
        skus = sku_info.get('ae_item_sku_info_d_t_o', [])
        
        # Get price of first SKU and currency from SKU level (reflects API target_currency parameter)
        first_sku_price = None
        target_currency = None
        if skus:
            first_sku = skus[0]
            # Try offer_sale_price first, then offer_bulk_sale_price
            first_sku_price = self._parse_float(first_sku.get('offer_sale_price') or first_sku.get('offer_bulk_sale_price'))
            target_currency = first_sku.get('currency_code')
        
        # Fallback to base_info currency if SKU currency not available
        if not target_currency:
            target_currency = base_info.get('currency_code')
        
        # Extract main image URL (first from image_urls)
        image_urls_str = multimedia.get('image_urls', '')
        main_image_url = image_urls_str.split(';')[0] if image_urls_str else None
        
        # Build product detail URL
        detail_url = f"https://www.aliexpress.com/item/{product_id}.html"
        
        # Extract video URL if available (from ae_video_dtos.ae_video_d_t_o[0].media_url)
        video_url = None
        video_dtos = multimedia.get('ae_video_dtos', {})
        video_list = video_dtos.get('ae_video_d_t_o', [])
        if video_list and isinstance(video_list, list) and len(video_list) > 0:
            video_url = video_list[0].get('media_url')
        
        # Create filtered product with all fields from API
        filtered_product = FilteredProduct(
            product_id=product_id,
            shop_id=seller_id,
            
            # Product details from nested DTOs
            product_title=base_info.get('subject'),
            product_detail_url=detail_url,
            product_main_image_url=main_image_url,
            product_video_url=video_url,
            
            # Pricing - simplified to just price and currency
            price=first_sku_price,  # Price of first SKU
            max_variant_price=self._parse_float(filter_result.get('max_variant_price')),
            currency=target_currency,  # Extracted from SKU level (EUR if requested)
            discount=None,  # Not directly available
            
            # Rating and category
            evaluate_rate=str(base_info.get('avg_evaluation_rating', 0.0)),
            category_id=str(base_info.get('category_id')),
            
            # Timestamps
            first_seen_at=now,
            last_seen_at=now,
            
            # API data
            raw_json_detail=product_data,
            
            # Filter results
            ship_to_country=filter_result.get('ship_to_country'),
            delivery_time=filter_result.get('delivery_time'),
            min_shipping_price=None,  # Will be updated after shipping info fetch
            min_delivery_days=None,
            max_delivery_days=None,
            
            # Status
            is_active=True
        )
        
        db.add(filtered_product)
        db.flush()  # Ensure it's in DB before adding related records
        
        # Extract and store product variants
        self._extract_and_store_variants_from_api(product_id, product_data, db)
        
        logger.info(f"✓ Created filtered_product entry for {product_id}")
        return filtered_product

    def _extract_and_store_variants_from_api(self, product_id: str, product_data: Dict, db):
        """Extract and store product variants from API data."""
        from src.common.database import ProductVariant
        
        try:
            result = product_data.get('aliexpress_ds_product_get_response', {}).get('result', {})
            sku_info = result.get('ae_item_sku_info_dtos', {})
            
            if not sku_info or 'ae_item_sku_info_d_t_o' not in sku_info:
                logger.info(f"No SKU info found for product {product_id}")
                return
            
            skus = sku_info['ae_item_sku_info_d_t_o']
            logger.info(f"Found {len(skus)} variants for product {product_id}")
            
            for sku in skus:
                sku_id = sku.get('sku_id')
                if not sku_id:
                    continue
                
                # Check if variant already exists
                existing = db.query(ProductVariant).filter(ProductVariant.sku_id == sku_id).first()
                if existing:
                    continue
                
                # Extract properties
                properties = []
                variant_key_parts = []
                
                if 'ae_sku_property_dtos' in sku:
                    props = sku['ae_sku_property_dtos'].get('ae_sku_property_d_t_o', [])
                    for prop in props:
                        prop_name = prop.get('sku_property_name')
                        prop_value = prop.get('sku_property_value')
                        
                        if prop_name and prop_value:
                            properties.append({
                                'name': prop_name,
                                'value': prop_value,
                                'property_id': str(prop.get('sku_property_id')) if prop.get('sku_property_id') else None,
                                'value_id': str(prop.get('property_value_id')) if prop.get('property_value_id') else None,
                                'definition_name': prop.get('property_value_definition_name')
                            })
                            
                            display_value = prop.get('property_value_definition_name') or prop_value
                            variant_key_parts.append(f"{prop_name}: {display_value}")
                
                variant_key = " + ".join(sorted(variant_key_parts)) if variant_key_parts else None
                
                # Get SKU image
                sku_image_url = None
                if 'ae_sku_property_dtos' in sku:
                    props = sku['ae_sku_property_dtos'].get('ae_sku_property_d_t_o', [])
                    if props:
                        sku_image_url = props[0].get('sku_image')
                
                variant = ProductVariant(
                    product_id=product_id,
                    sku_id=sku_id,
                    sku_attr=sku.get('sku_attr'),
                    offer_sale_price=self._parse_float(sku.get('offer_sale_price')),
                    currency_code=sku.get('currency_code'),
                    sku_available_stock=self._parse_int(sku.get('sku_available_stock')),
                    properties=properties,
                    variant_key=variant_key,
                    property_value_definition_name=properties[0].get('definition_name') if properties else None,
                    sku_image_url=sku_image_url,
                    raw_variant_data=sku
                )
                
                db.add(variant)
                logger.debug(f"Added variant {sku_id} for product {product_id}")
            
            logger.info(f"✓ Processed variants for product {product_id}")
            
        except Exception as e:
            logger.error(f"Error extracting variants for {product_id}: {e}")

    def _extract_and_save_images(self, product_id: str, product_data: Dict, db) -> int:
        """
        Extract and save product images to product_images table.
        Downloads images, calculates phash/dimensions, and uploads to S3.
        Extracts: hero image, gallery images, variant images, and description images.
        
        Returns:
            Number of images extracted
        """
        from src.common.database import ProductImage
        from src.ingestion.image_download import ImageDownloader
        from src.ingestion.s3_image_uploader import S3ImageUploader
        import re
        
        images_saved = 0
        sort_index = 0  # Maintain continuous sort_index across all image types
        
        # Initialize downloaders
        image_downloader = ImageDownloader()
        s3_uploader = S3ImageUploader()
        
        try:
            result = product_data.get('aliexpress_ds_product_get_response', {}).get('result', {})
            multimedia = result.get('ae_multimedia_info_dto', {})
            base_info = result.get('ae_item_base_info_dto', {})
            
            # Extract images from ae_multimedia_info_dto.image_urls
            image_urls_str = multimedia.get('image_urls', '')
            gallery_images = image_urls_str.split(';') if image_urls_str else []
            
            # First image is hero/main image
            if gallery_images and gallery_images[0]:
                image_url = gallery_images[0]
                
                # Download image (will skip if already exists)
                local_path, phash, status, width, height = image_downloader.download_image(
                    image_url, product_id, image_role='hero'
                )
                
                # Upload to S3 (will skip if s3_url already exists)
                s3_url = None
                if local_path and status == 'downloaded':
                    # Check if already uploaded by looking for existing record
                    existing = db.query(ProductImage).filter(
                        ProductImage.product_id == product_id,
                        ProductImage.image_url == image_url
                    ).first()
                    
                    if existing and existing.s3_url:
                        s3_url = existing.s3_url
                    else:
                        s3_url = s3_uploader.upload_image(local_path, product_id, 'hero')
                
                image = ProductImage(
                    product_id=product_id,
                    image_url=image_url,
                    image_role='hero',
                    sort_index=sort_index,
                    is_primary=True,
                    local_file_path=local_path,
                    phash=phash,
                    download_status=status,
                    width=width,
                    height=height,
                    s3_url=s3_url
                )
                db.add(image)
                images_saved += 1
                sort_index += 1
            
            # Remaining images are gallery images
            for image_url in gallery_images[1:]:
                if image_url:
                    local_path, phash, status, width, height = image_downloader.download_image(
                        image_url, product_id, image_role='gallery'
                    )
                    
                    s3_url = None
                    if local_path and status == 'downloaded':
                        existing = db.query(ProductImage).filter(
                            ProductImage.product_id == product_id,
                            ProductImage.image_url == image_url
                        ).first()
                        
                        if existing and existing.s3_url:
                            s3_url = existing.s3_url
                        else:
                            s3_url = s3_uploader.upload_image(local_path, product_id, 'gallery')
                    
                    image = ProductImage(
                        product_id=product_id,
                        image_url=image_url,
                        image_role='gallery',
                        sort_index=sort_index,
                        local_file_path=local_path,
                        phash=phash,
                        download_status=status,
                        width=width,
                        height=height,
                        s3_url=s3_url
                    )
                    db.add(image)
                    images_saved += 1
                    sort_index += 1
            
            # Extract variant images from SKU info
            sku_info = result.get('ae_item_sku_info_dtos', {})
            if sku_info and 'ae_item_sku_info_d_t_o' in sku_info:
                skus = sku_info['ae_item_sku_info_d_t_o']
                for sku in skus:
                    sku_id = sku.get('sku_id')
                    if 'ae_sku_property_dtos' in sku:
                        props = sku['ae_sku_property_dtos'].get('ae_sku_property_d_t_o', [])
                        for prop in props:
                            sku_image = prop.get('sku_image')
                            if sku_image:
                                local_path, phash, status, width, height = image_downloader.download_image(
                                    sku_image, product_id, sku_id, image_role='variant'
                                )
                                
                                s3_url = None
                                if local_path and status == 'downloaded':
                                    existing = db.query(ProductImage).filter(
                                        ProductImage.product_id == product_id,
                                        ProductImage.image_url == sku_image
                                    ).first()
                                    
                                    if existing and existing.s3_url:
                                        s3_url = existing.s3_url
                                    else:
                                        s3_url = s3_uploader.upload_image(local_path, product_id, 'variant')
                                
                                # Build variant_key: property_name:property_value_definition_name or property_name:property_value
                                prop_name = prop.get('sku_property_name')
                                prop_value_def = prop.get('property_value_definition_name')
                                prop_value = prop.get('sku_property_value')
                                variant_key = None
                                if prop_name:
                                    if prop_value_def:
                                        variant_key = f"{prop_name}:{prop_value_def}"
                                    elif prop_value:
                                        variant_key = f"{prop_name}:{prop_value}"
                                
                                image = ProductImage(
                                    product_id=product_id,
                                    image_url=sku_image,
                                    image_role='variant',
                                    sku_id=sku_id,
                                    variant_key=variant_key,
                                    property_name=prop_name,
                                    property_value=prop_value,
                                    property_value_definition_name=prop_value_def,
                                    sort_index=sort_index,
                                    local_file_path=local_path,
                                    phash=phash,
                                    download_status=status,
                                    width=width,
                                    height=height,
                                    s3_url=s3_url
                                )
                                db.add(image)
                                images_saved += 1
                                sort_index += 1
            
            # Extract images from product description (ae_item_base_info_dto.detail)
            detail_html = base_info.get('detail', '')
            if detail_html:
                # Extract all image URLs from HTML src attributes
                img_urls = re.findall(r'src=\"([^\"]+)\"', detail_html)
                # Get list of already saved image URLs to avoid duplicates
                existing_urls = set(gallery_images)
                for image_url in img_urls:
                    if image_url and image_url not in existing_urls:
                        local_path, phash, status, width, height = image_downloader.download_image(
                            image_url, product_id, image_role='other'
                        )
                        
                        s3_url = None
                        if local_path and status == 'downloaded':
                            existing = db.query(ProductImage).filter(
                                ProductImage.product_id == product_id,
                                ProductImage.image_url == image_url
                            ).first()
                            
                            if existing and existing.s3_url:
                                s3_url = existing.s3_url
                            else:
                                s3_url = s3_uploader.upload_image(local_path, product_id, 'other')
                        
                        image = ProductImage(
                            product_id=product_id,
                            image_url=image_url,
                            image_role='other',  # Description/detail images
                            sort_index=sort_index,
                            local_file_path=local_path,
                            phash=phash,
                            download_status=status,
                            width=width,
                            height=height,
                            s3_url=s3_url
                        )
                        db.add(image)
                        images_saved += 1
                        sort_index += 1
            
            logger.debug(f"Extracted {images_saved} images for product {product_id}")
            
        except Exception as e:
            logger.error(f"Error extracting images for {product_id}: {e}")
        
        return images_saved

    def _extract_and_save_videos(self, product_id: str, product_data: Dict, db) -> int:
        """
        Extract and save product videos to product_videos table.
        
        Returns:
            Number of videos extracted
        """
        from src.common.database import ProductVideo
        
        videos_saved = 0
        
        try:
            result = product_data.get('aliexpress_ds_product_get_response', {}).get('result', {})
            video_url = result.get('product_video_url')
            
            if video_url:
                video = ProductVideo(
                    product_id=product_id,
                    video_url=video_url,
                    download_status='pending'
                )
                db.add(video)
                videos_saved += 1
                logger.debug(f"Extracted video for product {product_id}")
            
        except Exception as e:
            logger.error(f"Error extracting videos for {product_id}: {e}")
        
        return videos_saved

    def _fetch_and_save_shipping_info_single(self, product_id: str, product_data: Dict, 
                                             filtered_product: FilteredProduct, db) -> bool:
        """
        Fetch and save shipping information for a single product.
        Updates the filtered_product with min_shipping_price and delivery days.
        
        Returns:
            True if shipping info was fetched and saved
        """
        try:
            # Get max delivery days threshold from environment
            max_delivery_days = int(get_env("MAX_DELIVERY_DAYS", 30))
            ship_to_country = get_env("ALIEXPRESS_TARGET_COUNTRY", "DE")
            
            # Extract max-priced variant information
            max_variant_info = self._extract_max_variant_info(None, product_data)
            if not max_variant_info:
                logger.warning(f"No max variant info found for product {product_id}")
                return False
            
            max_sku_id = max_variant_info['sku_id']
            
            # Query freight information
            freight_response = self.api_client.query_freight(
                product_id=product_id,
                selected_sku_id=max_sku_id,
                ship_to_country=ship_to_country,
                quantity=1
            )
            
            if not freight_response:
                logger.warning(f"No freight response for product {product_id}")
                return False
            
            # Extract delivery options
            result = freight_response.get('aliexpress_ds_freight_query_response', {}).get('result', {})
            if not isinstance(result, dict):
                return False
            
            delivery_options = result.get('delivery_options', [])
            if isinstance(delivery_options, dict):
                if 'delivery_option_d_t_o' in delivery_options:
                    delivery_list = delivery_options['delivery_option_d_t_o']
                elif 'aeop_logistics_dto' in delivery_options:
                    delivery_list = delivery_options['aeop_logistics_dto']
                else:
                    delivery_list = [delivery_options]
            elif isinstance(delivery_options, list):
                delivery_list = delivery_options
            else:
                return False
            
            # Find best shipping option (free first, then cheapest)
            valid_options = []
            free_options = []
            
            for option in delivery_list:
                if not isinstance(option, dict):
                    continue
                
                max_delivery = self._parse_int(option.get('max_delivery_days'))
                if max_delivery is not None and max_delivery > max_delivery_days:
                    continue
                
                is_free = self._parse_bool(option.get('free_shipping'))
                shipping_fee = self._parse_float(option.get('shipping_fee_cent'))
                
                if is_free:
                    free_options.append({'option': option, 'fee': 0.0})
                elif shipping_fee is not None:
                    valid_options.append({'option': option, 'fee': shipping_fee})
            
            all_options = free_options + valid_options
            if not all_options:
                return False
            
            # Select best option
            if free_options:
                best = min(free_options, key=lambda x: self._parse_int(x['option'].get('max_delivery_days', 999)))
            else:
                best = min(valid_options, key=lambda x: x['fee'])
            
            best_option = best['option']
            min_shipping_price = best['fee']
            
            # Save shipping info
            shipping_info = ShippingInfo(
                product_id=product_id,
                sku_id=max_sku_id,
                code=best_option.get('code'),
                company=best_option.get('company'),
                shipping_fee=min_shipping_price,
                shipping_fee_currency=best_option.get('shipping_fee_currency'),
                free_shipping=self._parse_bool(best_option.get('free_shipping')),
                min_delivery_days=self._parse_int(best_option.get('min_delivery_days')),
                max_delivery_days=self._parse_int(best_option.get('max_delivery_days')),
                guaranteed_delivery_days=self._parse_int(best_option.get('guaranteed_delivery_days')),
                ship_from_country=best_option.get('ship_from_country'),
                tracking=self._parse_bool(best_option.get('tracking')),
                raw_freight_response=freight_response
            )
            
            db.add(shipping_info)
            
            # Update filtered_product
            filtered_product.min_shipping_price = min_shipping_price
            filtered_product.min_delivery_days = self._parse_int(best_option.get('min_delivery_days'))
            filtered_product.max_delivery_days = self._parse_int(best_option.get('max_delivery_days'))
            
            logger.info(f"✓ Saved shipping info for product {product_id}: fee={min_shipping_price}, days={filtered_product.min_delivery_days}-{filtered_product.max_delivery_days}")
            return True
            
        except Exception as e:
            logger.error(f"Error fetching shipping info for {product_id}: {e}")
            return False

    def _apply_filtering_rules(self, product: Product, db) -> Dict:
        """
        Apply all filtering rules to a product.
        
        Returns:
            Dict with filtering results and extracted data
        """
        result = {
            'passed_price_rule': False,
            'passed_shipping_rule': False,
            'max_variant_price': None,
            'min_shipping_cost': None,
            'total_cost': None,
            'delivery_time': None,
            'shipping_type': 'Standard',
            'shipping_speed_rating': None,
            'api_enriched': False
        }
        
        # Get detailed product information
        product_data = self._get_enriched_product_data(product)
        if product_data:
            result['api_enriched'] = True
        
        # Apply price rule
        price_result = self._apply_price_rule(product, product_data, db)
        result.update(price_result)
        
        # Apply shipping rule
        shipping_result = self._apply_shipping_rule(product, product_data)
        result.update(shipping_result)
        
        return result

    def _get_enriched_product_data(self, product: Product) -> Optional[Dict]:
        """
        Get enriched product data from API or existing raw_json_detail.
        
        Returns:
            Product detail data dict or None
        """
        # First check if we already have detailed data
        if product.raw_json_detail:
            logger.debug(f"Using existing product detail data for {product.product_id}")
            return product.raw_json_detail
        
        # If API client available, fetch fresh data
        if self.api_client:
            try:
                logger.debug(f"Fetching product details from API for {product.product_id}")
                detail_data = self.api_client.get_product_details(product.product_id)
                return detail_data
            except Exception as e:
                logger.warning(f"Failed to fetch product details for {product.product_id}: {e}")
        
        return None

    def _apply_price_rule(self, product: Product, product_data: Optional[Dict], db=None) -> Dict:
        """
        Apply price filtering rule: (max_variant_price + min_shipping_cost) <= max_price_eur
        
        Args:
            product: Product to apply price rule to
            product_data: Product detail data
            db: Database session (optional, for getting shipping costs)
        
        Returns:
            Dict with price rule results
        """
        result = {
            'passed_price_rule': False,
            'max_variant_price': None,
            'min_shipping_cost': 0.0,  # Default to 0 if no shipping data
            'total_cost': None
        }
        
        try:
            # Extract variant prices from product detail data
            max_variant_price = self._extract_max_variant_price(product, product_data)
            
            # Get minimum shipping cost from existing data if available
            min_shipping_cost = self._get_min_shipping_cost(product, db) if db else 0.0
            
            if max_variant_price is not None:
                total_cost = max_variant_price + min_shipping_cost
                
                result.update({
                    'max_variant_price': max_variant_price,
                    'min_shipping_cost': min_shipping_cost,
                    'total_cost': total_cost,
                    'passed_price_rule': total_cost <= self.max_price_eur
                })
                
                logger.debug(f"Price rule for {product.product_id}: max_variant={max_variant_price}, shipping={min_shipping_cost}, total={total_cost}, limit={self.max_price_eur}, passed={result['passed_price_rule']}")
            else:
                # Fallback to existing price data
                if product.target_sale_price:
                    result.update({
                        'max_variant_price': product.target_sale_price,
                        'min_shipping_cost': min_shipping_cost,
                        'total_cost': product.target_sale_price + min_shipping_cost,
                        'passed_price_rule': (product.target_sale_price + min_shipping_cost) <= self.max_price_eur
                    })
                    logger.debug(f"Price rule fallback for {product.product_id}: using target_sale_price={product.target_sale_price}")
        
        except Exception as e:
            logger.error(f"Error applying price rule to {product.product_id}: {e}")
        
        return result

    def _apply_shipping_rule(self, product: Product, product_data: Optional[Dict]) -> Dict:
        """
        Apply shipping filtering rule: delivery_time <= max_delivery_days
        
        Returns:
            Dict with shipping rule results
        """
        result = {
            'passed_shipping_rule': False,
            'delivery_time': None,
            'ship_to_country': None,
            'shipping_type': 'Standard',
            'shipping_speed_rating': None
        }
        
        try:
            # Extract delivery time and shipping info
            delivery_time = self._extract_delivery_time(product_data)
            ship_to_country = self._extract_ship_to_country(product_data)
            shipping_type = self._extract_shipping_type(product_data)
            shipping_speed_rating = self._extract_shipping_speed_rating(product_data)
            
            result.update({
                'delivery_time': delivery_time,
                'ship_to_country': ship_to_country,
                'shipping_type': shipping_type,
                'shipping_speed_rating': shipping_speed_rating
            })
            
            # Apply shipping rule
            if delivery_time is not None:
                result['passed_shipping_rule'] = delivery_time <= self.max_delivery_days
                logger.debug(f"Shipping rule for {product.product_id}: delivery_time={delivery_time}, limit={self.max_delivery_days}, passed={result['passed_shipping_rule']}")
            else:
                # If no delivery time data, pass the rule but log the missing data
                result['passed_shipping_rule'] = True
                logger.warning(f"Shipping rule for {product.product_id}: no delivery time data available, passing by default")
        
        except Exception as e:
            logger.error(f"Error applying shipping rule to {product.product_id}: {e}")
        
        return result

    def _extract_max_variant_price(self, product: Optional[Product], product_data: Optional[Dict], product_id: str = None) -> Optional[float]:
        """Extract the highest variant price from product data."""
        max_variant_info = self._extract_max_variant_info(product, product_data, product_id)
        return max_variant_info['price'] if max_variant_info else None

    def _extract_max_variant_info(self, product: Optional[Product], product_data: Optional[Dict], product_id: str = None) -> Optional[Dict]:
        """Extract the highest variant price and its associated SKU ID from product data."""
        # Get product_id from either Product object or parameter
        pid = product.product_id if product else product_id
        
        if not product_data:
            if pid:
                logger.debug(f"No product data for {pid}")
            return None
        
        try:
            # Navigate to SKU information
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            sku_info = result.get('ae_item_sku_info_dtos', {})
            skus = sku_info.get('ae_item_sku_info_d_t_o', [])
            
            if pid:
                logger.debug(f"Found {len(skus)} SKUs for product {pid}")
            
            if not skus:
                return None
            
            max_price = 0.0
            max_price_sku_id = None
            prices_found = []
            
            for i, sku in enumerate(skus):
                sku_id = sku.get('sku_id')
                # Prioritize sale prices over regular prices
                price_fields = ['offer_sale_price', 'offer_bulk_sale_price']
                for field in price_fields:
                    if field in sku:
                        try:
                            price = float(sku[field])
                            prices_found.append(f"SKU{i}({sku_id}):{field}={price}")
                            if price > max_price:
                                max_price = price
                                max_price_sku_id = sku_id
                            break  # Stop after finding first valid price for this SKU
                        except (ValueError, TypeError):
                            continue
            
            if pid:
                logger.debug(f"Sale prices found for {pid}: {prices_found}, max_price={max_price}, max_sku_id={max_price_sku_id}")
            
            if max_price > 0 and max_price_sku_id:
                return {
                    'price': max_price,
                    'sku_id': max_price_sku_id
                }
            
            return None
            
        except Exception as e:
            if pid:
                logger.error(f"Error extracting variant prices for {pid}: {e}")
            else:
                logger.error(f"Error extracting variant prices: {e}")
            return None

    def _extract_delivery_time(self, product_data: Optional[Dict]) -> Optional[int]:
        """Extract delivery time from product data."""
        if not product_data:
            return None
        
        try:
            # Check logistics_info_dto.delivery_time (correct path based on output_product_get.json)
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            logistics_info = result.get('logistics_info_dto', {})
            
            delivery_time = logistics_info.get('delivery_time')
            if delivery_time is not None:
                return int(delivery_time)
            
            logger.debug(f"No delivery_time found in logistics_info_dto")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting delivery time: {e}")
            return None

    def _extract_ship_to_country(self, product_data: Optional[Dict]) -> Optional[str]:
        """Extract ship_to_country from product data."""
        if not product_data:
            return None
        
        try:
            # Check logistics_info_dto.ship_to_country
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            logistics_info = result.get('logistics_info_dto', {})
            
            ship_to_country = logistics_info.get('ship_to_country')
            return ship_to_country
            
        except Exception as e:
            logger.error(f"Error extracting ship_to_country: {e}")
            return None

    def _extract_shipping_type(self, product_data: Optional[Dict]) -> str:
        """Extract shipping type (Choice, Local+, Standard) from product data."""
        if not product_data:
            return 'Standard'
        
        try:
            # This is a simplified implementation
            # In a real scenario, you'd look for specific badges or indicators
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            
            # Look for Choice or Local+ indicators
            # This would need to be expanded based on actual API response structure
            
            return 'Standard'  # Default for now
            
        except Exception as e:
            logger.error(f"Error extracting shipping type: {e}")
            return 'Standard'

    def _extract_shipping_speed_rating(self, product_data: Optional[Dict]) -> Optional[str]:
        """Extract shipping speed rating from product data."""
        if not product_data:
            return None
        
        try:
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            
            return result.get('shipping_speed_rating')
            
        except Exception as e:
            logger.error(f"Error extracting shipping speed rating: {e}")
            return None

    def _get_min_shipping_cost(self, product: Product, db) -> float:
        """
        Get the minimum shipping cost for a product from existing data.
        
        Args:
            product: Product to get shipping cost for
            db: Database session
            
        Returns:
            Minimum shipping cost (0.0 if no data available)
        """
        try:
            # First, check if we have it in filtered_products
            from src.common.database import FilteredProduct
            filtered_product = db.query(FilteredProduct).filter(
                FilteredProduct.product_id == product.product_id
            ).first()
            
            if filtered_product and filtered_product.min_shipping_price is not None:
                logger.debug(f"Found existing min_shipping_price for product {product.product_id}: {filtered_product.min_shipping_price}")
                return filtered_product.min_shipping_price
            
            # If not in filtered_products, check shipping_info table
            from src.common.database import ShippingInfo
            shipping_info = db.query(ShippingInfo).filter(
                ShippingInfo.product_id == product.product_id
            ).first()
            
            if shipping_info and shipping_info.shipping_fee is not None:
                logger.debug(f"Found shipping fee in shipping_info for product {product.product_id}: {shipping_info.shipping_fee}")
                return shipping_info.shipping_fee
            
            # Default to 0.0 if no shipping data available
            logger.debug(f"No shipping cost data found for product {product.product_id}, using 0.0")
            return 0.0
            
        except Exception as e:
            logger.error(f"Error getting min shipping cost for product {product.product_id}: {e}")
            return 0.0

    def _create_filtered_product(self, product: Product, filter_result: Dict, db) -> FilteredProduct:
        """Create a new filtered product entry and fetch detailed shipping information."""
        filtered_product = FilteredProduct(
            # Copy all fields from the original product
            product_id=product.product_id,
            shop_id=product.shop_id,
            product_title=product.product_title,
            product_detail_url=product.product_detail_url,
            product_main_image_url=product.product_main_image_url,
            product_video_url=product.product_video_url,
            original_price=product.original_price,
            target_sale_price=product.target_sale_price,
            original_price_currency=product.original_price_currency,
            target_sale_price_currency=product.target_sale_price_currency,
            discount=product.discount,
            evaluate_rate=product.evaluate_rate,
            category_id=product.category_id,
            first_seen_at=product.first_seen_at,
            last_seen_at=product.last_seen_at,
            raw_json_detail=product.raw_json_detail,
            
            # Add the extra fields specific to filtered products
            ship_to_country=filter_result.get('ship_to_country'),
            delivery_time=filter_result.get('delivery_time'),
            min_delivery_days=None,  # Will be updated after freight queries
            max_delivery_days=None,  # Will be updated after freight queries
            max_variant_price=filter_result.get('max_variant_price'),
            min_shipping_price=None  # Will be updated after freight queries
        )
        
        # Add to database
        db.add(filtered_product)
        db.flush()  # Ensure filtered_product is in DB before adding shipping info
        
        # Extract and store product variants
        self._extract_and_store_variants(product, db)
        
        # Fetch detailed shipping information using freight query API
        self._fetch_and_store_shipping_info(product, db)
        
        return filtered_product

    def _fetch_and_store_shipping_info(self, product: Product, db):
        """
        Fetch shipping information for the max-priced variant only and store the cheapest valid option.
        
        Args:
            product: Product to fetch shipping info for
            db: Database session
        """
        if not self.api_client:
            logger.warning(f"No API client available for freight query for product {product.product_id}")
            return
            
        try:
            # Get max delivery days threshold from environment
            max_delivery_days = int(get_env("MAX_DELIVERY_DAYS", 30))
            
            # Get ship_to_country from environment or default to target country
            ship_to_country = get_env("ALIEXPRESS_TARGET_COUNTRY", "DE")
            
            # Extract max-priced variant information
            max_variant_info = self._extract_max_variant_info(product, product.raw_json_detail)
            if not max_variant_info:
                logger.warning(f"No max variant info found for product {product.product_id}")
                return
                
            max_sku_id = max_variant_info['sku_id']
            max_price = max_variant_info['price']
            
            logger.info(f"Processing max-priced variant for product {product.product_id}: SKU {max_sku_id}, price {max_price}")
            
            try:
                logger.debug(f"Querying freight for product {product.product_id}, max-priced SKU {max_sku_id}")
                
                # Query freight information for the max-priced SKU only
                freight_response = self.api_client.query_freight(
                    product_id=product.product_id,
                    selected_sku_id=max_sku_id,
                    ship_to_country=ship_to_country,
                    quantity=1
                )
                
                if not freight_response:
                    logger.warning(f"No freight response for product {product.product_id}, SKU {max_sku_id}")
                    return
                
                # Extract delivery options from response
                if isinstance(freight_response, dict) and 'aliexpress_ds_freight_query_response' in freight_response:
                    result = freight_response['aliexpress_ds_freight_query_response'].get('result', {})
                    
                    # Ensure result is a dictionary
                    if not isinstance(result, dict):
                        logger.warning(f"Unexpected result format for product {product.product_id}, SKU {max_sku_id}: result is {type(result)}")
                        return
                        
                    delivery_options = result.get('delivery_options', [])
                    
                    # Handle different possible structures for delivery_options
                    if isinstance(delivery_options, dict):
                        # Check for the actual API response structure
                        if 'delivery_option_d_t_o' in delivery_options:
                            delivery_list = delivery_options['delivery_option_d_t_o']
                        elif 'aeop_logistics_dto' in delivery_options:
                            delivery_list = delivery_options['aeop_logistics_dto']
                        elif 'aeop_ds_logistics_dto' in delivery_options:
                            delivery_list = delivery_options['aeop_ds_logistics_dto']
                        else:
                            # If it's a single delivery option in dict format, wrap it in a list
                            delivery_list = [delivery_options]
                    elif isinstance(delivery_options, list):
                        delivery_list = delivery_options
                    else:
                        logger.warning(f"Unexpected delivery_options format for product {product.product_id}, SKU {max_sku_id}: {type(delivery_options)}")
                        return
                    
                    logger.info(f"Found {len(delivery_list)} shipping options for product {product.product_id}, max-priced SKU {max_sku_id}")
                    
                    # Filter valid shipping options and prioritize free shipping
                    valid_options = []
                    free_shipping_options = []
                    
                    for i, option in enumerate(delivery_list):
                        # Ensure each option is a dictionary
                        if not isinstance(option, dict):
                            logger.warning(f"Skipping delivery option {i} for product {product.product_id}, SKU {max_sku_id}: option is {type(option)}, value: {option}")
                            continue
                        
                        # Check delivery time constraint
                        max_delivery = self._parse_int(option.get('max_delivery_days'))
                        if max_delivery is not None and max_delivery > max_delivery_days:
                            logger.debug(f"Skipping shipping option {i} for product {product.product_id}: delivery time {max_delivery} exceeds limit {max_delivery_days}")
                            continue
                        
                        # Check if this is a free shipping option
                        is_free_shipping = self._parse_bool(option.get('free_shipping'))
                        
                        # Extract shipping fee
                        shipping_fee = self._parse_float(option.get('shipping_fee_cent'))
                        if shipping_fee is None:
                            # Try alternative field names for shipping fee
                            shipping_fee = self._parse_float(option.get('logisticsFee'))
                        
                        # If free shipping is available and meets delivery time, prioritize it
                        if is_free_shipping:
                            free_shipping_options.append({
                                'option': option,
                                'shipping_fee': 0.0,  # Free shipping cost is 0
                                'index': i
                            })
                            logger.debug(f"Found free shipping option {i} for product {product.product_id}: delivery {max_delivery} days")
                        elif shipping_fee is not None:
                            valid_options.append({
                                'option': option,
                                'shipping_fee': shipping_fee,
                                'index': i
                            })
                    
                    # Prioritize free shipping options first, then regular options
                    all_valid_options = free_shipping_options + valid_options
                    
                    if not all_valid_options:
                        logger.warning(f"No valid shipping options found for product {product.product_id} (delivery time <= {max_delivery_days} days)")
                        return
                    
                    # Find the best option (free shipping first, then cheapest paid option)
                    if free_shipping_options:
                        # If we have free shipping options, pick the fastest free option
                        best_free_option = min(free_shipping_options, key=lambda x: self._parse_int(x['option'].get('max_delivery_days', 999)))
                        best_option = best_free_option['option']
                        min_shipping_price = 0.0
                        logger.info(f"Selected free shipping option for product {product.product_id}: delivery {self._parse_int(best_option.get('max_delivery_days'))} days")
                    else:
                        # No free shipping, pick the cheapest paid option
                        cheapest_option = min(valid_options, key=lambda x: x['shipping_fee'])
                        best_option = cheapest_option['option']
                        min_shipping_price = cheapest_option['shipping_fee']
                        logger.info(f"Selected cheapest paid shipping option for product {product.product_id}: fee {min_shipping_price}")
                    
                    logger.info(f"Selected shipping option for product {product.product_id}: fee {min_shipping_price}, delivery {self._parse_int(best_option.get('max_delivery_days'))} days")
                    
                    # Store only the best shipping option (free or cheapest paid)
                    shipping_info = ShippingInfo(
                        product_id=product.product_id,
                        sku_id=max_sku_id,
                        code=best_option.get('code'),
                        company=best_option.get('company'),
                        shipping_fee=min_shipping_price,
                        shipping_fee_currency=best_option.get('shipping_fee_currency'),
                        free_shipping=self._parse_bool(best_option.get('free_shipping')),
                        min_delivery_days=self._parse_int(best_option.get('min_delivery_days')),
                        max_delivery_days=self._parse_int(best_option.get('max_delivery_days')),
                        guaranteed_delivery_days=self._parse_int(best_option.get('guaranteed_delivery_days')),
                        ship_from_country=best_option.get('ship_from_country'),
                        tracking=self._parse_bool(best_option.get('tracking')),
                        raw_freight_response=freight_response
                    )
                    
                    # Add shipping info to database
                    db.add(shipping_info)
                    
                    # Update filtered_products with minimum shipping price and delivery days
                    filtered_product = db.query(FilteredProduct).filter(
                        FilteredProduct.product_id == product.product_id
                    ).first()
                    
                    if filtered_product:
                        filtered_product.min_shipping_price = min_shipping_price
                        filtered_product.min_delivery_days = self._parse_int(best_option.get('min_delivery_days'))
                        filtered_product.max_delivery_days = self._parse_int(best_option.get('max_delivery_days'))
                        logger.info(f"Updated product {product.product_id} with min shipping price: {min_shipping_price}")
                        logger.info(f"Updated product {product.product_id} with delivery days: {filtered_product.min_delivery_days}-{filtered_product.max_delivery_days}")
                    
                    logger.info(f"Stored cheapest valid shipping option for product {product.product_id}, min shipping price: {min_shipping_price}")
                        
                else:
                    logger.warning(f"Unexpected freight response format for product {product.product_id}, SKU {max_sku_id}: {freight_response}")
                    
            except Exception as e:
                logger.error(f"Error fetching shipping info for product {product.product_id}, SKU {max_sku_id}: {e}")
                
        except Exception as e:
            logger.error(f"Error fetching shipping info for product {product.product_id}: {e}")

    def _extract_and_store_variants(self, product: Product, db):
        """
        Extract and store all product variants from raw_json_detail.
        
        Args:
            product: Product to extract variants from
            db: Database session
        """
        try:
            if not product.raw_json_detail:
                logger.info(f"No raw_json_detail found for product {product.product_id}")
                return
            
            # Extract variant data from AliExpress API response
            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
            sku_info = result.get('ae_item_sku_info_dtos', {})
            
            if not sku_info or 'ae_item_sku_info_d_t_o' not in sku_info:
                logger.info(f"No SKU info found for product {product.product_id}")
                return
            
            skus = sku_info['ae_item_sku_info_d_t_o']
            logger.info(f"Found {len(skus)} variants for product {product.product_id}")
            
            for sku in skus:
                # Extract basic SKU information
                sku_id = sku.get('sku_id')
                if not sku_id:
                    logger.warning(f"SKU missing sku_id for product {product.product_id}")
                    continue
                
                # Check if variant already exists
                existing_variant = db.query(ProductVariant).filter(
                    ProductVariant.sku_id == sku_id
                ).first()
                
                if existing_variant:
                    logger.debug(f"Variant {sku_id} already exists, skipping")
                    continue
                
                # Extract pricing and stock information
                offer_sale_price = self._parse_float(sku.get('offer_sale_price'))
                currency_code = sku.get('currency_code')
                sku_available_stock = self._parse_int(sku.get('sku_available_stock'))
                
                # Extract ALL property information (multi-property support)
                properties = []
                variant_key_parts = []
                variant_label_parts = []
                
                # Extract property details from ae_sku_property_dtos
                if 'ae_sku_property_dtos' in sku:
                    props = sku['ae_sku_property_dtos'].get('ae_sku_property_d_t_o', [])
                    for prop in props:
                        prop_name = prop.get('sku_property_name')
                        prop_value = prop.get('sku_property_value')
                        prop_id = prop.get('sku_property_id')
                        prop_value_id = prop.get('property_value_id')
                        prop_value_def_name = prop.get('property_value_definition_name')
                        
                        if prop_name and prop_value:
                            # Add to properties array
                            properties.append({
                                'name': prop_name,
                                'value': prop_value,
                                'property_id': str(prop_id) if prop_id else None,
                                'value_id': str(prop_value_id) if prop_value_id else None,
                                'definition_name': prop_value_def_name
                            })
                            
                            # Build variant key parts with proper formatting - use definition name if available, fallback to prop_value
                            display_value = prop_value_def_name if prop_value_def_name else prop_value
                            variant_key_parts.append(f"{prop_name}: {display_value}")
                            variant_label_parts.append(display_value)
                
                # Create properly formatted variant key with spaces
                variant_key = " + ".join(sorted(variant_key_parts)) if variant_key_parts else None
                
                # Get legacy field for backward compatibility
                property_value_definition_name = None
                if properties:
                    property_value_definition_name = properties[0].get('definition_name')
                
                # Get SKU image URL
                sku_image_url = None
                if 'ae_sku_property_dtos' in sku:
                    props = sku['ae_sku_property_dtos'].get('ae_sku_property_d_t_o', [])
                    if props and len(props) > 0:
                        sku_image_url = props[0].get('sku_image')
                
                # Create ProductVariant record
                variant = ProductVariant(
                    product_id=product.product_id,
                    sku_id=sku_id,
                    sku_attr=sku.get('sku_attr'),
                    offer_sale_price=offer_sale_price,
                    currency_code=currency_code,
                    sku_available_stock=sku_available_stock,
                    properties=properties,  # JSON array of all properties
                    variant_key=variant_key,  # Properly formatted key like "Color: Red + Size: L"
                    property_value_definition_name=property_value_definition_name,
                    sku_image_url=sku_image_url,
                    raw_variant_data=sku
                )
                
                db.add(variant)
                logger.debug(f"Added variant {sku_id} for product {product.product_id}: {variant_key}")
            
            logger.info(f"Successfully processed variants for product {product.product_id}")
            
        except Exception as e:
            logger.error(f"Error extracting variants for product {product.product_id}: {e}")

    def _parse_float(self, value) -> Optional[float]:
        """Safely parse float value."""
        if value is None or value == "null":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_int(self, value) -> Optional[int]:
        """Safely parse integer value."""
        if value is None or value == "null":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _parse_bool(self, value) -> Optional[bool]:
        """Safely parse boolean value."""
        if value is None or value == "null":
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        return None


def run_product_filtering(limit: int = None, dry_run: bool = False) -> Dict[str, int]:
    """
    Main function to run product filtering from scraped_products table.
    
    Processes products one-by-one through complete workflow:
    - Fetch from AliExpress API
    - Apply filtering rules (price + shipping)
    - Populate filtered_products with ALL fields
    - Extract variants, images, videos
    - Fetch shipping info
    - Mark as extracted (or failed if doesn't pass filters)
    
    Args:
        limit: Maximum number of SUCCESSFUL products to process (failed products don't count)
        dry_run: Don't save to database
        
    Returns:
        Processing statistics
    """
    logger.info("Starting product filtering process")
    logger.info("Processing unextracted products from scraped_products table")
    
    filter_engine = ProductFilterEngine()
    stats = filter_engine.process_whitelisted_products(limit, dry_run)
    
    logger.info("Product filtering completed")
    logger.info(f"Statistics: {stats}")
    
    return stats