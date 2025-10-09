"""
Data synchronization module for Airtable integration.
Handles the mapping of database data to Airtable format for the two-table system.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from ..common.database import (
    FilteredProduct, ProductStatus, ProductImage, ProductVideo,
    get_db_session
)
from .client import AirtableClient

logger = logging.getLogger(__name__)


class AirtableDataSync:
    """
    Handles data synchronization between database and Airtable.
    Implements the two-table system: Products and Variants.
    """
    
    def __init__(self, dry_run: bool = False):
        self.client = AirtableClient()
        self.dry_run = dry_run
        
        # Get available fields from the base to avoid unknown field errors
        try:
            products_schema = self.client.products_table.schema()
            variants_schema = self.client.variants_table.schema()
            
            self.products_fields = {field.name for field in products_schema.fields}
            self.variants_fields = {field.name for field in variants_schema.fields}
            
            logger.info(f"Products table fields: {sorted(self.products_fields)}")
            logger.info(f"Variants table fields: {sorted(self.variants_fields)}")
        except Exception as e:
            logger.warning(f"Could not fetch table schemas: {e}")
            # Conservative fallback - only include basic fields that should exist
            self.products_fields = {
                'anon_product_id', 'title', 'hero_image', 'gallery_images', 
                'duplicate_status'
            }
            self.variants_fields = {
                'variant_id', 'anon_product_id', 'attribute_name', 'attribute_value',
                'price_eur', 'shipping_eur', 'total_eur', 'delivery_time', 'stock_quantity'
            }
        
    def sync_products(self, limit: Optional[int] = None, filter_status: Optional[str] = None) -> Dict[str, int]:
        """
        Sync products from database to Airtable Products table.
        Only syncs MASTER and UNIQUE products (not DUPLICATES).
        """
        logger.info(f"Starting products sync (limit: {limit}, filter: {filter_status}, dry_run: {self.dry_run})")
        
        with get_db_session() as db:
            # Query for MASTER and UNIQUE products only
            query = db.query(FilteredProduct).join(
                ProductStatus, FilteredProduct.product_id == ProductStatus.product_id
            ).filter(
                ProductStatus.status.in_(['MASTER', 'UNIQUE'])
            )
            
            if filter_status:
                query = query.filter(ProductStatus.status == filter_status)
            
            if limit:
                query = query.limit(limit)
            
            products = query.all()
            logger.info(f"Found {len(products)} products to sync")
            
            if not products:
                return {'created': 0, 'updated': 0}
            
            # Convert to Airtable format
            airtable_records = []
            for product in products:
                record = self._prepare_product_record(db, product)
                if record:
                    airtable_records.append(record)
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would sync {len(airtable_records)} product records")
                return {'created': 0, 'updated': 0}
            
            # Upsert to Airtable
            result = self.client.upsert_records(
                self.client.products_table_name, 
                airtable_records, 
                key_field='anon_product_id'
            )
            
            logger.info(f"Products sync completed: {result}")
            return result
    
    def sync_variants(self, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Sync product variants to Airtable Variants table.
        Each product becomes at least one variant (default variant).
        """
        logger.info(f"Starting variants sync (limit: {limit}, dry_run: {self.dry_run})")
        
        with get_db_session() as db:
            # Query for MASTER and UNIQUE products only
            query = db.query(FilteredProduct).join(
                ProductStatus, FilteredProduct.product_id == ProductStatus.product_id
            ).filter(
                ProductStatus.status.in_(['MASTER', 'UNIQUE'])
            )
            
            if limit:
                query = query.limit(limit)
            
            products = query.all()
            logger.info(f"Found {len(products)} products to create variants from")
            
            if not products:
                return {'created': 0, 'updated': 0}
            
            # Convert to Airtable format
            airtable_records = []
            for product in products:
                variants = self._prepare_variant_records(db, product)
                airtable_records.extend(variants)
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would sync {len(airtable_records)} variant records")
                return {'created': 0, 'updated': 0}
            
            # Upsert to Airtable using sku_id as unique key (variant_key is not unique across products)
            result = self.client.upsert_records(
                self.client.variants_table_name,
                airtable_records,
                key_field='sku_id'
            )
            
            logger.info(f"Variants sync completed: {result}")
            return result
    
    def _filter_fields(self, record_fields: Dict[str, Any], available_fields: set) -> Dict[str, Any]:
        """Filter record fields to only include those that exist in the Airtable base."""
        filtered = {}
        for field_name, value in record_fields.items():
            if field_name in available_fields:
                filtered[field_name] = value
            else:
                logger.debug(f"Skipping unknown field: {field_name}")
        return filtered
    
    def _prepare_product_record(self, db: Session, product: FilteredProduct) -> Optional[Dict]:
        """
        Prepare a single product record for Airtable Products table.
        """
        try:
            # Get product status info
            status_info = db.query(ProductStatus).filter(
                ProductStatus.product_id == product.product_id
            ).first()
            
            if not status_info:
                logger.warning(f"No status info found for product {product.product_id}")
                return None
            
            # Use actual product_id instead of anonymous
            product_id = product.product_id
            
            # Extract description from raw_json_detail
            description = self._extract_description(product)
            
            # Get images
            images = db.query(ProductImage).filter(
                ProductImage.product_id == product.product_id,
                ProductImage.s3_url.isnot(None)
            ).order_by(ProductImage.sort_index).all()
            
            # Find hero image (is_primary=True or image_role='hero')
            hero_image = None
            gallery_image_urls = []
            
            for img in images:
                if img.is_primary or img.image_role == 'hero':
                    hero_image = img.s3_url
                elif img.image_role == 'gallery':
                    gallery_image_urls.append(img.s3_url)
            
            # If no hero image found, use first available image
            if not hero_image and images:
                hero_image = images[0].s3_url
                # Remove from gallery if it's there
                if hero_image in gallery_image_urls:
                    gallery_image_urls.remove(hero_image)
            
            # Format gallery images - handle both text and attachment field types
            # For text fields: comma-separated URLs
            # For attachment fields: array of objects with url property
            gallery_images = ', '.join(gallery_image_urls) if gallery_image_urls else ''
            
            # Get video
            video = db.query(ProductVideo).filter(
                ProductVideo.product_id == product.product_id,
                ProductVideo.s3_url.isnot(None)
            ).first()
            
            video_url = video.s3_url if video else None
            
            # Count variants from raw_json_detail
            variant_count = self._count_variants(product)
            
            # Get pricing info from variants
            price_info = self._extract_pricing_from_variants(product)
            
            # Prepare the record fields (use field names that exist in user's base)
            record_fields = {
                'anon_product_id': product_id,  # Map product_id to existing anon_product_id field
                'title': product.product_title or '',
                'description': description,
                'hero_image': hero_image or '',
                'gallery_images': gallery_images,
                'video': video_url or '',
                'duplicate_status': status_info.status,
                'price_eur': price_info.get('min_price', float(product.target_sale_price or 0)),
                'shipping_eur': float(product.min_shipping_price or 0),
                'total_eur': price_info.get('min_price', float(product.target_sale_price or 0)) + float(product.min_shipping_price or 0),
                'delivery_time': f"{product.min_delivery_days or 0}-{product.max_delivery_days or 0} days",
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Filter to only include fields that exist in the base
            filtered_fields = self._filter_fields(record_fields, self.products_fields)
            
            record = {'fields': filtered_fields}
            
            return record
            
        except Exception as e:
            logger.error(f"Error preparing product record for {product.product_id}: {e}")
            return None
    
    def _prepare_variant_records(self, db: Session, product: FilteredProduct) -> List[Dict]:
        """
        Prepare variant records for a product.
        Extracts real variants from raw_json_detail if available.
        """
        try:
            product_id = product.product_id
            
            # Extract variants from raw_json_detail
            variants = self._extract_variants_from_json(product)
            
            if not variants:
                # Fallback to single default variant
                variants = [{
                    'sku_id': '',
                    'variant_key': f"{product_id}_default",
                    'attribute_name': 'Default',
                    'attribute_value': 'Standard',
                    'price_eur': float(product.target_sale_price or 0),
                    'stock_quantity': 1
                }]
            
            variant_records = []
            
            # Get images for variants (fallback to product images)
            all_images = db.query(ProductImage).filter(
                ProductImage.product_id == product.product_id,
                ProductImage.s3_url.isnot(None)
            ).order_by(ProductImage.sort_index).all()
            
            hero_image = None
            gallery_images = []
            
            for img in all_images:
                if img.is_primary or img.image_role == 'hero':
                    hero_image = img.s3_url
                else:
                    gallery_images.append(img.s3_url)
            
            if not hero_image and all_images:
                hero_image = all_images[0].s3_url
            
            # Create variant records
            for i, variant in enumerate(variants):
                is_recommended = (i == 0)  # First variant is recommended
                
                # Prepare variant record fields (use field names that exist in user's base)
                variant_fields = {
                    'variant_key': variant['variant_key'],
                    'anon_product_id': product_id,  # Map to existing anon_product_id field
                    'variant_label': variant.get('attribute_value', 'Default'),
                    'sku_id': variant.get('sku_id', ''),
                    'price_eur': variant.get('price_eur', 0),
                    'shipping_eur': float(product.min_shipping_price or 0),
                    'total_eur': variant.get('price_eur', 0) + float(product.min_shipping_price or 0),
                    'delivery_min_days': int(product.min_delivery_days or 0),
                    'delivery_max_days': int(product.max_delivery_days or 0),
                    'delivery_range': f"{product.min_delivery_days or 0}-{product.max_delivery_days or 0} days",
                    'variant_hero_image': hero_image or '',
                    'variant_images': ', '.join(gallery_images) if gallery_images else '',
                    'is_recommended': is_recommended,
                    'sync_timestamp': datetime.now().isoformat()
                }
                
                # Filter to only include fields that exist in the base
                filtered_fields = self._filter_fields(variant_fields, self.variants_fields)
                variant_records.append({'fields': filtered_fields})
            
            return variant_records
            
        except Exception as e:
            logger.error(f"Error preparing variant records for {product.product_id}: {e}")
            return []
    
    def _filter_fields(self, fields: Dict[str, Any], available_fields: set) -> Dict[str, Any]:
        """Filter fields to only include those that exist in the Airtable base."""
        return {k: v for k, v in fields.items() if k in available_fields}
    
    def _extract_description(self, product: FilteredProduct) -> str:
        """Extract product description from raw_json_detail."""
        try:
            if not product.raw_json_detail:
                return ''
            
            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
            base_info = result.get('ae_item_base_info_dto', {})
            
            # Try to get detail field, clean HTML tags
            detail = base_info.get('detail', '')
            if detail:
                # Basic HTML tag removal
                import re
                clean_text = re.sub(r'<[^>]+>', '', str(detail))
                return clean_text[:1000]  # Limit length
            
            return ''
        except Exception as e:
            logger.warning(f"Error extracting description for {product.product_id}: {e}")
            return ''
    
    def _count_variants(self, product: FilteredProduct) -> int:
        """Count number of variants from raw_json_detail."""
        try:
            variants = self._extract_variants_from_json(product)
            return len(variants) if variants else 1
        except Exception:
            return 1
    
    def _extract_pricing_from_variants(self, product: FilteredProduct) -> Dict[str, float]:
        """Extract pricing info from variants."""
        try:
            variants = self._extract_variants_from_json(product)
            if not variants:
                return {'min_price': float(product.target_sale_price or 0)}
            
            prices = [v.get('price_eur', 0) for v in variants if v.get('price_eur')]
            return {
                'min_price': min(prices) if prices else float(product.target_sale_price or 0),
                'max_price': max(prices) if prices else float(product.target_sale_price or 0)
            }
        except Exception:
            return {'min_price': float(product.target_sale_price or 0)}
    
    def _extract_variants_from_json(self, product: FilteredProduct) -> List[Dict]:
        """Extract variants from raw_json_detail."""
        try:
            if not product.raw_json_detail:
                return []
            
            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
            sku_info = result.get('ae_item_sku_info_dtos', {})
            
            if not sku_info or 'ae_item_sku_info_d_t_o' not in sku_info:
                return []
            
            skus = sku_info['ae_item_sku_info_d_t_o']
            variants = []
            
            for sku in skus:
                # Extract SKU attributes
                sku_id = sku.get('sku_id', '')
                price = float(sku.get('offer_sale_price', 0))
                stock = int(sku.get('sku_available_stock', 0))
                
                # Extract variant attributes (color, size, etc.)
                attribute_name = 'Default'
                attribute_value = 'Standard'
                variant_key = f"{product.product_id}_{sku_id}"  # Fallback
                
                if 'ae_sku_property_dtos' in sku:
                    props = sku['ae_sku_property_dtos'].get('ae_sku_property_d_t_o', [])
                    if props and len(props) > 0:
                        first_prop = props[0]
                        attribute_name = first_prop.get('sku_property_name', 'Default')
                        attribute_value = first_prop.get('sku_property_value', 'Standard')
                        
                        # Create variant_key in same format as product_images: property_name:property_value
                        if attribute_name and attribute_value:
                            variant_key = f"{attribute_name}:{attribute_value}"
                
                variant = {
                    'sku_id': sku_id,
                    'variant_key': variant_key,
                    'attribute_name': attribute_name,
                    'attribute_value': attribute_value,
                    'price_eur': price,
                    'stock_quantity': stock
                }
                variants.append(variant)
            
            return variants
            
        except Exception as e:
            logger.warning(f"Error extracting variants for {product.product_id}: {e}")
            return []


def sync_to_airtable(limit: Optional[int] = None, filter_status: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Main function to sync data to Airtable.
    Syncs both Products and Variants tables.
    """
    logger.info(f"Starting Airtable sync (limit: {limit}, filter: {filter_status}, dry_run: {dry_run})")
    
    sync_engine = AirtableDataSync(dry_run=dry_run)
    
    # Sync Products table
    products_result = sync_engine.sync_products(limit=limit, filter_status=filter_status)
    
    # Sync Variants table
    variants_result = sync_engine.sync_variants(limit=limit)
    
    result = {
        'products': products_result,
        'variants': variants_result,
        'total_created': products_result['created'] + variants_result['created'],
        'total_updated': products_result['updated'] + variants_result['updated']
    }
    
    logger.info(f"Airtable sync completed: {result}")
    return result