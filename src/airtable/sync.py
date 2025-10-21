"""
Data synchronization module for Airtable integration.
Handles the mapping of database data to Airtable format for the two-table system.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from ..common.database import (
    FilteredProduct, ProductStatus, ProductImage, ProductVideo, ProductVariant,
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
            
            # Try to get Product Mapping table schema
            try:
                mapping_schema = self.client.mapping_table.schema()
                self.mapping_fields = {field.name for field in mapping_schema.fields}
                logger.info(f"Product Mapping table fields: {sorted(self.mapping_fields)}")
            except Exception as mapping_error:
                logger.warning(f"Product Mapping table not found or inaccessible: {mapping_error}")
                self.mapping_fields = set()  # Empty set - table doesn't exist
            
            # Try to get SKU Mapping table schema
            try:
                sku_mapping_schema = self.client.sku_mapping_table.schema()
                self.sku_mapping_fields = {field.name for field in sku_mapping_schema.fields}
                logger.info(f"SKU Mapping table fields: {sorted(self.sku_mapping_fields)}")
            except Exception as sku_mapping_error:
                logger.warning(f"SKU Mapping table not found or inaccessible: {sku_mapping_error}")
                self.sku_mapping_fields = set()  # Empty set - table doesn't exist
            
            logger.info(f"Products table fields: {sorted(self.products_fields)}")
            logger.info(f"Variants table fields: {sorted(self.variants_fields)}")
        except Exception as e:
            logger.warning(f"Could not fetch table schemas: {e}")
            # Conservative fallback - only include basic fields that should exist
            self.products_fields = {
                'anon_product_id', 'title', 'description', 'specifications', 
                'hero_image', 'gallery_images', 'duplicate_status'
            }
            self.variants_fields = {
                'variant_id', 'anon_product_id', 'attribute_name', 'attribute_value',
                'price_eur', 'shipping_eur', 'total_eur', 'delivery_time', 'stock_quantity',
                'definition_name', 'anon_sku_id'
            }
            self.mapping_fields = set()
            self.sku_mapping_fields = {
                'anon_sku_id', 'real_sku_id', 'aliexpress_product_url', 
                'aliexpress_main_image', 'aliexpress_variant_image'
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
            
            # Upsert to Airtable using anon_sku_id as unique key (variant_key is not unique across products)
            result = self.client.upsert_records(
                self.client.variants_table_name,
                airtable_records,
                key_field='anon_sku_id'
            )
            
            logger.info(f"Variants sync completed: {result}")
            return result
    
    def sync_product_mapping(self, limit: Optional[int] = None, filter_status: Optional[str] = None) -> Dict[str, int]:
        """
        Sync product mapping data to Airtable Product Mapping table.
        Maps anonymous product IDs to real product IDs and original AliExpress URLs.
        """
        logger.info(f"Starting product mapping sync (limit: {limit}, filter: {filter_status}, dry_run: {self.dry_run})")
        
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
            logger.info(f"Found {len(products)} products to create mapping records from")
            
            if not products:
                return {'created': 0, 'updated': 0}
            
            # Convert to Airtable format
            airtable_records = []
            for product in products:
                record = self._prepare_mapping_record(product)
                if record:
                    airtable_records.append(record)
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would sync {len(airtable_records)} product mapping records")
                return {'created': 0, 'updated': 0}
            
            # Check if mapping table exists
            mapping_table_name = "Product Mapping"  # Default name
            if hasattr(self.client, 'mapping_table_name'):
                mapping_table_name = self.client.mapping_table_name
            
            # Upsert to Airtable
            result = self.client.upsert_records(
                mapping_table_name,
                airtable_records,
                key_field='anon_product_id'
            )
            
            logger.info(f"Product mapping sync completed: {result}")
            return result
    
    def _prepare_mapping_record(self, product: FilteredProduct) -> Optional[Dict]:
        """
        Prepare a product mapping record for Airtable Product Mapping table.
        """
        try:
            # Extract original AliExpress URLs from raw data
            aliexpress_product_url = ""
            aliexpress_main_image_url = ""
            
            # Get URLs from the original product data
            if hasattr(product, 'product_main_image_url') and product.product_main_image_url:
                aliexpress_main_image_url = product.product_main_image_url
            
            # Construct AliExpress product page URL from product_id
            aliexpress_product_url = f"https://www.aliexpress.com/item/{product.product_id}.html"
            
            # Get additional URLs from raw_json_detail if available
            if product.raw_json_detail:
                try:
                    result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
                    
                    # Get main image URL if not already set
                    if not aliexpress_main_image_url:
                        aliexpress_main_image_url = result.get('product_main_image_url', '')
                    
                except Exception as e:
                    logger.debug(f"Error extracting URLs from raw data for {product.product_id}: {e}")
            
            # Get video URL from product_video_url field (already extracted during harvest)
            aliexpress_video_url = product.product_video_url or ''
            
            # Generate anonymous ID using the client method
            anon_id = self.client.generate_anonymous_id(product.product_id)
            
            # Prepare the mapping record
            record_fields = {
                'anon_product_id': anon_id,  # Anonymous ID (matches other tables)
                'real_product_id': product.product_id,  # Real AliExpress product ID
                'aliexpress_product_url': aliexpress_product_url,
                'aliexpress_main_image_url': aliexpress_main_image_url,
                'aliexpress_video_url': aliexpress_video_url if 'aliexpress_video_url' in locals() else '',
                'sync_timestamp': datetime.now().isoformat()
            }
            
            return {'fields': record_fields}
            
        except Exception as e:
            logger.error(f"Error preparing mapping record for {product.product_id}: {e}")
            return None
    
    def sync_sku_mapping(self, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Sync SKU mapping data to Airtable SKU Mapping table.
        Maps anonymous SKU IDs to real SKU IDs and variant images.
        """
        logger.info(f"Starting SKU mapping sync (limit: {limit}, dry_run: {self.dry_run})")
        
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
            logger.info(f"Found {len(products)} products to create SKU mapping records from")
            
            if not products:
                return {'created': 0, 'updated': 0}
            
            # Convert to Airtable format
            all_sku_records = []
            for product in products:
                sku_records = self._prepare_sku_mapping_records(db, product)
                all_sku_records.extend(sku_records)
            
            logger.info(f"Prepared {len(all_sku_records)} SKU mapping records")
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would sync {len(all_sku_records)} SKU mapping records")
                return {'created': 0, 'updated': 0}
            
            if not all_sku_records:
                return {'created': 0, 'updated': 0}
            
            # Check if SKU mapping table exists
            sku_mapping_table_name = "SKU Mapping"  # Default name
            if hasattr(self.client, 'sku_mapping_table_name'):
                sku_mapping_table_name = self.client.sku_mapping_table_name
            
            # Upsert to Airtable
            result = self.client.upsert_records(
                sku_mapping_table_name,
                all_sku_records,
                key_field='anon_sku_id'
            )
            
            logger.info(f"SKU mapping sync completed: {result}")
            return result
    
    def _prepare_sku_mapping_records(self, db: Session, product: FilteredProduct) -> List[Dict]:
        """
        Prepare SKU mapping records for a product using the ProductVariant table.
        Maps anonymous SKU IDs to real AliExpress SKU data.
        """
        try:
            # Get variants from the ProductVariant table
            variants = db.query(ProductVariant).filter(
                ProductVariant.product_id == product.product_id
            ).all()
            
            if not variants:
                logger.warning(f"No variants found in ProductVariant table for SKU mapping {product.product_id}")
                return []
            
            # Build AliExpress product URL
            aliexpress_product_url = f"https://www.aliexpress.com/item/{product.product_id}.html"
            
            # Get main product image
            aliexpress_main_image_url = ""
            if hasattr(product, 'product_main_image_url') and product.product_main_image_url:
                aliexpress_main_image_url = product.product_main_image_url
            
            sku_mapping_records = []
            
            for variant in variants:
                # Generate anonymous SKU ID
                anon_sku_id = self.client.generate_anonymous_id(variant.sku_id)
                
                # Get variant-specific image URL (original AliExpress URL)
                aliexpress_variant_image = ""
                if hasattr(variant, 'sku_image_url') and variant.sku_image_url:
                    # Extract original URL if this is an S3 URL, otherwise use as-is
                    aliexpress_variant_image = variant.sku_image_url
                
                # Prepare SKU mapping record
                mapping_fields = {
                    'anon_sku_id': anon_sku_id,
                    'real_sku_id': variant.sku_id,
                    'aliexpress_product_url': aliexpress_product_url,
                    'aliexpress_main_image': aliexpress_main_image_url,
                    'aliexpress_variant_image': aliexpress_variant_image,
                    'sync_timestamp': datetime.now().isoformat()
                }
                
                sku_mapping_records.append({'fields': mapping_fields})
            
            logger.info(f"Prepared {len(sku_mapping_records)} SKU mapping records for {product.product_id}")
            return sku_mapping_records
            
        except Exception as e:
            logger.error(f"Error preparing SKU mapping records for {product.product_id}: {e}")
            return []
    
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
            
            # Generate anonymous product ID for Airtable
            product_id = self.client.generate_anonymous_id(product.product_id)
            
            # Extract description from raw_json_detail
            description = self._extract_description(product)
            
            # Extract specifications from raw_json_detail
            specifications = self._extract_specifications(product)
            
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
            
            # Store gallery images as comma-separated S3 URLs (not attachments)
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
            
            # Get recommended variant (first/cheapest variant)
            recommended_variant_sku = self._get_recommended_variant_sku(product)
            
            # Prepare the record fields (use field names that exist in user's base)
            record_fields = {
                'anon_product_id': product_id,  # Map product_id to existing anon_product_id field
                'title': product.product_title or '',
                'description': description,
                'specifications': specifications,
                'hero_image': hero_image or '',
                'gallery_images': gallery_images,
                'video': video_url or '',
                'duplicate_status': status_info.status,
                'price_eur': price_info.get('min_price', float(product.target_sale_price or 0)),
                'shipping_eur': float(product.min_shipping_price or 0),
                'total_eur': price_info.get('min_price', float(product.target_sale_price or 0)) + float(product.min_shipping_price or 0),
                'delivery_time': f"{product.min_delivery_days or 0}-{product.max_delivery_days or 0} days",
                'selected_variant': recommended_variant_sku or '',  # SKU ID of recommended variant
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
        Prepare variant records for a product using the existing ProductVariant table.
        """
        try:
            # Generate anonymous product ID for consistency with other tables
            product_id = self.client.generate_anonymous_id(product.product_id)
            
            # Get variants from the ProductVariant table
            variants = db.query(ProductVariant).filter(
                ProductVariant.product_id == product.product_id
            ).all()
            
            if not variants:
                logger.warning(f"No variants found in ProductVariant table for {product.product_id}")
                return []
            
            variant_records = []
            
            # Find the recommended variant (cheapest one)
            cheapest_variant = min(variants, key=lambda v: v.offer_sale_price or float('inf'))
            
            # Create variant records from ProductVariant table data
            for variant in variants:
                is_recommended = (variant.sku_id == cheapest_variant.sku_id)
                
                # Use the variant's own image URL
                variant_image_url = variant.sku_image_url or ''
                
                # Generate anonymous SKU ID
                anon_sku_id = self.client.generate_anonymous_id(variant.sku_id)
                
                # Build variant_key using property_value_definition_name with fallback to sku_property_value
                variant_key = self._build_variant_key(variant)
                
                # Prepare variant record fields
                variant_fields = {
                    'variant_key': variant_key,
                    'anon_product_id': product_id,
                    'definition_name': variant.property_value_definition_name or '',
                    'anon_sku_id': anon_sku_id,
                    'price_eur': float(variant.offer_sale_price or 0),
                    'shipping_eur': float(product.min_shipping_price or 0),
                    'total_eur': float(variant.offer_sale_price or 0) + float(product.min_shipping_price or 0),
                    'delivery_min_days': int(product.min_delivery_days or 0),
                    'delivery_max_days': int(product.max_delivery_days or 0),
                    'delivery_range': f"{product.min_delivery_days or 0}-{product.max_delivery_days or 0} days",
                    'variant_hero_image': variant_image_url,
                    'variant_images': variant_image_url,  # Use the specific variant image
                    'is_recommended': is_recommended,
                    'sync_timestamp': datetime.now().isoformat()
                }
                
                # Filter to only include fields that exist in the base
                filtered_fields = self._filter_fields(variant_fields, self.variants_fields)
                variant_records.append({'fields': filtered_fields})
            
            logger.info(f"Prepared {len(variant_records)} variant records from ProductVariant table for {product.product_id}")
            return variant_records
            
        except Exception as e:
            logger.error(f"Error preparing variant records for {product.product_id}: {e}")
            return []
    
    def _build_variant_key(self, variant: 'ProductVariant') -> str:
        """
        Build variant key using property_value_definition_name when available,
        falling back to sku_property_value when definition_name is null.
        """
        try:
            if not variant.properties or not isinstance(variant.properties, list):
                # Fallback to existing variant_key or generate one
                return variant.variant_key or f"Variant_{variant.sku_id}"
            
            # Build key from properties with proper fallback logic
            key_parts = []
            for prop in variant.properties:
                prop_name = prop.get('name', '')
                definition_name = prop.get('definition_name')
                prop_value = prop.get('value', '')
                
                if prop_name:
                    # Use definition_name if it exists and is not null, otherwise use value
                    display_value = definition_name if definition_name else prop_value
                    if display_value:
                        key_parts.append(f"{prop_name}: {display_value}")
            
            # Join all parts with " + " separator
            if key_parts:
                return " + ".join(key_parts)
            else:
                # Final fallback to existing variant_key or generate one
                return variant.variant_key or f"Variant_{variant.sku_id}"
                
        except Exception as e:
            logger.warning(f"Error building variant key for {variant.sku_id}: {e}")
            return variant.variant_key or f"Variant_{variant.sku_id}"
    
    def _filter_fields(self, fields: Dict[str, Any], available_fields: set) -> Dict[str, Any]:
        """Filter fields to only include those that exist in the Airtable base."""
        return {k: v for k, v in fields.items() if k in available_fields}
    
    def _get_recommended_variant_sku(self, product: FilteredProduct) -> Optional[str]:
        """
        Get the SKU ID of the recommended variant for a product.
        Uses the first variant (lowest price or most common combination).
        """
        try:
            # First try to get from ProductVariant table
            with get_db_session() as db:
                # Get the cheapest variant (recommended)
                variant = db.query(ProductVariant).filter(
                    ProductVariant.product_id == product.product_id
                ).order_by(
                    ProductVariant.offer_sale_price.nulls_last(),
                    ProductVariant.sku_id  # Secondary sort for consistency
                ).first()
                
                if variant:
                    return variant.sku_id
            
            # Fallback to extracting from raw_json_detail
            variants = self._extract_variants_from_json(product)
            if variants:
                # Return the SKU of the first (recommended) variant
                return variants[0].get('sku_id', '')
            
            return None
            
        except Exception as e:
            logger.warning(f"Error getting recommended variant for {product.product_id}: {e}")
            return None
    
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
    
    def _extract_specifications(self, product: FilteredProduct) -> str:
        """Extract product specifications from ae_item_properties in raw_json_detail."""
        try:
            if not product.raw_json_detail:
                return ''
            
            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
            properties = result.get('ae_item_properties', {})
            
            if not properties:
                return ''
            
            # Get the property list
            property_list = properties.get('ae_item_property', [])
            if not property_list:
                return ''
            
            # Convert to dictionary format for easy analysis
            specs = []
            for prop in property_list:
                attr_name = prop.get('attr_name', '').strip()
                attr_value = prop.get('attr_value', '').strip()
                
                if attr_name and attr_value:
                    # Format as "Name: Value" for readability
                    specs.append(f"{attr_name}: {attr_value}")
            
            # Join specifications with line breaks for multilineText field
            return '\n'.join(specs) if specs else ''
            
        except Exception as e:
            logger.warning(f"Error extracting specifications for {product.product_id}: {e}")
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
        """Extract variants from ProductVariant table (preferred) or raw_json_detail (fallback)."""
        try:
            # First try to get variants from our ProductVariant table
            with get_db_session() as db:
                db_variants = db.query(ProductVariant).filter(
                    ProductVariant.product_id == product.product_id
                ).all()
                
                if db_variants:
                    variants = []
                    for variant in db_variants:
                        # Use the first property for attribute name/value for backward compatibility
                        attribute_name = 'Default'
                        attribute_value = 'Standard'
                        
                        if variant.properties:
                            props = variant.properties if isinstance(variant.properties, list) else []
                            if props:
                                # For multi-property variants, use the variant_key as the label
                                if len(props) > 1:
                                    attribute_name = 'Multi-Property'
                                    attribute_value = variant.variant_key or 'Combined'
                                else:
                                    # Single property
                                    prop = props[0]
                                    attribute_name = prop.get('name', 'Default')
                                    attribute_value = prop.get('value', 'Standard')
                        
                        variant_dict = {
                            'sku_id': variant.sku_id,
                            'variant_key': variant.variant_key or f"{product.product_id}_{variant.sku_id}",
                            'attribute_name': attribute_name,
                            'attribute_value': attribute_value,
                            'price_eur': float(variant.offer_sale_price or 0),
                            'stock_quantity': int(variant.sku_available_stock or 0)
                        }
                        variants.append(variant_dict)
                    
                    return variants
            
            # Fallback to old method if no variants in database
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
                        
                        # Create variant_key with proper formatting
                        if attribute_name and attribute_value:
                            variant_key = f"{attribute_name}: {attribute_value}"
                
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
    Syncs Products, Variants, and Product Mapping tables.
    """
    logger.info(f"Starting Airtable sync (limit: {limit}, filter: {filter_status}, dry_run: {dry_run})")
    
    sync_engine = AirtableDataSync(dry_run=dry_run)
    
    # Sync Products table
    products_result = sync_engine.sync_products(limit=limit, filter_status=filter_status)
    
    # Sync Variants table
    variants_result = sync_engine.sync_variants(limit=limit)
    
    # Sync Product Mapping table
    mapping_result = {'created': 0, 'updated': 0}  # Default in case table doesn't exist
    try:
        mapping_result = sync_engine.sync_product_mapping(limit=limit, filter_status=filter_status)
    except Exception as e:
        logger.warning(f"Product Mapping table sync failed (table may not exist): {e}")
    
    # Sync SKU Mapping table
    sku_mapping_result = {'created': 0, 'updated': 0}  # Default in case table doesn't exist
    try:
        sku_mapping_result = sync_engine.sync_sku_mapping(limit=limit)
    except Exception as e:
        logger.warning(f"SKU Mapping table sync failed (table may not exist): {e}")
    
    result = {
        'products': products_result,
        'variants': variants_result,
        'mapping': mapping_result,
        'sku_mapping': sku_mapping_result,
        'total_created': (products_result['created'] + variants_result['created'] + 
                         mapping_result['created'] + sku_mapping_result['created']),
        'total_updated': (products_result['updated'] + variants_result['updated'] + 
                         mapping_result['updated'] + sku_mapping_result['updated'])
    }
    
    logger.info(f"Airtable sync completed: {result}")
    return result