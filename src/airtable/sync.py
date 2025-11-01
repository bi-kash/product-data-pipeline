"""
Data synchronization module for Airtable integration.
Handles the mapping of database data to Airtable format for the Products + Variants table system.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from ..common.database import (
    FilteredProduct, ProductStatus, ProductImage, ProductVideo, ProductVariant, ProductMapping, SKUMapping,
    get_db_session
)
from .client import AirtableClient

logger = logging.getLogger(__name__)


class AirtableDataSync:
    """
    Handles data synchronization between database and Airtable.
    Implements the Products + Variants table system with SKU mapping.
    """
    
    def __init__(self, dry_run: bool = False):
        self.client = AirtableClient()
        self.dry_run = dry_run
        
        # Get available fields from both Products and Variants tables
        try:
            products_schema = self.client.products_table.schema()
            self.products_fields = {field.name for field in products_schema.fields}
            logger.info(f"Products table fields: {sorted(self.products_fields)}")
            
            variants_schema = self.client.variants_table.schema()
            self.variants_fields = {field.name for field in variants_schema.fields}
            logger.info(f"Variants table fields: {sorted(self.variants_fields)}")
        except Exception as e:
            logger.error(f"Failed to get table schemas: {e}")
            raise
    
    def sync_products(self, limit: Optional[int] = None, filter_status: Optional[str] = None) -> Dict[str, int]:
        """
        Sync filtered products to Airtable Products table.
        
        Args:
            limit: Maximum number of products to sync
            filter_status: Filter by product status (MASTER, UNIQUE)
            
        Returns:
            Dict with sync statistics
        """
        logger.info(f"Starting products sync (limit: {limit}, filter: {filter_status}, dry_run: {self.dry_run})")
        
        with get_db_session() as db:
            # Base query for products to sync
            query = db.query(FilteredProduct).join(ProductStatus)
            
            # Apply status filter if specified
            if filter_status:
                query = query.filter(ProductStatus.status == filter_status)
            else:
                # Default: only sync MASTER and UNIQUE products
                query = query.filter(ProductStatus.status.in_(['MASTER', 'UNIQUE']))
            
            # Apply limit if specified
            if limit:
                query = query.limit(limit)
            
            products = query.all()
            
            logger.info(f"Found {len(products)} products to sync")
            
            if not products:
                return {'created': 0, 'updated': 0}
            
            # Prepare records for sync
            records_to_sync = []
            for product in products:
                record = self._prepare_product_record(db, product)
                if record:
                    records_to_sync.append(record)
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would sync {len(records_to_sync)} product records")
                return {'created': 0, 'updated': 0}
            
            # Perform the sync using upsert by anon_product_id
            results = self.client.upsert_products_by_anonymous_id(records_to_sync)
            
            # Update ProductMapping table with new records
            self._update_product_mapping(db, results)
            
            logger.info(f"Products sync completed: {results}")
            return results
    
    def sync_variants(self, limit: Optional[int] = None) -> Dict[str, int]:
        """
        Sync product variants to Airtable Variants table.
        
        Args:
            limit: Maximum number of variants to sync
            
        Returns:
            Dict with sync statistics
        """
        logger.info(f"Starting variants sync (limit: {limit}, dry_run: {self.dry_run})")
        
        with get_db_session() as db:
            # Query for variants of products that are being synced (MASTER/UNIQUE only)
            query = db.query(ProductVariant).join(
                FilteredProduct, ProductVariant.product_id == FilteredProduct.product_id
            ).join(ProductStatus)
            
            # Only sync variants for MASTER and UNIQUE products
            query = query.filter(ProductStatus.status.in_(['MASTER', 'UNIQUE']))
            
            # Apply limit if specified
            if limit:
                query = query.limit(limit)
            
            variants = query.all()
            
            logger.info(f"Found {len(variants)} variants to sync")
            
            if not variants:
                return {'created': 0, 'updated': 0}
            
            # Prepare records for sync
            records_to_sync = []
            for variant in variants:
                record = self._prepare_variant_record(db, variant)
                if record:
                    records_to_sync.append(record)
            
            if self.dry_run:
                logger.info(f"DRY RUN: Would sync {len(records_to_sync)} variant records")
                return {'created': 0, 'updated': 0}
            
            # Perform the sync using upsert by anon_variant_id
            results = self.client.upsert_variants_by_anonymous_sku_id(records_to_sync)
            
            # Update SKUMapping table with new records
            self._update_sku_mapping(db, results)
            
            logger.info(f"Variants sync completed: {results}")
            return results
    
    def _prepare_product_record(self, db: Session, product: FilteredProduct) -> Optional[Dict]:
        """
        Prepare a single product record for Airtable sync.
        
        Args:
            db: Database session
            product: FilteredProduct to prepare
            
        Returns:
            Dict with Airtable record data or None if preparation fails
        """
        try:
            # Get product status
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
            
            # Get images by role
            images = db.query(ProductImage).filter(
                ProductImage.product_id == product.product_id,
                ProductImage.s3_url.isnot(None)
            ).order_by(ProductImage.sort_index).all()
            
            # Organize images by role
            hero_image = ''
            gallery_images = []
            other_images = []
            variant_images = []
            
            for img in images:
                if img.image_role == 'hero' or img.is_primary:
                    hero_image = img.s3_url
                elif img.image_role == 'gallery':
                    gallery_images.append(img.s3_url)
                elif img.image_role == 'other':
                    other_images.append(img.s3_url)
                elif img.image_role == 'variant':
                    variant_images.append(img.s3_url)
            
            # Convert lists to comma-separated strings
            gallery_images_str = ', '.join(gallery_images) if gallery_images else ''
            other_images_str = ', '.join(other_images) if other_images else ''
            variant_images_str = ', '.join(variant_images) if variant_images else ''
            
            # Get video
            video = db.query(ProductVideo).filter(
                ProductVideo.product_id == product.product_id,
                ProductVideo.s3_url.isnot(None)
            ).first()
            
            video_url = video.s3_url if video else ''
            
            # Get pricing info from variants
            price_info = self._extract_pricing_from_variants(product)
            
            # Find the best variant to use as selected_variant (lowest price or first available)
            best_variant = self._find_best_variant(db, product.product_id)
            selected_variant_anon_id = ''
            if best_variant:
                selected_variant_anon_id = self.client.generate_anonymous_sku_id(best_variant.sku_id)
            
            # Prepare the record fields (original structure + selected_variant)
            record_fields = {
                'anon_product_id': product_id,
                'title': product.product_title or '',
                'description': description,
                'specifications': specifications,
                'hero_image': hero_image,
                'gallery_images': gallery_images_str,
                'other_images': other_images_str,
                'variant_images': variant_images_str,
                'video': video_url,
                'duplicate_status': status_info.status,
                'price_eur': price_info.get('min_price', float(product.target_sale_price or 0)),
                'shipping_eur': float(product.min_shipping_price or 0),
                'total_eur': price_info.get('min_price', float(product.target_sale_price or 0)) + float(product.min_shipping_price or 0),
                'delivery_time': f"{product.min_delivery_days or 0}-{product.max_delivery_days or 0} days",
                'selected_variant': selected_variant_anon_id,
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Filter to only include fields that exist in the base
            filtered_fields = self._filter_fields(record_fields, self.products_fields)
            
            record = {'fields': filtered_fields}
            
            return record
            
        except Exception as e:
            logger.error(f"Error preparing product record for {product.product_id}: {e}")
            return None
    
    def _prepare_variant_record(self, db: Session, variant: ProductVariant) -> Optional[Dict]:
        """
        Prepare a single variant record for Airtable Variants table.
        
        Args:
            db: Database session
            variant: ProductVariant to prepare
            
        Returns:
            Dict with Airtable record data or None if preparation fails
        """
        try:
            # Generate anonymous IDs
            anon_sku_id = self.client.generate_anonymous_sku_id(variant.sku_id)
            anon_product_id = self.client.generate_anonymous_id(variant.product_id)
            
            # Get variant-specific image if available
            variant_image = db.query(ProductImage).filter(
                ProductImage.product_id == variant.product_id,
                ProductImage.sku_id == variant.sku_id,
                ProductImage.s3_url.isnot(None)
            ).first()
            
            variant_image_url = variant_image.s3_url if variant_image else ''
            
            # Calculate total price with shipping
            variant_price = float(variant.offer_sale_price or 0)
            
            # Get the product for shipping info
            product = db.query(FilteredProduct).filter(
                FilteredProduct.product_id == variant.product_id
            ).first()
            
            shipping_price = float(product.min_shipping_price or 0) if product else 0
            total_price = variant_price + shipping_price
            
            # Prepare the record fields for Variants table
            record_fields = {
                'anon_variant_id': anon_sku_id,
                'anon_product_id': anon_product_id,
                'variant_key': variant.variant_key or '',
                'price_eur': variant_price,
                'shipping_eur': shipping_price,
                'total_eur': total_price,
                'stock': variant.sku_available_stock or 0,
                'variant_image': variant_image_url,
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Filter to only include fields that exist in the base
            filtered_fields = self._filter_fields(record_fields, self.variants_fields)
            
            record = {'fields': filtered_fields}
            
            return record
            
        except Exception as e:
            logger.error(f"Error preparing variant record for {variant.sku_id}: {e}")
            return None
    
    def _find_best_variant(self, db: Session, product_id: str) -> Optional[ProductVariant]:
        """
        Find the best variant for a product (lowest price, or first available if prices are equal).
        
        Args:
            db: Database session
            product_id: Product ID to find variants for
            
        Returns:
            Best ProductVariant or None if no variants found
        """
        try:
            variants = db.query(ProductVariant).filter(
                ProductVariant.product_id == product_id,
                ProductVariant.offer_sale_price.isnot(None)
            ).order_by(
                ProductVariant.offer_sale_price.asc(),
                ProductVariant.sku_id.asc()
            ).all()
            
            return variants[0] if variants else None
            
        except Exception as e:
            logger.debug(f"Error finding best variant for {product_id}: {e}")
            return None
    
    def _extract_description(self, product: FilteredProduct) -> str:
        """Extract product description from raw JSON data."""
        try:
            if not product.raw_json_detail:
                return ''
            
            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
            base_info = result.get('ae_item_base_info_dto', {})
            
            # Try to get subject (title) as description if no detail available
            subject = base_info.get('subject', '')
            
            return subject or ''
            
        except Exception as e:
            logger.debug(f"Error extracting description: {e}")
            return ''
    
    def _extract_specifications(self, product: FilteredProduct) -> str:
        """Extract product specifications from ae_item_properties."""
        try:
            if not product.raw_json_detail:
                return ''
            
            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})
            properties = result.get('ae_item_properties', {}).get('ae_item_property', [])
            
            if not properties:
                return ''
            
            # Format specifications as "Attribute Name: Attribute Value"
            specs = []
            for prop in properties:
                attr_name = prop.get('attr_name', '')
                attr_value = prop.get('attr_value', '')
                
                # Skip empty or invalid properties
                if not attr_name or not attr_value or attr_name == 'Choice':
                    continue
                
                specs.append(f"{attr_name}: {attr_value}")
            
            return '\n'.join(specs)
            
        except Exception as e:
            logger.debug(f"Error extracting specifications: {e}")
            return ''
    
    def _extract_pricing_from_variants(self, product: FilteredProduct) -> Dict[str, float]:
        """Extract pricing information from product variants."""
        try:
            with get_db_session() as db:
                variants = db.query(ProductVariant).filter(
                    ProductVariant.product_id == product.product_id
                ).all()
                
                if not variants:
                    return {'min_price': float(product.target_sale_price or 0)}
                
                prices = [float(v.offer_sale_price) for v in variants if v.offer_sale_price]
                
                if prices:
                    return {'min_price': min(prices)}
                else:
                    return {'min_price': float(product.target_sale_price or 0)}
                    
        except Exception as e:
            logger.debug(f"Error extracting pricing: {e}")
            return {'min_price': float(product.target_sale_price or 0)}
    
    def _find_best_variant(self, db: Session, product_id: str):
        """Find the best variant (lowest price) for a product."""
        try:
            variants = db.query(ProductVariant).filter(
                ProductVariant.product_id == product_id
            ).all()
            
            if not variants:
                return None
            
            # Find variant with lowest price
            best_variant = min(variants, key=lambda v: float(v.offer_sale_price or 0))
            return best_variant
            
        except Exception as e:
            logger.debug(f"Error finding best variant: {e}")
            return None
    
    def _prepare_variant_record(self, db: Session, variant: ProductVariant) -> Optional[Dict]:
        """
        Prepare a single variant record for Airtable sync.
        
        Args:
            db: Database session
            variant: ProductVariant to prepare
            
        Returns:
            Dict with Airtable record data or None if preparation fails
        """
        try:
            # Generate anonymous IDs
            anon_sku_id = self.client.generate_anonymous_sku_id(variant.sku_id)
            anon_product_id = self.client.generate_anonymous_id(variant.product_id)
            
            # Get variant image (hero image for this variant)
            variant_image = db.query(ProductImage).filter(
                ProductImage.product_id == variant.product_id,
                ProductImage.sku_id == variant.sku_id,
                ProductImage.s3_url.isnot(None)
            ).first()
            
            hero_image_url = variant_image.s3_url if variant_image else ''
            
            # Get variant-specific image (not hero)
            variant_specific_image = db.query(ProductImage).filter(
                ProductImage.product_id == variant.product_id,
                ProductImage.sku_id == variant.sku_id,
                ProductImage.image_role == 'variant',
                ProductImage.s3_url.isnot(None)
            ).first()
            
            variant_image_url = variant_specific_image.s3_url if variant_specific_image else ''
            
            # Prepare variant record fields
            record_fields = {
                'anon_sku_id': anon_sku_id,
                'anon_product_id': anon_product_id,
                'variant_key': variant.variant_key or '',
                'hero_image': hero_image_url,
                'variant_image': variant_image_url,
                'price_eur': float(variant.offer_sale_price or 0),
                'shipping_eur': 0,  # Variants inherit shipping from product
                'total_eur': float(variant.offer_sale_price or 0),
                'stock': variant.sku_available_stock or 0,
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Filter to only include fields that exist in the base
            filtered_fields = self._filter_fields(record_fields, self.variants_fields)
            
            record = {'fields': filtered_fields}
            
            return record
            
        except Exception as e:
            logger.error(f"Error preparing variant record for {variant.sku_id}: {e}")
            return None
    
    def _filter_fields(self, fields: Dict[str, Any], available_fields: set) -> Dict[str, Any]:
        """Filter fields to only include those that exist in the Airtable base."""
        return {k: v for k, v in fields.items() if k in available_fields}
    
    def _update_product_mapping(self, db: Session, sync_results: Dict) -> None:
        """Update ProductMapping table with sync results."""
        try:
            if 'records' not in sync_results:
                return
            
            for record_data in sync_results['records']:
                record_id = record_data.get('id')
                fields = record_data.get('fields', {})
                anon_product_id = fields.get('anon_product_id')
                
                if not record_id or not anon_product_id:
                    continue
                
                # Get the real product_id from the anonymous ID
                # First try to reverse it from existing mapping
                real_product_id = self.client.reverse_anonymous_id(anon_product_id)
                
                # If not found, we need to find it from the original sync data
                if not real_product_id:
                    # Find the product that generated this anonymous ID
                    all_products = db.query(FilteredProduct).all()
                    for prod in all_products:
                        if self.client.generate_anonymous_id(prod.product_id) == anon_product_id:
                            real_product_id = prod.product_id
                            break
                
                if not real_product_id:
                    logger.warning(f"Could not find real product ID for anonymous ID: {anon_product_id}")
                    continue
                
                # Update or create ProductMapping record
                mapping = db.query(ProductMapping).filter(
                    ProductMapping.anon_product_id == anon_product_id
                ).first()
                
                if mapping:
                    mapping.airtable_record_id = record_id
                    mapping.updated_at = datetime.now()
                else:
                    mapping = ProductMapping(
                        anon_product_id=anon_product_id,
                        product_id=real_product_id,
                        airtable_record_id=record_id
                    )
                    db.add(mapping)
            
            db.commit()
            logger.info(f"Updated ProductMapping for {len(sync_results['records'])} records")
            
        except Exception as e:
            logger.error(f"Error updating ProductMapping: {e}")
            db.rollback()
    
    def _update_sku_mapping(self, db: Session, sync_results: Dict) -> None:
        """Update SKUMapping table with sync results."""
        try:
            if 'records' not in sync_results:
                return
            
            for record_data in sync_results['records']:
                record_id = record_data.get('id')
                fields = record_data.get('fields', {})
                anon_sku_id = fields.get('anon_sku_id')
                anon_product_id = fields.get('anon_product_id')
                
                if not record_id or not anon_sku_id:
                    continue
                
                # Get the real SKU ID from the anonymous ID
                real_sku_id = self.client.reverse_anonymous_sku_id(anon_sku_id)
                
                # If not found, find it from variants
                if not real_sku_id:
                    all_variants = db.query(ProductVariant).all()
                    for variant in all_variants:
                        if self.client.generate_anonymous_sku_id(variant.sku_id) == anon_sku_id:
                            real_sku_id = variant.sku_id
                            break
                
                if not real_sku_id:
                    logger.warning(f"Could not find real SKU ID for anonymous ID: {anon_sku_id}")
                    continue
                
                # Get product_id from SKU
                variant = db.query(ProductVariant).filter(
                    ProductVariant.sku_id == real_sku_id
                ).first()
                
                if not variant:
                    logger.warning(f"Could not find variant for SKU ID: {real_sku_id}")
                    continue
                
                # Update or create SKUMapping record
                mapping = db.query(SKUMapping).filter(
                    SKUMapping.anon_sku_id == anon_sku_id
                ).first()
                
                if mapping:
                    mapping.airtable_record_id = record_id
                    mapping.updated_at = datetime.now()
                else:
                    mapping = SKUMapping(
                        anon_sku_id=anon_sku_id,
                        sku_id=real_sku_id,
                        product_id=variant.product_id,
                        airtable_record_id=record_id
                    )
                    db.add(mapping)
            
            db.commit()
            logger.info(f"Updated SKUMapping for {len(sync_results['records'])} records")
            
        except Exception as e:
            logger.error(f"Error updating SKUMapping: {e}")
            db.rollback()


def sync_to_airtable(limit: Optional[int] = None, filter_status: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Main function to sync data to Airtable Products + Variants tables.
    
    Args:
        limit: Maximum number of products to sync
        filter_status: Filter by product status (MASTER, UNIQUE)
        dry_run: If True, simulate sync without actually updating Airtable
        
    Returns:
        Dict with sync statistics
    """
    try:
        sync_engine = AirtableDataSync(dry_run=dry_run)
        
        # Sync products first
        products_result = sync_engine.sync_products(limit=limit, filter_status=filter_status)
        
        # Sync variants (limit applies to products, so variants might be more numerous)
        variants_limit = limit * 10 if limit else None  # Allow more variants than products
        variants_result = sync_engine.sync_variants(limit=variants_limit)
        
        # Return consolidated results
        results = {
            'products': products_result,
            'variants': variants_result,
            'total_created': products_result['created'] + variants_result['created'],
            'total_updated': products_result['updated'] + variants_result['updated']
        }
        
        logger.info(f"Airtable sync completed: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error in Airtable sync: {e}")
        raise