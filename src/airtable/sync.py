"""
Data synchronization module for Airtable integration.
Handles the mapping of database data to Airtable format for the single Products table system.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from ..common.database import (
    FilteredProduct, ProductStatus, ProductImage, ProductVideo, ProductVariant, ProductMapping,
    get_db_session
)
from .client import AirtableClient

logger = logging.getLogger(__name__)


class AirtableDataSync:
    """
    Handles data synchronization between database and Airtable.
    Implements the single Products table system with complete image and variant data.
    """
    
    def __init__(self, dry_run: bool = False):
        self.client = AirtableClient()
        self.dry_run = dry_run
        
        # Get available fields from the Products table
        try:
            products_schema = self.client.products_table.schema()
            self.products_fields = {field.name for field in products_schema.fields}
            logger.info(f"Products table fields: {sorted(self.products_fields)}")
        except Exception as e:
            logger.error(f"Failed to get Products table schema: {e}")
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
            
            # Prepare the record fields
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
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Filter to only include fields that exist in the base
            filtered_fields = self._filter_fields(record_fields, self.products_fields)
            
            record = {'fields': filtered_fields}
            
            return record
            
        except Exception as e:
            logger.error(f"Error preparing product record for {product.product_id}: {e}")
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


def sync_to_airtable(limit: Optional[int] = None, filter_status: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Main function to sync data to Airtable.
    
    Args:
        limit: Maximum number of products to sync
        filter_status: Filter by product status (MASTER, UNIQUE)
        dry_run: If True, simulate sync without actually updating Airtable
        
    Returns:
        Dict with sync statistics
    """
    try:
        sync_engine = AirtableDataSync(dry_run=dry_run)
        
        # Sync products
        products_result = sync_engine.sync_products(limit=limit, filter_status=filter_status)
        
        # Return consolidated results
        results = {
            'products': products_result,
            'total_created': products_result['created'],
            'total_updated': products_result['updated']
        }
        
        logger.info(f"Airtable sync completed: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error in Airtable sync: {e}")
        raise