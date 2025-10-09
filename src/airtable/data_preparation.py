"""
Airtable Data Preparation Module
Handles mapping of database data to Airtable format for Products and Variants tables.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from src.common.database import (
    get_db_session, FilteredProduct, ProductImage, ProductVideo,
    ProductStatus, ProductDetail
)
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

logger = logging.getLogger(__name__)

class AirtableDataPreparation:
    """Prepares database data for Airtable synchronization."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def prepare_products_data(self) -> List[Dict[str, Any]]:
        """
        Prepare Products table data from database.
        Returns list of records for Airtable Products table.
        """
        products_data = []
        
        with get_db_session() as db:
            # Get MASTER and UNIQUE products only
            master_unique_products = self._get_master_unique_products(db)
            
            for product in master_unique_products:
                try:
                    product_record = self._prepare_product_record(db, product)
                    if product_record:
                        products_data.append(product_record)
                except Exception as e:
                    self.logger.error(f"Error preparing product {product.product_id}: {e}")
                    continue
        
        self.logger.info(f"Prepared {len(products_data)} products for Airtable sync")
        return products_data
    
    def prepare_variants_data(self) -> List[Dict[str, Any]]:
        """
        Prepare Variants table data from database.
        Returns list of records for Airtable Variants table.
        """
        variants_data = []
        
        with get_db_session() as db:
            # Get all products that have variants (MASTER and UNIQUE)
            master_unique_products = self._get_master_unique_products(db)
            
            for product in master_unique_products:
                try:
                    product_variants = self._prepare_product_variants(db, product)
                    variants_data.extend(product_variants)
                except Exception as e:
                    self.logger.error(f"Error preparing variants for product {product.product_id}: {e}")
                    continue
        
        self.logger.info(f"Prepared {len(variants_data)} variants for Airtable sync")
        return variants_data
    
    def _get_master_unique_products(self, db: Session) -> List[FilteredProduct]:
        """Get all MASTER and UNIQUE products from the database."""
        return db.query(FilteredProduct).join(
            ProductStatus, FilteredProduct.product_id == ProductStatus.product_id
        ).filter(
            ProductStatus.status.in_(['MASTER', 'UNIQUE'])
        ).all()
    
    def _prepare_product_record(self, db: Session, product: FilteredProduct) -> Optional[Dict[str, Any]]:
        """Prepare a single product record for Airtable."""
        try:
            # Get product status
            status = db.query(ProductStatus).filter(
                ProductStatus.product_id == product.product_id
            ).first()
            
            # Get hero image (primary image)
            hero_image = self._get_hero_image(db, product.product_id)
            
            # Get gallery images
            gallery_images = self._get_gallery_images(db, product.product_id)
            
            # Get video
            video_url = self._get_product_video(db, product.product_id)
            
            # Generate anonymous product ID
            anon_product_id = self._generate_anon_product_id(product.product_id)
            
            # Get product details for description
            product_detail = db.query(ProductDetail).filter(
                ProductDetail.product_id == product.product_id
            ).first()
            
            # Get recommended variant
            recommended_variant_key = self._get_recommended_variant(db, product)
            
            return {
                'anon_product_id': anon_product_id,
                'title': product.product_title or '',
                'description': product_detail.product_description if product_detail else '',
                'hero_image': hero_image,
                'gallery_images': gallery_images,
                'video': video_url,
                'duplicate_status': status.status if status else 'UNKNOWN',
                'selected_variant': recommended_variant_key,
                # Rollup fields will be calculated by Airtable formulas
                'price': product.target_sale_price or '',
                'shipping_cost': '',  # Will be filled from selected variant
                'delivery_time': '',  # Will be filled from selected variant
                'total_cost': '',     # Will be filled from selected variant
                'sync_updated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error preparing product record for {product.product_id}: {e}")
            return None
    
    def _prepare_product_variants(self, db: Session, product: FilteredProduct) -> List[Dict[str, Any]]:
        """Prepare variant records for a single product."""
        variants = []
        
        try:
            anon_product_id = self._generate_anon_product_id(product.product_id)
            
            # For now, create a single "default" variant per product
            # This can be expanded later to handle actual variants from AliExpress
            variant_key = f"{anon_product_id}_default"
            
            # Get variant-specific images (if any)
            variant_images = self._get_variant_images(db, product.product_id)
            
            # If no variant images, inherit from product
            if not variant_images:
                variant_images = self._get_gallery_images(db, product.product_id)
            
            # Get hero image for variant
            variant_hero = self._get_variant_hero_image(db, product.product_id)
            if not variant_hero:
                variant_hero = self._get_hero_image(db, product.product_id)
            
            variant_record = {
                'variant_key': variant_key,
                'anon_product_id': anon_product_id,
                'sku_id': '',  # Not available in current schema
                'variant_label': 'Default',
                'price': product.target_sale_price or 0,
                'shipping_cost': 0,  # Not available in current schema
                'total_cost': product.target_sale_price or 0,
                'delivery_time_min': '',  # Not available in current schema
                'delivery_time_max': '',  # Not available in current schema
                'variant_hero_image': variant_hero,
                'variant_images': variant_images,
                'is_recommended': True,  # Default variant is recommended
                'sync_updated_at': datetime.now().isoformat()
            }
            
            variants.append(variant_record)
            
        except Exception as e:
            self.logger.error(f"Error preparing variants for product {product.product_id}: {e}")
        
        return variants
    
    def _get_hero_image(self, db: Session, product_id: str) -> str:
        """Get the hero/primary image for a product."""
        hero_image = db.query(ProductImage).filter(
            and_(
                ProductImage.product_id == product_id,
                ProductImage.s3_url.isnot(None),
                or_(
                    ProductImage.is_primary == True,
                    ProductImage.image_role == 'hero'
                )
            )
        ).first()
        
        if not hero_image:
            # Fallback to first available image
            hero_image = db.query(ProductImage).filter(
                and_(
                    ProductImage.product_id == product_id,
                    ProductImage.s3_url.isnot(None)
                )
            ).order_by(ProductImage.sort_index).first()
        
        return hero_image.s3_url if hero_image else ''
    
    def _get_gallery_images(self, db: Session, product_id: str) -> List[str]:
        """Get gallery images for a product."""
        gallery_images = db.query(ProductImage).filter(
            and_(
                ProductImage.product_id == product_id,
                ProductImage.s3_url.isnot(None),
                ProductImage.image_role == 'gallery'
            )
        ).order_by(ProductImage.sort_index).all()
        
        return [img.s3_url for img in gallery_images]
    
    def _get_variant_images(self, db: Session, product_id: str) -> List[str]:
        """Get variant-specific images for a product."""
        variant_images = db.query(ProductImage).filter(
            and_(
                ProductImage.product_id == product_id,
                ProductImage.s3_url.isnot(None),
                ProductImage.image_role == 'variant'
            )
        ).order_by(ProductImage.sort_index).all()
        
        return [img.s3_url for img in variant_images]
    
    def _get_variant_hero_image(self, db: Session, product_id: str) -> str:
        """Get variant hero image for a product."""
        variant_hero = db.query(ProductImage).filter(
            and_(
                ProductImage.product_id == product_id,
                ProductImage.s3_url.isnot(None),
                ProductImage.image_role == 'variant',
                ProductImage.is_primary == True
            )
        ).first()
        
        return variant_hero.s3_url if variant_hero else ''
    
    def _get_product_video(self, db: Session, product_id: str) -> str:
        """Get the first available video URL for a product."""
        video = db.query(ProductVideo).filter(
            and_(
                ProductVideo.product_id == product_id,
                ProductVideo.s3_url.isnot(None)
            )
        ).first()
        
        return video.s3_url if video else ''
    
    def _generate_anon_product_id(self, product_id: str) -> str:
        """Generate anonymous product ID from original product ID."""
        import hashlib
        return hashlib.md5(product_id.encode()).hexdigest()[:12]
    
    def _get_recommended_variant(self, db: Session, product: FilteredProduct) -> str:
        """Get the recommended variant key for a product."""
        # For now, return the default variant key
        anon_product_id = self._generate_anon_product_id(product.product_id)
        return f"{anon_product_id}_default"