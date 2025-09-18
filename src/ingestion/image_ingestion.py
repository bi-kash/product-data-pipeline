"""
Image ingestion module for extracting and storing product images.

This module processes raw product data to extract all available image URLs
(hero, gallery, and variant images) and stores them in the product_images table.
"""

import logging
from typing import List, Dict, Optional
from src.common.database import get_db_session, ProductImage, FilteredProduct, Product
from src.common.config import get_env

# Set up logging
logger = logging.getLogger(__name__)


class ImageIngestionEngine:
    """
    Engine for extracting and storing product images from raw JSON data.
    """

    def __init__(self):
        """Initialize the image ingestion engine."""
        logger.info("Image ingestion engine initialized")

    def ingest_all_images(self) -> Dict:
        """
        Process all filtered products and extract their images.
        
        Returns:
            Dict with ingestion statistics
        """
        stats = {
            'products_processed': 0,
            'images_extracted': 0,
            'hero_images': 0,
            'gallery_images': 0,
            'variant_images': 0,
            'errors': 0
        }

        with get_db_session() as db:
            # Get all filtered products
            filtered_products = db.query(FilteredProduct).all()
            logger.info(f"Found {len(filtered_products)} filtered products to process")

            for filtered_product in filtered_products:
                try:
                    # Get the original product data
                    product = db.query(Product).filter(
                        Product.product_id == filtered_product.product_id
                    ).first()

                    if not product:
                        logger.warning(f"Product {filtered_product.product_id} not found in products table")
                        continue

                    # Extract images for this product
                    images_extracted = self._extract_product_images(product, db)
                    stats['images_extracted'] += images_extracted['total']
                    stats['hero_images'] += images_extracted['hero']
                    stats['gallery_images'] += images_extracted['gallery']
                    stats['variant_images'] += images_extracted['variant']
                    stats['products_processed'] += 1

                    logger.info(f"Processed product {product.product_id}: "
                              f"{images_extracted['total']} images extracted")

                except Exception as e:
                    logger.error(f"Error processing product {filtered_product.product_id}: {e}")
                    stats['errors'] += 1

            # Commit all changes
            db.commit()

        logger.info(f"Image ingestion completed: {stats}")
        return stats

    def ingest_product_images(self, product_id: str) -> Dict:
        """
        Extract images for a specific product.
        
        Args:
            product_id: Product ID to process
            
        Returns:
            Dict with extraction results
        """
        with get_db_session() as db:
            product = db.query(Product).filter(Product.product_id == product_id).first()
            
            if not product:
                raise ValueError(f"Product {product_id} not found")

            results = self._extract_product_images(product, db)
            db.commit()
            
            return results

    def _extract_product_images(self, product: Product, db) -> Dict:
        """
        Extract all images for a product from its raw JSON data.
        
        Args:
            product: Product object with raw_json_detail
            db: Database session
            
        Returns:
            Dict with extraction statistics
        """
        stats = {'total': 0, 'hero': 0, 'gallery': 0, 'variant': 0}

        if not product.raw_json_detail:
            logger.warning(f"No raw JSON data for product {product.product_id}")
            return stats

        try:
            # Clear existing images for this product
            db.query(ProductImage).filter(
                ProductImage.product_id == product.product_id
            ).delete()

            result = product.raw_json_detail.get('aliexpress_ds_product_get_response', {}).get('result', {})

            # First, extract variant context from SKU data
            variant_context = self._extract_variant_context(result)

            # Track sort index across all image types
            current_sort_index = 0

            # Extract hero image (main product image)
            hero_extracted, current_sort_index = self._extract_hero_image(product, variant_context, current_sort_index, db)
            stats['hero'] += hero_extracted
            stats['total'] += hero_extracted

            # Extract gallery images
            gallery_extracted, current_sort_index = self._extract_gallery_images(result, product.product_id, variant_context, current_sort_index, db)
            stats['gallery'] += gallery_extracted
            stats['total'] += gallery_extracted

            # Extract variant images (if any)
            variant_extracted, current_sort_index = self._extract_variant_images(result, product.product_id, variant_context, current_sort_index, db)
            stats['variant'] += variant_extracted
            stats['total'] += variant_extracted

            logger.debug(f"Extracted images for product {product.product_id}: {stats}")

        except Exception as e:
            logger.error(f"Error extracting images for product {product.product_id}: {e}")

        return stats

    def _extract_variant_context(self, result_data: Dict) -> Dict:
        """
        Extract variant context information from SKU data.
        Only considers properties that have sku_image for variant keys.
        
        Args:
            result_data: Product result data from API
            
        Returns:
            Dict with variant context including primary property details and image mappings
        """
        variant_context = {
            'primary_property': None,  # Primary property details
            'all_properties': [],
            'image_to_property_map': {},
        }

        try:
            # Check SKU info for variant properties
            sku_info = result_data.get('ae_item_sku_info_dtos', {})
            skus = sku_info.get('ae_item_sku_info_d_t_o', [])
            
            image_properties = []
            
            for sku in skus:
                sku_props = sku.get('ae_sku_property_dtos', {}).get('ae_sku_property_d_t_o', [])
                
                for prop in sku_props:
                    sku_property_value = prop.get('sku_property_value')
                    sku_property_name = prop.get('sku_property_name', '')
                    sku_image = prop.get('sku_image')
                    property_value_definition_name = prop.get('property_value_definition_name', '')
                    
                    # Only process properties that have both sku_image and sku_property_value
                    if sku_image and sku_property_value:
                        property_details = {
                            'property_value': str(sku_property_value),
                            'property_name': sku_property_name,
                            'property_value_definition_name': property_value_definition_name,
                            'image': sku_image
                        }
                        
                        image_properties.append(property_details)
                        variant_context['all_properties'].append(property_details)
                        
                        # Map image to property details
                        variant_context['image_to_property_map'][sku_image] = property_details

            # Set primary property (first image-based property found)
            if image_properties:
                variant_context['primary_property'] = image_properties[0]

        except Exception as e:
            logger.error(f"Error extracting variant context: {e}")

        return variant_context

    def _extract_hero_image(self, product: Product, variant_context: Dict, start_sort_index: int, db) -> tuple[int, int]:
        """
        Extract the main/hero image for a product.
        
        Args:
            product: Product object
            variant_context: Variant context information
            start_sort_index: Starting sort index for this image type
            db: Database session
            
        Returns:
            Tuple of (number of hero images extracted, next available sort index)
        """
        if not product.product_main_image_url:
            logger.debug(f"No main image URL for product {product.product_id}")
            return 0, start_sort_index

        try:
            # Determine property details for hero image
            property_value = None
            property_name = None
            property_value_definition_name = None
            
            # Check if this hero image matches a specific property
            if product.product_main_image_url in variant_context.get('image_to_property_map', {}):
                prop_details = variant_context['image_to_property_map'][product.product_main_image_url]
                property_value = prop_details['property_value']
                property_name = prop_details['property_name']
                property_value_definition_name = prop_details['property_value_definition_name']
            else:
                # Use primary property if available
                primary_prop = variant_context.get('primary_property')
                if primary_prop:
                    property_value = primary_prop['property_value']
                    property_name = primary_prop['property_name']
                    property_value_definition_name = primary_prop['property_value_definition_name']

            hero_image = ProductImage(
                product_id=product.product_id,
                image_url=product.product_main_image_url,
                image_role='hero',
                property_value=property_value,
                property_name=property_name,
                property_value_definition_name=property_value_definition_name,
                sort_index=start_sort_index,
                is_primary=True
            )
            
            db.add(hero_image)
            logger.debug(f"Added hero image for product {product.product_id} with property: {property_value}")
            return 1, start_sort_index + 1

        except Exception as e:
            logger.error(f"Error adding hero image for product {product.product_id}: {e}")
            return 0, start_sort_index

    def _extract_gallery_images(self, result_data: Dict, product_id: str, variant_context: Dict, start_sort_index: int, db) -> tuple[int, int]:
        """
        Extract gallery images from the multimedia info.
        
        Args:
            result_data: Product result data from API
            product_id: Product ID
            variant_context: Variant context information
            start_sort_index: Starting sort index for this image type
            db: Database session
            
        Returns:
            Tuple of (number of gallery images extracted, next available sort index)
        """
        try:
            # Get multimedia info
            multimedia_info = result_data.get('ae_multimedia_info_dto', {})
            image_urls_str = multimedia_info.get('image_urls', '')

            if not image_urls_str:
                logger.debug(f"No gallery images found for product {product_id}")
                return 0, start_sort_index

            # Split the semicolon-separated URLs
            gallery_urls = [url.strip() for url in image_urls_str.split(';') if url.strip()]
            
            images_added = 0
            current_sort_index = start_sort_index
            
            for index, image_url in enumerate(gallery_urls):
                try:
                    # Determine property details for gallery image
                    property_value = None
                    property_name = None
                    property_value_definition_name = None
                    
                    # Check if this gallery image matches a specific property
                    if image_url in variant_context.get('image_to_property_map', {}):
                        prop_details = variant_context['image_to_property_map'][image_url]
                        property_value = prop_details['property_value']
                        property_name = prop_details['property_name']
                        property_value_definition_name = prop_details['property_value_definition_name']
                    else:
                        # Use primary property if available
                        primary_prop = variant_context.get('primary_property')
                        if primary_prop:
                            property_value = primary_prop['property_value']
                            property_name = primary_prop['property_name']
                            property_value_definition_name = primary_prop['property_value_definition_name']

                    gallery_image = ProductImage(
                        product_id=product_id,
                        image_url=image_url,
                        image_role='gallery',
                        property_value=property_value,
                        property_name=property_name,
                        property_value_definition_name=property_value_definition_name,
                        sort_index=current_sort_index,
                        is_primary=False
                    )
                    
                    db.add(gallery_image)
                    images_added += 1
                    current_sort_index += 1

                except Exception as e:
                    logger.error(f"Error adding gallery image {index} for product {product_id}: {e}")

            logger.debug(f"Added {images_added} gallery images for product {product_id}")
            return images_added, current_sort_index

        except Exception as e:
            logger.error(f"Error extracting gallery images for product {product_id}: {e}")
            return 0, start_sort_index

    def _extract_variant_images(self, result_data: Dict, product_id: str, variant_context: Dict, start_sort_index: int, db) -> tuple[int, int]:
        """
        Extract variant-specific images from SKU properties.
        
        Args:
            result_data: Product result data from API
            product_id: Product ID
            variant_context: Variant context information
            start_sort_index: Starting sort index for this image type
            db: Database session
            
        Returns:
            Tuple of (number of variant images extracted, next available sort index)
        """
        try:
            # Check for variant-specific image properties in SKUs
            sku_info = result_data.get('ae_item_sku_info_dtos', {})
            skus = sku_info.get('ae_item_sku_info_d_t_o', [])
            
            images_added = 0
            variant_images_seen = set()  # Track unique image URLs to avoid duplicates
            current_sort_index = start_sort_index
            
            for sku_index, sku in enumerate(skus):
                # Look for image-related properties in SKU attributes
                sku_props = sku.get('ae_sku_property_dtos', {}).get('ae_sku_property_d_t_o', [])
                
                for prop in sku_props:
                    sku_image = prop.get('sku_image')
                    sku_property_value = prop.get('sku_property_value')
                    sku_property_name = prop.get('sku_property_name', '')
                    property_value_definition_name = prop.get('property_value_definition_name', '')
                    
                    # If this property has both an image and a property value, it's a variant image
                    if sku_image and sku_property_value:
                        # Skip if we've already seen this image URL
                        if sku_image in variant_images_seen:
                            continue
                            
                        variant_images_seen.add(sku_image)
                        
                        try:
                            variant_image = ProductImage(
                                product_id=product_id,
                                image_url=sku_image,
                                image_role='variant',
                                property_value=str(sku_property_value),
                                property_name=sku_property_name,
                                property_value_definition_name=property_value_definition_name,
                                sort_index=current_sort_index,
                                is_primary=False
                            )
                            
                            db.add(variant_image)
                            images_added += 1
                            current_sort_index += 1
                            
                            logger.debug(f"Added variant image for product {product_id}: "
                                       f"property_value='{sku_property_value}' "
                                       f"property_name='{sku_property_name}' "
                                       f"definition='{property_value_definition_name}' "
                                       f"sort_index={current_sort_index-1} "
                                       f"url={sku_image}")

                        except Exception as e:
                            logger.error(f"Error adding variant image for product {product_id}: {e}")

            if images_added > 0:
                logger.debug(f"Added {images_added} variant images for product {product_id}")

            return images_added, current_sort_index

        except Exception as e:
            logger.error(f"Error extracting variant images for product {product_id}: {e}")
            return 0, start_sort_index

    def get_product_images(self, product_id: str) -> List[Dict]:
        """
        Get all images for a specific product.
        
        Args:
            product_id: Product ID to get images for
            
        Returns:
            List of image dictionaries
        """
        with get_db_session() as db:
            images = db.query(ProductImage).filter(
                ProductImage.product_id == product_id
            ).order_by(ProductImage.sort_index).all()
            
            return [
                {
                    'id': img.id,
                    'image_url': img.image_url,
                    'image_role': img.image_role,
                    'property_value': img.property_value,
                    'property_name': img.property_name,
                    'property_value_definition_name': img.property_value_definition_name,
                    'sort_index': img.sort_index,
                    'width': img.width,
                    'height': img.height,
                    'is_primary': img.is_primary
                }
                for img in images
            ]

    def clear_product_images(self, product_id: str) -> int:
        """
        Clear all images for a specific product.
        
        Args:
            product_id: Product ID to clear images for
            
        Returns:
            Number of images deleted
        """
        with get_db_session() as db:
            deleted_count = db.query(ProductImage).filter(
                ProductImage.product_id == product_id
            ).delete()
            
            db.commit()
            logger.info(f"Deleted {deleted_count} images for product {product_id}")
            return deleted_count


def main():
    """Main function for running image ingestion from command line."""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        engine = ImageIngestionEngine()
        
        if command == "ingest":
            # Ingest all images
            stats = engine.ingest_all_images()
            print(f"Image ingestion completed: {stats}")
            
        elif command == "product" and len(sys.argv) > 2:
            # Ingest images for specific product
            product_id = sys.argv[2]
            try:
                results = engine.ingest_product_images(product_id)
                print(f"Images extracted for product {product_id}: {results}")
            except ValueError as e:
                print(f"Error: {e}")
                
        elif command == "list" and len(sys.argv) > 2:
            # List images for specific product
            product_id = sys.argv[2]
            images = engine.get_product_images(product_id)
            print(f"Images for product {product_id}:")
            for img in images:
                print(f"  {img['image_role']}: {img['image_url']} (index: {img['sort_index']})")
                
        elif command == "clear" and len(sys.argv) > 2:
            # Clear images for specific product
            product_id = sys.argv[2]
            deleted = engine.clear_product_images(product_id)
            print(f"Deleted {deleted} images for product {product_id}")
            
        else:
            print("Usage:")
            print("  python -m src.ingestion.image_ingestion ingest")
            print("  python -m src.ingestion.image_ingestion product <product_id>")
            print("  python -m src.ingestion.image_ingestion list <product_id>")
            print("  python -m src.ingestion.image_ingestion clear <product_id>")
    else:
        print("No command specified. Use 'ingest' to process all products.")


if __name__ == "__main__":
    main()