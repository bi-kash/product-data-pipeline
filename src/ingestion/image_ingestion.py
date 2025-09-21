"""
Image ingestion module for extracting and storing product images.

This module processes raw product data to extract all available image URLs
(hero, gallery, and variant images) and stores them in the product_images table.
"""

import logging
from typing import List, Dict, Optional
from src.common.database import get_db_session, ProductImage, FilteredProduct, Product
from src.common.config import get_env
from src.ingestion.image_download import ImageDownloader

# Set up logging
logger = logging.getLogger(__name__)


class ImageIngestionEngine:
    """
    Engine for extracting and storing product images from raw JSON data.
    """

    def __init__(self, download_images: bool = False, download_dir: str = None):
        """
        Initialize the image ingestion engine.
        
        Args:
            download_images: Whether to automatically download images
            download_dir: Directory to store downloaded images
        """
        self.download_images = download_images
        self.image_downloader = ImageDownloader(download_dir) if download_images else None
        logger.info(f"Image ingestion engine initialized (download_images: {download_images})")

    def _process_image_url(self, image_url: str, product_id: str, sku_id: str = None, 
                          image_role: str = None) -> Dict:
        """
        Process an image URL, optionally downloading it and calculating pHash and dimensions.
        
        Args:
            image_url: URL of the image
            product_id: Product ID
            sku_id: SKU ID (optional)
            image_role: Image role (hero, gallery, variant)
            
        Returns:
            Dict with local_file_path, phash, download_status, width, and height
        """
        if self.download_images and self.image_downloader:
            local_path, phash, status, width, height = self.image_downloader.download_image(
                image_url, product_id, sku_id, image_role
            )
            return {
                'local_file_path': local_path,
                'phash': phash,
                'download_status': status,
                'width': width,
                'height': height
            }
        else:
            return {
                'local_file_path': None,
                'phash': None,
                'download_status': 'pending',
                'width': None,
                'height': None
            }

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

            # DEDUPLICATION STRATEGY: Check if hero image is also a variant
            # If so, store it only as variant with is_primary=True
            hero_url = self._get_hero_image_url(result)
            variant_urls = self._get_variant_image_urls(variant_context)
            
            # Determine if hero is also a variant
            hero_is_variant = hero_url and hero_url in variant_urls
            
            if hero_is_variant:
                logger.debug(f"Hero image is also a variant for product {product.product_id}, will store as variant only")
                # Extract variant images (hero will be handled as variant with is_primary=True)
                variant_extracted, current_sort_index = self._extract_variant_images_with_primary(result, product.product_id, variant_context, hero_url, current_sort_index, db)
                stats['variant'] += variant_extracted
                stats['total'] += variant_extracted
                stats['hero'] = 0  # Hero count is 0 since it's stored as variant
            else:
                # Extract hero image (normal case when it's not a variant)
                hero_extracted, current_sort_index = self._extract_hero_image(result, product.product_id, variant_context, current_sort_index, db)
                stats['hero'] += hero_extracted
                stats['total'] += hero_extracted

                # Extract variant images (normal case)
                variant_extracted, current_sort_index = self._extract_variant_images(result, product.product_id, variant_context, current_sort_index, db)
                stats['variant'] += variant_extracted
                stats['total'] += variant_extracted

            # Extract gallery images (remaining images from image_urls, excluding hero and variant images)
            gallery_extracted, current_sort_index = self._extract_gallery_images(result, product.product_id, variant_context, current_sort_index, db)
            stats['gallery'] += gallery_extracted
            stats['total'] += gallery_extracted

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
            Dict with variant context including primary property details, image mappings, and SKU mappings
        """
        variant_context = {
            'primary_property': None,  # Primary property details
            'all_properties': [],
            'image_to_property_map': {},
            'image_to_sku_map': {},  # Map image URLs to SKU IDs
        }

        try:
            # Check SKU info for variant properties
            sku_info = result_data.get('ae_item_sku_info_dtos', {})
            skus = sku_info.get('ae_item_sku_info_d_t_o', [])
            
            image_properties = []
            
            for sku in skus:
                sku_id = sku.get('sku_id')
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
                            'image': sku_image,
                            'sku_id': sku_id
                        }
                        
                        image_properties.append(property_details)
                        variant_context['all_properties'].append(property_details)
                        
                        # Map image to property details
                        variant_context['image_to_property_map'][sku_image] = property_details
                        
                        # Map image to SKU ID
                        if sku_id:
                            variant_context['image_to_sku_map'][sku_image] = sku_id

            # Set primary property (first image-based property found)
            if image_properties:
                variant_context['primary_property'] = image_properties[0]

        except Exception as e:
            logger.error(f"Error extracting variant context: {e}")

        return variant_context

    def _extract_hero_image(self, result_data: Dict, product_id: str, variant_context: Dict, start_sort_index: int, db) -> tuple[int, int]:
        """
        Extract the hero/main image for a product (first image from image_urls).
        
        Args:
            result_data: Product result data from API
            product_id: Product ID
            variant_context: Variant context information
            start_sort_index: Starting sort index for this image type
            db: Database session
            
        Returns:
            Tuple of (number of hero images extracted, next available sort index)
        """
        try:
            # Get multimedia info
            multimedia_info = result_data.get('ae_multimedia_info_dto', {})
            image_urls_str = multimedia_info.get('image_urls', '')

            if not image_urls_str:
                logger.debug(f"No image URLs found for product {product_id}")
                return 0, start_sort_index

            # Split the semicolon-separated URLs and get the first one as hero
            all_image_urls = [url.strip() for url in image_urls_str.split(';') if url.strip()]
            
            if not all_image_urls:
                logger.debug(f"No valid image URLs found for product {product_id}")
                return 0, start_sort_index

            hero_image_url = all_image_urls[0]

            # Determine property details and SKU ID for hero image
            property_value = None
            property_name = None
            property_value_definition_name = None
            property_id = None
            sku_id = None
            variant_key = None
            
            # Only set property details if this hero image is also a variant image
            if hero_image_url in variant_context.get('image_to_property_map', {}):
                prop_details = variant_context['image_to_property_map'][hero_image_url]
                property_value = prop_details['property_value']
                property_name = prop_details['property_name']
                property_value_definition_name = prop_details['property_value_definition_name']
                property_id = prop_details.get('property_id')
                sku_id = prop_details.get('sku_id')
                
                # Create variant key: property_name:property_value
                if property_name and property_value:
                    variant_key = f"{property_name}:{property_value}"
            # If not in variant map, keep all property fields as None

            # Process image URL (download if enabled)
            image_data = self._process_image_url(hero_image_url, product_id, sku_id, 'hero')

            hero_image = ProductImage(
                product_id=product_id,
                image_url=hero_image_url,
                image_role='hero',
                sku_id=sku_id,
                variant_key=variant_key,
                property_value=property_value,
                property_name=property_name,
                property_id=property_id,
                property_value_definition_name=property_value_definition_name,
                local_file_path=image_data['local_file_path'],
                phash=image_data['phash'],
                download_status=image_data['download_status'],
                width=image_data['width'],
                height=image_data['height'],
                sort_index=start_sort_index,
                is_primary=True
            )
            
            db.add(hero_image)
            logger.debug(f"Added hero image for product {product_id} with property: {property_value}, sku_id: {sku_id}, variant_key: {variant_key}")
            return 1, start_sort_index + 1

        except Exception as e:
            logger.error(f"Error adding hero image for product {product_id}: {e}")
            return 0, start_sort_index

    def _extract_gallery_images(self, result_data: Dict, product_id: str, variant_context: Dict, start_sort_index: int, db) -> tuple[int, int]:
        """
        Extract gallery images from the multimedia info.
        Gallery images are all images from image_urls EXCEPT:
        - The first image (hero image)
        - Any images that are variant images (found in ae_sku_property_d_t_o)
        
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
            all_image_urls = [url.strip() for url in image_urls_str.split(';') if url.strip()]
            
            if len(all_image_urls) <= 1:
                logger.debug(f"Only hero image available for product {product_id}, no gallery images")
                return 0, start_sort_index

            # Exclude the first image (hero) and any variant images
            gallery_urls = all_image_urls[1:]  # Skip first image (hero)
            
            # Get all variant image URLs to exclude them from gallery
            variant_image_urls = set(variant_context.get('image_to_property_map', {}).keys())
            
            # Filter out variant images from gallery
            gallery_urls = [url for url in gallery_urls if url not in variant_image_urls]
            
            if not gallery_urls:
                logger.debug(f"No gallery images remaining after filtering for product {product_id}")
                return 0, start_sort_index
            
            images_added = 0
            current_sort_index = start_sort_index
            
            for index, image_url in enumerate(gallery_urls):
                try:
                    # Gallery images from image_urls don't have properties unless they're also variant images
                    # (but we already filtered out variant images, so they should all be None)
                    # Keep all property fields as None for gallery images

                    # Process image URL (download if enabled)
                    image_data = self._process_image_url(image_url, product_id, None, 'gallery')

                    gallery_image = ProductImage(
                        product_id=product_id,
                        image_url=image_url,
                        image_role='gallery',
                        sku_id=None,  # Gallery images don't have SKU associations
                        variant_key=None,  # Gallery images don't have variant keys
                        property_value=None,
                        property_name=None,
                        property_id=None,
                        property_value_definition_name=None,
                        local_file_path=image_data['local_file_path'],
                        phash=image_data['phash'],
                        download_status=image_data['download_status'],
                        width=image_data['width'],
                        height=image_data['height'],
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
                sku_id = sku.get('sku_id')
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
                            # Create variant key: property_name:property_value
                            variant_key = f"{sku_property_name}:{sku_property_value}" if sku_property_name and sku_property_value else None
                            
                            # Process image URL (download if enabled)
                            image_data = self._process_image_url(sku_image, product_id, sku_id, 'variant')
                            
                            variant_image = ProductImage(
                                product_id=product_id,
                                image_url=sku_image,
                                image_role='variant',
                                sku_id=sku_id,
                                variant_key=variant_key,
                                property_value=str(sku_property_value),
                                property_name=sku_property_name,
                                property_id=prop.get('sku_property_id'),
                                property_value_definition_name=property_value_definition_name,
                                local_file_path=image_data['local_file_path'],
                                phash=image_data['phash'],
                                download_status=image_data['download_status'],
                                width=image_data['width'],
                                height=image_data['height'],
                                sort_index=current_sort_index,
                                is_primary=False
                            )
                            
                            db.add(variant_image)
                            images_added += 1
                            current_sort_index += 1
                            
                            logger.debug(f"Added variant image for product {product_id}: "
                                       f"sku_id='{sku_id}' "
                                       f"variant_key='{variant_key}' "
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
                    'sku_id': img.sku_id,
                    'variant_key': img.variant_key,
                    'property_value': img.property_value,
                    'property_name': img.property_name,
                    'property_id': img.property_id,
                    'property_value_definition_name': img.property_value_definition_name,
                    'local_file_path': img.local_file_path,
                    'phash': img.phash,
                    'download_status': img.download_status,
                    'sort_index': img.sort_index,
                    'width': img.width,
                    'height': img.height,
                    'is_primary': img.is_primary
                }
                for img in images
            ]

    def get_product_images_with_shipping(self, product_id: str) -> List[Dict]:
        """
        Get all images for a specific product along with their related shipping information.
        
        Args:
            product_id: Product ID to get images for
            
        Returns:
            List of image dictionaries with shipping information included
        """
        with get_db_session() as db:
            images = db.query(ProductImage).filter(
                ProductImage.product_id == product_id
            ).order_by(ProductImage.sort_index).all()
            
            result = []
            for img in images:
                image_data = {
                    'id': img.id,
                    'image_url': img.image_url,
                    'image_role': img.image_role,
                    'sku_id': img.sku_id,
                    'variant_key': img.variant_key,
                    'property_value': img.property_value,
                    'property_name': img.property_name,
                    'property_id': img.property_id,
                    'property_value_definition_name': img.property_value_definition_name,
                    'local_file_path': img.local_file_path,
                    'phash': img.phash,
                    'download_status': img.download_status,
                    'sort_index': img.sort_index,
                    'width': img.width,
                    'height': img.height,
                    'is_primary': img.is_primary,
                    'shipping_options': []
                }
                
                # Get related shipping information
                if img.sku_id:
                    shipping_options = img.get_related_shipping_info(db)
                    image_data['shipping_options'] = [
                        {
                            'id': ship.id,
                            'company': ship.company,
                            'shipping_fee': ship.shipping_fee,
                            'shipping_fee_currency': ship.shipping_fee_currency,
                            'min_delivery_days': ship.min_delivery_days,
                            'max_delivery_days': ship.max_delivery_days,
                            'free_shipping': ship.free_shipping,
                            'tracking': ship.tracking
                        }
                        for ship in shipping_options
                    ]
                
                result.append(image_data)
            
            return result

    def get_images_by_sku(self, product_id: str, sku_id: str) -> List[Dict]:
        """
        Get images for a specific product and SKU combination.
        
        Args:
            product_id: Product ID to get images for
            sku_id: SKU ID to filter by
            
        Returns:
            List of image dictionaries for the specified SKU
        """
        with get_db_session() as db:
            images = db.query(ProductImage).filter(
                ProductImage.product_id == product_id,
                ProductImage.sku_id == sku_id
            ).order_by(ProductImage.sort_index).all()
            
            return [
                {
                    'id': img.id,
                    'image_url': img.image_url,
                    'image_role': img.image_role,
                    'sku_id': img.sku_id,
                    'variant_key': img.variant_key,
                    'property_value': img.property_value,
                    'property_name': img.property_name,
                    'property_id': img.property_id,
                    'property_value_definition_name': img.property_value_definition_name,
                    'local_file_path': img.local_file_path,
                    'phash': img.phash,
                    'download_status': img.download_status,
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

    def _get_hero_image_url(self, result_data: Dict) -> str:
        """Get the hero image URL (first image from image_urls)."""
        try:
            multimedia_info = result_data.get('ae_multimedia_info_dto', {})
            image_urls = multimedia_info.get('image_urls', '').split(';')
            if image_urls and image_urls[0]:
                return image_urls[0].strip()
        except Exception as e:
            logger.debug(f"Error getting hero image URL: {e}")
        return None

    def _get_variant_image_urls(self, variant_context: Dict) -> set:
        """Get all variant image URLs as a set."""
        return set(variant_context.get('image_to_property_map', {}).keys())

    def _extract_variant_images_with_primary(self, result_data: Dict, product_id: str, 
                                            variant_context: Dict, hero_url: str, 
                                            start_sort_index: int, db) -> tuple[int, int]:
        """
        Extract variant images, marking the hero_url as is_primary=True if it exists.
        
        Args:
            result_data: Product result data
            product_id: Product ID
            variant_context: Variant context with image mappings
            hero_url: Hero image URL to mark as primary
            start_sort_index: Starting sort index
            db: Database session
            
        Returns:
            Tuple of (extracted_count, next_sort_index)
        """
        extracted_count = 0
        current_sort_index = start_sort_index

        try:
            image_to_property_map = variant_context.get('image_to_property_map', {})
            
            for image_url, prop_details in image_to_property_map.items():
                try:
                    property_value = prop_details['property_value']
                    property_name = prop_details['property_name']
                    property_value_definition_name = prop_details['property_value_definition_name']
                    property_id = prop_details.get('property_id')
                    sku_id = prop_details.get('sku_id')
                    
                    # Create variant key: property_name:property_value
                    variant_key = f"{property_name}:{property_value}" if property_name and property_value else None

                    # Check if this is the hero URL
                    is_primary = (image_url == hero_url)

                    # Process image URL (download if enabled)
                    image_data = self._process_image_url(image_url, product_id, sku_id, 'variant')

                    variant_image = ProductImage(
                        product_id=product_id,
                        image_url=image_url,
                        image_role='variant',
                        sku_id=sku_id,
                        variant_key=variant_key,
                        property_value=property_value,
                        property_name=property_name,
                        property_id=property_id,
                        property_value_definition_name=property_value_definition_name,
                        local_file_path=image_data['local_file_path'],
                        phash=image_data['phash'],
                        download_status=image_data['download_status'],
                        width=image_data['width'],
                        height=image_data['height'],
                        sort_index=current_sort_index,
                        is_primary=is_primary
                    )
                    
                    db.add(variant_image)
                    extracted_count += 1
                    current_sort_index += 1
                    
                    logger.debug(f"Added variant image for product {product_id}: {property_value} "
                               f"(sku_id: {sku_id}, is_primary: {is_primary}, variant_key: {variant_key})")

                except Exception as e:
                    logger.error(f"Error processing variant image {image_url} for product {product_id}: {e}")

        except Exception as e:
            logger.error(f"Error extracting variant images for product {product_id}: {e}")

        return extracted_count, current_sort_index


def main():
    """Main function for running image ingestion from command line."""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        # Check for download flag
        download_images = "--download" in sys.argv
        if download_images:
            sys.argv.remove("--download")
        
        engine = ImageIngestionEngine(download_images=download_images)
        
        if command == "ingest":
            # Ingest all images
            stats = engine.ingest_all_images()
            print(f"Image ingestion completed: {stats}")
            if download_images:
                print("✅ Images were downloaded automatically")
            
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
                variant_info = f" (sku: {img['sku_id']}, variant: {img['variant_key']})" if img['sku_id'] or img['variant_key'] else ""
                print(f"  {img['image_role']}: {img['image_url']} (index: {img['sort_index']}){variant_info}")
                
        elif command == "list-with-shipping" and len(sys.argv) > 2:
            # List images with shipping information
            product_id = sys.argv[2]
            images = engine.get_product_images_with_shipping(product_id)
            print(f"Images with shipping info for product {product_id}:")
            for img in images:
                variant_info = f" (sku: {img['sku_id']}, variant: {img['variant_key']})" if img['sku_id'] or img['variant_key'] else ""
                print(f"  {img['image_role']}: {img['image_url']} (index: {img['sort_index']}){variant_info}")
                
                if img['shipping_options']:
                    print(f"    Shipping options ({len(img['shipping_options'])}):")
                    for ship in img['shipping_options']:
                        print(f"      📦 {ship['company']}: {ship['shipping_fee_currency']}{ship['shipping_fee']} ({ship['min_delivery_days']}-{ship['max_delivery_days']} days)")
                else:
                    print(f"    No shipping options available")
                    
        elif command == "list-by-sku" and len(sys.argv) > 3:
            # List images for specific product and SKU
            product_id = sys.argv[2]
            sku_id = sys.argv[3]
            images = engine.get_images_by_sku(product_id, sku_id)
            print(f"Images for product {product_id}, SKU {sku_id}:")
            for img in images:
                print(f"  {img['image_role']}: {img['variant_key']} - {img['image_url']} (index: {img['sort_index']})")
                
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
            print("  python -m src.ingestion.image_ingestion list-with-shipping <product_id>")
            print("  python -m src.ingestion.image_ingestion list-by-sku <product_id> <sku_id>")
            print("  python -m src.ingestion.image_ingestion clear <product_id>")
    else:
        print("No command specified. Use 'ingest' to process all products.")


if __name__ == "__main__":
    main()