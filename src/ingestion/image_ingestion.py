"""
Image ingestion module for extracting and storing product images.

This module processes raw product data to extract all available image URLs
(hero, gallery, and variant images) and stores them in the product_images table.
"""

import logging
import os
from typing import List, Dict, Optional
from src.common.database import get_db_session, ProductImage, ProductVideo, FilteredProduct, Product
from src.common.config import get_env
from src.ingestion.image_download import ImageDownloader
from src.ingestion.s3_image_uploader import S3ImageUploader
from src.ingestion.video_download import VideoDownloader
from src.ingestion.s3_video_uploader import S3VideoUploader

# Set up logging
logger = logging.getLogger(__name__)


class ImageIngestionEngine:
    """
    Engine for extracting and storing product images from raw JSON data.
    """

    def __init__(self, download_images: bool = False, download_videos: bool = False, download_dir: str = None, upload_to_s3: bool = None):
        """
        Initialize the image and video ingestion engine.
        
        Args:
            download_images: Whether to automatically download images
            download_videos: Whether to automatically download videos
            download_dir: Directory to store downloaded files
            upload_to_s3: Whether to upload downloaded files to S3 (None = auto-detect)
        """
        self.download_images = download_images
        self.download_videos = download_videos
        self.image_downloader = ImageDownloader(download_dir) if download_images else None
        self.video_downloader = VideoDownloader(download_dir) if download_videos else None
        
        # Auto-detect S3 upload capability if not explicitly set
        if upload_to_s3 is None:
            upload_to_s3 = self._check_s3_credentials_available()
            if upload_to_s3:
                logger.info("S3 credentials detected - automatic S3 upload enabled")
        
        self.upload_to_s3 = upload_to_s3
        
        # Initialize S3 uploaders if S3 upload is enabled
        if self.upload_to_s3:
            try:
                self.s3_uploader = S3ImageUploader()
                self.s3_video_uploader = S3VideoUploader()
                logger.info("S3 uploaders initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize S3 uploaders: {e}")
                logger.warning("S3 upload will be disabled")
                self.upload_to_s3 = False
                self.s3_uploader = None
                self.s3_video_uploader = None
        else:
            self.s3_uploader = None
            self.s3_video_uploader = None
        
        # In-memory cache for phash-to-s3url mapping during current processing session
        # This prevents duplicate S3 uploads for images with same phash within the same batch
        self.phash_cache = {}
            
        logger.info(f"Image and video ingestion engine initialized (download_images: {download_images}, download_videos: {download_videos}, upload_to_s3: {self.upload_to_s3})")

    def _check_s3_credentials_available(self) -> bool:
        """
        Check if S3 credentials are configured in environment.
        
        Returns:
            True if S3 credentials are available, False otherwise
        """
        try:
            aws_access_key = get_env('AWS_ACCESS_KEY_ID')
            aws_secret_key = get_env('AWS_SECRET_ACCESS_KEY')
            bucket_name = get_env('S3_BUCKET_NAME')
            
            return bool(aws_access_key and aws_secret_key and bucket_name)
        except Exception:
            return False

    def _get_existing_s3_url(self, image_url: str) -> Optional[str]:
        """
        Check if this image URL already has an S3 URL in the database.
        
        Args:
            image_url: The image URL to check
            
        Returns:
            Existing S3 URL if found, None otherwise
        """
        try:
            with get_db_session() as db:
                existing_image = db.query(ProductImage).filter(
                    ProductImage.image_url == image_url,
                    ProductImage.s3_url.isnot(None)
                ).first()
                
                if existing_image:
                    return existing_image.s3_url
        except Exception as e:
            logger.debug(f"Error checking existing S3 URL: {e}")
        
        return None
    
    def _get_existing_image_metadata(self, image_url: str) -> Optional[Dict]:
        """
        Get complete metadata for an existing image from the database.
        
        Args:
            image_url: The image URL to check
            
        Returns:
            Dict with s3_url, phash, width, height, local_file_path if found, None otherwise
        """
        try:
            with get_db_session() as db:
                existing_image = db.query(ProductImage).filter(
                    ProductImage.image_url == image_url,
                    ProductImage.s3_url.isnot(None)
                ).first()
                
                if existing_image:
                    return {
                        's3_url': existing_image.s3_url,
                        'phash': existing_image.phash,
                        'width': existing_image.width,
                        'height': existing_image.height,
                        'local_file_path': existing_image.local_file_path
                    }
        except Exception as e:
            logger.debug(f"Error checking existing image metadata: {e}")
        
        return None
    
    def _get_existing_metadata_by_phash(self, phash: str) -> Optional[Dict]:
        """
        Get complete metadata for an existing image by phash from the database.
        This allows reusing S3 URLs for images with identical phash values.
        
        Args:
            phash: The perceptual hash to check
            
        Returns:
            Dict with s3_url, phash, width, height, local_file_path if found, None otherwise
        """
        try:
            with get_db_session() as db:
                existing_image = db.query(ProductImage).filter(
                    ProductImage.phash == phash,
                    ProductImage.s3_url.isnot(None)
                ).first()
                
                if existing_image:
                    return {
                        's3_url': existing_image.s3_url,
                        'phash': existing_image.phash,
                        'width': existing_image.width,
                        'height': existing_image.height,
                        'local_file_path': existing_image.local_file_path
                    }
        except Exception as e:
            logger.debug(f"Error checking existing metadata by phash: {e}")
        
        return None

    def _get_existing_local_path(self, image_url: str, product_id: str) -> Optional[str]:
        """
        Check if this image already exists locally based on URL pattern.
        
        Args:
            image_url: The image URL
            product_id: Product ID for organizing files
            
        Returns:
            Existing relative local path if found, None otherwise
        """
        try:
            if not self.image_downloader:
                return None
                
            # Extract filename from URL (same logic as image downloader)
            filename = self.image_downloader.extract_filename_from_url(image_url)
            
            # Check in product directory first
            product_dir = os.path.join(self.image_downloader.download_dir, product_id)
            product_file_path = os.path.join(product_dir, filename)
            
            if os.path.exists(product_file_path):
                return self.image_downloader._get_relative_path(product_file_path)
                
            # Check in root download directory
            root_file_path = os.path.join(self.image_downloader.download_dir, filename)
            if os.path.exists(root_file_path):
                return self.image_downloader._get_relative_path(root_file_path)
                
        except Exception as e:
            logger.debug(f"Error checking existing local path: {e}")
        
        return None

    def _process_image_url(self, image_url: str, product_id: str, sku_id: str = None, 
                          image_role: str = None) -> Dict:
        """
        Process an image URL, optionally downloading it and uploading to S3.
        Smart reuse: checks for existing local files and S3 URLs to avoid re-processing.
        Also reuses S3 URLs for images with identical phash values.
        
        Args:
            image_url: URL of the image
            product_id: Product ID
            sku_id: SKU ID (optional)
            image_role: Image role (hero, gallery, variant)
            
        Returns:
            Dict with local_file_path, phash, download_status, width, height, and s3_url
        """
        result = {
            'local_file_path': None,
            'phash': None,
            'download_status': 'pending',
            'width': None,
            'height': None,
            's3_url': None
        }
        
        # First check if this image already exists in the database with complete metadata
        existing_metadata = self._get_existing_image_metadata(image_url)
        if existing_metadata and existing_metadata.get('s3_url'):
            logger.debug(f"Reusing existing data from database for {image_url}: {existing_metadata['s3_url']}")
            result['s3_url'] = existing_metadata['s3_url']
            result['phash'] = existing_metadata.get('phash')
            result['width'] = existing_metadata.get('width')
            result['height'] = existing_metadata.get('height')
            result['local_file_path'] = existing_metadata.get('local_file_path')
            result['download_status'] = 'reused'
            return result
        
        if self.download_images and self.image_downloader:
            local_path, phash, status, width, height = self.image_downloader.download_image(
                image_url, product_id, sku_id, image_role
            )
            
            result.update({
                'local_file_path': local_path,
                'phash': phash,
                'download_status': status,
                'width': width,
                'height': height
            })
            
            logger.debug(f"Downloaded image {image_url} with phash: {phash}, status: {status}")
            
            # Check if an image with the same phash already exists in the database
            # First check in-memory cache for current session
            if phash and phash in self.phash_cache:
                logger.warning(f"⚠️ PHASH CACHE HIT - Reusing S3 URL from session cache for phash {phash}: {self.phash_cache[phash]} (image_url: {image_url})")
                result['s3_url'] = self.phash_cache[phash]
                result['download_status'] = 'reused_by_phash_cache'
                return result
            
            # Then check database for previously processed images
            if phash:
                existing_phash_metadata = self._get_existing_metadata_by_phash(phash)
                if existing_phash_metadata and existing_phash_metadata.get('s3_url'):
                    logger.warning(f"⚠️ PHASH DB HIT - Reusing S3 URL for identical image (phash: {phash}): {existing_phash_metadata['s3_url']} (image_url: {image_url})")
                    result['s3_url'] = existing_phash_metadata['s3_url']
                    result['download_status'] = 'reused_by_phash'
                    # Cache it for faster lookup in current session
                    self.phash_cache[phash] = existing_phash_metadata['s3_url']
                    return result
                else:
                    logger.debug(f"Phash {phash} not found in database, will upload new")
            
            # Upload to S3 if enabled and download was successful
            if (self.upload_to_s3 and self.s3_uploader and 
                status == 'downloaded' and local_path):
                
                # Convert relative path to absolute for S3 upload
                if not os.path.isabs(local_path):
                    # Get project root and construct absolute path
                    current_file = os.path.abspath(__file__)
                    src_dir = os.path.dirname(os.path.dirname(current_file))
                    project_root = os.path.dirname(src_dir)
                    absolute_path = os.path.join(project_root, local_path)
                else:
                    absolute_path = local_path
                
                # Check once more before upload if phash now exists in cache (race condition protection)
                if phash and phash in self.phash_cache:
                    logger.warning(f"⚠️ PHASH RACE CONDITION - Phash appeared in cache during processing, reusing: {self.phash_cache[phash]} (image_url: {image_url})")
                    result['s3_url'] = self.phash_cache[phash]
                    result['download_status'] = 'reused_by_phash_cache'
                    return result
                
                # Upload to S3
                logger.debug(f"Uploading to S3: {image_url} with phash {phash}")
                s3_url = self.s3_uploader.upload_image(
                    absolute_path, product_id, image_role or 'unknown'
                )
                
                if s3_url:
                    result['s3_url'] = s3_url
                    # Cache the phash-to-s3url mapping IMMEDIATELY after upload
                    if phash:
                        self.phash_cache[phash] = s3_url
                        logger.warning(f"✅ PHASH CACHED - Stored phash {phash} -> {s3_url} (image_url: {image_url})")
                    logger.info(f"Successfully uploaded image to S3: {s3_url}")
                else:
                    logger.warning(f"Failed to upload image to S3: {local_path}")
        
        return result

    def ingest_all_images(self) -> Dict:
        """
        Process all filtered products and extract their images and videos.
        
        Returns:
            Dict with ingestion statistics
        """
        stats = {
            'products_processed': 0,
            'images_extracted': 0,
            'hero_images': 0,
            'gallery_images': 0,
            'variant_images': 0,
            'other_images': 0,
            'videos_processed': 0,
            'videos_downloaded': 0,
            'videos_uploaded': 0,
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
                    stats['other_images'] += images_extracted['other']
                    
                    # Process video for this product if video processing is enabled
                    if self.download_videos:
                        video_stats = self._process_product_video(filtered_product, db)
                        stats['videos_processed'] += video_stats['processed']
                        stats['videos_downloaded'] += video_stats['downloaded']
                        stats['videos_uploaded'] += video_stats['uploaded']
                        stats['errors'] += video_stats['errors']
                    
                    stats['products_processed'] += 1

                    logger.info(f"Processed product {product.product_id}: "
                              f"{images_extracted['total']} images extracted "
                              f"(hero: {images_extracted['hero']}, gallery: {images_extracted['gallery']}, "
                              f"variant: {images_extracted['variant']}, other: {images_extracted['other']}), "
                              f"{stats.get('videos_processed', 0)} videos processed")

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

    def ingest_product_video(self, product_id: str) -> Dict:
        """
        Extract and process video for a specific product.
        
        Args:
            product_id: Product ID to process
            
        Returns:
            Dict with video processing results
        """
        with get_db_session() as db:
            # Get filtered product (since we're working with filtered products)
            filtered_product = db.query(FilteredProduct).filter(FilteredProduct.product_id == product_id).first()
            
            if not filtered_product:
                raise ValueError(f"FilteredProduct {product_id} not found")

            results = self._process_product_video(filtered_product, db)
            db.commit()
            
            return results

    def _process_product_video(self, filtered_product: FilteredProduct, db) -> Dict:
        """
        Process video for a filtered product.
        
        Args:
            filtered_product: FilteredProduct object with video URL
            db: Database session
            
        Returns:
            Dict with processing statistics
        """
        stats = {'processed': 0, 'downloaded': 0, 'uploaded': 0, 'errors': 0}

        if not filtered_product.product_video_url:
            logger.info(f"No video URL for product {filtered_product.product_id}")
            return stats

        video_url = filtered_product.product_video_url
        product_id = filtered_product.product_id
        
        # Check if video already exists in database
        existing_video = db.query(ProductVideo).filter(
            ProductVideo.product_id == product_id,
            ProductVideo.video_url == video_url
        ).first()
        
        if existing_video:
            # If video exists but has no S3 URL, process it for upload
            if not existing_video.s3_url and self.upload_to_s3 and self.s3_video_uploader:
                logger.info(f"Video exists for product {product_id} but has no S3 URL, processing for upload")
                video_record = existing_video
            else:
                logger.info(f"Video already fully processed for product {product_id}")
                return stats
        else:
            # Create new video record
            video_record = ProductVideo(
                product_id=product_id,
                video_url=video_url,
                download_status='pending'
            )
            
            db.add(video_record)
            db.flush()  # Get the ID
        
        stats['processed'] = 1
        
        # Download video if downloading is enabled or use existing local path
        local_path = video_record.local_file_path  # Use existing path if available
        download_status = video_record.download_status or 'pending'
        
        if self.download_videos and self.video_downloader:
            try:
                local_path, download_status = self.video_downloader.download_video(video_url, product_id)
                
                video_record.local_file_path = local_path
                video_record.download_status = download_status
                
                if download_status == 'downloaded':
                    stats['downloaded'] = 1
                    logger.info(f"Video downloaded: {product_id}")
                elif download_status == 'exists':
                    logger.info(f"Video already exists: {product_id}")
                    
            except Exception as e:
                logger.error(f"Error processing video for {product_id}: {e}")
                video_record.download_status = 'failed'
                stats['errors'] += 1
        
        # Upload to S3 if enabled and we have a local file (for both downloaded and existing files)
        if self.upload_to_s3 and self.s3_video_uploader and local_path and download_status in ['downloaded', 'exists']:
            try:
                upload_result = self.s3_video_uploader.upload_video(local_path, product_id)
                
                if upload_result['success']:
                    video_record.s3_url = upload_result['s3_url']
                    stats['uploaded'] = 1
                    logger.info(f"Video uploaded to S3: {upload_result['s3_url']}")
                else:
                    logger.error(f"S3 upload failed for {product_id}: {upload_result.get('error')}")
                    stats['errors'] += 1
            except Exception as e:
                logger.error(f"Error uploading video to S3 for {product_id}: {e}")
                stats['errors'] += 1
        
        return stats

    def _extract_product_images(self, product: Product, db) -> Dict:
        """
        Extract all images for a product from its raw JSON data.
        
        Args:
            product: Product object with raw_json_detail
            db: Database session
            
        Returns:
            Dict with extraction statistics
        """
        stats = {'total': 0, 'hero': 0, 'gallery': 0, 'variant': 0, 'other': 0}

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

            # Collect all URLs processed so far in this session
            processed_urls = set()
            
            # Get URLs from multimedia info (hero + gallery)
            multimedia_info = result.get('ae_multimedia_info_dto', {})
            image_urls_str = multimedia_info.get('image_urls', '')
            if image_urls_str:
                multimedia_urls = [url.strip() for url in image_urls_str.split(';') if url.strip()]
                processed_urls.update(multimedia_urls)
            
            # Get variant URLs
            if variant_context.get('image_to_property_map'):
                processed_urls.update(variant_context['image_to_property_map'].keys())
            
            # Extract other images from detail sections, excluding already processed URLs
            other_extracted, current_sort_index = self._extract_other_images(result, product.product_id, current_sort_index, db, processed_urls)
            stats['other'] = other_extracted
            stats['total'] += other_extracted

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
                s3_url=image_data['s3_url'],
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
                        s3_url=image_data['s3_url'],
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
                                s3_url=image_data['s3_url'],
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

    def _extract_other_images(self, result_data: Dict, product_id: str, start_sort_index: int, db, processed_urls: set = None) -> tuple[int, int]:
        """
        Extract additional images from detail sections (ae_item_base_info_dto.detail and mobile_detail).
        These are images found in the product description HTML and mobile detail JSON.
        
        Args:
            result_data: Product result data from API
            product_id: Product ID
            start_sort_index: Starting sort index for this image type
            db: Database session
            processed_urls: Set of URLs already processed as hero/gallery/variant images
            
        Returns:
            Tuple of (number of other images extracted, next available sort index)
        """
        import re
        import json
        
        try:
            base_info = result_data.get('ae_item_base_info_dto', {})
            detail_html = base_info.get('detail', '')
            mobile_detail_json = base_info.get('mobile_detail', '')
            
            other_image_urls = set()  # Use set to avoid duplicates
            
            # Extract images from HTML detail
            if detail_html:
                # Find all img src URLs in the HTML
                img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
                img_matches = re.findall(img_pattern, detail_html, re.IGNORECASE)
                
                for img_url in img_matches:
                    # Clean and validate URL
                    cleaned_url = img_url.strip()
                    if cleaned_url and (cleaned_url.startswith('http') or cleaned_url.startswith('//')):
                        # Handle protocol-relative URLs
                        if cleaned_url.startswith('//'):
                            cleaned_url = 'https:' + cleaned_url
                        other_image_urls.add(cleaned_url)
            
            # Extract images from mobile detail JSON
            if mobile_detail_json:
                try:
                    mobile_data = json.loads(mobile_detail_json)
                    
                    # Look for images in moduleList
                    if isinstance(mobile_data, dict) and 'moduleList' in mobile_data:
                        for module in mobile_data['moduleList']:
                            if isinstance(module, dict) and module.get('type') == 'image':
                                image_data = module.get('data', {})
                                if isinstance(image_data, dict):
                                    img_url = image_data.get('url', '')
                                    if img_url and (img_url.startswith('http') or img_url.startswith('//')):
                                        # Handle protocol-relative URLs
                                        if img_url.startswith('//'):
                                            img_url = 'https:' + img_url
                                        other_image_urls.add(img_url)
                                        
                except json.JSONDecodeError as e:
                    logger.debug(f"Could not parse mobile_detail JSON for product {product_id}: {e}")
            
            # Get existing other images from database to avoid duplicating them
            existing_other_images = db.query(ProductImage).filter(
                ProductImage.product_id == product_id,
                ProductImage.image_role == 'other'
            ).all()
            existing_other_urls = {img.image_url for img in existing_other_images}
            
            # Combine processed URLs from current session with existing other images
            if processed_urls is None:
                processed_urls = set()
            
            all_existing_urls = processed_urls | existing_other_urls
            
            # Filter out URLs that already exist as any image type
            new_other_urls = other_image_urls - all_existing_urls
            
            if not new_other_urls:
                logger.debug(f"No new other images found for product {product_id}")
                return 0, start_sort_index
            
            images_added = 0
            current_sort_index = start_sort_index
            
            for image_url in new_other_urls:
                try:
                    # Process image URL (download if enabled)
                    image_data = self._process_image_url(image_url, product_id, None, 'other')

                    other_image = ProductImage(
                        product_id=product_id,
                        image_url=image_url,
                        image_role='other',
                        sku_id=None,  # Other images don't have SKU associations
                        variant_key=None,  # Other images don't have variant keys
                        property_value=None,
                        property_name=None,
                        property_id=None,
                        property_value_definition_name=None,
                        local_file_path=image_data['local_file_path'],
                        phash=image_data['phash'],
                        download_status=image_data['download_status'],
                        width=image_data['width'],
                        height=image_data['height'],
                        s3_url=image_data['s3_url'],
                        sort_index=current_sort_index,
                        is_primary=False
                    )
                    
                    db.add(other_image)
                    images_added += 1
                    current_sort_index += 1
                    logger.debug(f"Added other image for product {product_id}: {image_url}")

                except Exception as e:
                    logger.error(f"Error adding other image for product {product_id}: {e}")

            logger.debug(f"Added {images_added} other images for product {product_id}")
            return images_added, current_sort_index

        except Exception as e:
            logger.error(f"Error extracting other images for product {product_id}: {e}")
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
                    's3_url': img.s3_url,
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
                    's3_url': img.s3_url,
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
                    's3_url': img.s3_url,
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
                        s3_url=image_data['s3_url'],
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
        
        # Check for S3 upload flag (optional override, defaults to auto-detect)
        upload_to_s3 = None  # Auto-detect by default
        if "--s3" in sys.argv:
            upload_to_s3 = True
            sys.argv.remove("--s3")
        elif "--no-s3" in sys.argv:
            upload_to_s3 = False
            sys.argv.remove("--no-s3")
        
        engine = ImageIngestionEngine(download_images=download_images, upload_to_s3=upload_to_s3)
        
        if command == "ingest":
            # Ingest all images
            stats = engine.ingest_all_images()
            print(f"Image ingestion completed: {stats}")
            if download_images:
                print("✅ Images were downloaded automatically")
            if engine.upload_to_s3:
                print("✅ Images were uploaded to S3 automatically")
            
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
            print("  python -m src.ingestion.image_ingestion ingest [--download] [--s3|--no-s3]")
            print("  python -m src.ingestion.image_ingestion product <product_id> [--download] [--s3|--no-s3]")
            print("  python -m src.ingestion.image_ingestion list <product_id>")
            print("  python -m src.ingestion.image_ingestion list-with-shipping <product_id>")
            print("  python -m src.ingestion.image_ingestion list-by-sku <product_id> <sku_id>")
            print("  python -m src.ingestion.image_ingestion clear <product_id>")
            print("")
            print("Options:")
            print("  --download   Download images to local storage")
            print("  --s3         Force S3 upload (overrides auto-detection)")
            print("  --no-s3      Disable S3 upload (overrides auto-detection)")
            print("")
            print("Note: S3 upload is automatically enabled when AWS credentials are configured")
    else:
        print("No command specified. Use 'ingest' to process all products.")


if __name__ == "__main__":
    main()