"""
Image download and processing module for product images.

This module handles downloading images from URLs, calculating perceptual hashes,
and storing images locally with proper file naming.
"""

import os
import hashlib
import logging
import requests
from urllib.parse import urlparse
from typing import Optional, Tuple
import imagehash
from PIL import Image
from io import BytesIO
from src.common.config import get_env

# Set up logging
logger = logging.getLogger(__name__)


class ImageDownloader:
    """
    Handles downloading and processing of product images.
    """

    def __init__(self, download_dir: str = None):
        """
        Initialize the image downloader.
        
        Args:
            download_dir: Directory to store downloaded images. 
                         Defaults to 'downloads/images' in project root.
        """
        self.download_dir = download_dir or os.path.join(os.getcwd(), 'downloads', 'images')
        self._ensure_download_dir()
        logger.info(f"Image downloader initialized with directory: {self.download_dir}")

    def _ensure_download_dir(self):
        """Ensure the download directory exists."""
        os.makedirs(self.download_dir, exist_ok=True)

    def _get_relative_path(self, absolute_path: str) -> str:
        """
        Convert absolute path to relative path from project root.
        
        Args:
            absolute_path: Absolute file path
            
        Returns:
            Relative path from project root
        """
        try:
            # Get project root (assuming it's the parent of src)
            current_file = os.path.abspath(__file__)
            src_dir = os.path.dirname(os.path.dirname(current_file))  # Go up from src/ingestion/
            project_root = os.path.dirname(src_dir)  # Go up from src/
            
            # Get relative path from project root
            relative_path = os.path.relpath(absolute_path, project_root)
            return relative_path
            
        except Exception as e:
            logger.error(f"Error converting to relative path: {e}")
            # Fallback: return the filename only
            return os.path.basename(absolute_path)

    def extract_filename_from_url(self, image_url: str) -> str:
        """
        Extract filename from AliExpress image URL.
        
        Args:
            image_url: The image URL
            
        Returns:
            Filename extracted from URL (e.g., 'Sa9f583b3bcdc43399001b6015c141896Q.jpg')
        """
        try:
            # Parse the URL and extract the path
            parsed_url = urlparse(image_url)
            path = parsed_url.path
            
            # Extract filename from path (last part after /)
            filename = os.path.basename(path)
            
            # Ensure it has an extension, default to .jpg if none
            if '.' not in filename:
                filename += '.jpg'
                
            return filename
            
        except Exception as e:
            logger.error(f"Error extracting filename from URL {image_url}: {e}")
            # Fallback: create filename from URL hash
            url_hash = hashlib.md5(image_url.encode()).hexdigest()[:16]
            return f"{url_hash}.jpg"

    def get_image_dimensions(self, image_data: bytes) -> Tuple[Optional[int], Optional[int]]:
        """
        Get width and height of an image from raw data.
        
        Args:
            image_data: Raw image data as bytes
            
        Returns:
            Tuple of (width, height) or (None, None) if extraction fails
        """
        try:
            # Open image from bytes
            image = Image.open(BytesIO(image_data))
            width, height = image.size
            return width, height
            
        except Exception as e:
            logger.error(f"Error getting image dimensions: {e}")
            return None, None

    def calculate_phash(self, image_data: bytes) -> Optional[str]:
        """
        Calculate perceptual hash of an image.
        
        Args:
            image_data: Raw image data as bytes
            
        Returns:
            Perceptual hash as hex string, or None if calculation fails
        """
        try:
            # Open image from bytes
            image = Image.open(BytesIO(image_data))
            
            # Convert to RGB if necessary
            if image.mode not in ('RGB', 'L'):
                image = image.convert('RGB')
            
            # Calculate perceptual hash
            phash = imagehash.phash(image)
            
            # Return as hex string
            return str(phash)
            
        except Exception as e:
            logger.error(f"Error calculating pHash: {e}")
            return None

    def download_image(self, image_url: str, product_id: str = None, 
                      sku_id: str = None, image_role: str = None) -> Tuple[str, str, str, int, int]:
        """
        Download an image from URL and calculate its pHash and dimensions.
        
        Args:
            image_url: URL of the image to download
            product_id: Product ID for organizing files
            sku_id: SKU ID for organizing files
            image_role: Image role (hero, gallery, variant)
            
        Returns:
            Tuple of (local_file_path, phash, download_status, width, height)
        """
        try:
            # Create filename
            filename = self.extract_filename_from_url(image_url)
            
            # Create subdirectory structure for organization
            if product_id:
                product_dir = os.path.join(self.download_dir, product_id)
                os.makedirs(product_dir, exist_ok=True)
                local_file_path = os.path.join(product_dir, filename)
            else:
                local_file_path = os.path.join(self.download_dir, filename)

            # Check if file already exists
            if os.path.exists(local_file_path):
                logger.debug(f"Image already exists: {local_file_path}")
                # Calculate pHash and get dimensions for existing file
                with open(local_file_path, 'rb') as f:
                    image_data = f.read()
                phash = self.calculate_phash(image_data)
                width, height = self.get_image_dimensions(image_data)
                relative_path = self._get_relative_path(local_file_path)
                return relative_path, phash, 'downloaded', width, height

            # Download the image
            logger.debug(f"Downloading image from {image_url}")
            
            # Set headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = requests.get(image_url, headers=headers, timeout=30, stream=True)
            response.raise_for_status()
            
            # Get image data
            image_data = response.content
            
            # Calculate pHash and get dimensions
            phash = self.calculate_phash(image_data)
            width, height = self.get_image_dimensions(image_data)
            
            # Save the image
            with open(local_file_path, 'wb') as f:
                f.write(image_data)
            
            # Convert to relative path for database storage
            relative_path = self._get_relative_path(local_file_path)
            
            logger.info(f"Downloaded image: {filename} (pHash: {phash}, {width}x{height})")
            return relative_path, phash, 'downloaded', width, height
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading image from {image_url}: {e}")
            return None, None, 'failed', None, None
        except Exception as e:
            logger.error(f"Unexpected error downloading image from {image_url}: {e}")
            return None, None, 'failed', None, None

    def batch_download_images(self, image_urls: list, product_id: str = None) -> dict:
        """
        Download multiple images in batch.
        
        Args:
            image_urls: List of image URLs to download
            product_id: Product ID for organizing files
            
        Returns:
            Dictionary with download results
        """
        results = {
            'downloaded': 0,
            'failed': 0,
            'already_exists': 0,
            'details': []
        }
        
        for url in image_urls:
            local_path, phash, status = self.download_image(url, product_id)
            
            result = {
                'url': url,
                'local_path': local_path,
                'phash': phash,
                'status': status
            }
            
            results['details'].append(result)
            
            if status == 'downloaded':
                if os.path.exists(local_path):
                    results['already_exists'] += 1
                else:
                    results['downloaded'] += 1
            elif status == 'failed':
                results['failed'] += 1
        
        return results

    def get_image_info(self, image_path: str) -> dict:
        """
        Get information about a downloaded image.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Dictionary with image information
        """
        try:
            if not os.path.exists(image_path):
                return {'error': 'File not found'}
            
            # Get file size
            file_size = os.path.getsize(image_path)
            
            # Get image dimensions
            with Image.open(image_path) as img:
                width, height = img.size
                format_type = img.format
                mode = img.mode
            
            # Calculate pHash
            with open(image_path, 'rb') as f:
                image_data = f.read()
            phash = self.calculate_phash(image_data)
            
            return {
                'file_size': file_size,
                'width': width,
                'height': height,
                'format': format_type,
                'mode': mode,
                'phash': phash
            }
            
        except Exception as e:
            logger.error(f"Error getting image info for {image_path}: {e}")
            return {'error': str(e)}

    def cleanup_failed_downloads(self, max_age_hours: int = 24):
        """
        Clean up failed or incomplete downloads.
        
        Args:
            max_age_hours: Maximum age in hours for failed downloads to keep
        """
        # This could be implemented to clean up partial downloads
        # or images that failed to process properly
        pass


def main():
    """Main function for testing image download functionality."""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        downloader = ImageDownloader()
        
        if command == "test" and len(sys.argv) > 2:
            # Test download single image
            image_url = sys.argv[2]
            product_id = sys.argv[3] if len(sys.argv) > 3 else "test"
            
            print(f"Testing download of: {image_url}")
            local_path, phash, status = downloader.download_image(image_url, product_id)
            
            print(f"Result:")
            print(f"  Local path: {local_path}")
            print(f"  pHash: {phash}")
            print(f"  Status: {status}")
            
            if local_path and os.path.exists(local_path):
                info = downloader.get_image_info(local_path)
                print(f"  Image info: {info}")
                
        elif command == "extract-filename" and len(sys.argv) > 2:
            # Test filename extraction
            image_url = sys.argv[2]
            filename = downloader.extract_filename_from_url(image_url)
            print(f"URL: {image_url}")
            print(f"Extracted filename: {filename}")
            
        else:
            print("Usage:")
            print("  python -m src.ingestion.image_download test <image_url> [product_id]")
            print("  python -m src.ingestion.image_download extract-filename <image_url>")
    else:
        print("No command specified.")


if __name__ == "__main__":
    main()