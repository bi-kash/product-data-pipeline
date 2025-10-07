"""
Video download and processing module for product videos.

This module handles downloading videos from URLs and storing them locally
with proper file naming.
"""

import os
import hashlib
import logging
import requests
from urllib.parse import urlparse
from typing import Optional, Tuple

# Set up logging
logger = logging.getLogger(__name__)


class VideoDownloader:
    """
    Handles downloading and processing of product videos.
    """

    def __init__(self, download_dir: str = None):
        """
        Initialize the video downloader.
        
        Args:
            download_dir: Directory to store downloaded videos. 
                         Defaults to 'downloads/videos' in project root.
        """
        self.download_dir = download_dir or os.path.join(os.getcwd(), 'downloads', 'videos')
        self._ensure_download_dir()
        logger.info(f"Video downloader initialized with directory: {self.download_dir}")

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
        project_root = os.getcwd()
        if absolute_path.startswith(project_root):
            return os.path.relpath(absolute_path, project_root)
        return absolute_path

    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """
        Determine file extension from URL or content type.
        
        Args:
            url: Video URL
            content_type: HTTP content type header
            
        Returns:
            File extension (e.g., '.mp4', '.webm')
        """
        # Try to get extension from URL
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        
        if path.endswith('.mp4'):
            return '.mp4'
        elif path.endswith('.webm'):
            return '.webm'
        elif path.endswith('.avi'):
            return '.avi'
        elif path.endswith('.mov'):
            return '.mov'
        elif path.endswith('.mkv'):
            return '.mkv'
        
        # Try to determine from content type
        if content_type:
            content_type = content_type.lower()
            if 'mp4' in content_type:
                return '.mp4'
            elif 'webm' in content_type:
                return '.webm'
            elif 'avi' in content_type:
                return '.avi'
            elif 'quicktime' in content_type or 'mov' in content_type:
                return '.mov'
        
        # Default to .mp4
        return '.mp4'

    def _generate_filename(self, video_url: str, product_id: str) -> str:
        """
        Generate a unique filename for the video.
        
        Args:
            video_url: Original video URL
            product_id: Product ID
            
        Returns:
            Generated filename
        """
        # Create a hash of the URL for uniqueness
        url_hash = hashlib.md5(video_url.encode()).hexdigest()[:8]
        return f"{product_id}_{url_hash}"

    def download_video(self, video_url: str, product_id: str) -> Tuple[Optional[str], str]:
        """
        Download a video from URL and save it locally.
        
        Args:
            video_url: URL of the video to download
            product_id: Product ID for filename generation
            
        Returns:
            Tuple of (local_file_path, status)
            - local_file_path: Path to downloaded file (None if failed)
            - status: 'downloaded', 'failed', 'exists'
        """
        try:
            # Generate filename
            base_filename = self._generate_filename(video_url, product_id)
            
            # Make initial request to get content type and check if file exists
            response = requests.head(video_url, timeout=30, allow_redirects=True)
            if response.status_code != 200:
                logger.warning(f"Video URL returned status {response.status_code}: {video_url}")
                return None, 'failed'
            
            # Determine file extension
            content_type = response.headers.get('content-type', '')
            extension = self._get_file_extension(video_url, content_type)
            filename = base_filename + extension
            
            # Full path
            file_path = os.path.join(self.download_dir, filename)
            
            # Check if file already exists
            if os.path.exists(file_path):
                logger.info(f"Video already exists: {filename}")
                return self._get_relative_path(file_path), 'exists'
            
            # Download the video
            logger.info(f"Downloading video: {video_url}")
            response = requests.get(video_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Write video to file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Video downloaded successfully: {filename} ({file_size} bytes)")
            
            return self._get_relative_path(file_path), 'downloaded'
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout downloading video: {video_url}")
            return None, 'failed'
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading video {video_url}: {e}")
            return None, 'failed'
        except Exception as e:
            logger.error(f"Unexpected error downloading video {video_url}: {e}")
            return None, 'failed'

    def get_video_info(self, file_path: str) -> dict:
        """
        Get basic information about a downloaded video file.
        
        Args:
            file_path: Path to the video file
            
        Returns:
            Dictionary with video information
        """
        try:
            if not os.path.exists(file_path):
                return {'error': 'File not found'}
            
            file_size = os.path.getsize(file_path)
            file_extension = os.path.splitext(file_path)[1].lower()
            
            return {
                'file_size': file_size,
                'extension': file_extension,
                'exists': True
            }
        except Exception as e:
            logger.error(f"Error getting video info for {file_path}: {e}")
            return {'error': str(e)}

    def cleanup_failed_downloads(self):
        """Remove any incomplete or corrupted video files."""
        try:
            for filename in os.listdir(self.download_dir):
                file_path = os.path.join(self.download_dir, filename)
                if os.path.isfile(file_path):
                    # Check if file is very small (likely incomplete)
                    file_size = os.path.getsize(file_path)
                    if file_size < 1024:  # Less than 1KB
                        logger.warning(f"Removing small/incomplete video file: {filename}")
                        os.remove(file_path)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def get_download_stats(self) -> dict:
        """
        Get statistics about downloaded videos.
        
        Returns:
            Dictionary with download statistics
        """
        try:
            if not os.path.exists(self.download_dir):
                return {'total_files': 0, 'total_size': 0}
            
            total_files = 0
            total_size = 0
            
            for filename in os.listdir(self.download_dir):
                file_path = os.path.join(self.download_dir, filename)
                if os.path.isfile(file_path):
                    total_files += 1
                    total_size += os.path.getsize(file_path)
            
            return {
                'total_files': total_files,
                'total_size': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2)
            }
        except Exception as e:
            logger.error(f"Error getting download stats: {e}")
            return {'error': str(e)}