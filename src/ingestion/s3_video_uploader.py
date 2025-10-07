"""
S3 Video Uploader module for uploading product videos to AWS S3 with anonymized UUIDs.

This module provides functionality to upload downloaded product videos to S3 bucket
with anonymized UUID filenames while maintaining the original file metadata.
"""

import logging
import os
import uuid
from typing import Optional, Dict
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from src.common.config import get_env

# Set up logging
logger = logging.getLogger(__name__)


class S3VideoUploader:
    """
    Service for uploading product videos to AWS S3 with anonymized UUIDs.
    
    Features:
    - Generates anonymized UUID filenames for privacy
    - Maintains original file extensions
    - Uploads videos to S3 (public access via bucket policy)
    - Returns public S3 URLs
    - Handles upload errors gracefully
    
    Note: For public access, configure your S3 bucket with a public read policy.
    ACLs are not used to ensure compatibility with buckets that have ACLs disabled.
    """
    
    def __init__(self):
        """Initialize S3 client with environment configuration."""
        self.aws_access_key_id = get_env('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = get_env('AWS_SECRET_ACCESS_KEY')
        self.aws_region = get_env('AWS_REGION', 'us-east-1')
        self.bucket_name = get_env('S3_BUCKET_NAME')
        self.videos_prefix = get_env('S3_VIDEOS_PREFIX', 'product-videos/')
        
        # Ensure videos prefix ends with /
        if self.videos_prefix and not self.videos_prefix.endswith('/'):
            self.videos_prefix += '/'
        
        # Validate required configuration
        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is required")
        if not self.aws_access_key_id:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")
        if not self.aws_secret_access_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            logger.info(f"S3VideoUploader initialized for bucket: {self.bucket_name}")
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def _generate_s3_key(self, original_filename: str) -> str:
        """
        Generate an anonymized S3 key with UUID.
        
        Args:
            original_filename: Original filename with extension
            
        Returns:
            S3 key with UUID and original extension
        """
        # Get file extension
        file_extension = Path(original_filename).suffix.lower()
        
        # Generate UUID for anonymization
        unique_id = str(uuid.uuid4())
        
        # Create S3 key
        s3_key = f"{self.videos_prefix}{unique_id}{file_extension}"
        
        return s3_key

    def _get_content_type(self, file_extension: str) -> str:
        """
        Get appropriate content type for video file.
        
        Args:
            file_extension: File extension (e.g., '.mp4')
            
        Returns:
            MIME content type
        """
        content_types = {
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.mkv': 'video/x-matroska',
            '.wmv': 'video/x-ms-wmv'
        }
        
        return content_types.get(file_extension.lower(), 'video/mp4')

    def upload_video(self, local_file_path: str, product_id: str = None) -> Dict[str, any]:
        """
        Upload a video file to S3 with anonymized UUID filename.
        
        Args:
            local_file_path: Path to the local video file
            product_id: Optional product ID for logging
            
        Returns:
            Dictionary containing:
            - success: Boolean indicating upload success
            - s3_url: Public S3 URL (if successful)
            - s3_key: S3 key used (if successful)
            - error: Error message (if failed)
            - file_size: Size of uploaded file in bytes
        """
        try:
            # Validate file exists
            if not os.path.exists(local_file_path):
                return {
                    'success': False,
                    'error': f'Local file not found: {local_file_path}'
                }
            
            # Get file info
            file_size = os.path.getsize(local_file_path)
            filename = os.path.basename(local_file_path)
            file_extension = Path(filename).suffix.lower()
            
            # Generate S3 key
            s3_key = self._generate_s3_key(filename)
            
            # Determine content type
            content_type = self._get_content_type(file_extension)
            
            # Upload to S3
            logger.info(f"Uploading video to S3: {filename} -> {s3_key} (Product: {product_id})")
            
            with open(local_file_path, 'rb') as file_data:
                self.s3_client.upload_fileobj(
                    file_data,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'Metadata': {
                            'original-product-id': product_id or 'unknown',
                            'video-type': 'product-video',
                            'uploaded-by': 'product-data-pipeline'
                        }
                        # Note: Not setting ACL to support buckets with ACLs disabled
                        # Public access should be configured via bucket policy
                    }
                )
            
            # Construct public URL
            s3_url = f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/{s3_key}"
            
            logger.info(f"Video uploaded successfully: {s3_url} ({file_size} bytes)")
            
            return {
                'success': True,
                's3_url': s3_url,
                's3_key': s3_key,
                'file_size': file_size
            }
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = f"AWS S3 error ({error_code}): {str(e)}"
            logger.error(f"Failed to upload video {local_file_path}: {error_message}")
            return {
                'success': False,
                'error': error_message
            }
        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
            logger.error(f"Failed to upload video {local_file_path}: {error_message}")
            return {
                'success': False,
                'error': error_message
            }

    def check_video_exists(self, s3_key: str) -> bool:
        """
        Check if a video already exists in S3.
        
        Args:
            s3_key: S3 key to check
            
        Returns:
            True if video exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking if video exists in S3: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error checking video in S3: {e}")
            return False

    def delete_video(self, s3_key: str) -> bool:
        """
        Delete a video from S3.
        
        Args:
            s3_key: S3 key of the video to delete
            
        Returns:
            True if successfully deleted, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Video deleted from S3: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete video from S3 {s3_key}: {e}")
            return False

    def get_video_url(self, s3_key: str) -> str:
        """
        Construct public S3 URL for a given key.
        
        Args:
            s3_key: S3 key of the video
            
        Returns:
            Public S3 URL
        """
        return f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/{s3_key}"

    def list_videos(self, limit: int = 100) -> list:
        """
        List videos in the S3 bucket.
        
        Args:
            limit: Maximum number of videos to return
            
        Returns:
            List of video information dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.videos_prefix,
                MaxKeys=limit
            )
            
            videos = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    videos.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'url': self.get_video_url(obj['Key'])
                    })
            
            return videos
            
        except Exception as e:
            logger.error(f"Failed to list videos from S3: {e}")
            return []

    def get_upload_stats(self) -> Dict[str, any]:
        """
        Get statistics about uploaded videos.
        
        Returns:
            Dictionary with upload statistics
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.videos_prefix
            )
            
            if 'Contents' not in response:
                return {
                    'total_videos': 0,
                    'total_size': 0,
                    'total_size_mb': 0
                }
            
            total_videos = len(response['Contents'])
            total_size = sum(obj['Size'] for obj in response['Contents'])
            
            return {
                'total_videos': total_videos,
                'total_size': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to get upload stats: {e}")
            return {
                'error': str(e)
            }