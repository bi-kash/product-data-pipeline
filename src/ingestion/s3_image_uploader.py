"""
S3 Image Uploader module for uploading product images to AWS S3 with anonymized UUIDs.

This module provides functionality to upload downloaded product images to S3 bucket
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


class S3ImageUploader:
    """
    Service for uploading product images to AWS S3 with anonymized UUIDs.
    
    Features:
    - Generates anonymized UUID filenames for privacy
    - Maintains original file extensions
    - Uploads images to S3 (public access via bucket policy)
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
        self.images_prefix = get_env('S3_IMAGES_PREFIX', 'product-images/')
        
        # Validate required configuration
        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is required")
        if not self.aws_access_key_id:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")
        if not self.aws_secret_access_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")
        
        # Ensure images prefix ends with /
        if self.images_prefix and not self.images_prefix.endswith('/'):
            self.images_prefix += '/'
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            logger.info(f"Initialized S3 client for bucket: {self.bucket_name}")
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
    
    def generate_uuid_filename(self, original_path: str) -> str:
        """
        Generate an anonymized UUID filename while preserving the file extension.
        
        Args:
            original_path: Original file path or filename
            
        Returns:
            UUID-based filename with original extension
        """
        original_file = Path(original_path)
        extension = original_file.suffix.lower()
        
        # Generate a new UUID for anonymization
        unique_id = str(uuid.uuid4())
        
        return f"{unique_id}{extension}"
    
    def upload_image(self, local_file_path: str, product_id: str, image_role: str = 'unknown') -> Optional[str]:
        """
        Upload a single image file to S3 with anonymized UUID filename.
        
        Args:
            local_file_path: Path to the local image file
            product_id: Product ID for logging purposes
            image_role: Role of the image (hero, gallery, variant) for logging
            
        Returns:
            Public S3 URL if successful, None if failed
        """
        if not os.path.exists(local_file_path):
            logger.error(f"Local file not found: {local_file_path}")
            return None
        
        try:
            # Generate anonymized filename
            uuid_filename = self.generate_uuid_filename(local_file_path)
            s3_key = f"{self.images_prefix}{uuid_filename}"
            
            # Determine content type based on file extension
            content_type = self._get_content_type(local_file_path)
            
            # Upload file to S3
            logger.info(f"Uploading {local_file_path} to s3://{self.bucket_name}/{s3_key}")
            
            with open(local_file_path, 'rb') as file_data:
                self.s3_client.upload_fileobj(
                    file_data,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'Metadata': {
                            'original-product-id': product_id,
                            'image-role': image_role,
                            'uploaded-by': 'product-data-pipeline'
                        }
                    }
                )
            
            # Generate public URL
            public_url = f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/{s3_key}"
            
            logger.info(f"Successfully uploaded image for product {product_id} ({image_role}): {public_url}")
            return public_url
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"AWS S3 error uploading {local_file_path}: {error_code} - {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error uploading {local_file_path}: {e}")
            return None
    
    def upload_multiple_images(self, image_paths: Dict[str, str], product_id: str) -> Dict[str, Optional[str]]:
        """
        Upload multiple images for a product.
        
        Args:
            image_paths: Dictionary mapping image_role to local file path
            product_id: Product ID for logging purposes
            
        Returns:
            Dictionary mapping image_role to S3 URL (or None if failed)
        """
        results = {}
        
        for image_role, local_path in image_paths.items():
            s3_url = self.upload_image(local_path, product_id, image_role)
            results[image_role] = s3_url
        
        successful_uploads = sum(1 for url in results.values() if url is not None)
        logger.info(f"Uploaded {successful_uploads}/{len(image_paths)} images for product {product_id}")
        
        return results
    
    def _get_content_type(self, file_path: str) -> str:
        """
        Determine content type based on file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MIME content type string
        """
        extension = Path(file_path).suffix.lower()
        
        content_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.webp': 'image/webp',
            '.bmp': 'image/bmp',
            '.tiff': 'image/tiff',
            '.tif': 'image/tiff'
        }
        
        return content_types.get(extension, 'application/octet-stream')
    
    def test_connection(self) -> bool:
        """
        Test S3 connection and bucket access.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to list objects in the bucket (limit to 1 for efficiency)
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"Failed to connect to S3 bucket {self.bucket_name}: {error_code} - {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error testing S3 connection: {e}")
            return False
    
    def get_bucket_info(self) -> Dict[str, str]:
        """
        Get information about the configured S3 bucket.
        
        Returns:
            Dictionary with bucket configuration details
        """
        return {
            'bucket_name': self.bucket_name,
            'region': self.aws_region,
            'images_prefix': self.images_prefix,
            'public_url_base': f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/"
        }
    
    def get_public_bucket_policy(self) -> str:
        """
        Get the recommended bucket policy for public read access.
        
        Returns:
            JSON string of the bucket policy
        """
        import json
        
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/{self.images_prefix}*"
                }
            ]
        }
        
        return json.dumps(policy, indent=2)


def main():
    """Test function for S3ImageUploader."""
    import sys
    
    # Test S3 connection
    try:
        uploader = S3ImageUploader()
        print("✅ S3ImageUploader initialized successfully")
        
        # Test connection
        if uploader.test_connection():
            print("✅ S3 connection test passed")
            
            # Print bucket info
            info = uploader.get_bucket_info()
            print(f"📊 Bucket info: {info}")
        else:
            print("❌ S3 connection test failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()