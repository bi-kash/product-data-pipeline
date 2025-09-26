"""
pHash Analyzer for fast image similarity detection.

This module compares perceptual hashes (pHash) that are already calculated 
and stored in the product_images table to find potential duplicates quickly.
"""

import logging
import os
from typing import List, Dict, Tuple, Set
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from src.common.database import ProductImage, FilteredProduct, get_db_session

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class PHashAnalyzer:
    """
    Analyzer for fast pHash-based duplicate detection.
    
    Uses pre-calculated pHash values from the product_images table
    to find potential duplicates before CLIP analysis.
    """

    def __init__(self):
        """
        Initialize the pHash analyzer.
        
        Note: Threshold logic is handled by the cascade analyzer.
        This class only provides the calculate_hamming_distance method.
        """
        logger.info("PHashAnalyzer initialized")

    def calculate_hamming_distance(self, hash1: str, hash2: str) -> int:
        """
        Calculate Hamming distance between two pHash strings.
        
        Args:
            hash1: First pHash as hex string
            hash2: Second pHash as hex string
            
        Returns:
            Hamming distance (number of differing bits)
        """
        if not hash1 or not hash2 or len(hash1) != len(hash2):
            return float('inf')  # Invalid comparison
        
        try:
            # Convert hex strings to integers and XOR them
            xor_result = int(hash1, 16) ^ int(hash2, 16)
            
            # Count number of 1 bits (differing bits)
            return bin(xor_result).count('1')
            
        except ValueError:
            logger.error(f"Error calculating Hamming distance: {hash1} vs {hash2}")
            return float('inf')

    def get_product_images_with_phash(self, db: Session, limit: int = None) -> Dict[str, List[Dict]]:
        """
        Get all product images that have pHash values, grouped by product.
        
        Args:
            db: Database session
            limit: Limit number of products to process (for testing)
            
        Returns:
            Dict mapping product_id to list of image data with pHash
        """
        logger.info(f"Fetching product images with pHash (limit: {limit})")
        
        query = db.query(ProductImage).filter(
            ProductImage.phash.isnot(None),
            ProductImage.phash != ''
        )
        
        if limit:
            # Get images for limited number of products
            product_ids = db.query(FilteredProduct.product_id).limit(limit).all()
            product_ids = [pid[0] for pid in product_ids]
            query = query.filter(ProductImage.product_id.in_(product_ids))
        
        images = query.all()
        
        # Group by product_id
        products_images = {}
        for image in images:
            if image.product_id not in products_images:
                products_images[image.product_id] = []
            
            products_images[image.product_id].append({
                'id': image.id,
                'image_url': image.image_url,
                'image_role': image.image_role,
                'phash': image.phash,
                'variant_key': image.variant_key,
                'is_primary': image.is_primary
            })
        
        logger.info(f"Found {len(products_images)} products with {sum(len(imgs) for imgs in products_images.values())} images")
        return products_images

