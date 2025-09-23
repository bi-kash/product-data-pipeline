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

    def __init__(self, threshold: int = None):
        """
        Initialize the pHash analyzer.
        
        Args:
            threshold: Maximum pHash difference to consider images similar.
                      If None, reads from PHASH_THRESHOLD environment variable (default: 5)
        """
        if threshold is None:
            threshold = int(os.getenv('PHASH_THRESHOLD', '5'))
        
        self.threshold = threshold
        logger.info(f"PHashAnalyzer initialized with threshold: {threshold}")

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

    def find_similar_images(self, products_images: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Find similar images across all products using pHash comparison.
        
        Args:
            products_images: Dict mapping product_id to list of image data
            
        Returns:
            List of similarity matches with metadata
        """
        logger.info("Starting pHash similarity analysis...")
        
        all_images = []
        # Flatten all images with product context
        for product_id, images in products_images.items():
            for image in images:
                image['product_id'] = product_id
                all_images.append(image)
        
        logger.info(f"Comparing {len(all_images)} images for similarity")
        
        similar_pairs = []
        processed_pairs = set()
        
        for i, img1 in enumerate(all_images):
            for j, img2 in enumerate(all_images[i+1:], i+1):
                # Skip same product comparisons (we're looking for cross-product duplicates)
                if img1['product_id'] == img2['product_id']:
                    continue
                
                # Create a unique pair identifier to avoid duplicate comparisons
                pair_key = tuple(sorted([img1['id'], img2['id']]))
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)
                
                # Calculate pHash difference
                difference = self.calculate_hamming_distance(img1['phash'], img2['phash'])
                
                if difference <= self.threshold:
                    similar_pairs.append({
                        'product1_id': img1['product_id'],
                        'product2_id': img2['product_id'],
                        'image1_id': img1['id'],
                        'image2_id': img2['id'],
                        'image1_url': img1['image_url'],
                        'image2_url': img2['image_url'],
                        'image1_role': img1['image_role'],
                        'image2_role': img2['image_role'],
                        'phash1': img1['phash'],
                        'phash2': img2['phash'],
                        'phash_difference': difference,
                        'similarity_score': 1.0 - (difference / 64.0),  # Normalize to 0-1
                        'is_primary_match': img1['is_primary'] or img2['is_primary']
                    })
        
        logger.info(f"Found {len(similar_pairs)} similar image pairs (threshold: {self.threshold})")
        return similar_pairs

    def group_similar_products(self, similar_pairs: List[Dict]) -> List[Set[str]]:
        """
        Group products that have similar images into duplicate clusters.
        
        Args:
            similar_pairs: List of similar image pairs from find_similar_images
            
        Returns:
            List of sets, each containing product IDs that are potentially duplicates
        """
        logger.info("Grouping products into duplicate clusters...")
        
        # Build adjacency graph of similar products
        product_graph = {}
        for pair in similar_pairs:
            pid1, pid2 = pair['product1_id'], pair['product2_id']
            
            if pid1 not in product_graph:
                product_graph[pid1] = set()
            if pid2 not in product_graph:
                product_graph[pid2] = set()
            
            product_graph[pid1].add(pid2)
            product_graph[pid2].add(pid1)
        
        # Find connected components (duplicate groups)
        visited = set()
        duplicate_groups = []
        
        for product_id in product_graph:
            if product_id not in visited:
                # Depth-first search to find connected component
                group = set()
                stack = [product_id]
                
                while stack:
                    current = stack.pop()
                    if current not in visited:
                        visited.add(current)
                        group.add(current)
                        
                        # Add unvisited neighbors to stack
                        for neighbor in product_graph.get(current, []):
                            if neighbor not in visited:
                                stack.append(neighbor)
                
                if len(group) > 1:  # Only keep groups with multiple products
                    duplicate_groups.append(group)
        
        logger.info(f"Found {len(duplicate_groups)} potential duplicate groups")
        for i, group in enumerate(duplicate_groups):
            logger.debug(f"  Group {i+1}: {len(group)} products - {list(group)[:3]}...")
        
        return duplicate_groups

    def analyze_duplicates(self, db: Session, limit: int = None) -> Dict:
        """
        Main method to analyze duplicates using pHash comparison.
        
        Args:
            db: Database session
            limit: Limit number of products to process (for testing)
            
        Returns:
            Dict with analysis results
        """
        logger.info(f"Starting pHash duplicate analysis (limit: {limit})")
        
        # Get all product images with pHash
        products_images = self.get_product_images_with_phash(db, limit)
        
        if not products_images:
            logger.warning("No products with pHash found")
            return {
                'total_products': 0,
                'total_images': 0,
                'similar_pairs': [],
                'duplicate_groups': [],
                'stats': {
                    'products_with_images': 0,
                    'potential_duplicates': 0,
                    'largest_group_size': 0
                }
            }
        
        # Find similar images
        similar_pairs = self.find_similar_images(products_images)
        
        # Group into duplicate clusters
        duplicate_groups = self.group_similar_products(similar_pairs)
        
        # Calculate statistics
        total_images = sum(len(imgs) for imgs in products_images.values())
        largest_group_size = max(len(group) for group in duplicate_groups) if duplicate_groups else 0
        potential_duplicates = sum(len(group) for group in duplicate_groups)
        
        results = {
            'total_products': len(products_images),
            'total_images': total_images,
            'similar_pairs': similar_pairs,
            'duplicate_groups': duplicate_groups,
            'stats': {
                'products_with_images': len(products_images),
                'similar_pairs_found': len(similar_pairs),
                'duplicate_groups_found': len(duplicate_groups),
                'potential_duplicates': potential_duplicates,
                'largest_group_size': largest_group_size,
                'threshold_used': self.threshold
            }
        }
        
        logger.info(f"pHash analysis complete: {len(duplicate_groups)} groups, {potential_duplicates} potential duplicates")
        return results

    def get_candidates_for_clip(self, duplicate_groups: List[Set[str]]) -> List[str]:
        """
        Get list of product IDs that should be analyzed with CLIP.
        
        Args:
            duplicate_groups: List of product ID sets from group_similar_products
            
        Returns:
            List of unique product IDs that need CLIP analysis
        """
        candidates = set()
        for group in duplicate_groups:
            candidates.update(group)
        
        candidate_list = list(candidates)
        logger.info(f"Generated {len(candidate_list)} candidates for CLIP analysis")
        return candidate_list


def main():
    """Test function for the pHash analyzer."""
    import sys
    from src.common.config import get_env
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Get threshold from environment or use default
    threshold = int(get_env('PHASH_THRESHOLD', '5'))
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    
    print(f"🔍 Testing pHash Analyzer (threshold: {threshold}, limit: {limit})")
    
    analyzer = PHashAnalyzer(threshold=threshold)
    
    with get_db_session() as db:
        results = analyzer.analyze_duplicates(db, limit=limit)
        
        print(f"\n📊 pHash Analysis Results:")
        print(f"  📸 Total products: {results['total_products']}")
        print(f"  🖼️  Total images: {results['total_images']}")
        print(f"  🔗 Similar pairs: {results['stats']['similar_pairs_found']}")
        print(f"  📦 Duplicate groups: {results['stats']['duplicate_groups_found']}")
        print(f"  🎯 Potential duplicates: {results['stats']['potential_duplicates']}")
        print(f"  📏 Largest group: {results['stats']['largest_group_size']} products")
        
        if results['duplicate_groups']:
            print(f"\n🔍 Top duplicate groups:")
            for i, group in enumerate(results['duplicate_groups'][:5]):
                print(f"  Group {i+1}: {len(group)} products - {list(group)[:3]}...")


if __name__ == "__main__":
    main()