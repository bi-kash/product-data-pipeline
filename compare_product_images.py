#!/usr/bin/env python3
"""
Product Image pHash Comparison Script

This script compares pHash values of all images between two products
and finds the SKU ID pair with the smallest difference.

Usage:
    python compare_product_images.py <product_id_1> <product_id_2>
    
Example:
    python compare_product_images.py 1005009917334390 1005009935945771
"""

import sys
import argparse
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# Add src to path for imports
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from common.database import get_db_session, ProductImage

# Import pHash analyzer directly - we'll implement hamming distance calculation here
import imagehash
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ImageInfo:
    """Information about a product image."""
    id: int
    sku_id: str
    image_role: str
    phash: str
    local_file_path: str
    width: Optional[int]
    height: Optional[int]
    variant_key: Optional[str]
    property_id: Optional[int]


@dataclass
class ComparisonResult:
    """Result of comparing two images."""
    image1: ImageInfo
    image2: ImageInfo
    hamming_distance: int
    similarity_percentage: float


class ProductImageComparer:
    """Compares pHash values between images of two products."""
    
    def __init__(self):
        """Initialize the comparer."""
        logger.info("ProductImageComparer initialized")
    
    def calculate_hamming_distance(self, hash1: str, hash2: str) -> int:
        """
        Calculate hamming distance between two pHash strings.
        
        Args:
            hash1: First pHash as hex string
            hash2: Second pHash as hex string
            
        Returns:
            Hamming distance (number of different bits)
        """
        if not hash1 or not hash2:
            return 64  # Maximum distance if either hash is missing
        
        try:
            # Convert hex strings to integers and XOR them
            val1 = int(hash1, 16) if isinstance(hash1, str) else hash1
            val2 = int(hash2, 16) if isinstance(hash2, str) else hash2
            
            # Count different bits
            return bin(val1 ^ val2).count('1')
        except (ValueError, TypeError):
            logger.error(f"Error calculating hamming distance: hash1={hash1}, hash2={hash2}")
            return 64
    
    def get_product_images(self, db, product_id: str) -> List[ImageInfo]:
        """
        Get all images with pHash for a product.
        
        Args:
            db: Database session
            product_id: Product ID to get images for
            
        Returns:
            List of ImageInfo objects with pHash data
        """
        images = db.query(ProductImage).filter(
            ProductImage.product_id == product_id,
            ProductImage.phash.isnot(None)
        ).all()
        
        result = []
        for img in images:
            result.append(ImageInfo(
                id=img.id,
                sku_id=img.sku_id or 'unknown',
                image_role=img.image_role or 'unknown',
                phash=img.phash,
                local_file_path=img.local_file_path or 'unknown',
                width=img.width,
                height=img.height,
                variant_key=img.variant_key,
                property_id=img.property_id
            ))
        
        logger.info(f"Found {len(result)} images with pHash for product {product_id}")
        return result
    
    def compare_images(self, image1: ImageInfo, image2: ImageInfo) -> ComparisonResult:
        """
        Compare pHash between two images.
        
        Args:
            image1: First image
            image2: Second image
            
        Returns:
            ComparisonResult with hamming distance and similarity
        """
        hamming_distance = self.calculate_hamming_distance(
            image1.phash, image2.phash
        )
        
        # Calculate similarity percentage (64 is max hamming distance for pHash)
        similarity_percentage = (64 - hamming_distance) / 64 * 100
        
        return ComparisonResult(
            image1=image1,
            image2=image2,
            hamming_distance=hamming_distance,
            similarity_percentage=similarity_percentage
        )
    
    def compare_products(self, product1_id: str, product2_id: str) -> Dict:
        """
        Compare all images between two products.
        
        Args:
            product1_id: First product ID
            product2_id: Second product ID
            
        Returns:
            Dict with comparison results
        """
        logger.info(f"Comparing images between products {product1_id} and {product2_id}")
        
        with get_db_session() as db:
            # Get images for both products
            images1 = self.get_product_images(db, product1_id)
            images2 = self.get_product_images(db, product2_id)
            
            if not images1:
                logger.error(f"No images with pHash found for product {product1_id}")
                return {
                    'error': f'No images with pHash found for product {product1_id}',
                    'product1_id': product1_id,
                    'product2_id': product2_id
                }
            
            if not images2:
                logger.error(f"No images with pHash found for product {product2_id}")
                return {
                    'error': f'No images with pHash found for product {product2_id}',
                    'product1_id': product1_id,
                    'product2_id': product2_id
                }
            
            # Compare all image pairs
            all_comparisons = []
            
            for img1 in images1:
                for img2 in images2:
                    comparison = self.compare_images(img1, img2)
                    all_comparisons.append(comparison)
            
            # Find the best match (lowest hamming distance)
            best_match = min(all_comparisons, key=lambda x: x.hamming_distance)
            
            # Calculate statistics
            total_comparisons = len(all_comparisons)
            avg_distance = sum(c.hamming_distance for c in all_comparisons) / total_comparisons
            avg_similarity = sum(c.similarity_percentage for c in all_comparisons) / total_comparisons
            
            # Group by similarity ranges
            exact_matches = [c for c in all_comparisons if c.hamming_distance == 0]
            very_similar = [c for c in all_comparisons if 0 < c.hamming_distance <= 5]
            similar = [c for c in all_comparisons if 5 < c.hamming_distance <= 15]
            different = [c for c in all_comparisons if c.hamming_distance > 15]
            
            return {
                'product1_id': product1_id,
                'product2_id': product2_id,
                'product1_image_count': len(images1),
                'product2_image_count': len(images2),
                'total_comparisons': total_comparisons,
                'best_match': best_match,
                'statistics': {
                    'avg_hamming_distance': avg_distance,
                    'avg_similarity_percentage': avg_similarity,
                    'exact_matches': len(exact_matches),
                    'very_similar': len(very_similar),
                    'similar': len(similar),
                    'different': len(different)
                },
                'all_comparisons': all_comparisons
            }


def print_comparison_results(results: Dict):
    """Print formatted comparison results."""
    if 'error' in results:
        print(f"\n❌ Error: {results['error']}")
        return
    
    best = results['best_match']
    stats = results['statistics']
    
    print(f"\n🔍 Product Image Comparison Results")
    print(f"📦 Product 1: {results['product1_id']} ({results['product1_image_count']} images)")
    print(f"📦 Product 2: {results['product2_id']} ({results['product2_image_count']} images)")
    print(f"🔢 Total comparisons: {results['total_comparisons']}")
    
    print(f"\n🏆 Best Match (Smallest pHash Difference):")
    print(f"  🎯 SKU ID 1: {best.image1.sku_id}")
    print(f"  🎯 SKU ID 2: {best.image2.sku_id}")
    print(f"  📏 Hamming Distance: {best.hamming_distance}")
    print(f"  📊 Similarity: {best.similarity_percentage:.1f}%")
    print(f"  🖼️  Image 1: ID={best.image1.id}, Role={best.image1.image_role}")
    print(f"      📁 Path: {best.image1.local_file_path}")
    print(f"      📐 Size: {best.image1.width}x{best.image1.height}")
    print(f"  🖼️  Image 2: ID={best.image2.id}, Role={best.image2.image_role}")
    print(f"      📁 Path: {best.image2.local_file_path}")
    print(f"      📐 Size: {best.image2.width}x{best.image2.height}")
    
    print(f"\n📈 Comparison Statistics:")
    print(f"  📊 Average Hamming Distance: {stats['avg_hamming_distance']:.1f}")
    print(f"  📊 Average Similarity: {stats['avg_similarity_percentage']:.1f}%")
    print(f"  ✅ Exact matches (distance=0): {stats['exact_matches']}")
    print(f"  🎯 Very similar (distance=1-5): {stats['very_similar']}")
    print(f"  📸 Similar (distance=6-15): {stats['similar']}")
    print(f"  ❌ Different (distance>15): {stats['different']}")


def print_all_comparisons(results: Dict, limit: int = 10):
    """Print all comparisons, sorted by hamming distance."""
    if 'error' in results:
        return
    
    comparisons = results['all_comparisons']
    sorted_comparisons = sorted(comparisons, key=lambda x: x.hamming_distance)
    
    print(f"\n📋 All Comparisons (showing top {min(limit, len(sorted_comparisons))}):")
    print(f"{'Rank':<4} {'SKU1':<15} {'SKU2':<15} {'Distance':<8} {'Similarity':<10} {'Role1':<8} {'Role2':<8}")
    print("-" * 80)
    
    for i, comp in enumerate(sorted_comparisons[:limit], 1):
        print(f"{i:<4} {comp.image1.sku_id:<15} {comp.image2.sku_id:<15} "
              f"{comp.hamming_distance:<8} {comp.similarity_percentage:<8.1f}% "
              f"{comp.image1.image_role:<8} {comp.image2.image_role:<8}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Compare pHash values of images between two products',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python compare_product_images.py 1005009917334390 1005009935945771
    python compare_product_images.py 1005009917334390 1005009935945771 --show-all 5
    python compare_product_images.py 1005009917334390 1005009935945771 --verbose
        """
    )
    
    parser.add_argument('product1_id', help='First product ID')
    parser.add_argument('product2_id', help='Second product ID')
    parser.add_argument('--show-all', type=int, default=0, metavar='N',
                       help='Show all comparisons (top N results)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate inputs
    if not args.product1_id or not args.product2_id:
        print("❌ Error: Both product IDs are required")
        parser.print_help()
        sys.exit(1)
    
    if args.product1_id == args.product2_id:
        print("❌ Error: Product IDs must be different")
        sys.exit(1)
    
    try:
        # Create comparer and run comparison
        comparer = ProductImageComparer()
        results = comparer.compare_products(args.product1_id, args.product2_id)
        
        # Print results
        print_comparison_results(results)
        
        if args.show_all > 0 and 'all_comparisons' in results:
            print_all_comparisons(results, args.show_all)
        
        # Exit code based on results
        if 'error' in results:
            sys.exit(1)
        else:
            print(f"\n✅ Comparison completed successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()