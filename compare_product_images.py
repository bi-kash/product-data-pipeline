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
from dotenv import load_dotenv

# Add src to path for imports
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.common.database import get_db_session, ProductImage

# Import pHash analyzer directly - we'll implement hamming distance calculation here
import imagehash
from PIL import Image

# Load environment variables for cascade configuration
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cascade configuration from environment
PHASH_DUPLICATE_THRESHOLD = int(os.getenv('PHASH_DUPLICATE_THRESHOLD', '2'))
PHASH_AMBIGUOUS_THRESHOLD = int(os.getenv('PHASH_AMBIGUOUS_THRESHOLD', '18'))
CLIP_DUPLICATE_THRESHOLD = float(os.getenv('CLIP_DUPLICATE_THRESHOLD', '0.95'))

# Try to import CLIP analyzer
CLIP_AVAILABLE = False
try:
    from src.duplicate_detection.clip_analyzer import CLIPAnalyzer
    CLIP_AVAILABLE = True
    logger.info("CLIP analyzer available for cascade comparisons")
except ImportError as e:
    logger.warning(f"CLIP analyzer not available: {e}")
    logger.info("Will use pHash-only comparisons")


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
    cascade_decision: str = "PHASH_ONLY"  # PHASH_DUPLICATE, PHASH_DIFFERENT, PHASH_AMBIGUOUS, CLIP_DUPLICATE, CLIP_DIFFERENT
    clip_similarity: Optional[float] = None
    is_duplicate_by_cascade: bool = False


class ProductImageComparer:
    """Compares pHash values between images of two products with intelligent cascade."""
    
    def __init__(self):
        """Initialize the comparer."""
        self.clip_analyzer = None
        if CLIP_AVAILABLE:
            try:
                self.clip_analyzer = CLIPAnalyzer()
                logger.info("ProductImageComparer initialized with CLIP support")
            except Exception as e:
                logger.warning(f"Failed to initialize CLIP analyzer: {e}")
                logger.info("Using pHash-only mode")
        else:
            logger.info("ProductImageComparer initialized in pHash-only mode")
        
        logger.info(f"Cascade thresholds: pHash duplicate≤{PHASH_DUPLICATE_THRESHOLD}, "
                   f"ambiguous {PHASH_DUPLICATE_THRESHOLD+1}-{PHASH_AMBIGUOUS_THRESHOLD}, "
                   f"different>{PHASH_AMBIGUOUS_THRESHOLD}, CLIP duplicate≥{CLIP_DUPLICATE_THRESHOLD}")
    
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
        Compare images using intelligent cascade: pHash first, then CLIP if ambiguous.
        
        Args:
            image1: First image
            image2: Second image
            
        Returns:
            ComparisonResult with cascade decision and similarity scores
        """
        hamming_distance = self.calculate_hamming_distance(
            image1.phash, image2.phash
        )
        
        # Calculate similarity percentage (64 is max hamming distance for pHash)
        similarity_percentage = (64 - hamming_distance) / 64 * 100
        
        # Apply cascade decision logic
        if hamming_distance <= PHASH_DUPLICATE_THRESHOLD:
            # Definitely duplicate - skip CLIP
            return ComparisonResult(
                image1=image1,
                image2=image2,
                hamming_distance=hamming_distance,
                similarity_percentage=similarity_percentage,
                cascade_decision="PHASH_DUPLICATE",
                is_duplicate_by_cascade=True
            )
        
        elif hamming_distance > PHASH_AMBIGUOUS_THRESHOLD:
            # Definitely different - skip CLIP
            return ComparisonResult(
                image1=image1,
                image2=image2,
                hamming_distance=hamming_distance,
                similarity_percentage=similarity_percentage,
                cascade_decision="PHASH_DIFFERENT",
                is_duplicate_by_cascade=False
            )
        
        else:
            # Ambiguous zone - use CLIP analysis if available
            clip_similarity = None
            cascade_decision = "PHASH_AMBIGUOUS"
            is_duplicate_by_cascade = False
            
            if self.clip_analyzer and image1.local_file_path and image2.local_file_path:
                try:
                    # Extract embeddings for both images
                    embedding1 = self.clip_analyzer.extract_image_embedding(image1.local_file_path, image1.id)
                    embedding2 = self.clip_analyzer.extract_image_embedding(image2.local_file_path, image2.id)
                    
                    if embedding1 is not None and embedding2 is not None:
                        # Calculate similarity using embeddings
                        clip_similarity = self.clip_analyzer.calculate_similarity(embedding1, embedding2)
                        
                        if clip_similarity >= CLIP_DUPLICATE_THRESHOLD:
                            cascade_decision = "CLIP_DUPLICATE"
                            is_duplicate_by_cascade = True
                        else:
                            cascade_decision = "CLIP_DIFFERENT" 
                            is_duplicate_by_cascade = False
                            
                        logger.debug(f"CLIP analysis: similarity={clip_similarity:.3f}, decision={cascade_decision}")
                    else:
                        logger.warning("Failed to extract embeddings for CLIP analysis")
                        cascade_decision = "PHASH_AMBIGUOUS"
                    
                except Exception as e:
                    logger.warning(f"CLIP analysis failed: {e}")
                    cascade_decision = "PHASH_AMBIGUOUS"
            else:
                logger.debug("CLIP analysis not available for ambiguous case")
            
            return ComparisonResult(
                image1=image1,
                image2=image2,
                hamming_distance=hamming_distance,
                similarity_percentage=similarity_percentage,
                cascade_decision=cascade_decision,
                clip_similarity=clip_similarity,
                is_duplicate_by_cascade=is_duplicate_by_cascade
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
            
            # Group by cascade decisions
            phash_duplicates = [c for c in all_comparisons if c.cascade_decision == "PHASH_DUPLICATE"]
            phash_different = [c for c in all_comparisons if c.cascade_decision == "PHASH_DIFFERENT"]
            phash_ambiguous = [c for c in all_comparisons if c.cascade_decision == "PHASH_AMBIGUOUS"]
            clip_duplicates = [c for c in all_comparisons if c.cascade_decision == "CLIP_DUPLICATE"]
            clip_different = [c for c in all_comparisons if c.cascade_decision == "CLIP_DIFFERENT"]
            
            # Count duplicates by cascade
            duplicates_by_cascade = [c for c in all_comparisons if c.is_duplicate_by_cascade]
            
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
                'cascade_statistics': {
                    'phash_duplicates': len(phash_duplicates),
                    'phash_different': len(phash_different), 
                    'phash_ambiguous': len(phash_ambiguous),
                    'clip_duplicates': len(clip_duplicates),
                    'clip_different': len(clip_different),
                    'total_duplicates_by_cascade': len(duplicates_by_cascade)
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
    print(f"  🤖 Cascade Decision: {best.cascade_decision}")
    if best.clip_similarity is not None:
        print(f"  🧠 CLIP Similarity: {best.clip_similarity:.3f}")
    print(f"  ✅ Is Duplicate: {best.is_duplicate_by_cascade}")
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
    
    # Add cascade statistics if available
    if 'cascade_statistics' in results:
        cascade_stats = results['cascade_statistics']
        print(f"\n🤖 Intelligent Cascade Analysis:")
        print(f"  📏 pHash Duplicates (≤{PHASH_DUPLICATE_THRESHOLD}): {cascade_stats['phash_duplicates']}")
        print(f"  🔍 pHash Ambiguous ({PHASH_DUPLICATE_THRESHOLD+1}-{PHASH_AMBIGUOUS_THRESHOLD}): {cascade_stats['phash_ambiguous']}")
        print(f"  📏 pHash Different (>{PHASH_AMBIGUOUS_THRESHOLD}): {cascade_stats['phash_different']}")
        if cascade_stats['clip_duplicates'] > 0 or cascade_stats['clip_different'] > 0:
            print(f"  🧠 CLIP Duplicates (≥{CLIP_DUPLICATE_THRESHOLD:.2f}): {cascade_stats['clip_duplicates']}")
            print(f"  🧠 CLIP Different (<{CLIP_DUPLICATE_THRESHOLD:.2f}): {cascade_stats['clip_different']}")
        print(f"  ✅ Total Duplicates by Cascade: {cascade_stats['total_duplicates_by_cascade']}")
        
        # Show CLIP usage efficiency
        clip_analyzed = cascade_stats['clip_duplicates'] + cascade_stats['clip_different']
        efficiency = (results['total_comparisons'] - clip_analyzed) / results['total_comparisons'] * 100
        print(f"  ⚡ Efficiency: {efficiency:.1f}% (avoided CLIP on {results['total_comparisons'] - clip_analyzed}/{results['total_comparisons']} comparisons)")


def print_all_comparisons(results: Dict, limit: int = 10):
    """Print all comparisons, sorted by hamming distance."""
    if 'error' in results:
        return
    
    comparisons = results['all_comparisons']
    sorted_comparisons = sorted(comparisons, key=lambda x: x.hamming_distance)
    
    print(f"\n📋 All Comparisons (showing top {min(limit, len(sorted_comparisons))}):")
    print(f"{'Rank':<4} {'SKU1':<15} {'SKU2':<15} {'pDist':<6} {'Sim%':<6} {'Cascade':<12} {'CLIP':<7} {'Dup':<4} {'Role1':<6} {'Role2':<6}")
    print("-" * 95)
    
    for i, comp in enumerate(sorted_comparisons[:limit], 1):
        clip_str = f"{comp.clip_similarity:.3f}" if comp.clip_similarity is not None else "N/A"
        dup_str = "✅" if comp.is_duplicate_by_cascade else "❌"
        print(f"{i:<4} {comp.image1.sku_id:<15} {comp.image2.sku_id:<15} "
              f"{comp.hamming_distance:<6} {comp.similarity_percentage:<5.1f} "
              f"{comp.cascade_decision:<12} {clip_str:<7} {dup_str:<4} "
              f"{comp.image1.image_role:<6} {comp.image2.image_role:<6}")


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