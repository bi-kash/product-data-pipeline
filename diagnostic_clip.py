#!/usr/bin/env python3
"""
Detailed diagnostic script to show individual CLIP similarity values and decisions.
"""

import sys
import logging
from pathlib import Path

# Add the src directory to Python path
src_dir = Path(__file__).parent / 'src'
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from duplicate_detection.duplicate_detector import DuplicateDetector
from common.database import get_db_session

# Configure logging
logging.basicConfig(level=logging.WARNING)  # Reduce noise
logger = logging.getLogger(__name__)

def detailed_clip_analysis():
    """Show detailed CLIP similarity values and decisions."""
    
    print("🔍 DETAILED CLIP ANALYSIS")
    print("=" * 60)
    
    try:
        detector = DuplicateDetector()
        
        with get_db_session() as db:
            print("Running cascade duplicate detection with detailed logging...")
            results = detector.detect_cascade_duplicates(db, limit=10, dry_run=True)
            
            print("\n📊 RESULTS SUMMARY:")
            print(f"  Total pairs analyzed: {results['cascade_stats']['total_pairs']}")
            print(f"  pHash different: {results['cascade_stats']['phash_different']}")
            print(f"  pHash exact matches: {results['cascade_stats']['phash_exact']}")
            print(f"  Sent to CLIP: {results['cascade_stats']['clip_analyzed']}")
            print(f"  CLIP confirmed duplicates: {results['cascade_stats']['clip_confirmed']}")
            print(f"  CLIP review suspects: {results['cascade_stats']['clip_review_suspect']}")
            
            print("\n🔍 DUPLICATE PAIRS (Auto-confirmed by CLIP):")
            if results['duplicate_pairs']:
                for i, pair in enumerate(results['duplicate_pairs'], 1):
                    print(f"  {i}. Products {pair['product1_id']} ↔ {pair['product2_id']}")
                    if pair['clip_similarity']:
                        print(f"     CLIP similarity: {pair['clip_similarity']:.4f} | Stage: {pair['stage']}")
                    if pair['phash_difference'] is not None:
                        print(f"     pHash difference: {pair['phash_difference']}")
                    print()
            else:
                print("  No duplicate pairs found.")
            
            print("🔍 REVIEW SUSPECT PAIRS (Need manual review):")
            if results.get('review_suspect_pairs'):
                for i, pair in enumerate(results['review_suspect_pairs'], 1):
                    print(f"  {i}. Products {pair['product1_id']} ↔ {pair['product2_id']}")
                    if pair['clip_similarity']:
                        print(f"     CLIP similarity: {pair['clip_similarity']:.4f} | Stage: {pair['stage']}")
                    if pair['phash_difference'] is not None:
                        print(f"     pHash difference: {pair['phash_difference']}")
                    print()
            else:
                print("  No review suspect pairs found.")
            
            print("✅ Analysis complete!")
            
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function."""
    detailed_clip_analysis()

if __name__ == "__main__":
    main()