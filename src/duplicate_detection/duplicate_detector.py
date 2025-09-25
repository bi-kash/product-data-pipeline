"""
Main duplicate detection engine that orchestrates the analysis pipeline.

This module coordinates between pHash and CLIP analyzers to find potential
duplicates efficiently using an intelligent cascade approach.
"""

import os
import logging
import time
from typing import List, Dict, Set, Optional
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..common.database import ProductStatus, FilteredProduct, ProductImage, get_db_session, create_tables_if_not_exist
from .cascade_analyzer import IntelligentCascadeAnalyzer, CascadeDecision
from .master_selector import MasterSelector

# Load environment variables
load_dotenv()

def get_env(key: str, default: str) -> str:
    """Get environment variable with fallback."""
    return os.getenv(key, default)

logger = logging.getLogger(__name__)


class DuplicateDetector:
    """
    Main duplicate detection engine using intelligent cascade analysis.
    
    Uses cheap checks first (metadata, pHash) and escalates to expensive 
    CLIP analysis only when necessary.
    """

    def __init__(self, phash_threshold: int = None, clip_threshold: float = None,
                 clip_enabled: bool = True, max_images_per_product: int = None):
        """
        Initialize the duplicate detector with cascade analyzer.
        
        Args:
            phash_threshold: Deprecated - now handled by cascade config
            clip_threshold: Deprecated - now handled by cascade config  
            clip_enabled: Whether to enable CLIP analysis
            max_images_per_product: Max images per product to analyze
        """
        logger.info("Initializing DuplicateDetector with intelligent cascade")
        
        # Initialize cascade analyzer (reads config from .env)
        self.cascade_analyzer = IntelligentCascadeAnalyzer()
        self.master_selector = MasterSelector()
        
        # Log the actual configuration values used
        config = self.cascade_analyzer.config
        logger.info(f"Cascade config: pHash duplicate≤{config.phash_duplicate_threshold}, "
                   f"ambiguous={config.phash_duplicate_threshold+1}-{config.phash_ambiguous_threshold}, "
                   f"different>{config.phash_ambiguous_threshold}, "
                   f"CLIP duplicate≥{config.clip_duplicate_threshold}, "
                   f"max_images={config.max_images_per_product}")

    def ensure_tables_exist(self):
        """Ensure all required database tables exist."""
        try:
            create_tables_if_not_exist()
            logger.info("Database tables verified/created")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise

    def clear_existing_status(self, db: Session, product_ids: List[str] = None):
        """
        Clear existing status entries for products.
        
        Args:
            db: Database session
            product_ids: List of product IDs to clear (default: all)
        """
        query = db.query(ProductStatus)
        
        if product_ids:
            query = query.filter(ProductStatus.product_id.in_(product_ids))
        
        deleted_count = query.delete()
        db.commit()
        
        logger.info(f"Cleared {deleted_count} existing status entries")

    def mark_unique_products(self, db: Session, all_product_ids: Set[str], 
                            duplicate_product_ids: Set[str]):
        """
        Mark products that are not duplicates as UNIQUE.
        
        Args:
            db: Database session
            all_product_ids: Set of all product IDs being analyzed
            duplicate_product_ids: Set of product IDs that are duplicates
        """
        unique_product_ids = all_product_ids - duplicate_product_ids
        
        logger.info(f"Marking {len(unique_product_ids)} products as UNIQUE")
        
        for product_id in unique_product_ids:
            # Calculate total landed cost for unique products too
            price = self.master_selector.calculate_lowest_price(db, product_id)
            
            unique_status = ProductStatus(
                product_id=product_id,
                status='UNIQUE',
                duplicate_master_id=None,
                total_landed_cost=price,
                detection_method='NO_DUPLICATES'
            )
            db.add(unique_status)
        
        db.commit()

    def save_status_assignments(self, db: Session, status_assignments: List[Dict]):
        """
        Save status assignments to the database.
        
        Args:
            db: Database session
            status_assignments: List of status assignment dictionaries
        """
        logger.info(f"Saving {len(status_assignments)} status assignments to database")
        
        for assignment in status_assignments:
            status_entry = ProductStatus(**assignment)
            db.add(status_entry)
        
        db.commit()
        logger.info("Status assignments saved successfully")

    def detect_duplicates(self, db: Session, limit: int = None, 
                         dry_run: bool = False) -> Dict:
        """
        Run the intelligent cascade duplicate detection pipeline.
        
        Args:
            db: Database session
            limit: Limit number of products to process (for testing)
            dry_run: If True, don't save results to database
            
        Returns:
            Dict with complete detection results
        """
        start_time = time.time()
        logger.info(f"Starting cascade duplicate detection (limit: {limit}, dry_run: {dry_run})")
        
        try:
            # Ensure tables exist
            self.ensure_tables_exist()
            
            # Get all products to analyze
            logger.info("� Getting products for analysis...")
            query = db.query(FilteredProduct.product_id).join(ProductImage, 
                FilteredProduct.product_id == ProductImage.product_id).distinct()
            
            if limit:
                query = query.limit(limit)
            
            all_product_ids = [row.product_id for row in query.all()]
            logger.info(f"Found {len(all_product_ids)} products with images")
            
            if len(all_product_ids) < 2:
                logger.info("Not enough products for duplicate detection")
                return self._create_empty_results(start_time, all_product_ids)
            
            # Cascade analysis - compare all pairs efficiently
            logger.info("🧬 Running intelligent cascade analysis...")
            cascade_decisions = []
            duplicate_pairs = []
            
            # Track cascade stage statistics
            cascade_stats = {
                'metadata_shortcuts': 0,
                'phash_exact': 0,
                'phash_near': 0,
                'phash_different': 0,
                'phash_ambiguous': 0,
                'clip_analyzed': 0,
                'clip_confirmed': 0,
                'total_pairs': 0
            }
            
            total_pairs = len(all_product_ids) * (len(all_product_ids) - 1) // 2
            cascade_stats['total_pairs'] = total_pairs
            logger.info(f"Analyzing {total_pairs} product pairs...")
            
            # Analyze all pairs using cascade
            for i, product1_id in enumerate(all_product_ids):
                for j, product2_id in enumerate(all_product_ids[i+1:], start=i+1):
                    decision = self.cascade_analyzer.analyze_product_pair(product1_id, product2_id, db)
                    cascade_decisions.append(decision)
                    
                    # Update statistics
                    stage = decision.decision_stage.lower()
                    if 'metadata' in stage:
                        cascade_stats['metadata_shortcuts'] += 1
                    elif stage == 'phash_duplicate':
                        cascade_stats['phash_exact'] += 1
                    elif stage == 'phash_different':
                        cascade_stats['phash_different'] += 1
                    elif stage == 'phash_ambiguous':
                        cascade_stats['phash_ambiguous'] += 1
                    elif 'clip' in stage:
                        # Count as both ambiguous (went to CLIP) and CLIP analyzed
                        cascade_stats['phash_ambiguous'] += 1
                        cascade_stats['clip_analyzed'] += 1
                        if decision.is_duplicate:
                            cascade_stats['clip_confirmed'] += 1
                    
                    # Collect duplicate pairs for grouping
                    if decision.is_duplicate:
                        duplicate_pairs.append({
                            'product1_id': decision.product1_id,
                            'product2_id': decision.product2_id,
                            'phash_difference': decision.phash_difference,
                            'clip_similarity': decision.clip_similarity,
                            'confidence': decision.confidence,
                            'stage': decision.decision_stage
                        })
            
            logger.info(f"Cascade analysis complete: {len(duplicate_pairs)} duplicate pairs found")
            logger.info(f"Stage breakdown: metadata={cascade_stats['metadata_shortcuts']}, "
                       f"pHash_exact={cascade_stats['phash_exact']}, "
                       f"pHash_near={cascade_stats['phash_near']}, "
                       f"pHash_ambiguous={cascade_stats['phash_ambiguous']}, "
                       f"CLIP_analyzed={cascade_stats['clip_analyzed']}")
            
            # Group duplicate pairs into connected components
            duplicate_groups = self._group_duplicate_pairs(duplicate_pairs)
            logger.info(f"Found {len(duplicate_groups)} duplicate groups")
            
            if not duplicate_groups:
                # No duplicates found - mark all as unique
                if not dry_run:
                    if not limit:
                        self.clear_existing_status(db)
                    self.mark_unique_products(db, set(all_product_ids), set())
                
                return self._create_results(start_time, cascade_stats, [], [], all_product_ids)
            
            # Master selection for duplicate groups
            logger.info("👑 Running master selection...")
            master_results = []
            
            for group in duplicate_groups:
                # Convert cascade decisions to format expected by master selector
                phash_pairs = []
                clip_pairs = []
                
                for decision in cascade_decisions:
                    if (decision.product1_id in group and decision.product2_id in group and 
                        decision.is_duplicate):
                        
                        if decision.phash_difference is not None:
                            phash_pairs.append({
                                'product1_id': decision.product1_id,
                                'product2_id': decision.product2_id,
                                'phash_difference': decision.phash_difference,
                                'similarity_score': 1.0 - (decision.phash_difference / 64.0)
                            })
                        
                        if decision.clip_similarity is not None:
                            clip_pairs.append({
                                'product1_id': decision.product1_id,
                                'product2_id': decision.product2_id,
                                'clip_similarity': decision.clip_similarity
                            })
                
                # Process this group through master selection
                group_results = self.master_selector.process_duplicate_groups(
                    db, [group], phash_pairs, clip_pairs
                )
                master_results.extend(group_results)
            
            # Save results to database
            if not dry_run:
                logger.info("💾 Saving results to database...")
                
                # Clear existing status for analyzed products to avoid conflicts
                if all_product_ids:
                    logger.info(f"Clearing existing status for {len(all_product_ids)} analyzed products")
                    self.clear_existing_status(db, all_product_ids)
                
                # Generate and save status assignments
                if master_results:
                    status_assignments = self.master_selector.generate_status_assignments(master_results)
                    self.save_status_assignments(db, status_assignments)
                
                # Mark remaining products as unique
                all_duplicate_products = set()
                for result in master_results:
                    if result['master_id']:
                        all_duplicate_products.add(result['master_id'])
                        all_duplicate_products.update(result['duplicate_ids'])
                
                unique_products = set(all_product_ids) - all_duplicate_products
                if unique_products:
                    self.mark_unique_products(db, unique_products, all_duplicate_products)
            
            total_time = time.time() - start_time
            logger.info(f"✅ Cascade detection complete in {total_time:.2f}s")
            
            return self._create_results(start_time, cascade_stats, master_results, duplicate_pairs, all_product_ids)
            
        except Exception as e:
            logger.error(f"Error during cascade duplicate detection: {e}")
            return {
                'stage': 'ERROR',
                'total_time': time.time() - start_time,
                'error': str(e),
                'cascade_stats': cascade_stats if 'cascade_stats' in locals() else {},
                'master_results': [],
                'duplicate_pairs': [],
                'final_stats': {
                    'total_analyzed': len(all_product_ids) if 'all_product_ids' in locals() else 0,
                    'unique_products': 0,
                    'master_products': 0,
                    'duplicate_products': 0
                }
            }
    
    def _group_duplicate_pairs(self, duplicate_pairs: List[Dict]) -> List[Set[str]]:
        """
        Group duplicate pairs into connected components.
        
        Args:
            duplicate_pairs: List of duplicate pair dictionaries
            
        Returns:
            List of sets, each containing product IDs in the same group
        """
        # Build adjacency list
        graph = {}
        for pair in duplicate_pairs:
            p1, p2 = pair['product1_id'], pair['product2_id']
            if p1 not in graph:
                graph[p1] = set()
            if p2 not in graph:
                graph[p2] = set()
            graph[p1].add(p2)
            graph[p2].add(p1)
        
        # Find connected components using DFS
        visited = set()
        groups = []
        
        def dfs(node, current_group):
            if node in visited:
                return
            visited.add(node)
            current_group.add(node)
            for neighbor in graph.get(node, []):
                dfs(neighbor, current_group)
        
        for node in graph:
            if node not in visited:
                group = set()
                dfs(node, group)
                if len(group) > 1:  # Only include groups with multiple products
                    groups.append(group)
        
        return groups
    
    def _create_empty_results(self, start_time: float, all_product_ids: List[str]) -> Dict:
        """Create results dictionary for when no duplicates are found."""
        return {
            'stage': 'NO_DUPLICATES',
            'total_time': time.time() - start_time,
            'cascade_stats': {
                'total_pairs': 0,
                'metadata_shortcuts': 0,
                'phash_exact': 0,
                'phash_near': 0,
                'phash_different': 0,
                'phash_ambiguous': 0,
                'clip_analyzed': 0,
                'clip_confirmed': 0
            },
            'master_results': [],
            'duplicate_pairs': [],
            'final_stats': {
                'total_analyzed': len(all_product_ids),
                'unique_products': len(all_product_ids),
                'master_products': 0,
                'duplicate_products': 0
            }
        }
    
    def _create_results(self, start_time: float, cascade_stats: Dict, 
                       master_results: List[Dict], duplicate_pairs: List[Dict],
                       all_product_ids: List[str]) -> Dict:
        """Create comprehensive results dictionary."""
        total_masters = len(master_results)
        total_duplicates = sum(len(result['duplicate_ids']) for result in master_results)
        total_unique = len(all_product_ids) - total_masters - total_duplicates
        
        return {
            'stage': 'COMPLETE',
            'total_time': time.time() - start_time,
            'cascade_stats': cascade_stats,
            'master_results': master_results,
            'duplicate_pairs': duplicate_pairs,
            'final_stats': {
                'total_analyzed': len(all_product_ids),
                'unique_products': total_unique,
                'master_products': total_masters,
                'duplicate_products': total_duplicates
            }
        }

    def get_detection_status(self, db: Session) -> Dict:
        """
        Get current detection status from the database.
        
        Args:
            db: Database session
            
        Returns:
            Dict with current status statistics
        """
        try:
            status_counts = {}
            statuses = db.query(ProductStatus.status).all()
            
            for status in statuses:
                status_name = status[0]
                status_counts[status_name] = status_counts.get(status_name, 0) + 1
            
            total_products = db.query(FilteredProduct).count()
            analyzed_products = sum(status_counts.values())
            
            return {
                'total_products': total_products,
                'analyzed_products': analyzed_products,
                'unanalyzed_products': total_products - analyzed_products,
                'status_breakdown': status_counts,
                'analysis_coverage': analyzed_products / total_products if total_products > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting detection status: {e}")
            return {
                'error': str(e),
                'total_products': 0,
                'analyzed_products': 0,
                'status_breakdown': {}
            }

    def print_cascade_results(self, results: Dict):
        """Print detailed cascade analysis results."""
        print(f"\n🧬 Cascade Analysis Results:")
        print(f"  ⏱️  Processing time: {results['total_time']:.2f} seconds")
        print(f"  📊 Stage completed: {results['stage']}")
        print(f"  🔢 Total products analyzed: {results['final_stats']['total_analyzed']}")
        print(f"  ✅ Unique products: {results['final_stats']['unique_products']}")
        print(f"  👑 Master products: {results['final_stats']['master_products']}")
        print(f"  📄 Duplicate products: {results['final_stats']['duplicate_products']}")
        
        if 'cascade_stats' in results:
            stats = results['cascade_stats']
            print(f"\n🔍 Cascade Stage Breakdown:")
            print(f"  📊 Total pairs analyzed: {stats['total_pairs']}")
            print(f"  🏷️  Metadata shortcuts: {stats['metadata_shortcuts']}")
            print(f"  🎯 pHash exact matches: {stats['phash_exact']}")
            print(f"  📸 pHash near duplicates: {stats['phash_near']}")
            print(f"  ❌ pHash different: {stats['phash_different']}")
            print(f"  ❓ pHash ambiguous (sent to CLIP): {stats['phash_ambiguous']}")
            print(f"  🤖 CLIP analyzed: {stats['clip_analyzed']}")
            print(f"  ✅ CLIP confirmed duplicates: {stats['clip_confirmed']}")
            
            # Calculate efficiency metrics
            clip_avoided = stats['metadata_shortcuts'] + stats['phash_exact'] + stats['phash_near'] + stats['phash_different']
            if stats['total_pairs'] > 0:
                efficiency = (clip_avoided / stats['total_pairs']) * 100
                print(f"  ⚡ CLIP avoidance efficiency: {efficiency:.1f}%")


def main():
    """Test function for the cascade duplicate detector."""
    import sys
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    print(f"🧬 Testing Intelligent Cascade Duplicate Detection")
    
    # Parse command line arguments
    limit = None
    dry_run = False
    
    for arg in sys.argv[1:]:
        if arg.startswith('--limit='):
            limit = int(arg.split('=')[1])
        elif arg == '--dry-run':
            dry_run = True
    
    print(f"   Limit: {limit or 'None'}")
    print(f"   Dry run: {dry_run}")
    
    # Initialize detector
    detector = DuplicateDetector()
    
    with get_db_session() as db:
        # Run cascade detection
        results = detector.detect_duplicates(db, limit=limit, dry_run=dry_run)
        
        # Print detailed results
        detector.print_cascade_results(results)
        
        # Show current status
        print(f"\n📊 Current Database Status:")
        status = detector.get_detection_status(db)
        for status_name, count in status['status_breakdown'].items():
            print(f"  {status_name}: {count}")


if __name__ == "__main__":
    main()