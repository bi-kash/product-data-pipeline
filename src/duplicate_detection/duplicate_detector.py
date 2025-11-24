"""
Main duplicate detection engine that orchestrates the analysis pipeline.

This module coordinates between pHash and CLIP analyzers to find potential
duplicates efficiently using an intelligent cascade approach.
"""

import os
import logging
import time
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..common.database import ProductStatus, FilteredProduct, ProductImage, get_db_session, create_tables_if_not_exist
from .cascade_analyzer import IntelligentCascadeAnalyzer, CascadeDecision
from .master_selector import MasterSelector

# Load environment variables
load_dotenv()

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
                total_landed_cost=price
            )
            db.add(unique_status)
        
        db.commit()

    def mark_review_suspect_products(self, db: Session, review_suspect_product_ids: Set[str], 
                                   review_suspect_pairs: List[Dict] = None):
        """
        Mark products that need manual review as REVIEW_SUSPECT.
        
        Args:
            db: Database session
            review_suspect_product_ids: Set of product IDs that need manual review
            review_suspect_pairs: List of review suspect pair dictionaries with CLIP similarity data
        """
        logger.info(f"Marking {len(review_suspect_product_ids)} products as REVIEW_SUSPECT")
        
        # Create lookups for CLIP similarity data and master relationships by product ID
        clip_similarity_lookup = {}
        phash_difference_lookup = {}
        master_id_lookup = {}
        
        if review_suspect_pairs:
            for pair in review_suspect_pairs:
                product1_id = pair['product1_id']
                product2_id = pair['product2_id'] 
                clip_sim = pair.get('clip_similarity')
                phash_diff = pair.get('phash_difference')
                
                # Store the best (highest) CLIP similarity for each product
                if clip_sim is not None:
                    if product1_id not in clip_similarity_lookup or clip_sim > clip_similarity_lookup[product1_id]:
                        clip_similarity_lookup[product1_id] = clip_sim
                    if product2_id not in clip_similarity_lookup or clip_sim > clip_similarity_lookup[product2_id]:
                        clip_similarity_lookup[product2_id] = clip_sim
                
                # Store the best (lowest) pHash difference for each product
                if phash_diff is not None:
                    if product1_id not in phash_difference_lookup or phash_diff < phash_difference_lookup[product1_id]:
                        phash_difference_lookup[product1_id] = phash_diff
                    if product2_id not in phash_difference_lookup or phash_diff < phash_difference_lookup[product2_id]:
                        phash_difference_lookup[product2_id] = phash_diff
                
                # Find existing masters for review suspect products
                existing_master1 = self._find_existing_master_for_product(db, product1_id)
                existing_master2 = self._find_existing_master_for_product(db, product2_id)
                
                if existing_master1:
                    # Product1 should point to existing master
                    master_id_lookup[product1_id] = existing_master1
                if existing_master2:
                    # Product2 should point to existing master  
                    master_id_lookup[product2_id] = existing_master2
                
                # If neither has an existing master, create a temporary master relationship
                if not existing_master1 and not existing_master2:
                    # Choose the cheaper product as temporary master for both
                    price1 = self.master_selector.calculate_lowest_price(db, product1_id)
                    price2 = self.master_selector.calculate_lowest_price(db, product2_id)
                    if price1 is not None and price2 is not None:
                        if price1 <= price2:
                            # Product1 is cheaper - product2 points to product1, product1 has no master (will get NULL)
                            master_id_lookup[product2_id] = product1_id
                        else:
                            # Product2 is cheaper - product1 points to product2, product2 has no master (will get NULL)
                            master_id_lookup[product1_id] = product2_id
                    else:
                        # If price comparison fails, use alphabetical order
                        if product1_id < product2_id:
                            master_id_lookup[product2_id] = product1_id
                        else:
                            master_id_lookup[product1_id] = product2_id
        
        for product_id in review_suspect_product_ids:
            # Calculate total landed cost for review suspect products
            price = self.master_selector.calculate_lowest_price(db, product_id)
            
            # Get master assignment, find cheapest existing master if not found (orphaned review suspect)
            master_id = master_id_lookup.get(product_id)
            if master_id is None:
                # Find the cheapest existing master to point to
                cheapest_master = self._find_cheapest_existing_master(db)
                if cheapest_master:
                    logger.warning(f"Orphaned REVIEW_SUSPECT product {product_id}, pointing to cheapest master {cheapest_master}")
                    master_id = cheapest_master
                else:
                    logger.error(f"No existing masters found for orphaned REVIEW_SUSPECT product {product_id}, skipping")
                    continue  # Skip this product if no masters exist
            
            review_status = ProductStatus(
                product_id=product_id,
                status='REVIEW_SUSPECT',
                duplicate_master_id=master_id,
                total_landed_cost=price,
                phash_difference=phash_difference_lookup.get(product_id),
                clip_similarity=clip_similarity_lookup.get(product_id)
            )
            db.add(review_status)
        
        db.commit()

    def _find_existing_master_for_product(self, db: Session, product_id: str) -> Optional[str]:
        """
        Find if a product already has an existing master in the database.
        Returns the master ID if found, None otherwise.
        """
        existing_status = db.query(ProductStatus).filter(
            ProductStatus.product_id == product_id
        ).first()
        
        if existing_status:
            if existing_status.status == 'MASTER':
                return product_id  # Product is already a master
            elif existing_status.status == 'DUPLICATE' and existing_status.duplicate_master_id:
                return existing_status.duplicate_master_id  # Product already has a master
        
        return None  # No existing master found

    def _find_cheapest_existing_master(self, db: Session) -> Optional[str]:
        """
        Find the cheapest existing master in the database.
        Returns the master ID if found, None if no masters exist.
        """
        # Get all existing masters
        existing_masters = db.query(ProductStatus.product_id).filter(
            ProductStatus.status == 'MASTER'
        ).all()
        
        if not existing_masters:
            return None
        
        # Find the cheapest master
        cheapest_master_id = None
        cheapest_price = float('inf')
        
        for master_row in existing_masters:
            master_id = master_row.product_id
            price = self.master_selector.calculate_lowest_price(db, master_id)
            if price is not None and price < cheapest_price:
                cheapest_price = price
                cheapest_master_id = master_id
        
        return cheapest_master_id

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
            review_suspect_pairs = []  # New: track review suspect pairs
            
            # Track cascade stage statistics
            cascade_stats = {
                'metadata_shortcuts': 0,
                'phash_exact': 0,
                'phash_near': 0,
                'phash_different': 0,
                'phash_ambiguous': 0,
                'clip_analyzed': 0,
                'clip_confirmed': 0,
                'clip_review_suspect': 0,  # New: track review suspect cases
                'total_pairs': 0,
                'total_products_analyzed': len(all_product_ids),  # All products get pHash analysis
                'products_passed_to_clip': set(),  # Track unique products that had CLIP analysis
                'total_images_phash': 0,
                'total_images_clip': 0
            }
            
            total_pairs = len(all_product_ids) * (len(all_product_ids) - 1) // 2
            cascade_stats['total_pairs'] = total_pairs
            
            # Get max workers from environment (default to CPU count)
            max_workers = int(os.getenv('MAX_WORKERS', os.cpu_count() or 4))
            logger.info(f"Analyzing {total_pairs} product pairs using {max_workers} workers...")
            
            # Generate all product pairs to analyze
            product_pairs = [
                (all_product_ids[i], all_product_ids[j])
                for i in range(len(all_product_ids))
                for j in range(i+1, len(all_product_ids))
            ]
            
            # Analyze pairs in parallel using thread pool
            completed_pairs = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_pair = {
                    executor.submit(
                        self.cascade_analyzer.analyze_product_pair,
                        p1, p2, db
                    ): (p1, p2)
                    for p1, p2 in product_pairs
                }
                
                # Process results as they complete
                for future in as_completed(future_to_pair):
                    try:
                        decision = future.result()
                        cascade_decisions.append(decision)
                        
                        completed_pairs += 1
                        if completed_pairs % 100 == 0 or completed_pairs == total_pairs:
                            logger.info(f"Progress: {completed_pairs}/{total_pairs} pairs analyzed ({completed_pairs*100//total_pairs}%)")
                        
                        # Update statistics
                        stage = decision.decision_stage.lower()
                        if 'metadata' in stage:
                            cascade_stats['metadata_shortcuts'] += 1
                        elif stage == 'phash_duplicate':
                            # Further categorize by exact vs near duplicates
                            if decision.phash_difference == 0:
                                cascade_stats['phash_exact'] += 1
                            else:
                                cascade_stats['phash_near'] += 1
                        elif stage == 'phash_different':
                            cascade_stats['phash_different'] += 1
                        elif stage == 'phash_ambiguous':
                            cascade_stats['phash_ambiguous'] += 1
                        elif 'clip' in stage:
                            # Count as both ambiguous (went to CLIP) and CLIP analyzed
                            cascade_stats['phash_ambiguous'] += 1
                            cascade_stats['clip_analyzed'] += 1
                            # Track unique products that had CLIP analysis
                            cascade_stats['products_passed_to_clip'].add(decision.product1_id)
                            cascade_stats['products_passed_to_clip'].add(decision.product2_id)
                            
                            # Count specific CLIP outcomes
                            if decision.decision_stage == 'CLIP_DUPLICATE':
                                cascade_stats['clip_confirmed'] += 1
                            elif decision.decision_stage == 'CLIP_REVIEW_SUSPECT':
                                cascade_stats['clip_review_suspect'] += 1
                        
                        # Count images analyzed in this pair
                        if decision.phash_difference is not None:
                            # This pair had pHash analysis, so images were processed
                            cascade_stats['total_images_phash'] += 2  # Approximate, could be more
                        
                        if decision.clip_similarity is not None:
                            # This pair had CLIP analysis
                            cascade_stats['total_images_clip'] += 2  # Approximate, could be more
                        
                        # Collect pairs for grouping and review
                        if decision.is_duplicate:
                            duplicate_pairs.append({
                                'product1_id': decision.product1_id,
                                'product2_id': decision.product2_id,
                                'phash_difference': decision.phash_difference,
                                'clip_similarity': decision.clip_similarity,
                                'confidence': decision.confidence,
                                'stage': decision.decision_stage
                            })
                        elif decision.decision_stage == 'CLIP_REVIEW_SUSPECT':
                            # Collect review suspect pairs for manual review
                            review_suspect_pairs.append({
                                'product1_id': decision.product1_id,
                                'product2_id': decision.product2_id,
                                'phash_difference': decision.phash_difference,
                                'clip_similarity': decision.clip_similarity,
                                'confidence': decision.confidence,
                                'stage': decision.decision_stage
                            })
                    
                    except Exception as e:
                        p1, p2 = future_to_pair[future]
                        logger.error(f"Error analyzing pair ({p1}, {p2}): {e}")
                        # Continue with other pairs
            
            logger.info(f"Cascade analysis complete: {len(duplicate_pairs)} duplicate pairs, {len(review_suspect_pairs)} review suspect pairs found")
            logger.info(f"📊 Products analyzed: {cascade_stats['total_products_analyzed']} total products (all via pHash)")
            logger.info(f"📊 Products passed to CLIP: {len(cascade_stats['products_passed_to_clip'])} products")
            logger.info(f"🖼️ Images analyzed: ~{cascade_stats['total_images_phash']} via pHash, ~{cascade_stats['total_images_clip']} via CLIP")
            logger.info(f"Stage breakdown: metadata={cascade_stats['metadata_shortcuts']}, "
                       f"pHash_exact={cascade_stats['phash_exact']}, "
                       f"pHash_near={cascade_stats['phash_near']}, "
                       f"pHash_ambiguous={cascade_stats['phash_ambiguous']}, "
                       f"CLIP_analyzed={cascade_stats['clip_analyzed']}, "
                       f"CLIP_confirmed={cascade_stats['clip_confirmed']}, "
                       f"CLIP_review_suspect={cascade_stats['clip_review_suspect']}")
            
            # Group duplicate pairs into connected components
            duplicate_groups = self._group_duplicate_pairs(duplicate_pairs)
            logger.info(f"Found {len(duplicate_groups)} duplicate groups")
            
            # Handle review suspect pairs - mark them for manual review
            review_suspect_product_ids = set()
            if review_suspect_pairs:
                logger.info(f"🔍 Marking {len(review_suspect_pairs)} review suspect pairs for manual review...")
                for pair in review_suspect_pairs:
                    review_suspect_product_ids.add(pair['product1_id'])
                    review_suspect_product_ids.add(pair['product2_id'])
                logger.info(f"Total products marked for review: {len(review_suspect_product_ids)}")
            
            if not duplicate_groups:
                # No duplicates found - mark all as unique (except review suspects)
                if not dry_run:
                    if not limit:
                        self.clear_existing_status(db)
                    unique_product_ids = set(all_product_ids) - review_suspect_product_ids
                    self.mark_unique_products(db, unique_product_ids, set())
                    if review_suspect_product_ids:
                        self.mark_review_suspect_products(db, review_suspect_product_ids, review_suspect_pairs)
                
                return self._create_results(start_time, cascade_stats, [], [], review_suspect_pairs, all_product_ids)
            
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
                
                # Mark remaining products as unique (excluding review suspects)
                unique_products = set(all_product_ids) - all_duplicate_products - review_suspect_product_ids
                if unique_products:
                    self.mark_unique_products(db, unique_products, all_duplicate_products)
                
                # Mark review suspect products (exclude products already assigned as duplicates)
                remaining_review_suspects = review_suspect_product_ids - all_duplicate_products
                if remaining_review_suspects:
                    # Filter review suspect pairs to only include remaining suspects
                    filtered_pairs = [
                        pair for pair in review_suspect_pairs
                        if pair['product1_id'] in remaining_review_suspects or pair['product2_id'] in remaining_review_suspects
                    ]
                    self.mark_review_suspect_products(db, remaining_review_suspects, filtered_pairs)
            
            total_time = time.time() - start_time
            logger.info(f"✅ Cascade detection complete in {total_time:.2f}s")
            
            return self._create_results(start_time, cascade_stats, master_results, duplicate_pairs, review_suspect_pairs, all_product_ids)
            
        except Exception as e:
            logger.error(f"Error during cascade duplicate detection: {e}")
            return {
                'stage': 'ERROR',
                'total_time': time.time() - start_time,
                'error': str(e),
                'cascade_stats': cascade_stats if 'cascade_stats' in locals() else {
                    'total_products_analyzed': 0,
                    'products_passed_to_clip': 0,
                    'total_images_phash': 0,
                    'total_images_clip': 0
                },
                'master_results': [],
                'duplicate_pairs': [],
                'review_suspect_pairs': [],
                'final_stats': {
                    'total_analyzed': len(all_product_ids) if 'all_product_ids' in locals() else 0,
                    'unique_products': 0,
                    'master_products': 0,
                    'duplicate_products': 0,
                    'review_suspect_products': 0
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
                'clip_confirmed': 0,
                'clip_review_suspect': 0
            },
            'master_results': [],
            'duplicate_pairs': [],
            'review_suspect_pairs': [],
            'final_stats': {
                'total_analyzed': len(all_product_ids),
                'unique_products': len(all_product_ids),
                'master_products': 0,
                'duplicate_products': 0,
                'review_suspect_products': 0
            }
        }
    
    def _create_results(self, start_time: float, cascade_stats: Dict, 
                       master_results: List[Dict], duplicate_pairs: List[Dict],
                       review_suspect_pairs: List[Dict], all_product_ids: List[str]) -> Dict:
        """Create comprehensive results dictionary."""
        total_masters = len(master_results)
        total_duplicates = sum(len(result['duplicate_ids']) for result in master_results)
        total_review_suspect = len(set(
            pair['product1_id'] for pair in review_suspect_pairs
        ).union(set(
            pair['product2_id'] for pair in review_suspect_pairs
        )))
        total_unique = len(all_product_ids) - total_masters - total_duplicates - total_review_suspect
        
        # Convert set to count for JSON serialization
        products_passed_to_clip_count = len(cascade_stats.get('products_passed_to_clip', set()))
        cascade_stats_serializable = cascade_stats.copy()
        cascade_stats_serializable['products_passed_to_clip'] = products_passed_to_clip_count
        
        return {
            'stage': 'COMPLETE',
            'total_time': time.time() - start_time,
            'cascade_stats': cascade_stats_serializable,
            'master_results': master_results,
            'duplicate_pairs': duplicate_pairs,
            'review_suspect_pairs': review_suspect_pairs,
            'final_stats': {
                'total_analyzed': len(all_product_ids),
                'unique_products': total_unique,
                'master_products': total_masters,
                'duplicate_products': total_duplicates,
                'review_suspect_products': total_review_suspect,
                'products_analyzed_phash': cascade_stats.get('total_products_analyzed', len(all_product_ids)),
                'products_analyzed_clip': products_passed_to_clip_count,
                'images_analyzed_phash': cascade_stats.get('total_images_phash', 0),
                'images_analyzed_clip': cascade_stats.get('total_images_clip', 0)
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
        print(f"  🔍 Review suspect products: {results['final_stats'].get('review_suspect_products', 0)}")
        
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
            print(f"  🔍 CLIP review suspect: {stats.get('clip_review_suspect', 0)}")
            
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