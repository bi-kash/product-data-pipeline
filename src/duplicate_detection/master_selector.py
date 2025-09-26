"""
Master Selector for choosing the best product from duplicate groups.

This module implements the logic to select the "master" product from
detected duplicate groups based on total_landed_cost and deterministic tie-breakers.
"""

import logging
from typing import List, Dict, Set, Optional, Tuple
from decimal import Decimal
from sqlalchemy.orm import Session
from src.common.database import FilteredProduct, ShippingInfo, get_db_session

logger = logging.getLogger(__name__)


class MasterSelector:
    """
    Selector for choosing master products from duplicate groups.
    
    Selects the product with the lowest total_landed_cost 
    (item_price_eur + shipping_cost_eur) as the master product.
    Uses deterministic tie-breakers for consistent results.
    """

    def __init__(self):
        """Initialize the master selector."""
        logger.info("MasterSelector initialized")

    def calculate_lowest_price(self, db: Session, product_id: str) -> Optional[float]:
        """
        Get the target sale price for a product.
        
        Args:
            db: Database session
            product_id: Product ID to get price for
            
        Returns:
            Target sale price or None if not found
        """
        try:
            product = db.query(FilteredProduct).filter(
                FilteredProduct.product_id == product_id
            ).first()
            
            if not product:
                logger.warning(f"Product {product_id} not found in filtered_products")
                return None
            
            target_price = float(product.target_sale_price or 0)
            logger.debug(f"Product {product_id}: target_sale_price={target_price:.2f}")
            return target_price
            
        except Exception as e:
            logger.error(f"Error getting target sale price for {product_id}: {e}")
            return None

    def get_product_metadata(self, db: Session, product_id: str) -> Dict:
        """
        Get metadata for a product to use in tie-breaking.
        
        Args:
            db: Database session
            product_id: Product ID
            
        Returns:
            Dict with product metadata
        """
        try:
            product = db.query(FilteredProduct).filter(
                FilteredProduct.product_id == product_id
            ).first()
            
            if not product:
                return {}
            
            return {
                'product_id': product_id,
                'title': product.product_title or '',
                'seller_id': product.shop_id or '',
                'created_at': product.first_seen_at,
                'final_price_eur': float(product.target_sale_price or 0),
                'original_price_eur': float(product.original_price or 0),
                'rating': float(product.evaluate_rate or 0),
                'order_count': 0  # Not available in FilteredProduct
            }
            
        except Exception as e:
            logger.error(f"Error getting metadata for {product_id}: {e}")
            return {}

    def apply_tie_breaker(self, candidates: List[Dict]) -> str:
        """
        Apply deterministic tie-breaker rules when products have the same total_landed_cost.
        
        Tie-breaker priority:
        1. Higher rating
        2. Higher order count  
        3. Lower original price (better discount)
        4. Lexicographically first product_id (for absolute determinism)
        
        Args:
            candidates: List of product data dictionaries with same cost
            
        Returns:
            Product ID of the selected master
        """
        if len(candidates) == 1:
            return candidates[0]['product_id']
        
        logger.debug(f"Applying tie-breaker for {len(candidates)} products with same cost")
        
        # Sort by tie-breaker criteria
        sorted_candidates = sorted(candidates, key=lambda x: (
            -x.get('rating', 0),           # Higher rating first (descending)
            -x.get('order_count', 0),      # Higher order count first (descending)  
            x.get('original_price_eur', float('inf')),  # Lower original price first (ascending)
            x.get('product_id', '')        # Lexicographically first (ascending)
        ))
        
        selected = sorted_candidates[0]
        logger.debug(f"Tie-breaker selected: {selected['product_id']} "
                    f"(rating: {selected.get('rating', 0)}, "
                    f"orders: {selected.get('order_count', 0)}, "
                    f"orig_price: {selected.get('original_price_eur', 0)})")
        
        return selected['product_id']

    def select_master_from_group(self, db: Session, product_group: Set[str]) -> Dict:
        """
        Select the master product from a duplicate group.
        
        Args:
            db: Database session
            product_group: Set of product IDs in the duplicate group
            
        Returns:
            Dict with master selection results
        """
        if len(product_group) < 2:
            logger.warning(f"Group has less than 2 products: {product_group}")
            return {
                'master_id': list(product_group)[0] if product_group else None,
                'duplicate_ids': [],
                'selection_method': 'SINGLE_PRODUCT',
                'total_landed_costs': {},
                'tie_breaker_applied': False
            }
        
        logger.debug(f"Selecting master from group of {len(product_group)} products")
        
        # Calculate target sale price for each product
        product_prices = {}
        product_metadata = {}
        
        for product_id in product_group:
            price = self.calculate_lowest_price(db, product_id)
            metadata = self.get_product_metadata(db, product_id)
            
            if price is not None:
                product_prices[product_id] = price
                product_metadata[product_id] = metadata
            else:
                logger.warning(f"Could not get price for product {product_id}, excluding from selection")
        
        if not product_prices:
            logger.error(f"No valid prices found for group: {product_group}")
            return {
                'master_id': None,
                'duplicate_ids': list(product_group),
                'selection_method': 'ERROR_NO_PRICES',
                'product_prices': {},
                'tie_breaker_applied': False
            }
        
        # Find the minimum price
        min_price = min(product_prices.values())
        
        # Get all products with the minimum price
        min_price_products = [
            product_id for product_id, price in product_prices.items() 
            if abs(price - min_price) < 0.01  # Allow for small floating point differences
        ]
        
        # Determine selection method
        tie_breaker_applied = len(min_price_products) > 1
        
        if tie_breaker_applied:
            # Apply tie-breaker
            candidates = [product_metadata[pid] for pid in min_price_products]
            for candidate in candidates:
                candidate['target_sale_price'] = product_prices[candidate['product_id']]
            
            master_id = self.apply_tie_breaker(candidates)
            selection_method = 'TIE_BREAKER'
        else:
            # Single product with minimum price
            master_id = min_price_products[0]
            selection_method = 'LOWEST_PRICE'
        
        # Determine duplicate IDs
        duplicate_ids = [pid for pid in product_group if pid != master_id]
        
        result = {
            'master_id': master_id,
            'duplicate_ids': duplicate_ids,
            'selection_method': selection_method,
            'product_prices': product_prices,
            'tie_breaker_applied': tie_breaker_applied,
            'min_price': min_price,
            'master_price': product_prices.get(master_id),
            'group_size': len(product_group)
        }
        
        logger.info(f"Selected master {master_id} from group of {len(product_group)} "
                   f"(method: {selection_method}, price: {min_price:.2f})")
        
        return result

    def process_duplicate_groups(self, db: Session, duplicate_groups: List[Set[str]], 
                                phash_similar_pairs: List[Dict] = None,
                                clip_similar_pairs: List[Dict] = None) -> List[Dict]:
        """
        Process all duplicate groups and select masters.
        
        Args:
            db: Database session
            duplicate_groups: List of product ID sets representing duplicate groups
            phash_similar_pairs: List of similar image pairs with pHash data
            clip_similar_pairs: List of similar product pairs with CLIP data
            
        Returns:
            List of master selection results for each group
        """
        logger.info(f"Processing {len(duplicate_groups)} duplicate groups for master selection")
        
        # Build pHash similarity lookup for faster access
        phash_data = {}
        if phash_similar_pairs:
            for pair in phash_similar_pairs:
                key1 = (pair['product1_id'], pair['product2_id'])
                key2 = (pair['product2_id'], pair['product1_id'])
                phash_data[key1] = pair
                phash_data[key2] = pair
        
        # Build CLIP similarity lookup for faster access
        clip_data = {}
        if clip_similar_pairs:
            for pair in clip_similar_pairs:
                key1 = (pair['product1_id'], pair['product2_id'])
                key2 = (pair['product2_id'], pair['product1_id'])
                clip_data[key1] = pair
                clip_data[key2] = pair
        
        results = []
        
        for i, group in enumerate(duplicate_groups):
            logger.debug(f"Processing group {i+1}/{len(duplicate_groups)} with {len(group)} products")
            
            result = self.select_master_from_group(db, group)
            result['group_index'] = i
            result['phash_data'] = phash_data  # Pass pHash data to result
            result['clip_data'] = clip_data    # Pass CLIP data to result
            results.append(result)
        
        # Calculate summary statistics
        total_masters = sum(1 for r in results if r['master_id'])
        total_duplicates = sum(len(r['duplicate_ids']) for r in results)
        tie_breakers_used = sum(1 for r in results if r['tie_breaker_applied'])
        
        logger.info(f"Master selection complete: {total_masters} masters, "
                   f"{total_duplicates} duplicates, {tie_breakers_used} tie-breakers used")
        
        return results

    def generate_status_assignments(self, master_results: List[Dict]) -> List[Dict]:
        """
        Generate status assignments for the product_status table.
        
        Args:
            master_results: Results from process_duplicate_groups
            
        Returns:
            List of dictionaries ready for ProductStatus table insertion
        """
        logger.info("Generating status assignments for product_status table")
        
        status_assignments = []
        
        for result in master_results:
            if not result['master_id']:
                continue
            
            phash_data = result.get('phash_data', {})
            clip_data = result.get('clip_data', {})
            master_id = result['master_id']
            
            # Master product assignment
            status_assignments.append({
                'product_id': master_id,
                'status': 'MASTER',
                'duplicate_master_id': None,  # Masters don't point to anyone
                'total_landed_cost': result.get('master_price'),
                'phash_difference': None,  # Masters don't have pHash difference
                'clip_similarity': None,  # Masters don't have CLIP similarity
            })
            
            # Duplicate product assignments
            for duplicate_id in result['duplicate_ids']:
                # Find the single best pair (lowest pHash) and use both pHash and CLIP from that same pair
                best_phash_difference = None
                best_clip_similarity = None
                best_pair_other_product = None
                
                # Compare with all other products in the group to find best pair
                all_products_in_group = [master_id] + result['duplicate_ids']
                
                for other_product_id in all_products_in_group:
                    if other_product_id == duplicate_id:
                        continue
                    
                    # Check if this pair exists in pHash data
                    phash_pair = phash_data.get((duplicate_id, other_product_id))
                    if phash_pair:
                        phash_diff = phash_pair['phash_difference']
                        # Lower pHash difference is better (more similar)
                        if best_phash_difference is None or phash_diff < best_phash_difference:
                            best_phash_difference = phash_diff
                            best_pair_other_product = other_product_id
                
                # Now get CLIP similarity from the SAME pair that provided the best pHash
                if best_pair_other_product:
                    clip_pair = clip_data.get((duplicate_id, best_pair_other_product))
                    if clip_pair:
                        best_clip_similarity = clip_pair['clip_similarity']
                
                status_assignments.append({
                    'product_id': duplicate_id,
                    'status': 'DUPLICATE',
                    'duplicate_master_id': master_id,
                    'total_landed_cost': result['product_prices'].get(duplicate_id),
                    'phash_difference': best_phash_difference,
                    'clip_similarity': best_clip_similarity,
                })
        
        logger.info(f"Generated {len(status_assignments)} status assignments")
        return status_assignments

    def reassign_master_if_better(self, db: Session, new_candidate_id: str, current_master_id: str) -> Dict:
        """
        Check if a product should become the new master of its duplicate group.
        If so, reassign the entire group to point to the new master.
        
        Args:
            db: Database session
            new_candidate_id: Product ID that might become the new master
            current_master_id: Current master product ID
            
        Returns:
            Dict with reassignment results
        """
        logger.info(f"Checking if {new_candidate_id} should replace master {current_master_id}")
        
        # Get all products that currently point to the current master
        from src.common.database import ProductStatus
        
        current_group_products = db.query(ProductStatus.product_id).filter(
            ProductStatus.duplicate_master_id == current_master_id
        ).all()
        
        # Add the current master to the group
        full_group = {current_master_id, new_candidate_id}
        full_group.update([p.product_id for p in current_group_products])
        
        logger.info(f"Full duplicate group: {full_group} (size: {len(full_group)})")
        
        # Run master selection on the full group
        selection_result = self.select_master_from_group(db, full_group)
        new_master_id = selection_result['master_id']
        
        result = {
            'reassignment_needed': new_master_id != current_master_id,
            'old_master_id': current_master_id,
            'new_master_id': new_master_id,
            'group_size': len(full_group),
            'selection_method': selection_result['selection_method'],
            'affected_products': list(full_group),
            'selection_details': selection_result
        }
        
        if result['reassignment_needed']:
            logger.info(f"Master reassignment needed: {current_master_id} -> {new_master_id}")
            
            # Perform the reassignment
            updated_count = self._update_group_master_assignments(
                db, full_group, current_master_id, new_master_id
            )
            result['updated_count'] = updated_count
            
        else:
            logger.info(f"No reassignment needed, {current_master_id} remains master")
            result['updated_count'] = 0
        
        return result
    
    def _update_group_master_assignments(self, db: Session, product_group: Set[str], 
                                       old_master_id: str, new_master_id: str) -> int:
        """
        Update all products in a group to point to a new master.
        
        Args:
            db: Database session  
            product_group: Set of all product IDs in the group
            old_master_id: Previous master ID
            new_master_id: New master ID
            
        Returns:
            Number of products updated
        """
        from src.common.database import ProductStatus
        
        updated_count = 0
        
        # Update or create status for new master
        new_master_status = db.query(ProductStatus).filter(
            ProductStatus.product_id == new_master_id
        ).first()
        
        if new_master_status:
            new_master_status.status = 'MASTER'
            new_master_status.duplicate_master_id = None
        else:
            new_master_status = ProductStatus(
                product_id=new_master_id,
                status='MASTER',
                duplicate_master_id=None
            )
            db.add(new_master_status)
        updated_count += 1
        
        # Update old master to become duplicate
        old_master_status = db.query(ProductStatus).filter(
            ProductStatus.product_id == old_master_id
        ).first()
        
        if old_master_status:
            old_master_status.status = 'DUPLICATE'
            old_master_status.duplicate_master_id = new_master_id
            updated_count += 1
        
        # Update all other products to point to new master
        for product_id in product_group:
            if product_id in {new_master_id, old_master_id}:
                continue
                
            product_status = db.query(ProductStatus).filter(
                ProductStatus.product_id == product_id
            ).first()
            
            if product_status:
                product_status.status = 'DUPLICATE' 
                product_status.duplicate_master_id = new_master_id
                updated_count += 1
        
        logger.info(f"Updated {updated_count} products to point to new master {new_master_id}")
        return updated_count


def main():
    """Test the master selector with command line arguments."""
    import sys
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    print(f"🎯 Testing Master Selector")
    
    # Test with some product IDs
    test_products = sys.argv[1:] if len(sys.argv) > 1 else []
    
    if not test_products:
        print("💡 Usage: python master_selector.py <product_id1> <product_id2> ...")
        print("💡 Or provide product IDs as command line arguments to test selection")
        return
    
    selector = MasterSelector()
    
    with get_db_session() as db:
        # Test with a single group
        test_group = set(test_products)
        print(f"🔍 Testing master selection for group: {test_group}")
        
        result = selector.select_master_from_group(db, test_group)
        
        print(f"\n📊 Master Selection Result:")
        print(f"  🎯 Master: {result['master_id']}")
        print(f"  📦 Duplicates: {result['duplicate_ids']}")
        print(f"  💰 Master cost: {result.get('master_cost', 'N/A')}")
        print(f"  🔧 Method: {result['selection_method']}")
        print(f"  ⚖️  Tie-breaker used: {result['tie_breaker_applied']}")
        
        print(f"\n💰 Cost breakdown:")
        for product_id, cost in result['total_landed_costs'].items():
            marker = "👑" if product_id == result['master_id'] else "📦"
            print(f"  {marker} {product_id}: €{cost:.2f}")
        
        # Generate status assignments
        status_assignments = selector.generate_status_assignments([result])
        print(f"\n📋 Status assignments:")
        for assignment in status_assignments:
            print(f"  {assignment['product_id']}: {assignment['status']} "
                  f"(master: {assignment['duplicate_master_id'] or 'N/A'})")


if __name__ == "__main__":
    main()