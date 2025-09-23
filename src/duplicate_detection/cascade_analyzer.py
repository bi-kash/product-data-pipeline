"""
Intelligent Cascade Analyzer for efficient duplicate detection.

Implements a cascading decision tree that uses cheap checks first (metadata, pHash)
and only escalates to expensive CLIP analysis when necessary.

Decision Flow:
1. Pre-checks: Image quality, metadata shortcuts
2. pHash analysis with cascade thresholds:
   - H=0: Exact duplicate (skip CLIP)
   - H=1-8: Near duplicate (skip CLIP)
   - H=9-18: Ambiguous (send to CLIP)
   - H>18: Different (skip CLIP)
3. CLIP analysis only for ambiguous cases
4. Final decision based on CLIP similarity
"""

import os
import logging
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from difflib import SequenceMatcher

from ..common.database import ProductImage, FilteredProduct, get_db_session
from .phash_analyzer import PHashAnalyzer
from .clip_analyzer import CLIPAnalyzer

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class CascadeConfig:
    """Configuration for the intelligent cascade analyzer."""
    
    # Image quality thresholds
    min_image_size: int = 64
    skip_low_quality: bool = True
    
    # pHash cascade thresholds
    phash_exact_duplicate: int = 0
    phash_near_duplicate_max: int = 8
    phash_ambiguous_min: int = 9
    phash_ambiguous_max: int = 18
    phash_different_min: int = 19
    
    # CLIP thresholds
    clip_duplicate_min: float = 0.85
    clip_review_min: float = 0.75
    clip_different_max: float = 0.74
    
    # Metadata shortcuts
    skip_identical_sku: bool = True
    use_filename_check: bool = True
    use_title_similarity: bool = True
    
    # Processing limits
    max_images_per_product: int = 5
    
    @classmethod
    def from_env(cls) -> 'CascadeConfig':
        """Load configuration from environment variables."""
        return cls(
            min_image_size=int(os.getenv('MIN_IMAGE_SIZE', '64')),
            skip_low_quality=os.getenv('SKIP_LOW_QUALITY', 'true').lower() == 'true',
            
            phash_exact_duplicate=int(os.getenv('PHASH_EXACT_DUPLICATE', '0')),
            phash_near_duplicate_max=int(os.getenv('PHASH_NEAR_DUPLICATE_MAX', '8')),
            phash_ambiguous_min=int(os.getenv('PHASH_AMBIGUOUS_MIN', '9')),
            phash_ambiguous_max=int(os.getenv('PHASH_AMBIGUOUS_MAX', '18')),
            phash_different_min=int(os.getenv('PHASH_DIFFERENT_MIN', '19')),
            
            clip_duplicate_min=float(os.getenv('CLIP_DUPLICATE_MIN', '0.85')),
            clip_review_min=float(os.getenv('CLIP_REVIEW_MIN', '0.75')),
            clip_different_max=float(os.getenv('CLIP_DIFFERENT_MAX', '0.74')),
            
            skip_identical_sku=os.getenv('SKIP_IDENTICAL_SKU', 'true').lower() == 'true',
            use_filename_check=os.getenv('USE_FILENAME_CHECK', 'true').lower() == 'true',
            use_title_similarity=os.getenv('USE_TITLE_SIMILARITY', 'true').lower() == 'true',
            
            max_images_per_product=int(os.getenv('MAX_IMAGES_PER_PRODUCT', '5'))
        )


@dataclass
class CascadeDecision:
    """Result of cascade analysis for a product pair."""
    
    product1_id: str
    product2_id: str
    is_duplicate: bool
    confidence: float
    decision_stage: str  # 'METADATA', 'PHASH_EXACT', 'PHASH_NEAR', 'PHASH_DIFFERENT', 'CLIP'
    phash_difference: Optional[int] = None
    clip_similarity: Optional[float] = None
    reason: str = ""
    skipped_clip: bool = False


class IntelligentCascadeAnalyzer:
    """
    Intelligent cascade analyzer that uses cheap checks first and escalates only when needed.
    """
    
    def __init__(self, config: Optional[CascadeConfig] = None):
        """
        Initialize the cascade analyzer.
        
        Args:
            config: Cascade configuration. If None, loads from environment.
        """
        self.config = config or CascadeConfig.from_env()
        self.phash_analyzer = PHashAnalyzer()
        
        # Only initialize CLIP if we might need it
        self.clip_analyzer = None
        try:
            self.clip_analyzer = CLIPAnalyzer()
            logger.info(f"Cascade analyzer initialized with CLIP support")
        except Exception as e:
            logger.warning(f"CLIP unavailable, using pHash-only mode: {e}")
        
        logger.info(f"Cascade config: pHash exact={self.config.phash_exact_duplicate}, "
                   f"near≤{self.config.phash_near_duplicate_max}, "
                   f"ambiguous={self.config.phash_ambiguous_min}-{self.config.phash_ambiguous_max}, "
                   f"CLIP duplicate≥{self.config.clip_duplicate_min}")
    
    def _check_image_quality(self, image: ProductImage) -> bool:
        """
        Check if image meets quality thresholds.
        
        Args:
            image: ProductImage to check
            
        Returns:
            True if image quality is acceptable
        """
        if not self.config.skip_low_quality:
            return True
            
        # Check image dimensions
        if image.width and image.height:
            if image.width < self.config.min_image_size or image.height < self.config.min_image_size:
                logger.debug(f"Rejecting low quality image {image.id}: {image.width}x{image.height}")
                return False
        
        return True
    
    def _check_metadata_shortcuts(self, product1: FilteredProduct, product2: FilteredProduct) -> Optional[CascadeDecision]:
        """
        Check for metadata-based shortcuts to avoid heavy analysis.
        
        Args:
            product1: First product
            product2: Second product
            
        Returns:
            CascadeDecision if shortcut applies, None otherwise
        """
        # Skip identical SKUs
        if self.config.skip_identical_sku and product1.product_id == product2.product_id:
            return CascadeDecision(
                product1_id=product1.product_id,
                product2_id=product2.product_id,
                is_duplicate=True,
                confidence=1.0,
                decision_stage='METADATA',
                reason='Identical SKU/product_id',
                skipped_clip=True
            )
        
        # Check title similarity
        if self.config.use_title_similarity and product1.product_title and product2.product_title:
            title_similarity = SequenceMatcher(None, product1.product_title, product2.product_title).ratio()
            if title_similarity > 0.9:  # Very similar titles
                return CascadeDecision(
                    product1_id=product1.product_id,
                    product2_id=product2.product_id,
                    is_duplicate=True,
                    confidence=title_similarity,
                    decision_stage='METADATA',
                    reason=f'High title similarity: {title_similarity:.3f}',
                    skipped_clip=True
                )
        
        return None
    
    def _analyze_phash_cascade(self, product1_id: str, product2_id: str, db: Session) -> CascadeDecision:
        """
        Perform pHash analysis with cascade decision logic.
        
        Args:
            product1_id: First product ID
            product2_id: Second product ID
            db: Database session
            
        Returns:
            CascadeDecision with pHash results and next steps
        """
        # Get images for both products with quality filtering
        images1 = db.query(ProductImage).filter(
            ProductImage.product_id == product1_id,
            ProductImage.phash.isnot(None),
            ProductImage.image_role.in_(['hero', 'variant'])  # Focus on key images
        ).limit(self.config.max_images_per_product).all()
        
        images2 = db.query(ProductImage).filter(
            ProductImage.product_id == product2_id,
            ProductImage.phash.isnot(None),
            ProductImage.image_role.in_(['hero', 'variant'])
        ).limit(self.config.max_images_per_product).all()
        
        # Filter by quality
        images1 = [img for img in images1 if self._check_image_quality(img)]
        images2 = [img for img in images2 if self._check_image_quality(img)]
        
        if not images1 or not images2:
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,
                confidence=0.0,
                decision_stage='PHASH_NO_IMAGES',
                reason='Insufficient quality images for comparison',
                skipped_clip=True
            )
        
        # Find the best (lowest) pHash difference
        best_phash_diff = float('inf')
        
        for img1 in images1:
            for img2 in images2:
                if img1.phash and img2.phash:
                    diff = self.phash_analyzer.calculate_hamming_distance(img1.phash, img2.phash)
                    if diff < best_phash_diff:
                        best_phash_diff = diff
        
        if best_phash_diff == float('inf'):
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,
                confidence=0.0,
                decision_stage='PHASH_ERROR',
                reason='Could not calculate pHash difference',
                skipped_clip=True
            )
        
        # Apply cascade decision logic
        phash_diff = int(best_phash_diff)
        
        if phash_diff == self.config.phash_exact_duplicate:
            # Exact duplicate
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=True,
                confidence=1.0,
                decision_stage='PHASH_EXACT',
                phash_difference=phash_diff,
                reason='Exact pHash match',
                skipped_clip=True
            )
        
        elif phash_diff <= self.config.phash_near_duplicate_max:
            # Near duplicate - high confidence, skip CLIP
            confidence = 1.0 - (phash_diff / self.config.phash_near_duplicate_max * 0.2)  # 0.8-1.0 range
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=True,
                confidence=confidence,
                decision_stage='PHASH_NEAR',
                phash_difference=phash_diff,
                reason=f'Near duplicate (pHash diff: {phash_diff})',
                skipped_clip=True
            )
        
        elif phash_diff >= self.config.phash_different_min:
            # Likely different - skip CLIP
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,
                confidence=0.9,  # High confidence they're different
                decision_stage='PHASH_DIFFERENT',
                phash_difference=phash_diff,
                reason=f'Likely different (pHash diff: {phash_diff})',
                skipped_clip=True
            )
        
        else:
            # Ambiguous zone - needs CLIP analysis
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,  # TBD by CLIP
                confidence=0.5,  # Uncertain
                decision_stage='PHASH_AMBIGUOUS',
                phash_difference=phash_diff,
                reason=f'Ambiguous (pHash diff: {phash_diff}) - needs CLIP',
                skipped_clip=False
            )
    
    def _analyze_clip_decision(self, decision: CascadeDecision, db: Session) -> CascadeDecision:
        """
        Perform CLIP analysis for ambiguous cases.
        
        Args:
            decision: Existing cascade decision from pHash stage
            db: Database session
            
        Returns:
            Updated CascadeDecision with CLIP results
        """
        if not self.clip_analyzer:
            # Fall back to pHash decision if CLIP unavailable
            decision.reason += " (CLIP unavailable)"
            decision.skipped_clip = True
            return decision
        
        # Get CLIP similarity for the product pair
        try:
            # This is a simplified approach - in practice you'd want to integrate 
            # with the existing CLIP analyzer or extend it for pairwise comparison
            clip_results = self.clip_analyzer.analyze_candidates(
                db, [decision.product1_id, decision.product2_id], ['hero', 'variant']
            )
            
            # Find similarity for this specific pair
            clip_similarity = None
            for pair in clip_results.get('similar_pairs', []):
                if ((pair['product1_id'] == decision.product1_id and pair['product2_id'] == decision.product2_id) or
                    (pair['product1_id'] == decision.product2_id and pair['product2_id'] == decision.product1_id)):
                    clip_similarity = pair['clip_similarity']
                    break
            
            if clip_similarity is None:
                # No CLIP similarity found - treat as different
                decision.is_duplicate = False
                decision.confidence = 0.8
                decision.decision_stage = 'CLIP_NO_SIMILARITY'
                decision.reason = 'No CLIP similarity found'
                return decision
            
            # Apply CLIP decision thresholds
            decision.clip_similarity = clip_similarity
            
            if clip_similarity >= self.config.clip_duplicate_min:
                decision.is_duplicate = True
                decision.confidence = clip_similarity
                decision.decision_stage = 'CLIP_DUPLICATE'
                decision.reason = f'CLIP confirmed duplicate (sim: {clip_similarity:.3f})'
            elif clip_similarity >= self.config.clip_review_min:
                decision.is_duplicate = True  # Mark as duplicate but lower confidence
                decision.confidence = clip_similarity * 0.8  # Reduce confidence
                decision.decision_stage = 'CLIP_REVIEW'
                decision.reason = f'CLIP suggests review (sim: {clip_similarity:.3f})'
            else:
                decision.is_duplicate = False
                decision.confidence = 1.0 - clip_similarity
                decision.decision_stage = 'CLIP_DIFFERENT'
                decision.reason = f'CLIP confirmed different (sim: {clip_similarity:.3f})'
            
        except Exception as e:
            logger.error(f"CLIP analysis failed for {decision.product1_id} vs {decision.product2_id}: {e}")
            decision.reason += f" (CLIP failed: {e})"
            decision.skipped_clip = True
        
        return decision
    
    def analyze_product_pair(self, product1_id: str, product2_id: str, db: Session) -> CascadeDecision:
        """
        Analyze a product pair using the intelligent cascade.
        
        Args:
            product1_id: First product ID
            product2_id: Second product ID
            db: Database session
            
        Returns:
            CascadeDecision with final duplicate determination
        """
        # Get product metadata
        product1 = db.query(FilteredProduct).filter(FilteredProduct.product_id == product1_id).first()
        product2 = db.query(FilteredProduct).filter(FilteredProduct.product_id == product2_id).first()
        
        if not product1 or not product2:
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,
                confidence=0.0,
                decision_stage='ERROR',
                reason='Product(s) not found',
                skipped_clip=True
            )
        
        # Stage 1: Metadata shortcuts
        metadata_decision = self._check_metadata_shortcuts(product1, product2)
        if metadata_decision:
            return metadata_decision
        
        # Stage 2: pHash cascade analysis
        phash_decision = self._analyze_phash_cascade(product1_id, product2_id, db)
        
        # Stage 3: CLIP analysis if needed
        if not phash_decision.skipped_clip:
            return self._analyze_clip_decision(phash_decision, db)
        
        return phash_decision
    
    def analyze_product_group(self, product_ids: List[str], db: Session) -> List[CascadeDecision]:
        """
        Analyze all pairs within a product group.
        
        Args:
            product_ids: List of product IDs to analyze
            db: Database session
            
        Returns:
            List of CascadeDecisions for all pairs
        """
        decisions = []
        
        for i, product1_id in enumerate(product_ids):
            for j, product2_id in enumerate(product_ids[i+1:], start=i+1):
                decision = self.analyze_product_pair(product1_id, product2_id, db)
                decisions.append(decision)
                
                logger.debug(f"Pair {product1_id} vs {product2_id}: "
                           f"{decision.decision_stage}, duplicate={decision.is_duplicate}, "
                           f"confidence={decision.confidence:.3f}")
        
        return decisions