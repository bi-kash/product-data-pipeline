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
    
    # pHash cascade thresholds (simplified)
    phash_duplicate_threshold: int = 8      # ≤8: Duplicate, no CLIP needed
    phash_ambiguous_threshold: int = 18     # 9-18: Ambiguous, send to CLIP
                                           # >18: Different, no CLIP needed
    
    # CLIP thresholds (three-tier system)
    clip_duplicate_threshold: float = 0.96  # ≥0.96: Confirmed duplicate
    clip_ambiguous_threshold: float = 0.94  # 0.94-0.96: Ambiguous, needs review
                                           # <0.94: Different products
    
    # Processing limits
    max_images_per_product: int = 5

    @classmethod
    def from_env(cls) -> 'CascadeConfig':
        """Load configuration from environment variables."""
        return cls(
            min_image_size=int(os.getenv('MIN_IMAGE_SIZE', '64')),
            
            phash_duplicate_threshold=int(os.getenv('PHASH_DUPLICATE_THRESHOLD', '8')),
            phash_ambiguous_threshold=int(os.getenv('PHASH_AMBIGUOUS_THRESHOLD', '18')),
            
            clip_duplicate_threshold=float(os.getenv('CLIP_DUPLICATE_THRESHOLD', '0.96')),
            clip_ambiguous_threshold=float(os.getenv('CLIP_AMBIGUOUS_THRESHOLD', '0.94')),
            
            max_images_per_product=int(os.getenv('CLIP_MAX_IMAGES_PER_PRODUCT', '5'))
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
        
        logger.info(f"Cascade config: pHash duplicate≤{self.config.phash_duplicate_threshold}, "
                   f"ambiguous={self.config.phash_duplicate_threshold+1}-{self.config.phash_ambiguous_threshold}, "
                   f"different>{self.config.phash_ambiguous_threshold}, "
                   f"CLIP duplicate≥{self.config.clip_duplicate_threshold}, "
                   f"ambiguous={self.config.clip_ambiguous_threshold}-{self.config.clip_duplicate_threshold}, "
                   f"different<{self.config.clip_ambiguous_threshold}")
    
    def _check_image_quality(self, image: ProductImage) -> bool:
        """
        Check if image meets minimum size requirements.
        
        Args:
            image: ProductImage to check
            
        Returns:
            True if image size is acceptable
        """
        # Check minimum image dimensions only
        if image.width and image.height:
            if image.width < self.config.min_image_size or image.height < self.config.min_image_size:
                logger.debug(f"Rejecting small image {image.id}: {image.width}x{image.height}")
                return False
        
        return True
    
    def _check_metadata_shortcuts(self, product1: FilteredProduct, product2: FilteredProduct) -> Optional[CascadeDecision]:
        """
        Simplified cascade - no metadata shortcuts, go straight to pHash analysis.
        
        Args:
            product1: First product
            product2: Second product
            
        Returns:
            None - always proceed to pHash analysis for simplicity
        """
        # Simplified approach: skip all metadata shortcuts, use only pHash -> CLIP cascade
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
        
        # Apply simplified cascade decision logic
        phash_diff = int(best_phash_diff)
        
        if phash_diff <= self.config.phash_duplicate_threshold:
            # Definitely duplicate - skip CLIP analysis
            # Calculate confidence (protect against division by zero)
            if self.config.phash_duplicate_threshold > 0:
                confidence = 1.0 - (phash_diff / self.config.phash_duplicate_threshold * 0.2)  # 0.8-1.0 range
            else:
                print("Warning: phash_duplicate_threshold is 0, setting confidence to 1.0")
                confidence = 1.0 if phash_diff == 0 else 0.95  # Perfect match or very close
            
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=True,
                confidence=confidence,
                decision_stage='PHASH_DUPLICATE',
                phash_difference=phash_diff,
                reason=f'pHash duplicate (diff: {phash_diff} ≤ {self.config.phash_duplicate_threshold})',
                skipped_clip=True
            )
        
        elif phash_diff > self.config.phash_ambiguous_threshold:
            # Definitely different - skip CLIP analysis
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,
                confidence=0.9,  # High confidence they're different
                decision_stage='PHASH_DIFFERENT',
                phash_difference=phash_diff,
                reason=f'pHash different (diff: {phash_diff} > {self.config.phash_ambiguous_threshold})',
                skipped_clip=True
            )
        
        else:
            # Ambiguous zone (between duplicate and different thresholds) - needs CLIP analysis
            return CascadeDecision(
                product1_id=product1_id,
                product2_id=product2_id,
                is_duplicate=False,  # TBD by CLIP
                confidence=0.5,  # Uncertain
                decision_stage='PHASH_AMBIGUOUS',
                phash_difference=phash_diff,
                reason=f'pHash ambiguous (diff: {phash_diff}, {self.config.phash_duplicate_threshold+1}-{self.config.phash_ambiguous_threshold}) - needs CLIP',
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

        # Get CLIP similarity for the product pair using dedicated pairwise comparison
        try:
            clip_similarity = self._calculate_clip_similarity_for_pair(
                decision.product1_id, decision.product2_id, db
            )
            
            if clip_similarity is None:
                # No CLIP similarity found - treat as different
                decision.is_duplicate = False
                decision.confidence = 0.8
                decision.decision_stage = 'CLIP_NO_SIMILARITY'
                decision.reason = 'No CLIP similarity found'
                return decision
            
            # Apply three-tier CLIP decision system
            decision.clip_similarity = clip_similarity
            
            if clip_similarity >= self.config.clip_duplicate_threshold:
                # CLIP confirmed duplicate (≥0.96)
                decision.is_duplicate = True
                decision.confidence = clip_similarity
                decision.decision_stage = 'CLIP_DUPLICATE'
                decision.reason = f'CLIP confirmed duplicate (sim: {clip_similarity:.3f} ≥ {self.config.clip_duplicate_threshold})'
            elif clip_similarity >= self.config.clip_ambiguous_threshold:
                # CLIP ambiguous - needs human review (0.94-0.96)
                decision.is_duplicate = False  # Don't mark as duplicate, but flag for review
                decision.confidence = 0.5  # Uncertain
                decision.decision_stage = 'CLIP_REVIEW_SUSPECT'
                decision.reason = f'CLIP ambiguous - needs review (sim: {clip_similarity:.3f} in range {self.config.clip_ambiguous_threshold}-{self.config.clip_duplicate_threshold})'
            else:
                # CLIP confirmed different (<0.94)
                decision.is_duplicate = False
                decision.confidence = 1.0 - clip_similarity
                decision.decision_stage = 'CLIP_DIFFERENT'
                decision.reason = f'CLIP confirmed different (sim: {clip_similarity:.3f} < {self.config.clip_ambiguous_threshold})'
            
        except Exception as e:
            logger.error(f"CLIP analysis failed for {decision.product1_id} vs {decision.product2_id}: {e}")
            decision.reason += f" (CLIP failed: {e})"
            decision.skipped_clip = True
        
        return decision

    def _calculate_clip_similarity_for_pair(self, product1_id: str, product2_id: str, db: Session) -> Optional[float]:
        """
        Calculate CLIP similarity for a specific product pair only.
        
        This is a lightweight method that only processes the two specific products,
        unlike analyze_candidates which processes all combinations.
        
        Args:
            product1_id: First product ID
            product2_id: Second product ID
            db: Database session
            
        Returns:
            CLIP similarity score (0-1) or None if comparison failed
        """
        try:
            # Get images for both products
            images1 = db.query(ProductImage).filter(
                ProductImage.product_id == product1_id,
                ProductImage.local_file_path.isnot(None),
                ProductImage.image_role.in_(['hero', 'variant'])
            ).limit(self.config.max_images_per_product).all()
            
            images2 = db.query(ProductImage).filter(
                ProductImage.product_id == product2_id,
                ProductImage.local_file_path.isnot(None),
                ProductImage.image_role.in_(['hero', 'variant'])
            ).limit(self.config.max_images_per_product).all()
            
            # Filter by quality
            images1 = [img for img in images1 if self._check_image_quality(img)]
            images2 = [img for img in images2 if self._check_image_quality(img)]
            
            if not images1 or not images2:
                logger.debug(f"Insufficient images for CLIP comparison: {product1_id} ({len(images1)} imgs) vs {product2_id} ({len(images2)} imgs)")
                return None
            
            # Extract embeddings for both products
            embeddings1 = []
            embeddings2 = []
            
            for img in images1:
                if img.local_file_path:
                    embedding = self.clip_analyzer.extract_image_embedding(img.local_file_path, img.id)
                    if embedding is not None:
                        embeddings1.append(embedding)
            
            for img in images2:
                if img.local_file_path:
                    embedding = self.clip_analyzer.extract_image_embedding(img.local_file_path, img.id)
                    if embedding is not None:
                        embeddings2.append(embedding)
            
            if not embeddings1 or not embeddings2:
                logger.debug(f"Failed to extract embeddings: {product1_id} ({len(embeddings1)}) vs {product2_id} ({len(embeddings2)})")
                return None
            
            # Calculate maximum similarity between any pair of embeddings
            import numpy as np
            max_similarity = 0.0
            
            for emb1 in embeddings1:
                for emb2 in embeddings2:
                    # Cosine similarity (with zero-norm protection)
                    norm1 = np.linalg.norm(emb1)
                    norm2 = np.linalg.norm(emb2)
                    
                    if norm1 > 0 and norm2 > 0:
                        similarity = np.dot(emb1, emb2) / (norm1 * norm2)
                        max_similarity = max(max_similarity, similarity)
                    else:
                        logger.warning(f"Skipping similarity calculation due to zero norm embedding")
            
            logger.debug(f"CLIP similarity for {product1_id} vs {product2_id}: {max_similarity:.3f}")
            return float(max_similarity)
            
        except Exception as e:
            logger.error(f"Error calculating CLIP similarity for {product1_id} vs {product2_id}: {e}")
            return None
    
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
            logger.debug(f"Escalating to CLIP: {product1_id} vs {product2_id} (pHash: {phash_decision.phash_difference})")
            return self._analyze_clip_decision(phash_decision, db)
        else:
            logger.debug(f"Skipping CLIP: {product1_id} vs {product2_id} (pHash: {phash_decision.phash_difference}, stage: {phash_decision.decision_stage})")
        
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