"""
CLIP Analyzer for semantic image similarity detection.

This module uses CLIP (Contrastive Language-Image Pre-training) to perform
semantic analysis on images for refined duplicate detection. Designed to be
CPU-capable by default.
"""

import os
import logging
import numpy as np
from typing import List, Dict, Tuple, Optional, Set
from pathlib import Path
import pickle
from PIL import Image
from dotenv import load_dotenv
from sqlalchemy.orm import Session

# Load environment variables
load_dotenv()

# Import CLIP - will handle both CPU and GPU modes
try:
    import torch
    import clip
    CLIP_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("CLIP library loaded successfully")
except ImportError:
    CLIP_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.error("CLIP library not available. Install with: pip install ftfy regex tqdm torch torchvision")

from src.common.database import ProductImage, get_db_session
from src.common.config import get_env


class CLIPAnalyzer:
    """
    Semantic image similarity analyzer using CLIP.
    
    Designed to work on CPU by default, with optional GPU acceleration.
    Stores embeddings locally for faster subsequent analysis.
    """

    def __init__(self, model_name: str = None, device: str = None, 
                 embeddings_dir: str = None, similarity_threshold: float = None):
        """
        Initialize the CLIP analyzer.
        
        Args:
            model_name: CLIP model to use. If None, reads from CLIP_MODEL env var (default: ViT-B/32)
            device: Device to use ('cpu', 'cuda', or 'auto'). If None, reads from CLIP_DEVICE env var (default: auto)
            embeddings_dir: Directory to store embeddings (default: embeddings/)
            similarity_threshold: Cosine similarity threshold for duplicates. If None, reads from CLIP_THRESHOLD env var (default: 0.85)
        """
        if not CLIP_AVAILABLE:
            raise ImportError("CLIP library not available. Install with: pip install ftfy regex tqdm torch torchvision")
        
        # Load configuration from environment variables
        if model_name is None:
            model_name = os.getenv('CLIP_MODEL', 'ViT-B/32')
        if device is None:
            device = os.getenv('CLIP_DEVICE', 'auto')
        if similarity_threshold is None:
            similarity_threshold = float(os.getenv('CLIP_THRESHOLD', '0.85'))
        
        self.model_name = model_name
        self.similarity_threshold = similarity_threshold
        
        # Set up device
        if device == "auto":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self.device = device
        
        # Set up embeddings directory
        self.embeddings_dir = Path(embeddings_dir or "embeddings")
        self.embeddings_dir.mkdir(exist_ok=True)
        
        # Load model
        logger.info(f"Loading CLIP model {model_name} on {self.device}")
        self.model, self.preprocess = clip.load(model_name, device=self.device)
        self.model.eval()  # Set to evaluation mode
        
        logger.info(f"CLIPAnalyzer initialized (device: {self.device}, threshold: {similarity_threshold})")

    def get_embedding_path(self, image_path: str) -> Path:
        """Get the file path for storing an image's embedding based on filename."""
        # Extract filename without path and extension, then add .pkl
        image_filename = os.path.splitext(os.path.basename(image_path))[0]
        return self.embeddings_dir / f"{image_filename}.pkl"

    def extract_image_embedding(self, image_path: str, image_id: int = None) -> Optional[np.ndarray]:
        """
        Extract CLIP embedding for a single image.
        
        Args:
            image_path: Path to the image file
            image_id: Image ID for caching (optional)
            
        Returns:
            Numpy array of the embedding, or None if extraction fails
        """
        # Check if embedding is already cached (use filename-based caching)
        embedding_path = self.get_embedding_path(image_path)
        if embedding_path.exists():
            try:
                with open(embedding_path, 'rb') as f:
                    embedding = pickle.load(f)
                logger.debug(f"Loaded cached embedding for {os.path.basename(image_path)}")
                return embedding
            except Exception as e:
                logger.warning(f"Failed to load cached embedding for {os.path.basename(image_path)}: {e}")
        
        try:
            # Load and preprocess image
            if not os.path.exists(image_path):
                logger.error(f"Image file not found: {image_path}")
                return None
            
            image = Image.open(image_path).convert('RGB')
            image_tensor = self.preprocess(image).unsqueeze(0).to(self.device)
            
            # Extract embedding
            with torch.no_grad():
                embedding = self.model.encode_image(image_tensor)
                embedding = embedding.cpu().numpy().flatten()
                
                # Normalize embedding (handle zero norm)
                norm = np.linalg.norm(embedding)
                if norm > 0:
                    embedding = embedding / norm
                else:
                    logger.warning(f"Zero norm embedding for {image_path}, returning zero vector")
                    return np.zeros_like(embedding)
            
            # Cache embedding using filename-based key
            try:
                with open(embedding_path, 'wb') as f:
                    pickle.dump(embedding, f)
                logger.debug(f"Cached embedding for {os.path.basename(image_path)}")
            except Exception as e:
                logger.warning(f"Failed to cache embedding for {os.path.basename(image_path)}: {e}")
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to extract embedding from {image_path}: {e}")
            return None

    def calculate_similarity(self, embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """
        Calculate cosine similarity between two embeddings.
        
        Args:
            embedding1: First embedding
            embedding2: Second embedding
            
        Returns:
            Cosine similarity score (0-1), clamped to valid range to handle floating-point precision
        """
        try:
            # Calculate norms
            norm1 = np.linalg.norm(embedding1)
            norm2 = np.linalg.norm(embedding2)
            
            # Handle zero norms
            if norm1 == 0 or norm2 == 0:
                logger.warning("One or both embeddings have zero norm, returning 0 similarity")
                return 0.0
            
            # Calculate cosine similarity
            similarity = np.dot(embedding1, embedding2) / (norm1 * norm2)
            # Clamp to valid range [-1, 1] to handle floating-point precision errors
            similarity = np.clip(similarity, -1.0, 1.0)
            return float(similarity)
        except Exception as e:
            logger.error(f"Failed to calculate similarity: {e}")
            return 0.0

    

    def extract_embeddings_for_products(self, products_images: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Extract CLIP embeddings for all images in the provided products.
        
        Args:
            products_images: Dict mapping product_id to list of image data
            
        Returns:
            Dict mapping product_id to list of image data with embeddings
        """
        logger.info("Extracting CLIP embeddings for product images...")
        
        total_images = sum(len(imgs) for imgs in products_images.values())
        processed = 0
        
        for product_id, images in products_images.items():
            logger.debug(f"Processing {len(images)} images for product {product_id}")
            
            for image_data in images:
                # Extract embedding
                embedding = self.extract_image_embedding(
                    image_data['local_file_path'], 
                    image_data['id']
                )
                
                image_data['embedding'] = embedding
                image_data['embedding_success'] = embedding is not None
                
                processed += 1
                if processed % 10 == 0 or processed == total_images:
                    logger.info(f"Processed {processed}/{total_images} embeddings")
        
        # Count successful extractions
        successful = sum(
            1 for images in products_images.values() 
            for img in images if img.get('embedding_success', False)
        )
        
        logger.info(f"Successfully extracted {successful}/{total_images} embeddings")
        return products_images

    def find_similar_products_clip(self, products_images: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Find similar products using CLIP embeddings.
        
        Args:
            products_images: Dict with product images and their embeddings
            
        Returns:
            List of similarity matches with CLIP scores
        """
        logger.info("Analyzing product similarity using CLIP embeddings...")
        
        product_ids = list(products_images.keys())
        similar_pairs = []
        
        for i, product1_id in enumerate(product_ids):
            for j, product2_id in enumerate(product_ids[i+1:], i+1):
                images1 = products_images[product1_id]
                images2 = products_images[product2_id]
                
                max_similarity = 0.0
                best_match = None
                
                # Compare all image pairs between the two products
                for img1 in images1:
                    if not img1.get('embedding_success'):
                        continue
                        
                    for img2 in images2:
                        if not img2.get('embedding_success'):
                            continue
                        
                        similarity = self.calculate_similarity(
                            img1['embedding'], img2['embedding']
                        )
                        
                        if similarity > max_similarity:
                            max_similarity = similarity
                            best_match = {
                                'image1_id': img1['id'],
                                'image2_id': img2['id'],
                                'image1_role': img1['image_role'],
                                'image2_role': img2['image_role'],
                                'image1_url': img1['image_url'],
                                'image2_url': img2['image_url']
                            }
                
                # Add to results if similarity exceeds threshold
                if max_similarity >= self.similarity_threshold and best_match:
                    similar_pairs.append({
                        'product1_id': product1_id,
                        'product2_id': product2_id,
                        'clip_similarity': max_similarity,
                        'is_similar': True,
                        **best_match
                    })
        
        logger.info(f"Found {len(similar_pairs)} similar product pairs with CLIP (threshold: {self.similarity_threshold})")
        return similar_pairs

    def analyze_candidates(self, db: Session, candidate_product_ids: List[str], 
                          image_roles: List[str] = None) -> Dict:
        """
        Analyze candidate products using CLIP for semantic similarity.
        
        Args:
            db: Database session
            candidate_product_ids: List of product IDs to analyze
            image_roles: Image roles to include in analysis
            
        Returns:
            Dict with CLIP analysis results
        """
        logger.info(f"Starting CLIP analysis for {len(candidate_product_ids)} candidate products")
        
        # Get images for analysis
        products_images = self.get_product_images_for_clip(db, candidate_product_ids, image_roles)
        
        if not products_images:
            logger.warning("No images found for CLIP analysis")
            return {
                'total_products': 0,
                'total_images': 0,
                'similar_pairs': [],
                'stats': {
                    'embeddings_extracted': 0,
                    'similar_pairs_found': 0,
                    'threshold_used': self.similarity_threshold
                }
            }
        
        # Extract embeddings
        products_with_embeddings = self.extract_embeddings_for_products(products_images)
        
        # Find similar products
        similar_pairs = self.find_similar_products_clip(products_with_embeddings)
        
        # Calculate statistics
        total_images = sum(len(imgs) for imgs in products_images.values())
        embeddings_extracted = sum(
            1 for images in products_images.values() 
            for img in images if img.get('embedding_success', False)
        )
        
        results = {
            'total_products': len(products_images),
            'total_images': total_images,
            'similar_pairs': similar_pairs,
            'stats': {
                'embeddings_extracted': embeddings_extracted,
                'similar_pairs_found': len(similar_pairs),
                'threshold_used': self.similarity_threshold,
                'device_used': self.device,
                'model_used': self.model_name
            }
        }
        
        logger.info(f"CLIP analysis complete: {len(similar_pairs)} similar pairs found")
        return results

    def clear_embeddings_cache(self):
        """Clear all cached embeddings."""
        import shutil
        if self.embeddings_dir.exists():
            shutil.rmtree(self.embeddings_dir)
            self.embeddings_dir.mkdir(exist_ok=True)
            logger.info("Cleared embeddings cache")


def main():
    """Test function for the CLIP analyzer."""
    import sys
    from src.common.config import get_env
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    if not CLIP_AVAILABLE:
        print("❌ CLIP library not available. Install with:")
        print("   pip install ftfy regex tqdm torch torchvision")
        return
    
    # Get configuration from environment
    threshold = float(get_env('CLIP_THRESHOLD', '0.85'))
    device = get_env('CLIP_DEVICE', 'auto')
    model_name = get_env('CLIP_MODEL', 'ViT-B/32')
    
    print(f"🤖 Testing CLIP Analyzer")
    print(f"   Model: {model_name}")
    print(f"   Device: {device}")
    print(f"   Threshold: {threshold}")
    
    try:
        analyzer = CLIPAnalyzer(
            model_name=model_name,
            device=device,
            similarity_threshold=threshold
        )
        
        # Test with some candidate products (you would get these from pHash analyzer)
        candidate_ids = sys.argv[1:] if len(sys.argv) > 1 else []
        
        if not candidate_ids:
            print("💡 Usage: python clip_analyzer.py <product_id1> <product_id2> ...")
            print("💡 Or provide product IDs as command line arguments")
            return
        
        print(f"🔍 Analyzing {len(candidate_ids)} candidate products...")
        
        with get_db_session() as db:
            results = analyzer.analyze_candidates(db, candidate_ids)
            
            print(f"\n📊 CLIP Analysis Results:")
            print(f"  📦 Products analyzed: {results['total_products']}")
            print(f"  🖼️  Images processed: {results['total_images']}")
            print(f"  🧠 Embeddings extracted: {results['stats']['embeddings_extracted']}")
            print(f"  🎯 Similar pairs found: {results['stats']['similar_pairs_found']}")
            print(f"  💻 Device used: {results['stats']['device_used']}")
            
            if results['similar_pairs']:
                print(f"\n🔍 Similar product pairs:")
                for pair in results['similar_pairs'][:5]:
                    print(f"  {pair['product1_id']} ↔ {pair['product2_id']} "
                          f"(similarity: {pair['clip_similarity']:.3f})")
                          
    except Exception as e:
        logger.error(f"Error testing CLIP analyzer: {e}")
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()