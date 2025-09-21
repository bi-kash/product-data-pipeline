#!/usr/bin/env python3
"""
Standalone pHash testing utility.

This script tests perceptual hash (pHash) functionality on images
to verify image similarity detection and duplicate identification.
"""

import os
import sys
import requests
import imagehash
from PIL import Image
from io import BytesIO
import hashlib
from typing import Optional, List, Dict, Tuple
import json


class PHashTester:
    """
    Utility class for testing perceptual hash functionality.
    """
    
    def __init__(self):
        """Initialize the pHash tester."""
        self.test_results = []
        
    def calculate_phash_from_url(self, image_url: str) -> Optional[str]:
        """
        Calculate pHash directly from an image URL.
        
        Args:
            image_url: URL of the image
            
        Returns:
            Perceptual hash as hex string, or None if calculation fails
        """
        try:
            # Set headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }
            
            print(f"📥 Downloading image from: {image_url}")
            response = requests.get(image_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Open image from bytes
            image_data = response.content
            image = Image.open(BytesIO(image_data))
            
            # Convert to RGB if necessary
            if image.mode not in ('RGB', 'L'):
                image = image.convert('RGB')
            
            # Calculate perceptual hash
            phash = imagehash.phash(image)
            
            print(f"✅ pHash calculated: {str(phash)}")
            print(f"📏 Image dimensions: {image.size}")
            print(f"🎨 Image mode: {image.mode}")
            print(f"📁 Image format: {image.format}")
            
            return str(phash)
            
        except Exception as e:
            print(f"❌ Error calculating pHash from URL {image_url}: {e}")
            return None
    
    def calculate_phash_from_file(self, file_path: str) -> Optional[str]:
        """
        Calculate pHash from a local image file.
        
        Args:
            file_path: Path to the local image file
            
        Returns:
            Perceptual hash as hex string, or None if calculation fails
        """
        try:
            print(f"📁 Loading image from file: {file_path}")
            
            with open(file_path, 'rb') as f:
                image_data = f.read()
            
            image = Image.open(BytesIO(image_data))
            
            # Convert to RGB if necessary
            if image.mode not in ('RGB', 'L'):
                image = image.convert('RGB')
            
            # Calculate perceptual hash
            phash = imagehash.phash(image)
            
            print(f"✅ pHash calculated: {str(phash)}")
            print(f"📏 Image dimensions: {image.size}")
            print(f"🎨 Image mode: {image.mode}")
            print(f"📁 Image format: {image.format}")
            
            return str(phash)
            
        except Exception as e:
            print(f"❌ Error calculating pHash from file {file_path}: {e}")
            return None
    
    def compare_hashes(self, hash1: str, hash2: str) -> int:
        """
        Compare two perceptual hashes and return the difference.
        
        Args:
            hash1: First pHash
            hash2: Second pHash
            
        Returns:
            Hamming distance between the hashes (0 = identical, higher = more different)
        """
        try:
            # Convert hex strings back to ImageHash objects for comparison
            ihash1 = imagehash.hex_to_hash(hash1)
            ihash2 = imagehash.hex_to_hash(hash2)
            
            # Calculate difference (Hamming distance)
            difference = ihash1 - ihash2
            
            return difference
            
        except Exception as e:
            print(f"❌ Error comparing hashes: {e}")
            return -1
    
    def test_duplicate_detection(self, image_urls: List[str]) -> Dict:
        """
        Test duplicate detection across multiple image URLs.
        
        Args:
            image_urls: List of image URLs to test
            
        Returns:
            Dict with test results
        """
        print(f"\n🔍 Testing duplicate detection across {len(image_urls)} images...")
        
        results = {
            'hashes': {},
            'comparisons': [],
            'potential_duplicates': []
        }
        
        # Calculate pHash for each image
        for i, url in enumerate(image_urls):
            print(f"\n--- Image {i+1}/{len(image_urls)} ---")
            phash = self.calculate_phash_from_url(url)
            if phash:
                results['hashes'][url] = phash
        
        # Compare all pairs
        urls_with_hashes = list(results['hashes'].keys())
        print(f"\n🔍 Comparing {len(urls_with_hashes)} images for duplicates...")
        
        for i in range(len(urls_with_hashes)):
            for j in range(i + 1, len(urls_with_hashes)):
                url1 = urls_with_hashes[i]
                url2 = urls_with_hashes[j]
                hash1 = results['hashes'][url1]
                hash2 = results['hashes'][url2]
                
                difference = self.compare_hashes(hash1, hash2)
                
                comparison = {
                    'url1': url1,
                    'url2': url2,
                    'hash1': hash1,
                    'hash2': hash2,
                    'difference': difference,
                    'is_similar': difference <= 5,  # Threshold for similarity
                    'is_duplicate': difference == 0  # Exact match
                }
                
                results['comparisons'].append(comparison)
                
                if difference <= 5:  # Similar images
                    results['potential_duplicates'].append(comparison)
                    similarity_level = "🔴 DUPLICATE" if difference == 0 else f"🟡 SIMILAR (diff: {difference})"
                    print(f"{similarity_level}")
                    print(f"  📸 Image 1: {url1[-50:]}")
                    print(f"  📸 Image 2: {url2[-50:]}")
                    print(f"  🔢 Hash 1: {hash1}")
                    print(f"  🔢 Hash 2: {hash2}")
        
        return results
    
    def test_sample_images(self) -> Dict:
        """
        Test with some sample AliExpress images from the database.
        
        Returns:
            Dict with test results
        """
        # Sample URLs from our test data
        sample_urls = [
            "https://ae01.alicdn.com/kf/S423d885fb36b4e399840a36b7caee26em.jpg",
            "https://ae01.alicdn.com/kf/S47ad5db7ec5c48f19c1b5dddc41d8e9ap.jpg", 
            "https://ae01.alicdn.com/kf/Sc0527b7b2b3f4012907f8cbe2e04b28fM.jpg",
            "https://ae01.alicdn.com/kf/S423d885fb36b4e399840a36b7caee26em.jpg",  # Duplicate of first one
        ]
        
        print("🧪 Testing with sample AliExpress images...")
        return self.test_duplicate_detection(sample_urls)
    
    def save_results(self, results: Dict, filename: str = "phash_test_results.json"):
        """
        Save test results to a JSON file.
        
        Args:
            results: Test results dictionary
            filename: Output filename
        """
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"💾 Results saved to {filename}")
        except Exception as e:
            print(f"❌ Error saving results: {e}")


def main():
    """Main function for testing pHash functionality."""
    print("🔧 pHash Testing Utility")
    print("=" * 50)
    
    tester = PHashTester()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "url" and len(sys.argv) > 2:
            # Test single URL
            url = sys.argv[2]
            print(f"🧪 Testing single URL: {url}")
            phash = tester.calculate_phash_from_url(url)
            if phash:
                print(f"🎯 Final pHash: {phash}")
            
        elif command == "file" and len(sys.argv) > 2:
            # Test local file
            file_path = sys.argv[2]
            print(f"🧪 Testing local file: {file_path}")
            phash = tester.calculate_phash_from_file(file_path)
            if phash:
                print(f"🎯 Final pHash: {phash}")
            
        elif command == "compare" and len(sys.argv) > 3:
            # Compare two inputs (URLs or files)
            input1 = sys.argv[2]
            input2 = sys.argv[3]
            
            # Determine if inputs are URLs or local files
            is_url1 = input1.startswith('http')
            is_url2 = input2.startswith('http')
            
            print(f"🧪 Comparing two images:")
            print(f"  📸 Input 1: {input1} ({'URL' if is_url1 else 'Local file'})")
            print(f"  📸 Input 2: {input2} ({'URL' if is_url2 else 'Local file'})")
            
            # Calculate hashes based on input type
            hash1 = tester.calculate_phash_from_url(input1) if is_url1 else tester.calculate_phash_from_file(input1)
            print(f"\n" + "─" * 50)
            hash2 = tester.calculate_phash_from_url(input2) if is_url2 else tester.calculate_phash_from_file(input2)
            
            if hash1 and hash2:
                difference = tester.compare_hashes(hash1, hash2)
                print(f"\n🔍 Comparison Results:")
                print(f"  🔢 Hash 1: {hash1}")
                print(f"  🔢 Hash 2: {hash2}")
                print(f"  📊 Difference: {difference}")
                
                if difference == 0:
                    print(f"  🔴 IDENTICAL IMAGES")
                elif difference <= 5:
                    print(f"  🟡 SIMILAR IMAGES (threshold: ≤5)")
                else:
                    print(f"  🟢 DIFFERENT IMAGES")
            
        elif command == "batch" and len(sys.argv) > 2:
            # Test multiple URLs from file
            urls_file = sys.argv[2]
            try:
                with open(urls_file, 'r') as f:
                    urls = [line.strip() for line in f if line.strip()]
                print(f"🧪 Testing {len(urls)} URLs from file: {urls_file}")
                results = tester.test_duplicate_detection(urls)
                tester.save_results(results)
            except FileNotFoundError:
                print(f"❌ File not found: {urls_file}")
            
        elif command == "sample":
            # Test with sample images
            results = tester.test_sample_images()
            tester.save_results(results)
            
            # Print summary
            print(f"\n📊 Test Summary:")
            print(f"  📸 Images tested: {len(results['hashes'])}")
            print(f"  🔍 Comparisons made: {len(results['comparisons'])}")
            print(f"  🎯 Potential duplicates: {len(results['potential_duplicates'])}")
            
        else:
            print_usage()
    else:
        print_usage()


def print_usage():
    """Print usage instructions."""
    print("\n📖 Usage:")
    print("  python test_phash.py url <image_url>                    - Test single URL")
    print("  python test_phash.py file <file_path>                   - Test local file")
    print("  python test_phash.py compare <url1> <url2>              - Compare two URLs")
    print("  python test_phash.py batch <urls_file>                  - Test URLs from file")
    print("  python test_phash.py sample                             - Test with sample images")
    print("\n🎯 Examples:")
    print("  python test_phash.py url 'https://ae01.alicdn.com/kf/example.jpg'")
    print("  python test_phash.py compare 'https://url1.jpg' 'https://url2.jpg'")
    print("  python test_phash.py sample")


if __name__ == "__main__":
    main()