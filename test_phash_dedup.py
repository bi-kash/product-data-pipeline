"""
Test script to verify phash deduplication is working correctly.
"""

from src.common.database import get_db_session, ProductImage
from sqlalchemy import func

def check_phash_duplicates():
    """Check for phashes with multiple different S3 URLs."""
    
    with get_db_session() as db:
        # Find phashes that have multiple different S3 URLs
        duplicates = db.query(
            ProductImage.phash,
            func.count(func.distinct(ProductImage.s3_url)).label('url_count'),
            func.count(ProductImage.id).label('total_images')
        ).filter(
            ProductImage.phash.isnot(None),
            ProductImage.s3_url.isnot(None)
        ).group_by(
            ProductImage.phash
        ).having(
            func.count(func.distinct(ProductImage.s3_url)) > 1
        ).all()
        
        print(f'Found {len(duplicates)} phashes with multiple different S3 URLs')
        
        if duplicates:
            print('\nPhashes that should be fixed:')
            for phash, url_count, total_images in duplicates:
                print(f'  Phash: {phash} - {url_count} different S3 URLs across {total_images} images')
        else:
            print('\n✅ All phashes have unique S3 URLs - deduplication is working correctly!')
        
        return len(duplicates)

if __name__ == "__main__":
    check_phash_duplicates()
