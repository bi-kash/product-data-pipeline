#!/usr/bin/env python3
"""
Main entry point for the Product Data Pipeline CLI.
This module provides command-line interfaces for all pipeline processes.
"""

import os
import argparse
import sys
from src.review.merchant_review import export_pending_merchants, import_review_results
from src.harvester.merchant_harvester import init_harvest as original_init_harvest
from src.harvester.merchant_harvester import delta_harvest as original_delta_harvest
from src.harvester.merchant_harvester import harvest_status as original_harvest_status
from src.session.session_manager import create_session, refresh_session_token, auto_refresh_session, list_sessions, force_unlock_database, get_oauth_authorization_url
from src.filter.product_filter import run_product_filtering
from src.duplicate_detection.duplicate_detector import DuplicateDetector
from src.common.database import get_db_session
from src.common.logging_config import setup_logging
from src.airtable.sync import sync_to_airtable
from src.airtable.base_creator import create_base_command

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Set up main logger
logger = setup_logging("main")


# Wrapper functions to maintain compatibility with original CLI
def init_harvest(limit=None, dry_run=False):
    """Initialize merchant harvest."""
    original_init_harvest(limit=limit, dry_run=dry_run)


def delta_harvest(limit=None, dry_run=False):
    """Run incremental merchant harvest."""
    original_delta_harvest(limit=limit, dry_run=dry_run)


def harvest_status():
    """Show harvest job status."""
    original_harvest_status()


def filter_products(limit=None, dry_run=False):
    """Filter products from whitelisted sellers based on business rules."""
    run_product_filtering(
        limit=limit,
        dry_run=dry_run
    )


def detect_duplicates(limit=None, dry_run=False, phash_only=False):
    """Run duplicate detection and master selection."""
    logger.info(f"Starting duplicate detection (limit: {limit}, dry_run: {dry_run}, phash_only: {phash_only})")
    
    try:
        # Initialize detector with optional CLIP disable
        detector = DuplicateDetector(clip_enabled=not phash_only)
        
        with get_db_session() as db:
            results = detector.detect_duplicates(db, limit=limit, dry_run=dry_run)
            
            print(f"\n🔍 Duplicate Detection Results:")
            print(f"  ⏱️  Processing time: {results['total_time']:.2f} seconds")
            print(f"  📊 Stage completed: {results['stage']}")
            print(f"  🔢 Total products analyzed: {results['final_stats']['total_analyzed']}")
            print(f"  ✅ Unique products: {results['final_stats']['unique_products']}")
            print(f"  👑 Master products: {results['final_stats']['master_products']}")
            print(f"  📄 Duplicate products: {results['final_stats']['duplicate_products']}")
            
            # Show cascade stats if available
            if 'cascade_stats' in results:
                stats = results['cascade_stats']
                print(f"\n🔍 Cascade Stage Breakdown:")
                print(f"  � Total pairs analyzed: {stats['total_pairs']}")
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
            
            if dry_run:
                print(f"\n💡 This was a dry run - no changes were saved to the database")
            else:
                print(f"\n✅ Results saved to database")
                
    except Exception as e:
        logger.error(f"Error in duplicate detection: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


def detect_status():
    """Show duplicate detection status and statistics."""
    logger.info("Getting duplicate detection status")
    
    try:
        detector = DuplicateDetector()
        
        with get_db_session() as db:
            status = detector.get_detection_status(db)
            
            if 'error' in status:
                print(f"❌ Error getting status: {status['error']}")
                return
            
            print(f"\n📊 Duplicate Detection Status:")
            print(f"  📦 Total products in database: {status['total_products']}")
            print(f"  🔍 Products analyzed: {status['analyzed_products']}")
            print(f"  ❓ Unanalyzed products: {status['unanalyzed_products']}")
            print(f"  📈 Analysis coverage: {status['analysis_coverage']:.1%}")
            
            print(f"\n📋 Status Breakdown:")
            for status_name, count in status['status_breakdown'].items():
                emoji = {
                    'UNIQUE': '✅',
                    'MASTER': '👑',
                    'DUPLICATE': '📄',
                    'REVIEW_SUSPECT': '⚠️'
                }.get(status_name, '❓')
                print(f"  {emoji} {status_name}: {count}")
                
    except Exception as e:
        logger.error(f"Error getting detection status: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


def import_reviewed_suspects(input_file, dry_run=False):
    """Import reviewed suspect duplicates and update database."""
    logger.info(f"Importing reviewed suspects from {input_file} (dry_run: {dry_run})")
    
    try:
        import csv
        from src.common.database import ProductStatus
        
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return
        
        # Read CSV file
        updates = []
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                # Support both old and new column formats
                product_id = (row.get('duplicate_product_id', '') or row.get('suspect_product_id', '')).strip()
                decision = (row.get('status', '') or row.get('review_decision', '')).strip().upper()
                notes = row.get('notes', '').strip()
                
                if not product_id:
                    continue
                    
                if decision in ['DUPLICATE', 'UNIQUE', 'UNCERTAIN']:
                    updates.append({
                        'product_id': product_id,
                        'decision': decision,
                        'notes': notes,
                        'master_id': row.get('master_product_id', '').strip()
                    })
        
        if not updates:
            print("📝 No valid review decisions found in CSV")
            return
        
        # Process updates
        with get_db_session() as db:
            updated_count = 0
            skipped_count = 0
            
            for update in updates:
                product_id = update['product_id']
                decision = update['decision']
                master_id = update['master_id']
                
                # Find the product in database
                product_status = db.query(ProductStatus).filter(
                    ProductStatus.product_id == product_id
                ).first()
                
                if not product_status:
                    print(f"⚠️  Product {product_id} not found in database")
                    skipped_count += 1
                    continue
                
                if product_status.status != 'REVIEW_SUSPECT':
                    print(f"⚠️  Product {product_id} is not REVIEW_SUSPECT (current: {product_status.status})")
                    skipped_count += 1
                    continue
                
                # Determine new status based on decision
                if decision == 'DUPLICATE':
                    new_status = 'DUPLICATE'
                    new_master_id = master_id if master_id else product_status.duplicate_master_id
                elif decision == 'UNIQUE':
                    new_status = 'UNIQUE'
                    new_master_id = None
                elif decision == 'UNCERTAIN':
                    # Keep as REVIEW_SUSPECT but could add a flag or note
                    print(f"ℹ️  Product {product_id} marked as UNCERTAIN, keeping as REVIEW_SUSPECT")
                    skipped_count += 1
                    continue
                else:
                    print(f"⚠️  Invalid decision '{decision}' for product {product_id}")
                    skipped_count += 1
                    continue
                
                if dry_run:
                    print(f"🔄 Would update {product_id}: {product_status.status} -> {new_status}")
                    if new_master_id:
                        print(f"   Master ID: {product_status.duplicate_master_id} -> {new_master_id}")
                    
                    # Check if master reassignment would be needed
                    if decision == 'DUPLICATE' and product_status.duplicate_master_id:
                        from src.duplicate_detection.master_selector import MasterSelector
                        selector = MasterSelector()
                        reassign_result = selector.reassign_master_if_better(
                            db, product_id, product_status.duplicate_master_id
                        )
                        if reassign_result['reassignment_needed']:
                            print(f"   🔄 Would also reassign master: {reassign_result['old_master_id']} -> {reassign_result['new_master_id']}")
                            print(f"   📊 Would affect {reassign_result['group_size']} products")
                        else:
                            print(f"   ℹ️  No master reassignment needed")
                else:
                    # Handle master reassignment for DUPLICATE decision
                    if decision == 'DUPLICATE' and product_status.duplicate_master_id:
                        from src.duplicate_detection.master_selector import MasterSelector
                        selector = MasterSelector()
                        reassign_result = selector.reassign_master_if_better(
                            db, product_id, product_status.duplicate_master_id
                        )
                        
                        if reassign_result['reassignment_needed']:
                            print(f"🔄 Master reassigned: {reassign_result['old_master_id']} -> {reassign_result['new_master_id']}")
                            print(f"📊 Updated {reassign_result['updated_count']} products in group")
                            updated_count += reassign_result['updated_count']
                        else:
                            # No reassignment needed, just update this product
                            product_status.status = new_status
                            product_status.duplicate_master_id = new_master_id
                            updated_count += 1
                            print(f"✅ Updated {product_id}: REVIEW_SUSPECT -> {new_status}")
                    else:
                        # Simple status update (UNIQUE or no reassignment needed)
                        product_status.status = new_status
                        product_status.duplicate_master_id = new_master_id
                        updated_count += 1
                        print(f"✅ Updated {product_id}: REVIEW_SUSPECT -> {new_status}")
            
            if not dry_run and updated_count > 0:
                db.commit()
                print(f"💾 Committed {updated_count} updates to database")
            
            print(f"\n📊 Summary:")
            print(f"   Updated: {updated_count}")
            print(f"   Skipped: {skipped_count}")
            print(f"   Total processed: {len(updates)}")
            
    except Exception as e:
        logger.error(f"Error importing reviewed suspects: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


def export_suspect_duplicates(output_file):
    """Export suspect duplicates to CSV for manual review."""
    logger.info(f"Exporting suspect duplicates to {output_file}")
    
    try:
        import csv
        from src.common.database import ProductStatus, FilteredProduct, ProductImage, ProductVideo
        
        with get_db_session() as db:
            # Query suspects with their product details and potential masters
            suspects_query = db.query(
                ProductStatus.product_id,
                ProductStatus.duplicate_master_id,
                ProductStatus.phash_difference,
                ProductStatus.clip_similarity,
                ProductStatus.total_landed_cost,
                FilteredProduct.product_title,
                FilteredProduct.target_sale_price
            ).join(
                FilteredProduct, ProductStatus.product_id == FilteredProduct.product_id
            ).filter(
                ProductStatus.status == 'REVIEW_SUSPECT'
            ).all()
            
            if not suspects_query:
                print("📝 No suspect duplicates found")
                return
            
            logger.info(f"Found {len(suspects_query)} suspect products")
            
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_file)
            if output_dir:  # Only create directory if there's a directory part
                os.makedirs(output_dir, exist_ok=True)
            
            # Prepare CSV data
            csv_data = []
            for suspect in suspects_query:
                # Get master product details if available
                master_title = ""
                master_price = ""
                
                if suspect.duplicate_master_id:
                    master_query = db.query(
                        FilteredProduct.product_title,
                        FilteredProduct.target_sale_price
                    ).filter(
                        FilteredProduct.product_id == suspect.duplicate_master_id
                    ).first()
                    
                    if master_query:
                        master_title = master_query.product_title or ""
                        master_price = master_query.target_sale_price or ""

                
                
                # Get S3 image URLs for duplicate product
                duplicate_images = db.query(ProductImage).filter(
                    ProductImage.product_id == suspect.product_id,
                    ProductImage.s3_url.isnot(None)
                ).order_by(ProductImage.sort_index).all()
                
                # Get S3 image URLs for master product
                master_images = []
                if suspect.duplicate_master_id:
                    master_images = db.query(ProductImage).filter(
                        ProductImage.product_id == suspect.duplicate_master_id,
                        ProductImage.s3_url.isnot(None)
                    ).order_by(ProductImage.sort_index).all()
                
                # Get S3 video URLs for duplicate product
                duplicate_videos = db.query(ProductVideo).filter(
                    ProductVideo.product_id == suspect.product_id,
                    ProductVideo.s3_url.isnot(None)
                ).all()
                
                # Get S3 video URLs for master product
                master_videos = []
                if suspect.duplicate_master_id:
                    master_videos = db.query(ProductVideo).filter(
                        ProductVideo.product_id == suspect.duplicate_master_id,
                        ProductVideo.s3_url.isnot(None)
                    ).all()
                
                # Find main images (hero or primary)
                duplicate_main_image = ""
                master_main_image = ""
                
                # Get main image for duplicate (prefer is_primary=True, then hero role, then first image)
                for img in duplicate_images:
                    if img.is_primary or img.image_role == 'hero':
                        duplicate_main_image = img.s3_url
                        break
                if not duplicate_main_image and duplicate_images:
                    duplicate_main_image = duplicate_images[0].s3_url
                
                # Get main image for master
                for img in master_images:
                    if img.is_primary or img.image_role == 'hero':
                        master_main_image = img.s3_url
                        break
                if not master_main_image and master_images:
                    master_main_image = master_images[0].s3_url
                
                # Find the closest matching images (highest CLIP similarity)
                # For now, we'll use the main images as the closest match since we have CLIP similarity at product level
                master_image = master_main_image  # This is the closest match from master
                duplicate_image = duplicate_main_image  # This is the closest match from duplicate
                
                # Add CLIP similarity info to indicate this is the closest match
                if suspect.clip_similarity:
                    master_image = f"{master_main_image} (CLIP: {suspect.clip_similarity:.4f})" if master_main_image else f"No image (CLIP: {suspect.clip_similarity:.4f})"
                    duplicate_image = f"{duplicate_main_image} (CLIP: {suspect.clip_similarity:.4f})" if duplicate_main_image else f"No image (CLIP: {suspect.clip_similarity:.4f})"
                
                # Get all other images (excluding the main ones)
                master_other_images = [img.s3_url for img in master_images if img.s3_url != master_main_image]
                duplicate_other_images = [img.s3_url for img in duplicate_images if img.s3_url != duplicate_main_image]
                
                # Format image lists as pipe-separated
                master_images_list = " | ".join(master_other_images)
                duplicate_images_list = " | ".join(duplicate_other_images)
                
                # Get video URLs (take first video if available)
                duplicate_video = duplicate_videos[0].s3_url if duplicate_videos else ""
                master_video = master_videos[0].s3_url if master_videos else ""
                
                csv_data.append({
                    'master_product_id': suspect.duplicate_master_id or "",
                    'duplicate_product_id': suspect.product_id,
                    'master_title': master_title,
                    'duplicate_title': suspect.product_title or "",
                    'master_price': master_price,
                    'duplicate_price': suspect.target_sale_price or "",
                    'duplicate_cost': suspect.total_landed_cost or "",
                    'master_image': master_image,
                    'duplicate_image': duplicate_image,
                    'master_main_image': master_main_image,
                    'duplicate_main_image': duplicate_main_image,
                    'master_images': master_images_list,
                    'duplicate_images': duplicate_images_list,
                    'master_video': master_video,
                    'duplicate_video': duplicate_video,
                    'phash_difference': suspect.phash_difference or "",
                    'clip_similarity': f"{suspect.clip_similarity:.4f}" if suspect.clip_similarity else "",
                    'status': "",  # Empty column for review decisions (DUPLICATE, UNIQUE, UNCERTAIN)
                    'notes': ""  # Empty column for manual input
                })
            
            # Write to CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'master_product_id', 'duplicate_product_id', 'master_title', 'duplicate_title',
                    'master_price', 'duplicate_price', 'duplicate_cost',
                    'master_image', 'duplicate_image', 'master_main_image', 'duplicate_main_image',
                    'master_images', 'duplicate_images', 'master_video', 'duplicate_video',
                    'phash_difference', 'clip_similarity', 'status', 'notes'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(csv_data)
            
            print(f"✅ Exported {len(csv_data)} suspect cases to {output_file}")
            print(f"� Review workflow:")
            print(f"   1. Open {output_file} in Excel or similar")
            print(f"   2. Compare suspect vs master products")
            print(f"   3. Fill 'status' column with: DUPLICATE, UNIQUE, or UNCERTAIN")
            print(f"   4. Add explanatory notes in 'notes' column")
            print(f"   5. Import decisions with: python main.py detect:import-reviewed --input {output_file}")
            
    except Exception as e:
        logger.error(f"Error exporting suspect duplicates: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


def main():
    """Main entry point for the command-line interface."""
    parser = argparse.ArgumentParser(
        description="Product Data Pipeline - A modular system for processing product data."
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Harvest commands
    harvest_init_parser = subparsers.add_parser(
        "harvest:init", help="Initialize merchant harvest"
    )
    harvest_init_parser.add_argument(
        "--limit", type=int, help="Limit the number of entries to process"
    )
    harvest_init_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate without writing data"
    )
    # No --use-keywords argument needed; always uses categories if present, else keywords

    harvest_delta_parser = subparsers.add_parser(
        "harvest:delta", help="Run incremental merchant harvest"
    )
    harvest_delta_parser.add_argument(
        "--limit", type=int, help="Limit the number of entries to process"
    )
    harvest_delta_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate without writing data"
    )

    subparsers.add_parser("harvest:status", help="Show harvest job status")

    # Filter commands
    filter_parser = subparsers.add_parser(
        "filter:products", help="Filter products from whitelisted sellers based on business rules"
    )
    filter_parser.add_argument(
        "--limit", type=int, help="Limit the number of products to process"
    )
    filter_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate without writing data"
    )

    # Review commands
    review_export_parser = subparsers.add_parser(
        "review:export-pending", help="Export pending merchants for review"
    )
    review_export_parser.add_argument(
        "--output",
        type=str,
        default="data/pending_merchants.csv",
        help="Output CSV file path",
    )

    review_import_parser = subparsers.add_parser(
        "review:import-results", help="Import merchant review results"
    )
    review_import_parser.add_argument(
        "--input",
        type=str,
        default="data/reviewed_merchants.csv",
        help="Input CSV file path",
    )
    review_import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate import without updating the database",
    )

    # Session management commands
    session_create_parser = subparsers.add_parser(
        "create_session", help="Create a new AliExpress API session"
    )
    session_create_parser.add_argument(
        "--code", 
        type=str, 
        required=True,
        help="Authorization code from AliExpress"
    )

    session_refresh_parser = subparsers.add_parser(
        "refresh_session", help="Refresh session token (will use database session if available, otherwise requires tokens)"
    )
    session_refresh_parser.add_argument(
        "--token",
        type=str,
        help="Current access token (session parameter for API)"
    )
    session_refresh_parser.add_argument(
        "--refresh-token",
        type=str,
        help="Refresh token to use"
    )

    subparsers.add_parser("list_sessions", help="List all sessions")
    
    subparsers.add_parser("get_code_link", help="Get AliExpress OAuth authorization URL")
    
    subparsers.add_parser("unlock_database", help="Force unlock database if it's stuck")

    # Duplicate detection commands (Module C)
    detect_duplicates_parser = subparsers.add_parser(
        "detect:duplicates", help="Run duplicate detection and master selection"
    )
    detect_duplicates_parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of products to analyze (for testing)"
    )
    detect_duplicates_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate detection without updating the database"
    )
    detect_duplicates_parser.add_argument(
        "--phash-only",
        action="store_true",
        help="Use only pHash analysis, skip CLIP"
    )

    detect_status_parser = subparsers.add_parser(
        "detect:status", help="Show duplicate detection status and statistics"
    )

    detect_export_parser = subparsers.add_parser(
        "detect:export-suspects", help="Export REVIEW_SUSPECT cases for manual review"
    )
    detect_export_parser.add_argument(
        "--output",
        type=str,
        default="data/suspect_duplicates.csv",
        help="Output CSV file path"
    )

    detect_import_parser = subparsers.add_parser(
        "detect:import-reviewed", help="Import reviewed suspect duplicates and update database"
    )
    detect_import_parser.add_argument(
        "--input",
        type=str,
        default="data/suspect_duplicates.csv",
        help="Input CSV file path with review decisions"
    )
    detect_import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes"
    )

    # Airtable synchronization commands (Module D)
    airtable_sync_parser = subparsers.add_parser(
        "airtable:sync", help="Sync products and variants to Airtable"
    )
    airtable_sync_parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of products to sync (for testing)"
    )
    airtable_sync_parser.add_argument(
        "--filter",
        type=str,
        choices=["MASTER", "UNIQUE"],
        help="Filter products by status (MASTER or UNIQUE only)"
    )
    airtable_sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without actually syncing to Airtable"
    )

    # Airtable base creation command
    airtable_create_parser = subparsers.add_parser(
        "airtable:create-base", help="Create Airtable base with Products and Variants tables"
    )
    airtable_create_parser.add_argument(
        "--name",
        type=str,
        default="Product Pipeline",
        help="Name for the new base (default: Product Pipeline)"
    )
    airtable_create_parser.add_argument(
        "--workspace-id",
        type=str,
        help="Workspace ID to create base in (optional, uses personal workspace if not provided)"
    )
    airtable_create_parser.add_argument(
        "--list-workspaces",
        action="store_true",
        help="List available workspaces instead of creating base"
    )
    airtable_create_parser.add_argument(
        "--test-token",
        action="store_true",
        help="Test Personal Access Token configuration"
    )

    # Parse arguments
    args = parser.parse_args()

    # Execute command
    if args.command == "harvest:init":
        init_harvest(limit=args.limit, dry_run=args.dry_run)
    elif args.command == "harvest:delta":
        delta_harvest(limit=args.limit, dry_run=args.dry_run)
    elif args.command == "harvest:status":
        harvest_status()
    elif args.command == "filter:products":
        filter_products(
            limit=args.limit,
            dry_run=args.dry_run
        )
    elif args.command == "review:export-pending":
        export_pending_merchants(output_file=args.output)
    elif args.command == "review:import-results":
        import_review_results(input_file=args.input, dry_run=args.dry_run)
    elif args.command == "create_session":
        result = create_session(args.code)
        if result['success']:
            print(f"✅ Session created successfully!")
            print(f"Code: {result['code']}")
            print(f"Token: {result['token'][:20]}...")
            print(f"Session ID: {result['session_id']}")
            print(f"\n📋 Full API Response:")
            import json
            print(json.dumps(result['response'], indent=2))
        else:
            print(f"❌ Failed to create session: {result['message']}")
            if result.get('oauth_url'):
                print(f"\n🔗 Get authorization code from: {result['oauth_url']}")
            sys.exit(1)
    elif args.command == "refresh_session":
        result = auto_refresh_session(
            access_token=getattr(args, 'token', None),
            refresh_token=getattr(args, 'refresh_token', None)
        )
        if result['success']:
            print(f"✅ Token refreshed successfully!")
            print(f"Code: {result['code']}")
            print(f"New Token: {result['token'][:20]}...")
            print(f"\n📋 Full API Response:")
            import json
            print(json.dumps(result['response'], indent=2))
        else:
            if result.get('needs_tokens'):
                print(f"❌ {result['message']}")
                if result.get('suggest_create'):
                    print(f"\n💡 No session found in database. You have two options:")
                    print(f"   1. Provide tokens: python main.py refresh_session --token YOUR_ACCESS_TOKEN --refresh-token YOUR_REFRESH_TOKEN")
                    print(f"   2. Create new session: python main.py create_session --code YOUR_CODE")
                    if result.get('oauth_url'):
                        print(f"\n🔗 Get authorization code from: {result['oauth_url']}")
                else:
                    print(f"\n💡 Usage: python main.py refresh_session --token YOUR_ACCESS_TOKEN --refresh-token YOUR_REFRESH_TOKEN")
            elif result.get('suggest_create'):
                print(f"❌ {result['message']}")
                if result.get('oauth_url'):
                    print(f"\n🔗 Get authorization code from: {result['oauth_url']}")
            else:
                print(f"❌ Failed to refresh token: {result['message']}")
            sys.exit(1)
    elif args.command == "list_sessions":
        sessions = list_sessions()
        if sessions:
            print(f"📋 Found {len(sessions)} session(s):")
            for session in sessions:
                status = "🟢 Active" if session['is_active'] else "🔴 Inactive"
                print(f"\n- Code: {session['code']}")
                print(f"  Status: {status}")
                print(f"  Type: {session['token_type']}")
                print(f"  User: {session['user_nick'] or 'N/A'}")
                print(f"  Account: {session['account'] or 'N/A'}")
                print(f"  Created: {session['created_at']}")
                print(f"  Updated: {session['updated_at']}")
        else:
            print("No sessions found.")
    
    elif args.command == "get_code_link":
        auth_url = get_oauth_authorization_url()
        print("🔗 AliExpress OAuth Authorization URL:")
        print(f"{auth_url}")
        print("\n📋 Instructions:")
        print("1. Open the URL above in your browser")
        print("2. Log in to your AliExpress account")
        print("3. Authorize the application")
        print("4. Copy the authorization code from the callback URL")
        print("5. Use the code with: python main.py create_session --code YOUR_CODE")
    
    elif args.command == "unlock_database":
        print("🔓 Attempting to force unlock database...")
        success = force_unlock_database()
        if success:
            print("✅ Database unlocked successfully!")
        else:
            print("❌ Could not unlock database. You may need to:")
            print("   1. Check if another process is using the database")
            print("   2. Restart the application")
            print("   3. Manually delete and recreate the database file")
    
    elif args.command == "detect:duplicates":
        detect_duplicates(
            limit=args.limit,
            dry_run=args.dry_run,
            phash_only=args.phash_only
        )
    elif args.command == "detect:status":
        detect_status()
    elif args.command == "detect:export-suspects":
        export_suspect_duplicates(output_file=args.output)
    elif args.command == "detect:import-reviewed":
        import_reviewed_suspects(input_file=args.input, dry_run=args.dry_run)
    elif args.command == "airtable:sync":
        result = sync_to_airtable(
            limit=args.limit,
            filter_status=args.filter,
            dry_run=args.dry_run
        )
        print(f"✅ Airtable sync completed!")
        print(f"📊 Products: {result['products']['created']} created, {result['products']['updated']} updated")
        print(f"📦 Variants: {result['variants']['created']} created, {result['variants']['updated']} updated") 
        print(f"📈 Total: {result['total_created']} created, {result['total_updated']} updated")
    elif args.command == "airtable:create-base":
        create_base_command(
            base_name=args.name,
            workspace_id=args.workspace_id,
            list_workspaces=args.list_workspaces,
            test_token=args.test_token
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
