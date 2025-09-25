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
from src.session.session_manager import create_session, refresh_session_token, auto_refresh_session, list_sessions, force_unlock_database
from src.filter.product_filter import run_product_filtering
from src.duplicate_detection.duplicate_detector import DuplicateDetector
from src.common.database import get_db_session
from src.common.logging_config import setup_logging

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


def filter_products(max_price_eur=None, max_delivery_days=None, limit=None, dry_run=False):
    """Filter products from whitelisted sellers based on business rules."""
    run_product_filtering(
        max_price_eur=max_price_eur,
        max_delivery_days=max_delivery_days,
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


def export_suspect_duplicates(output_file):
    """Export REVIEW_SUSPECT cases for manual review."""
    logger.info(f"Exporting suspect duplicates to {output_file}")
    
    try:
        from src.common.database import ProductStatus
        
        with get_db_session() as db:
            suspects = db.query(ProductStatus).filter(
                ProductStatus.status == 'REVIEW_SUSPECT'
            ).all()
            
            if not suspects:
                print("📝 No suspect duplicates found")
                return
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Export to CSV (implementation would go here)
            print(f"📄 Would export {len(suspects)} suspect cases to {output_file}")
            print("💡 This feature is ready for implementation when REVIEW_SUSPECT logic is added")
            
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
        "--max-price", type=float, help="Maximum total price in EUR (variant + shipping)"
    )
    filter_parser.add_argument(
        "--max-delivery", type=int, help="Maximum delivery time in days"
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
            max_price_eur=args.max_price,
            max_delivery_days=args.max_delivery,
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
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
