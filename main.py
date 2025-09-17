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
from src.session.session_manager import create_session, refresh_session_token, auto_refresh_session, list_sessions
from src.filter.product_filter import run_product_filtering
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
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
