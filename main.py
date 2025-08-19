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
from src.common.logging_config import setup_logging

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Set up main logger
logger = setup_logging('main')


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

    # Parse arguments
    args = parser.parse_args()

    # Execute command
    if args.command == "harvest:init":
        init_harvest(
            limit=args.limit, dry_run=args.dry_run
        )
    elif args.command == "harvest:delta":
        delta_harvest(
            limit=args.limit, dry_run=args.dry_run
        )
    elif args.command == "harvest:status":
        harvest_status()
    elif args.command == "review:export-pending":
        export_pending_merchants(output_file=args.output)
    elif args.command == "review:import-results":
        import_review_results(input_file=args.input, dry_run=args.dry_run)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
