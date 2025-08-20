"""
Merchant review module for exporting and importing merchant reviews.
"""

import csv
import os
from datetime import datetime
from src.common.database import get_pending_sellers, update_seller_approval
from src.common.logging_config import setup_logging

# Configure logging
logger = setup_logging("merchant_review")


def export_pending_merchants(output_file="data/pending_merchants.csv"):
    """
    Export pending merchants for review.

    Args:
        output_file: Path to output CSV file
    """
    # Get pending sellers
    pending_sellers = get_pending_sellers()

    # Create directories if they don't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Export to CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow(["shop_id", "shop_url", "shop_name", "approval_status", "note"])

        # Write sellers
        for seller in pending_sellers:
            writer.writerow(
                [
                    seller["shop_id"],
                    seller["shop_url"],
                    seller.get("shop_name", ""),
                    seller["approval_status"],
                    seller["note"] or "",
                ]
            )

    logger.info(f"Exported {len(pending_sellers)} pending merchants to {output_file}")
    print(f"Exported {len(pending_sellers)} pending merchants to {output_file}")


def import_review_results(input_file, dry_run=False):
    """
    Import merchant review results.

    Args:
        input_file: Path to input CSV file
        dry_run: If True, don't write to database
    """
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        print(f"Error: Input file not found: {input_file}")
        return

    # Stats
    stats = {
        "total": 0,
        "updated": 0,
        "errors": 0,
        "skipped": 0,
        "whitelisted": 0,
        "blacklisted": 0,
        "unchanged": 0,
        "invalid": 0,
    }

    # Import from CSV
    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Validate header
        required_fields = ["shop_id", "shop_url", "approval_status", "note"]
        header = reader.fieldnames

        missing_fields = [field for field in required_fields if field not in header]
        if missing_fields:
            logger.error(
                f"CSV file is missing required fields: {', '.join(missing_fields)}"
            )
            print(
                f"Error: CSV file is missing required fields: {', '.join(missing_fields)}"
            )
            return

        # Process rows
        for row in reader:
            stats["total"] += 1

            shop_id = row["shop_id"].strip()
            approval_status = row["approval_status"].strip().upper()
            note = row["note"].strip() if row["note"] else None

            # Normalize approval status to handle different formats
            if approval_status == "WHITE_LIST" or approval_status == "WHITELIST":
                approval_status = "WHITELIST"
            elif approval_status == "BLACK_LIST" or approval_status == "BLACKLIST":
                approval_status = "BLACKLIST"

            # Validate approval status
            valid_statuses = ["PENDING", "WHITELIST", "BLACKLIST"]
            if approval_status not in valid_statuses:
                logger.warning(
                    f"Invalid approval status '{approval_status}' for seller {shop_id}"
                )
                stats["invalid"] += 1
                continue

            # Skip if still PENDING
            if approval_status == "PENDING":
                stats["skipped"] += 1
                logger.debug(f"Skipping seller {shop_id} - status is still PENDING")
                continue

            # Update in database
            if not dry_run:
                try:
                    if update_seller_approval(
                        shop_id=shop_id, approval_status=approval_status, note=note
                    ):
                        stats["updated"] += 1

                        # Count by status type
                        if approval_status == "WHITELIST":
                            stats["whitelisted"] += 1
                        elif approval_status == "BLACKLIST":
                            stats["blacklisted"] += 1
                    else:
                        stats["errors"] += 1
                        logger.warning(f"Seller {shop_id} not found in database")
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error updating seller {shop_id}: {e}")
            else:
                stats["updated"] += 1
                logger.info(
                    f"[DRY RUN] Would update seller {shop_id} to {approval_status}"
                )

    # Log summary
    logger.info(f"Import complete. Summary:")
    logger.info(f"- Total processed: {stats['total']}")
    logger.info(f"- Updated: {stats['updated']}")
    logger.info(f"- Whitelisted: {stats['whitelisted']}")
    logger.info(f"- Blacklisted: {stats['blacklisted']}")
    logger.info(f"- Skipped (still pending): {stats['skipped']}")
    logger.info(f"- Invalid status: {stats['invalid']}")
    logger.info(f"- Unchanged: {stats['unchanged']}")
    logger.info(f"- Errors: {stats['errors']}")

    # Print summary
    print(f"\nImport {'simulation ' if dry_run else ''}complete:")
    print(f"- Total processed: {stats['total']}")
    print(f"- Updated: {stats['updated']}")
    print(f"- Whitelisted: {stats['whitelisted']}")
    print(f"- Blacklisted: {stats['blacklisted']}")
    print(f"- Skipped (still pending): {stats['skipped']}")
    print(f"- Invalid status: {stats['invalid']}")
    print(f"- Unchanged: {stats['unchanged']}")
    print(f"- Errors: {stats['errors']}")
