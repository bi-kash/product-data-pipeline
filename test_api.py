#!/usr/bin/env python3
"""
Comprehensive test script for the AliExpress RapidAPI client.
This script tests all the functionality needed for Module 1.
"""

import os
import json
from dotenv import load_dotenv
from src.common.aliexpress_client import AliExpressClient
from src.common.database import (
    create_tables_if_not_exist,
    get_seller_approval_counts,
    get_recent_job_runs,
)
from src.harvester.merchant_harvester import init_harvest
from src.review.merchant_review import export_pending_merchants, import_review_results
from src.common.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Configure logging
logger = setup_logging('test_api')


def test_api_search():
    """Test the RapidAPI search endpoint."""
    print("\n=== Testing RapidAPI Product Search ===")

    client = AliExpressClient()
    print(f"Using RapidAPI host: {client.rapidapi_host}")
    print(f"Target currency: {client.target_currency}")
    print(f"Target country: {client.target_country}")

    keywords = ["jewelry", "necklace"]
    for keyword in keywords:
        print(f"\nSearching for '{keyword}'...")
        products = client.search_products(keyword, page_no=1, page_size=3)

        if not products:
            print(f"⚠️ No products found for '{keyword}'")
            continue

        print(f"✓ Found {len(products)} products")

        # Show sample data
        for i, product in enumerate(products[:2], 1):
            print(f"\nProduct {i}:")
            print(f"- ID: {product.get('product_id')}")
            print(
                f"- Price: {product.get('target_sale_price')} {product.get('target_sale_price_currency', '')}"
            )
            print(f"- Shop: {product.get('shop_name')} (ID: {product.get('shop_id')})")

    return True


def test_merchant_extraction():
    """Test merchant information extraction."""
    print("\n=== Testing Merchant Extraction ===")

    client = AliExpressClient()
    products = client.search_products("jewelry", page_no=1, page_size=2)

    if not products:
        print("⚠️ No products to extract merchants from")
        return False

    for i, product in enumerate(products[:2], 1):
        seller_info = client.get_seller_info_from_product(product)

        if not seller_info:
            print(f"⚠️ Could not extract seller from product {i}")
            continue

        print(f"\nSeller {i}:")
        print(f"- ID: {seller_info['shop_id']}")
        print(f"- Shop URL: {seller_info['shop_url']}")
        print(f"- Shop Name: {seller_info.get('shop_name', 'N/A')}")

    return True


def test_database():
    """Test database tables creation."""
    print("\n=== Testing Database Creation ===")

    try:
        create_tables_if_not_exist()
        print("✓ Database tables created successfully")

        # Check seller counts
        counts = get_seller_approval_counts()
        print("\nCurrent seller counts:")
        print(f"- PENDING: {counts['PENDING']}")
        print(f"- WHITELIST: {counts['WHITELIST']}")
        print(f"- BLACKLIST: {counts['BLACKLIST']}")
        print(f"- TOTAL: {counts['TOTAL']}")

        # Check job runs
        job_runs = get_recent_job_runs(limit=3)
        print("\nRecent job runs:")
        if job_runs:
            for job in job_runs:
                print(
                    f"- {job['job_type']} (ID: {job['id']}) - Found: {job['found_count']}, New: {job['new_count']}"
                )
        else:
            print("No job runs found")

        return True
    except Exception as e:
        print(f"⚠️ Error testing database: {e}")
        return False


def test_mini_harvest():
    """Test a mini harvest with small limit."""
    print("\n=== Testing Mini Harvest ===")

    try:
        # Run with a very small limit
        init_harvest(limit=2, dry_run=True)
        print("✓ Mini harvest completed successfully")
        return True
    except Exception as e:
        print(f"⚠️ Error running mini harvest: {e}")
        return False


def test_review_export():
    """Test review export functionality."""
    print("\n=== Testing Review Export ===")

    try:
        # Export to a test file
        test_output = "data/test_pending_merchants.csv"
        export_pending_merchants(output_file=test_output)

        # Verify file exists and has content
        if not os.path.exists(test_output):
            print(f"⚠️ Export file not created: {test_output}")
            return False

        with open(test_output, "r") as f:
            content = f.read()
            print(
                f"✓ Export file created with {len(content.splitlines()) - 1} merchants"
            )

        return True
    except Exception as e:
        print(f"⚠️ Error testing review export: {e}")
        return False


def main():
    """Run all tests"""
    print("\nRapidAPI AliExpress Client Test Suite")
    print("=====================================")

    # Run tests
    tests = {
        "API Search": test_api_search,
        "Merchant Extraction": test_merchant_extraction,
        "Database": test_database,
        "Mini Harvest": test_mini_harvest,
        "Review Export": test_review_export,
    }

    results = {}
    for name, test_func in tests.items():
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"⚠️ Exception in {name}: {e}")
            results[name] = False

    # Show summary
    print("\nTest Results Summary")
    print("===================")

    all_passed = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "⚠️ FAIL"
        print(f"{name}: {status}")
        all_passed = all_passed and passed

    print(
        f"\nOverall: {'✓ All tests passed!' if all_passed else '⚠️ Some tests failed'}"
    )

    return 0 if all_passed else 1


if __name__ == "__main__":
    main()
