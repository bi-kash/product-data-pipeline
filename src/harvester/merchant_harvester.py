#!/usr/bin/env python3
"""
Merchant harvester module for collecting merchant IDs via product search.
Uses RapidAPI client for reliable AliExpress data access.
"""

import json
from tqdm import tqdm
from datetime import datetime
from src.common.database import (
    create_tables_if_not_exist,
    start_job_run,
    complete_job_run,
    upsert_seller,
    upsert_product,
    get_seller_approval_counts,
    get_recent_job_runs,
    associate_product_category,
    get_product_categories_stats,
)
from src.common.config import (
    get_search_keywords,
    get_search_categories,
    create_example_env_file,
    get_api_page_size,
)
from src.common.aliexpress_client import AliExpressClient
from src.common.logging_config import setup_logging

# Configure logging
logger = setup_logging('merchant_harvester')


def _harvest_merchants(keywords=None, categories=None, limit=None, dry_run=False, job_type="HARVEST"):
    """
    Harvest merchants from AliExpress using provided keywords or categories via RapidAPI.

    Args:
        keywords: List of keywords to search for
        categories: List of (category_id, category_name) tuples to search for
        limit: Maximum number of products to process (None for unlimited)
        dry_run: If True, don't write to database
        job_type: Type of harvest job (HARVEST_INIT or HARVEST_DELTA)

    Returns:
        Dict with harvest statistics
    """
    # Ensure we have at least one of keywords or categories
    if not keywords and not categories:
        raise ValueError("Must provide either keywords or categories for search")

    client = AliExpressClient()
    job_id = None if dry_run else start_job_run(job_type)

    stats = {
        "total_products_processed": 0,
        "unique_sellers_found": 0,
        "new_sellers_added": 0,
        "sellers_updated": 0,
        "errors": 0,
    }

    unique_sellers = set()

    try:
        # Process keywords if provided
        if keywords:
            for keyword in keywords:
                logger.info(f"Searching for keyword: {keyword}")
                page = 1
                page_size = get_api_page_size()  # Get page size from config

                while True:
                    logger.info(f"Processing page {page} for keyword '{keyword}'")
                    result = client.search_products(
                        keyword, page_no=page, page_size=page_size
                    )
                    products = result[0]
                    total_count = result[1]

                    if not products:
                        logger.info(f"No more results for keyword '{keyword}'")
                        break
                        
                    # Log progress compared to total
                    processed_so_far = stats["total_products_processed"]
                    logger.info(f"Processing page {page} - found {len(products)} products (progress: {processed_so_far}/{total_count} - {processed_so_far/total_count:.2%})")

                    for position, product in enumerate(
                        tqdm(products, desc=f"Processing {keyword} (page {page})")
                    ):
                        stats["total_products_processed"] += 1

                        seller_info = client.get_seller_info_from_product(product)
                        if not seller_info:
                            stats["errors"] += 1
                            continue

                        shop_id = seller_info["shop_id"]
                        product_id = str(product.get("product_id", ""))

                        if not product_id:
                            stats["errors"] += 1
                            continue

                        # Skip if this seller has already been processed in this run
                        if shop_id in unique_sellers:
                            # Even if we skip this seller, we should still track that this
                            # product was found via this keyword search
                            if not dry_run:
                                try:
                                    associate_product_category(
                                        product_id=product_id,
                                        category_id=keyword,
                                        category_name=keyword,
                                        search_page=page,
                                        position_in_results=position + 1,
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Error associating product {product_id} with keyword {keyword}: {e}"
                                    )
                                    stats["errors"] += 1
                            continue

                        unique_sellers.add(shop_id)
                        stats["unique_sellers_found"] += 1

                        # Insert or update the seller in the database
                        if not dry_run:
                            try:
                                # First, upsert the seller
                                is_new_seller = upsert_seller(
                                    shop_id=shop_id,
                                    shop_url=seller_info["shop_url"],
                                    shop_name=seller_info.get("shop_name"),
                                    raw_json=seller_info["raw_json"],
                                )

                                if is_new_seller:
                                    stats["new_sellers_added"] += 1
                                else:
                                    stats["sellers_updated"] += 1

                                # Now, upsert the product
                                product_title = product.get("product_title", "")
                                product_detail_url = product.get(
                                    "product_detail_url", ""
                                )
                                product_main_image_url = product.get(
                                    "product_main_image_url", ""
                                )

                                # Handle prices
                                original_price = None
                                target_sale_price = None
                                original_price_currency = None
                                target_sale_price_currency = None

                                if (
                                    "original_price" in product
                                    and product["original_price"]
                                ):
                                    try:
                                        original_price = float(
                                            product["original_price"]
                                        )
                                        original_price_currency = product.get(
                                            "original_price_currency"
                                        )
                                    except (ValueError, TypeError):
                                        pass

                                if (
                                    "target_sale_price" in product
                                    and product["target_sale_price"]
                                ):
                                    try:
                                        target_sale_price = float(
                                            product["target_sale_price"]
                                        )
                                        target_sale_price_currency = product.get(
                                            "target_sale_price_currency"
                                        )
                                    except (ValueError, TypeError):
                                        pass

                                upsert_product(
                                    product_id=product_id,
                                    shop_id=shop_id,
                                    product_title=product_title,
                                    product_detail_url=product_detail_url,
                                    product_main_image_url=product_main_image_url,
                                    original_price=original_price,
                                    target_sale_price=target_sale_price,
                                    original_price_currency=original_price_currency,
                                    target_sale_price_currency=target_sale_price_currency,
                                    discount=product.get("discount"),
                                    evaluate_rate=product.get("evaluate_rate"),
                                    first_level_category_name=product.get(
                                        "first_level_category_name"
                                    ),
                                    second_level_category_name=product.get(
                                        "second_level_category_name"
                                    ),
                                    raw_json=product,
                                )

                                # Associate product with keyword as category
                                associate_product_category(
                                    product_id=product_id,
                                    category_id=keyword,
                                    category_name=keyword,
                                    search_page=page,
                                    position_in_results=position + 1,
                                )

                            except Exception as e:
                                logger.error(
                                    f"Error upserting seller/product {shop_id}: {e}"
                                )
                                stats["errors"] += 1

                        # Stop if we've reached the limit
                        if limit and stats["total_products_processed"] >= limit:
                            logger.info(
                                f"Reached limit of {limit} products. Stopping harvest."
                            )
                            return stats

                    # Always continue to the next page as long as we got products
                    # Only stop if we get no products or if we've reached the maximum pages we want to process
                    # (We could add a MAX_PAGES limit here if needed)
                    if not products:
                        logger.info(f"No products returned for keyword '{keyword}', ending pagination")
                        break

                    page += 1

        # Process categories if provided
        if categories:
            # Extract all category IDs for batch search
            all_category_ids = [cat_id for cat_id, _ in categories]
            category_id_to_name = {
                str(cat_id): cat_name for cat_id, cat_name in categories
            }

            # Create a string representation for logging
            category_names_str = ", ".join(
                [f"{name} ({id})" for id, name in categories]
            )
            logger.info(f"Searching for all categories at once: {category_names_str}")

            page = 1
            page_size = get_api_page_size()  # Get page size from config

            while True:
                logger.info(f"Processing page {page} for all categories")
                result = client.search_products_by_category(
                    all_category_ids, page_no=page, page_size=page_size
                )
                products = result[0]
                total_count = result[1]

                if not products:
                    logger.info("No more results for the categories")
                    break
                    
                # Log progress compared to total
                processed_so_far = stats["total_products_processed"]
                logger.info(f"Processing page {page} - found {len(products)} products (progress: {processed_so_far}/{total_count} - {processed_so_far/total_count:.4%})")

                for position, product in enumerate(
                    tqdm(products, desc=f"Processing all categories (page {page})")
                ):
                    stats["total_products_processed"] += 1

                    seller_info = client.get_seller_info_from_product(product)
                    if not seller_info:
                        stats["errors"] += 1
                        continue

                    shop_id = seller_info["shop_id"]
                    product_id = str(product.get("product_id", ""))

                    if not product_id:
                        stats["errors"] += 1
                        continue

                    # Determine which category this product belongs to based on the first_level_category_id or second_level_category_id
                    first_level_cat_id = product.get("first_level_category_id")
                    second_level_cat_id = product.get("second_level_category_id")

                    # Try to match the product to our category list
                    matching_cat_id = None
                    if (
                        first_level_cat_id
                        and str(first_level_cat_id) in category_id_to_name
                    ):
                        matching_cat_id = first_level_cat_id
                    elif (
                        second_level_cat_id
                        and str(second_level_cat_id) in category_id_to_name
                    ):
                        matching_cat_id = second_level_cat_id
                    else:
                        # Default to the first category in our list as a fallback
                        matching_cat_id = all_category_ids[0]

                    matching_cat_name = category_id_to_name.get(
                        str(matching_cat_id), "Unknown"
                    )

                    # Skip if this seller has already been processed in this run
                    if shop_id in unique_sellers:
                        # Even if we skip this seller, we should still track that this
                        # product was found via this category search
                        if not dry_run:
                            try:
                                associate_product_category(
                                    product_id=product_id,
                                    category_id=matching_cat_id,
                                    category_name=matching_cat_name,
                                    search_page=page,
                                    position_in_results=position + 1,
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error associating product {product_id} with category {matching_cat_id}: {e}"
                                )
                                stats["errors"] += 1
                        continue

                    unique_sellers.add(shop_id)
                    stats["unique_sellers_found"] += 1
                    if not dry_run:
                        try:
                            # First, upsert the seller
                            is_new_seller = upsert_seller(
                                shop_id=shop_id,
                                shop_url=seller_info["shop_url"],
                                shop_name=seller_info.get("shop_name"),
                                raw_json=seller_info["raw_json"],
                            )

                            if is_new_seller:
                                stats["new_sellers_added"] += 1
                            else:
                                stats["sellers_updated"] += 1

                            # Now, upsert the product
                            product_title = product.get("product_title", "")
                            product_detail_url = product.get("product_detail_url", "")
                            product_main_image_url = product.get(
                                "product_main_image_url", ""
                            )

                            # Handle prices
                            original_price = None
                            target_sale_price = None
                            original_price_currency = None
                            target_sale_price_currency = None

                            if (
                                "original_price" in product
                                and product["original_price"]
                            ):
                                try:
                                    original_price = float(product["original_price"])
                                    original_price_currency = product.get(
                                        "original_price_currency"
                                    )
                                except (ValueError, TypeError):
                                    pass

                            if (
                                "target_sale_price" in product
                                and product["target_sale_price"]
                            ):
                                try:
                                    target_sale_price = float(
                                        product["target_sale_price"]
                                    )
                                    target_sale_price_currency = product.get(
                                        "target_sale_price_currency"
                                    )
                                except (ValueError, TypeError):
                                    pass

                            upsert_product(
                                product_id=product_id,
                                shop_id=shop_id,
                                product_title=product_title,
                                product_detail_url=product_detail_url,
                                product_main_image_url=product_main_image_url,
                                original_price=original_price,
                                target_sale_price=target_sale_price,
                                original_price_currency=original_price_currency,
                                target_sale_price_currency=target_sale_price_currency,
                                discount=product.get("discount"),
                                evaluate_rate=product.get("evaluate_rate"),
                                first_level_category_name=product.get(
                                    "first_level_category_name"
                                ),
                                second_level_category_name=product.get(
                                    "second_level_category_name"
                                ),
                                raw_json=product,
                            )

                            # Associate product with category
                            associate_product_category(
                                product_id=product_id,
                                category_id=matching_cat_id,
                                category_name=matching_cat_name,
                                search_page=page,
                                position_in_results=position + 1,
                            )

                        except Exception as e:
                            logger.error(
                                f"Error upserting seller/product {shop_id}: {e}"
                            )
                            stats["errors"] += 1

                    # Stop if we've reached the limit
                    if limit and stats["total_products_processed"] >= limit:
                        logger.info(
                            f"Reached limit of {limit} products. Stopping harvest."
                        )
                        return stats

                # Always continue to the next page as long as we got products
                # Only stop if we get no products or if we've reached the maximum pages we want to process
                # (We could add a MAX_PAGES limit here if needed)
                if not products:
                    logger.info("No products returned for this page, ending pagination")
                    break

                page += 1

    finally:
        # Complete the job run
        if job_id and not dry_run:
            complete_job_run(
                job_id=job_id,
                found=stats["unique_sellers_found"],
                new=stats["new_sellers_added"],
                skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                errors=stats["errors"],
                details=stats,
            )

    return stats


def init_harvest(limit=None, dry_run=False, use_categories=True):
    """
    Initialize merchant harvest process.

    Args:
        limit: Maximum number of products to process
        dry_run: If True, don't write to database
        use_categories: If True, use categories instead of keywords
    """
    create_example_env_file()

    if not dry_run:
        # Ensure tables exist
        create_tables_if_not_exist()

    logger.info(f"Starting initial merchant harvest{'(DRY RUN)' if dry_run else ''}")
    if limit:
        logger.info(f"Processing limit: {limit} products")

    if use_categories:
        # Get search categories
        categories = get_search_categories()

        if categories:
            logger.info(f"Loaded {len(categories)} search categories")
            stats = _harvest_merchants(
                categories=categories, limit=limit, dry_run=dry_run, job_type="HARVEST_INIT"
            )
        else:
            logger.warning("No categories found. Falling back to keywords.")
            use_categories = False

    if not use_categories:
        # Get search keywords
        keywords = get_search_keywords()
        logger.info(f"Loaded {len(keywords)} search keywords")
        stats = _harvest_merchants(keywords=keywords, limit=limit, dry_run=dry_run, job_type="HARVEST_INIT")

    logger.info(f"Harvest complete. Summary:")
    logger.info(f"- Products processed: {stats['total_products_processed']}")
    logger.info(f"- Unique sellers found: {stats['unique_sellers_found']}")
    logger.info(f"- New sellers added: {stats['new_sellers_added']}")
    logger.info(f"- Existing sellers updated: {stats['sellers_updated']}")
    logger.info(f"- Errors: {stats['errors']}")

    if not dry_run:
        # Show seller approval counts
        counts = get_seller_approval_counts()
        logger.info(f"Current seller counts by status:")
        logger.info(f"- PENDING: {counts['PENDING']}")
        logger.info(f"- WHITELIST: {counts['WHITELIST']}")
        logger.info(f"- BLACKLIST: {counts['BLACKLIST']}")
        logger.info(f"- TOTAL: {counts['TOTAL']}")


def delta_harvest(limit=None, dry_run=False, use_categories=True):
    """
    Run incremental merchant harvest.

    Args:
        limit: Maximum number of products to process
        dry_run: If True, don't write to database
        use_categories: If True, use categories instead of keywords
    """
    logger.info(f"Starting delta merchant harvest{'(DRY RUN)' if dry_run else ''}")

    if not dry_run:
        # Ensure tables exist
        create_tables_if_not_exist()

    if limit:
        logger.info(f"Processing limit: {limit} products")

    if use_categories:
        # Get search categories
        categories = get_search_categories()

        if categories:
            logger.info(f"Loaded {len(categories)} search categories")
            stats = _harvest_merchants(
                categories=categories, limit=limit, dry_run=dry_run, job_type="HARVEST_DELTA"
            )
        else:
            logger.warning("No categories found. Falling back to keywords.")
            use_categories = False

    if not use_categories:
        # Get search keywords
        keywords = get_search_keywords()
        logger.info(f"Loaded {len(keywords)} search keywords")
        stats = _harvest_merchants(keywords=keywords, limit=limit, dry_run=dry_run, job_type="HARVEST_DELTA")
    
    logger.info(f"Harvest complete. Summary:")
    logger.info(f"- Products processed: {stats['total_products_processed']}")
    logger.info(f"- Unique sellers found: {stats['unique_sellers_found']}")
    logger.info(f"- New sellers added: {stats['new_sellers_added']}")
    logger.info(f"- Existing sellers updated: {stats['sellers_updated']}")
    logger.info(f"- Errors: {stats['errors']}")

    if not dry_run:
        # Show seller approval counts
        counts = get_seller_approval_counts()
        logger.info(f"Current seller counts by status:")
        logger.info(f"- PENDING: {counts['PENDING']}")
        logger.info(f"- WHITELIST: {counts['WHITELIST']}")
        logger.info(f"- BLACKLIST: {counts['BLACKLIST']}")
        logger.info(f"- TOTAL: {counts['TOTAL']}")


def harvest_status():
    """Show harvest job status."""
    # Get seller approval counts
    counts = get_seller_approval_counts()

    print("\n===== SELLER STATUS =====")
    print(f"PENDING:   {counts['PENDING']}")
    print(f"WHITELIST: {counts['WHITELIST']}")
    print(f"BLACKLIST: {counts['BLACKLIST']}")
    print(f"TOTAL:     {counts['TOTAL']}")
    print()

    # Get recent job runs
    job_runs = get_recent_job_runs(limit=5)

    print("===== RECENT JOB RUNS =====")
    if not job_runs:
        print("No job runs found.")
    else:
        print(
            "{:<5} {:<15} {:<20} {:<10} {:<10} {:<10} {:<10}".format(
                "ID", "JOB TYPE", "START TIME", "FOUND", "NEW", "SKIPPED", "ERRORS"
            )
        )
        print("-" * 85)

        for job in job_runs:
            print(
                "{:<5} {:<15} {:<20} {:<10} {:<10} {:<10} {:<10}".format(
                    job["id"],
                    job["job_type"],
                    job["start_time"].strftime("%Y-%m-%d %H:%M"),
                    job["found_count"],
                    job["new_count"],
                    job["skipped_count"],
                    job["error_count"],
                )
            )

    # Get category stats
    category_stats = get_product_categories_stats()

    print("\n===== CATEGORY SEARCH STATS =====")
    print(
        f"Total category-product associations: {category_stats['total_associations']}"
    )

    if category_stats["top_categories"]:
        print("\nTop categories by product count:")
        print("{:<8} {:<30} {:<10}".format("ID", "CATEGORY", "PRODUCTS"))
        print("-" * 50)

        for item in category_stats["top_categories"]:
            print(
                "{:<8} {:<30} {:<10}".format(
                    item["category_id"],
                    item["category_name"][:28],
                    item["product_count"],
                )
            )

    print("\n===== SQL QUERIES FOR ANALYSIS =====")
    print("# Get merchants found in the last 24 hours:")
    print(
        "SELECT COUNT(*) FROM sellers WHERE first_seen_at >= NOW() - INTERVAL '24 hours';"
    )
    print("\n# Get distribution of approval status:")
    print("SELECT approval_status, COUNT(*) FROM sellers GROUP BY approval_status;")
    print("\n# Get average number of merchants found per job run:")
    print("SELECT AVG(found_count) FROM job_runs WHERE job_type = 'harvest';")
    print("\n# Get products by category:")
    print(
        "SELECT pc.category_name, COUNT(*) FROM product_categories pc GROUP BY pc.category_name ORDER BY COUNT(*) DESC;"
    )
    print("\n# Get all products for a specific category:")
    print(
        "SELECT p.product_id, p.product_title FROM products p JOIN product_categories pc ON p.product_id = pc.product_id WHERE pc.category_id = 1509;"
    )
