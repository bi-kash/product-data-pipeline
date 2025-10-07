#!/usr/bin/env python3
"""
Merchant harvester module for collecting merchant IDs via product search.
Uses RapidAPI client for reliable AliExpress data access.
"""

import json
from tqdm import tqdm
from datetime import datetime
from copy import deepcopy
from src.common.database import (
    create_tables_if_not_exist,
    start_job_run,
    complete_job_run,
    update_job_run_progress,
    upsert_seller,
    upsert_product,
    get_seller_approval_counts,
    get_recent_job_runs,
    get_db_session,
    JobRun,
    update_seller_approval,
)
from src.common.config import (
    get_search_keywords,
    get_search_category,
    create_example_env_file,
    get_api_page_size,
    use_keywords,
    get_blacklisted_title_terms,
)
from src.common.official_aliexpress_client import OfficialAliExpressClient as AliExpressClient
from src.common.logging_config import setup_logging

# Configure logging
logger = setup_logging("merchant_harvester")


def _get_aliexpress_client():
    """
    Initialize the Official AliExpress API client.
    
    Returns:
        AliExpressClient: Initialized client instance
        
    Raises:
        ValueError: If initialization fails
    """
    try:
        client = AliExpressClient()
        logger.info("AliExpress client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize AliExpress client: {e}")
        raise


def prepare_json_safe_dict(data):
    """
    Convert a dictionary to be JSON safe (handles datetime objects)

    Args:
        data: Dictionary to convert

    Returns:
        JSON safe dictionary
    """
    if not data:
        return data

    # Create a deep copy to avoid modifying the original
    result = deepcopy(data)

    # Convert any datetime objects to ISO format strings
    for key, value in result.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()

    return result


def _process_products(
    products,
    client,
    stats,
    unique_sellers,
    page=1,
    category_id=None,
    category_name=None,
    dry_run=False,
    limit=None,
    all_categories=None,
    job_id=None,
):
    """
    Process a batch of products from the API response.

    Args:
        products: List of product dictionaries
        client: API client instance
        stats: Dictionary to track statistics
        unique_sellers: Set to track unique sellers processed
        page: Current page number
        category_id: ID of the category being searched
        category_name: Name of the category being searched
        dry_run: If True, don't write to database
        limit: Maximum number of products to process
        all_categories: List of all category IDs (for category search)
        job_id: ID of the current job run
    """
    # Initialize product counts in stats if not present
    if "products_added" not in stats:
        stats["products_added"] = 0
    from src.common.config import get_blacklisted_title_terms

    # Get blacklisted terms for product titles
    blacklisted_terms = get_blacklisted_title_terms()

    # Log progress compared to total
    processed_so_far = stats["total_products_processed"]
    logger.info(
        f"Processing batch - found {len(products)} products (processed so far: {processed_so_far})"
    )

    # Process each product
    for position, product in enumerate(
        tqdm(products, desc=f"Processing products (page {page})")
    ):
        stats["total_products_processed"] += 1

        # Check for blacklisted terms in the product title
        product_title = product.get("product_title", "").lower()

        # Check for blacklisted terms in the title
        blacklisted_term_found = False
        blacklisted_term = None
        if blacklisted_terms and product_title:
            for term in blacklisted_terms:
                if term in product_title:
                    blacklisted_term_found = True
                    blacklisted_term = term
                    break

        if blacklisted_term_found:
            logger.info(
                f"SKIPPING PRODUCT: {product.get('product_id', 'unknown')} - Contains blacklisted term: '{blacklisted_term}' - Title: {product_title[:50]}..."
            )
            # Count as "skipped" in stats
            stats["products_skipped_blacklisted_terms"] = stats.get("products_skipped_blacklisted_terms", 0) + 1

            # Skip this product entirely - don't add to database, don't blacklist seller
            logger.debug(
                f"Skipping product with blacklisted term '{blacklisted_term}' in title: {product_title[:50]}..."
            )
            continue

        # Get both seller info and product details in one API call to avoid duplication
        seller_and_product = client.get_seller_and_product_details(product)
        seller_info = seller_and_product['seller_info']
        product_details = seller_and_product['product_details']
        
        if not seller_info:
            stats["errors"] += 1
            continue

        shop_id = seller_info["shop_id"]
        product_id = str(product.get("product_id", ""))

        if not product_id:
            stats["errors"] += 1
            continue

        # If this seller has already been processed in this run, don't count it as a new seller
        # but still process the product
        seller_already_processed = shop_id in unique_sellers

        if not seller_already_processed:
            unique_sellers.add(shop_id)
            stats["unique_sellers_found"] += 1

        # Insert or update the seller in the database
        if not dry_run:
            try:
                # Only upsert the seller if we haven't processed it yet in this run
                is_new_seller = False
                if not seller_already_processed:
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
                product_main_image_url = product.get("product_main_image_url", "")
                
                # Extract video URL from product details if available
                product_video_url = None
                if product_details:
                    try:
                        # Navigate to the video URL in the product details JSON
                        result = product_details.get("result", {})
                        multimedia_info = result.get("ae_multimedia_info_dto", {})
                        video_dtos = multimedia_info.get("ae_video_dtos", {})
                        video_list = video_dtos.get("ae_video_d_t_o", [])
                        
                        if isinstance(video_list, list) and video_list:
                            first_video = video_list[0]
                            product_video_url = first_video.get("media_url")
                        elif isinstance(video_list, dict):
                            product_video_url = video_list.get("media_url")
                    except (AttributeError, KeyError, TypeError):
                        # If video extraction fails, just continue without video URL
                        pass

                # Handle prices
                original_price = None
                target_sale_price = None
                original_price_currency = None
                target_sale_price_currency = None

                if "original_price" in product and product["original_price"]:
                    try:
                        original_price = float(product["original_price"])
                        original_price_currency = product.get("original_price_currency")
                    except (ValueError, TypeError):
                        pass

                if "target_sale_price" in product and product["target_sale_price"]:
                    try:
                        target_sale_price = float(product["target_sale_price"])
                        target_sale_price_currency = product.get(
                            "target_sale_price_currency"
                        )
                    except (ValueError, TypeError):
                        pass

                # Upsert the product (always process every product)
                # Use the product details we already fetched (to avoid duplicate API call)
                raw_json_detail = product_details if product_details else None

                upsert_product(
                    product_id=product_id,
                    shop_id=shop_id,
                    product_title=product_title,
                    product_detail_url=product_detail_url,
                    product_main_image_url=product_main_image_url,
                    product_video_url=product_video_url,
                    original_price=original_price,
                    target_sale_price=target_sale_price,
                    original_price_currency=original_price_currency,
                    target_sale_price_currency=target_sale_price_currency,
                    discount=product.get("discount"),
                    evaluate_rate=product.get("evaluate_rate"),
                    category_id=product.get("category_id"),  # Use category_id from converted product format
                    raw_json_search=product,  # Store text search result in raw_json_search
                    raw_json_detail=raw_json_detail,  # Store detailed product information
                )

                # Count products added to the database
                stats["products_added"] = stats.get("products_added", 0) + 1

                # Previously associated product with category (removed)

            except Exception as e:
                logger.error(f"Error upserting seller/product {shop_id}: {e}")
                stats["errors"] += 1

        # Stop if we've reached the limit
        if limit and stats["total_products_processed"] >= limit:
            logger.info(
                f"Reached limit of {limit} products. Stopping batch processing."
            )
            return



def init_harvest(limit=None, dry_run=False):
    """
    Initialize merchant harvest process - performs a complete product/seller harvest.

    This function implements the search strategy based on the configuration:

    1. When USE_KEYWORDS=true:
       - First searches using each keyword in KEYWORDS (separate API calls)
       - Then searches using all categories in CATEGORIES (another API call)
       - A single job_run record tracks all activity
       - Keywords and categories columns in job_run reflect what was actually used

    2. When USE_KEYWORDS=false (recommended):
       - Only searches using categories in CATEGORIES
       - Avoids potential supply-heavy keyword searches
       - More efficient (fewer API calls)
       - Only the categories column is populated in job_run

    The function tracks which keywords and categories were actually used during the harvest,
    and updates the job_run record after each page of results is processed. This ensures
    that even if the harvest is interrupted, the job_run record will accurately reflect
    which search parameters were used.

    Args:
        limit: Maximum number of products to process (None for unlimited)
        dry_run: If True, don't write to database
    """
    create_example_env_file()

    # Set up the harvest with common initialization code
    job_id, categories, all_keywords, used_keywords, used_categories, stats = (
        _setup_harvest("HARVEST_INIT", limit, dry_run)
    )

    # Unique sellers set for the entire process
    unique_sellers = set()

    client = _get_aliexpress_client()

    try:
        # If USE_KEYWORDS is true: Use both keywords AND categories
        # If USE_KEYWORDS is false: Use ONLY categories
        if use_keywords() and all_keywords:
            # Process keywords
            logger.info(f"Using {len(all_keywords)} keywords from .env")

            for keyword in all_keywords:
                logger.info(f"Searching for keyword: {keyword}")
                page = 1
                page_size = get_api_page_size()
                while True:
                    logger.info(f"Processing page {page} for keyword '{keyword}'")
                    result = client.search_products(
                        keyword, page_no=page, page_size=page_size
                    )
                    products = result[0]
                    total_count = result[1]
                    original_count = result[2] if len(result) > 2 else len(products)
                    all_below_threshold = result[3] if len(result) > 3 else False

                    # Check if we have no products at all from API
                    if not products and original_count == 0:
                        logger.info(f"No more results for keyword '{keyword}'")
                        break

                    # Check if we have original products from API but zero after filtering
                    if original_count > 0 and not products:
                        logger.info(f"Page {page} had {original_count} products but 0 after filtering, continuing to next page")
                        page += 1
                        continue
                    # Add keyword to used keywords list if we found products
                    if keyword not in used_keywords:
                        used_keywords.append(keyword)

                    # Process the products for this page
                    _process_products(
                        products=products,
                        stats=stats,
                        unique_sellers=unique_sellers,
                        job_id=job_id,
                        page=page,
                        category_id=keyword,  # Use keyword as category for tracking
                        category_name=keyword,
                        limit=limit,
                        dry_run=dry_run,
                        client=client,
                    )

                    # Update job progress after processing each page
                    if job_id and not dry_run:
                        # Convert current used keywords/categories to comma-separated strings for progress updates
                        keywords_str = (
                            ",".join(used_keywords) if used_keywords else None
                        )
                        categories_str = (
                            ",".join([str(c) for c in used_categories])
                            if used_categories
                            else None
                        )

                        update_job_run_progress(
                            job_id=job_id,
                            found=stats["unique_sellers_found"],
                            new=stats["new_sellers_added"],
                            skipped=stats["unique_sellers_found"]
                            - stats["new_sellers_added"],
                            errors=stats["errors"],
                            details=prepare_json_safe_dict(stats),
                            keywords=keywords_str,
                            categories=categories_str,
                        )

                    # Stop if we've reached the limit
                    if limit and stats["total_products_processed"] >= limit:
                        logger.info(
                            f"Reached limit of {limit} products. Stopping harvest."
                        )
                        break

                    page += 1

                # Stop if we've reached the limit
                if limit and stats["total_products_processed"] >= limit:
                    break
        # Category-only search has been removed - all searches now use keywords (with category)

    except KeyboardInterrupt:
        # Handle keyboard interrupt (Ctrl+C)
        logger.warning("Harvest interrupted by keyboard (Ctrl+C)")
        stats["exception"] = "KeyboardInterrupt"
    except Exception as e:
        # Handle any other exception
        logger.error(f"Exception during harvest: {str(e)}")
        stats["exception"] = str(e)
    finally:
        # Complete job run - this runs even after exceptions
        if job_id and not dry_run:
            # No need to update the database directly here since we're already updating during progress
            # Just complete the job run with the current keywords and categories
            keywords_str = ",".join(used_keywords) if used_keywords else None
            categories_str = (
                ",".join([str(c) for c in used_categories]) if used_categories else None
            )

            complete_job_run(
                job_id=job_id,
                found=stats["unique_sellers_found"],
                new=stats["new_sellers_added"],
                skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                errors=stats["errors"],
                details=prepare_json_safe_dict(stats),
                keywords=keywords_str,
                categories=categories_str,
            )

    logger.info(f"Harvest complete. Summary:")
    logger.info(f"- Products processed: {stats['total_products_processed']}")
    logger.info(f"- Products added to database: {stats.get('products_added', 0)}")
    logger.info(f"- Products skipped (blacklisted terms): {stats.get('products_skipped_blacklisted_terms', 0)}")
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

    return stats


def _setup_harvest(job_type, limit=None, dry_run=False):
    """
    Common setup for both initial and delta harvests.

    Args:
        job_type: Type of harvest job (HARVEST_INIT or HARVEST_DELTA)
        limit: Optional limit on the number of products to process
        dry_run: If True, don't write to database

    Returns:
        Tuple of (job_id, categories, all_keywords, used_keywords, used_categories, stats)
    """
    harvest_type = "initial" if job_type == "HARVEST_INIT" else "delta"
    logger.info(
        f"Starting {harvest_type} merchant harvest{'(DRY RUN)' if dry_run else ''}"
    )

    if not dry_run:
        # Ensure tables exist
        create_tables_if_not_exist()

    if limit:
        logger.info(f"Processing limit: {limit} products")

    # Get single category (used in both modes)
    single_category = get_search_category()
    categories = [single_category] if single_category else []

    # Get keywords if needed
    all_keywords = get_search_keywords() if use_keywords() else None

    # We'll track which keywords and categories were actually used
    used_keywords = []
    # Initialize used_categories with all available categories
    # so that they'll be in the job_run even if we interrupt early
    used_categories = categories.copy() if categories else []

    # Initially create job run with no keywords/categories - we'll update later
    job_id = (
        None
        if dry_run
        else start_job_run(job_type=job_type, keywords=None, categories=None)
    )

    # Create unified stats dictionary
    stats = {
        "total_products_processed": 0,
        "products_skipped_blacklisted_terms": 0,  # Count of products skipped due to blacklisted terms in title
        "unique_sellers_found": 0,
        "new_sellers_added": 0,
        "sellers_updated": 0,
        "errors": 0,
        "start_time": datetime.utcnow().isoformat(),
    }

    return job_id, categories, all_keywords, used_keywords, used_categories, stats


def delta_harvest(limit=None, dry_run=False):
    """
    Run incremental merchant harvest - updates the database with new products/sellers.

    Similar to init_harvest, but typically used for regular updates after the initial harvest.
    This function implements the search strategy based on the configuration:

    1. When USE_KEYWORDS=true:
       - First searches using each keyword in KEYWORDS (separate API calls)
       - Then searches using all categories in CATEGORIES (another API call)
       - A single job_run record tracks all activity
       - Keywords and categories columns in job_run reflect what was actually used

    2. When USE_KEYWORDS=false (recommended):
       - Only searches using categories in CATEGORIES
       - Avoids potential supply-heavy keyword searches
       - More efficient (fewer API calls)
       - Only the categories column is populated in job_run

    To avoid harvesting jewelry supplies (beads, findings, etc.), it's recommended to:
    1. Set USE_KEYWORDS=false and use only specific finished jewelry categories
    2. Avoid supply-heavy categories like Jewelry Making (201238105)

    The job_run tracking ensures that even if the harvest is interrupted, the record will
    accurately reflect which keywords and categories were used in the search.

    Args:
        limit: Maximum number of products to process (None for unlimited)
        dry_run: If True, don't write to database
    """
    # Set up the harvest with common initialization code
    job_id, categories, all_keywords, used_keywords, used_categories, stats = (
        _setup_harvest("HARVEST_DELTA", limit, dry_run)
    )

    # Unique sellers set for the entire process
    unique_sellers = set()

    client = _get_aliexpress_client()

    try:
        # If USE_KEYWORDS is true: Use both keywords AND categories
        # If USE_KEYWORDS is false: Use ONLY categories
        if use_keywords() and all_keywords:
            # Process keywords
            logger.info(f"Using {len(all_keywords)} keywords from .env")

            for keyword in all_keywords:
                logger.info(f"Searching for keyword: {keyword}")
                page = 1
                page_size = get_api_page_size()

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

                    # Add keyword to used keywords list if we found products
                    if keyword not in used_keywords:
                        used_keywords.append(keyword)

                    # Process the products for this page
                    _process_products(
                        products=products,
                        stats=stats,
                        unique_sellers=unique_sellers,
                        job_id=job_id,
                        page=page,
                        category_id=keyword,  # Use keyword as category for tracking
                        category_name=keyword,
                        limit=limit,
                        dry_run=dry_run,
                        client=client,
                    )

                    # Update job progress after processing each page
                    if job_id and not dry_run:
                        # Convert current used keywords/categories to comma-separated strings for progress updates
                        keywords_str = (
                            ",".join(used_keywords) if used_keywords else None
                        )
                        categories_str = (
                            ",".join([str(c) for c in used_categories])
                            if used_categories
                            else None
                        )

                        update_job_run_progress(
                            job_id=job_id,
                            found=stats["unique_sellers_found"],
                            new=stats["new_sellers_added"],
                            skipped=stats["unique_sellers_found"]
                            - stats["new_sellers_added"],
                            errors=stats["errors"],
                            details=prepare_json_safe_dict(stats),
                            keywords=keywords_str,
                            categories=categories_str,
                        )

                    # Stop if we've reached the limit
                    if limit and stats["total_products_processed"] >= limit:
                        logger.info(
                            f"Reached limit of {limit} products. Stopping harvest."
                        )
                        break

                    page += 1

                # Stop if we've reached the limit
                if limit and stats["total_products_processed"] >= limit:
                    break
        # Category-only search has been removed - all searches now use keywords (with category)

    except KeyboardInterrupt:
        # Handle keyboard interrupt (Ctrl+C)
        logger.warning("Harvest interrupted by keyboard (Ctrl+C)")
        stats["exception"] = "KeyboardInterrupt"
    except Exception as e:
        # Handle any other exception
        logger.error(f"Exception during harvest: {str(e)}")
        stats["exception"] = str(e)
    finally:
        # Complete job run - this runs even after exceptions
        if job_id and not dry_run:
            # No need to update the database directly here since we're already updating during progress
            # Just complete the job run with the current keywords and categories
            keywords_str = ",".join(used_keywords) if used_keywords else None
            categories_str = (
                ",".join([str(c) for c in used_categories]) if used_categories else None
            )

            complete_job_run(
                job_id=job_id,
                found=stats["unique_sellers_found"],
                new=stats["new_sellers_added"],
                skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                errors=stats["errors"],
                details=prepare_json_safe_dict(stats),
                keywords=keywords_str,
                categories=categories_str,
            )

    logger.info(f"Harvest complete. Summary:")
    logger.info(f"- Products processed: {stats['total_products_processed']}")
    logger.info(f"- Products added to database: {stats.get('products_added', 0)}")
    logger.info(f"- Products skipped (blacklisted terms): {stats.get('products_skipped_blacklisted_terms', 0)}")
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

    return stats


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

    # Category stats section removed

    print("\n===== SQL QUERIES FOR ANALYSIS =====")
    print("# Get merchants found in the last 24 hours:")
    print(
        "SELECT COUNT(*) FROM sellers WHERE first_seen_at >= NOW() - INTERVAL '24 hours';"
    )
    print("\n# Get distribution of approval status:")
    print("SELECT approval_status, COUNT(*) FROM sellers GROUP BY approval_status;")
    print("\n# Get average number of merchants found per job run:")
    print("SELECT AVG(found_count) FROM job_runs WHERE job_type = 'harvest';")
    # Product category SQL queries removed
