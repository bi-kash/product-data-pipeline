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
    associate_product_category,
    get_product_categories_stats,
    get_db_session,
    JobRun,
)
from src.common.config import (
    get_search_keywords,
    get_search_categories,
    create_example_env_file,
    get_api_page_size,
    use_keywords,
)
from src.common.aliexpress_client import AliExpressClient
from src.common.logging_config import setup_logging

# Configure logging
logger = setup_logging('merchant_harvester')


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


def _process_products(products, stats, unique_sellers, job_id, page, category_id, category_name, limit, dry_run, client, all_categories=None):
    """
    Process a batch of products from API search and update the database.
    
    This function handles both keyword-based and category-based search results,
    updating the database with products and sellers found. It also tracks which
    products were found via which search method for analytics purposes.
    
    When processing keyword-based search results:
    - category_id and category_name parameters contain the keyword
    - all_categories is None
    
    When processing category-based search results:
    - category_id and category_name are None
    - all_categories contains the list of category IDs used in the search
    
    The function ensures products are associated with the correct categories in
    the database and avoids duplicate processing of sellers already seen in the
    current harvest run.
    
    Args:
        products: List of products from API
        stats: Statistics dictionary to update
        unique_sellers: Set of unique seller IDs already processed
        job_id: Job run ID
        page: Current page number
        category_id: Category ID for product association or keyword for keyword search
        category_name: Category name for product association or keyword for keyword search
        limit: Maximum products to process
        dry_run: If True, don't write to database
        client: AliExpressClient instance
        all_categories: List of all category IDs (for category search)
    """
    # Log progress compared to total
    processed_so_far = stats["total_products_processed"]
    logger.info(f"Processing batch - found {len(products)} products (processed so far: {processed_so_far})")
    
    # Process each product
    for position, product in enumerate(tqdm(products, desc=f"Processing products (page {page})")):
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
            # product was found via this search
            if not dry_run:
                try:
                    if all_categories:
                        # Associate with each provided category
                        for cat_id in all_categories:
                            associate_product_category(
                                product_id=product_id,
                                category_id=cat_id,
                                category_name=str(cat_id),
                                search_page=page,
                                position_in_results=position + 1,
                            )
                    elif category_id:
                        associate_product_category(
                            product_id=product_id,
                            category_id=category_id,
                            category_name=category_name,
                            search_page=page,
                            position_in_results=position + 1,
                        )
                except Exception as e:
                    logger.error(f"Error associating product {product_id} with category: {e}")
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
                product_detail_url = product.get("product_detail_url", "")
                product_main_image_url = product.get("product_main_image_url", "")
                
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
                        target_sale_price_currency = product.get("target_sale_price_currency")
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
                    first_level_category_name=product.get("first_level_category_name"),
                    second_level_category_name=product.get("second_level_category_name"),
                    raw_json=product,
                )
                
                # Associate product with category
                if all_categories:
                    # Associate with each provided category
                    for cat_id in all_categories:
                        associate_product_category(
                            product_id=product_id,
                            category_id=cat_id,
                            category_name=str(cat_id),
                            search_page=page,
                            position_in_results=position + 1,
                        )
                elif category_id:
                    associate_product_category(
                        product_id=product_id,
                        category_id=category_id,
                        category_name=category_name,
                        search_page=page,
                        position_in_results=position + 1,
                    )
                    
            except Exception as e:
                logger.error(f"Error upserting seller/product {shop_id}: {e}")
                stats["errors"] += 1
                
        # Stop if we've reached the limit
        if limit and stats["total_products_processed"] >= limit:
            logger.info(f"Reached limit of {limit} products. Stopping batch processing.")
            return


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
    # Pass keywords and categories to start_job_run to record them in the database
    job_id = None if dry_run else start_job_run(
        job_type=job_type, 
        keywords=keywords,
        categories=categories
    )

    stats = {
        "total_products_processed": 0,
        "unique_sellers_found": 0,
        "new_sellers_added": 0,
        "sellers_updated": 0,
        "errors": 0,
        "start_time": datetime.utcnow().isoformat(),  # Track when we started as ISO string
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
                    
                    # Update job progress after processing each page
                    if job_id and not dry_run:
                        update_job_run_progress(
                            job_id=job_id,
                            found=stats["unique_sellers_found"],
                            new=stats["new_sellers_added"],
                            skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                            errors=stats["errors"],
                            details=prepare_json_safe_dict(stats),
                        )

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
            # Handle categories as a flat list of IDs
            all_category_ids = categories
            # Create a dictionary of category ID to name (use ID as name since we don't have names)
            category_id_to_name = {str(cat_id): str(cat_id) for cat_id in categories}
            
            # Create a string representation for logging
            category_names_str = ", ".join([str(cat_id) for cat_id in categories])
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
                
                # Update job progress after processing each page
                if job_id and not dry_run:
                    update_job_run_progress(
                        job_id=job_id,
                        found=stats["unique_sellers_found"],
                        new=stats["new_sellers_added"],
                        skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                        errors=stats["errors"],
                        details=prepare_json_safe_dict(stats),
                    )

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

                    # For API search by category, we already know the product belongs to one of our categories
                    # Default to the first category in our list as a consistent behavior
                    matching_cat_id = all_category_ids[0]
                    matching_cat_name = str(matching_cat_id)  # Use ID as name

                    # Skip if this seller has already been processed in this run
                    if shop_id in unique_sellers:
                        # Even if we skip this seller, we should still track that this
                        # product was found via this category search
                        if not dry_run:
                            try:
                                # Associate with each provided category
                                for cat_id in all_category_ids:
                                    associate_product_category(
                                        product_id=product_id,
                                        category_id=cat_id,
                                        category_name=str(cat_id),
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

                            # Associate product with each category
                            for cat_id in all_category_ids:
                                associate_product_category(
                                    product_id=product_id,
                                    category_id=cat_id,
                                    category_name=str(cat_id),
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

    except KeyboardInterrupt:
        # Handle keyboard interrupt (Ctrl+C)
        logger.warning("Harvest interrupted by keyboard (Ctrl+C)")
        stats["exception"] = "KeyboardInterrupt"
    except Exception as e:
        # Handle any other exception
        logger.error(f"Exception during harvest: {str(e)}")
        stats["exception"] = str(e)
    finally:
        # Complete the job run - this runs even after exceptions
        if job_id and not dry_run:
            complete_job_run(
                job_id=job_id,
                found=stats["unique_sellers_found"],
                new=stats["new_sellers_added"],
                skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                errors=stats["errors"],
                details=prepare_json_safe_dict(stats),
            )

    return stats


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

    if not dry_run:
        # Ensure tables exist
        create_tables_if_not_exist()

    logger.info(f"Starting initial merchant harvest{'(DRY RUN)' if dry_run else ''}")
    if limit:
        logger.info(f"Processing limit: {limit} products")

    # Get categories first (used in both modes)
    categories = get_search_categories()
    
    # Get keywords if needed
    all_keywords = get_search_keywords() if use_keywords() else None
    
    # We'll track which keywords and categories were actually used
    used_keywords = []
    # Initialize used_categories with all available categories
    # so that they'll be in the job_run even if we interrupt early
    used_categories = categories.copy() if categories else []
    
    # Initially create job run with no keywords/categories - we'll update later
    job_id = None if dry_run else start_job_run(
        job_type="HARVEST_INIT",
        keywords=None,
        categories=None
    )
    
    # Create unified stats dictionary
    stats = {
        "total_products_processed": 0,
        "unique_sellers_found": 0,
        "new_sellers_added": 0,
        "sellers_updated": 0,
        "errors": 0,
        "start_time": datetime.utcnow().isoformat(),
    }
    
    # Unique sellers set for the entire process
    unique_sellers = set()
    
    client = AliExpressClient()
    
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
                        client=client
                    )
                    
                    # Update job progress after processing each page
                    if job_id and not dry_run:
                        # Convert current used keywords/categories to comma-separated strings for progress updates
                        keywords_str = ",".join(used_keywords) if used_keywords else None
                        categories_str = ",".join([str(c) for c in used_categories]) if used_categories else None
                        
                        update_job_run_progress(
                            job_id=job_id,
                            found=stats["unique_sellers_found"],
                            new=stats["new_sellers_added"],
                            skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                            errors=stats["errors"],
                            details=prepare_json_safe_dict(stats),
                            keywords=keywords_str,
                            categories=categories_str
                        )
                    
                    # Stop if we've reached the limit
                    if limit and stats["total_products_processed"] >= limit:
                        logger.info(f"Reached limit of {limit} products. Stopping harvest.")
                        break
                        
                    page += 1
                    
                # Stop if we've reached the limit
                if limit and stats["total_products_processed"] >= limit:
                    break
                    
        # Process categories if available
        if categories:
            logger.info(f"Using {len(categories)} categories from .env")
            
            # Use all categories in a single search
            logger.info(f"Searching for categories: {categories}")
            page = 1
            page_size = get_api_page_size()
            
            while True:
                logger.info(f"Processing page {page} for categories")
                result = client.search_products_by_category(
                    categories, page_no=page, page_size=page_size
                )
                products = result[0]
                total_count = result[1]
                
                if not products:
                    logger.info(f"No more results for categories")
                    break
                
                # No need to add categories here as we've already added them at the beginning
                
                # Process the products for this page
                _process_products(
                    products=products,
                    stats=stats,
                    unique_sellers=unique_sellers,
                    job_id=job_id,
                    page=page,
                    category_id=None,  # Will be handled inside function for each product
                    category_name=None,
                    limit=limit,
                    dry_run=dry_run,
                    client=client,
                    all_categories=categories  # Pass all categories to associate with each product
                )
                
                # Update job progress after processing each page
                if job_id and not dry_run:
                    # Convert current used keywords/categories to comma-separated strings for progress updates
                    keywords_str = ",".join(used_keywords) if used_keywords else None
                    categories_str = ",".join([str(c) for c in used_categories]) if used_categories else None
                    
                    update_job_run_progress(
                        job_id=job_id,
                        found=stats["unique_sellers_found"],
                        new=stats["new_sellers_added"],
                        skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                        errors=stats["errors"],
                        details=prepare_json_safe_dict(stats),
                        keywords=keywords_str,
                        categories=categories_str
                    )
                
                # Stop if we've reached the limit
                if limit and stats["total_products_processed"] >= limit:
                    logger.info(f"Reached limit of {limit} products. Stopping harvest.")
                    break
                    
                page += 1
    
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
            categories_str = ",".join([str(c) for c in used_categories]) if used_categories else None
            
            complete_job_run(
                job_id=job_id,
                found=stats["unique_sellers_found"],
                new=stats["new_sellers_added"],
                skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                errors=stats["errors"],
                details=prepare_json_safe_dict(stats),
                keywords=keywords_str,
                categories=categories_str
            )
        
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
            
    return stats

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
    logger.info(f"Starting delta merchant harvest{'(DRY RUN)' if dry_run else ''}")

    if not dry_run:
        # Ensure tables exist
        create_tables_if_not_exist()

    if limit:
        logger.info(f"Processing limit: {limit} products")

    # Get categories first (used in both modes)
    categories = get_search_categories()
    
    # Get keywords if needed
    all_keywords = get_search_keywords() if use_keywords() else None
    
    # We'll track which keywords and categories were actually used
    used_keywords = []
    # Initialize used_categories with all available categories
    # so that they'll be in the job_run even if we interrupt early
    used_categories = categories.copy() if categories else []
    
    # Initially create job run with no keywords/categories - we'll update later
    job_id = None if dry_run else start_job_run(
        job_type="HARVEST_DELTA",
        keywords=None,
        categories=None
    )
    
    # Create unified stats dictionary
    stats = {
        "total_products_processed": 0,
        "unique_sellers_found": 0,
        "new_sellers_added": 0,
        "sellers_updated": 0,
        "errors": 0,
        "start_time": datetime.utcnow().isoformat(),
    }
    
    # Unique sellers set for the entire process
    unique_sellers = set()
    
    client = AliExpressClient()
    
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
                        client=client
                    )
                    
                    # Update job progress after processing each page
                    if job_id and not dry_run:
                        # Convert current used keywords/categories to comma-separated strings for progress updates
                        keywords_str = ",".join(used_keywords) if used_keywords else None
                        categories_str = ",".join([str(c) for c in used_categories]) if used_categories else None
                        
                        update_job_run_progress(
                            job_id=job_id,
                            found=stats["unique_sellers_found"],
                            new=stats["new_sellers_added"],
                            skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                            errors=stats["errors"],
                            details=prepare_json_safe_dict(stats),
                            keywords=keywords_str,
                            categories=categories_str
                        )
                    
                    # Stop if we've reached the limit
                    if limit and stats["total_products_processed"] >= limit:
                        logger.info(f"Reached limit of {limit} products. Stopping harvest.")
                        break
                        
                    page += 1
                    
                # Stop if we've reached the limit
                if limit and stats["total_products_processed"] >= limit:
                    break
                    
        # Process categories if available
        if categories:
            logger.info(f"Using {len(categories)} categories from .env")
            
            # Use all categories in a single search
            logger.info(f"Searching for categories: {categories}")
            page = 1
            page_size = get_api_page_size()
            
            while True:
                logger.info(f"Processing page {page} for categories")
                result = client.search_products_by_category(
                    categories, page_no=page, page_size=page_size
                )
                products = result[0]
                total_count = result[1]
                
                if not products:
                    logger.info(f"No more results for categories")
                    break
                
                # No need to add categories here as we've already added them at the beginning
                
                # Process the products for this page
                _process_products(
                    products=products,
                    stats=stats,
                    unique_sellers=unique_sellers,
                    job_id=job_id,
                    page=page,
                    category_id=None,  # Will be handled inside function for each product
                    category_name=None,
                    limit=limit,
                    dry_run=dry_run,
                    client=client,
                    all_categories=categories  # Pass all categories to associate with each product
                )
                
                # Update job progress after processing each page
                if job_id and not dry_run:
                    # Convert current used keywords/categories to comma-separated strings for progress updates
                    keywords_str = ",".join(used_keywords) if used_keywords else None
                    categories_str = ",".join([str(c) for c in used_categories]) if used_categories else None
                    
                    update_job_run_progress(
                        job_id=job_id,
                        found=stats["unique_sellers_found"],
                        new=stats["new_sellers_added"],
                        skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                        errors=stats["errors"],
                        details=prepare_json_safe_dict(stats),
                        keywords=keywords_str,
                        categories=categories_str
                    )
                
                # Stop if we've reached the limit
                if limit and stats["total_products_processed"] >= limit:
                    logger.info(f"Reached limit of {limit} products. Stopping harvest.")
                    break
                    
                page += 1
    
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
            categories_str = ",".join([str(c) for c in used_categories]) if used_categories else None
            
            complete_job_run(
                job_id=job_id,
                found=stats["unique_sellers_found"],
                new=stats["new_sellers_added"],
                skipped=stats["unique_sellers_found"] - stats["new_sellers_added"],
                errors=stats["errors"],
                details=prepare_json_safe_dict(stats),
                keywords=keywords_str,
                categories=categories_str
            )
        
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
