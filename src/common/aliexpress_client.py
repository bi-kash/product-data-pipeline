"""
AliExpress API client using RapidAPI service.
This implementation uses RapidAPI endpoints for reliable and simple access to AliExpress data.
"""

import os
import json
import time
import logging
import requests
from datetime import datetime
from urllib.parse import urlencode
from src.common.config import get_env

# Configure logging
logging.basicConfig(
    level=getattr(logging, get_env("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AliExpressClient:
    """Client for interacting with AliExpress API via RapidAPI."""

    def __init__(self):
        """Initialize the AliExpress RapidAPI client."""
        # RapidAPI keys
        self.rapidapi_key = get_env("RAPIDAPI_KEY")
        self.rapidapi_host = get_env(
            "RAPIDAPI_HOST", "aliexpress-true-api.p.rapidapi.com"
        )

        logger.info(
            f"Initializing AliExpress RapidAPI client with host: {self.rapidapi_host}"
        )
        if not self.rapidapi_key:
            logger.error("No RapidAPI key provided. API will not work!")

        # Headers for RapidAPI
        self.headers = {
            "x-rapidapi-host": self.rapidapi_host,
            "x-rapidapi-key": self.rapidapi_key,
        }

        # API configuration
        self.base_url = f"https://{self.rapidapi_host}"
        self.target_currency = get_env("ALIEXPRESS_TARGET_CURRENCY", "EUR")
        self.target_language = get_env("ALIEXPRESS_TARGET_LANGUAGE", "EN")
        self.target_country = get_env("ALIEXPRESS_TARGET_COUNTRY", "DE")

        # Rate limiting and retry configuration
        self.rate_limit_delay = int(get_env("RATE_LIMIT_DELAY", "1"))
        self.max_retries = int(get_env("MAX_RETRIES", "3"))
        self.timeout = int(get_env("TIMEOUT", "30"))

        # Flag to indicate if API access is working
        self.api_working = False

        # Flag to use mock data if API fails
        self.use_mock_data = get_env("USE_MOCK_DATA", "false").lower() == "true"

    def _call_api(self, endpoint, params=None, retry_count=0):
        """
        Make an API call to the RapidAPI service.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            retry_count: Current retry count

        Returns:
            API response as dictionary
        """

        url = f"{self.base_url}/{endpoint}"

        try:
            logger.info(f"Calling API: {url}")
            logger.debug(f"Params: {params}")

            response = requests.get(
                url=url, headers=self.headers, params=params, timeout=self.timeout
            )

            # Log response status
            logger.info(f"Response status: {response.status_code}")

            # Check for API errors
            if response.status_code != 200:
                logger.error(f"API error ({response.status_code}): {response.text}")
                if retry_count < self.max_retries:
                    retry_count += 1
                    wait_time = self.rate_limit_delay * (
                        2**retry_count
                    )  # Exponential backoff
                    logger.info(
                        f"Retrying in {wait_time} seconds... (Attempt {retry_count}/{self.max_retries})"
                    )
                    time.sleep(wait_time)
                    return self._call_api(endpoint, params, retry_count)

                if self.use_mock_data:
                    logger.warning("Using mock data as fallback")
                    return self._get_mock_data(endpoint)
                return None

            # Parse response
            data = response.json()

            # Add a delay to avoid rate limiting
            time.sleep(self.rate_limit_delay)
            self.api_working = True

            return data

        except Exception as e:
            logger.error(f"Error calling API: {e}")
            if retry_count < self.max_retries:
                retry_count += 1
                wait_time = self.rate_limit_delay * (
                    2**retry_count
                )  # Exponential backoff
                logger.info(
                    f"Retrying in {wait_time} seconds... (Attempt {retry_count}/{self.max_retries})"
                )
                time.sleep(wait_time)
                return self._call_api(endpoint, params, retry_count)

            if self.use_mock_data:
                logger.warning("Using mock data as fallback")
                return self._get_mock_data(endpoint)
            return None

    def _get_mock_data(self, endpoint):
        """
        Get mock data for testing when API is not available.

        Args:
            endpoint: API endpoint that was called

        Returns:
            Mock data for the given endpoint
        """
        if "products" in endpoint:
            return {
                "current_record_count": 10,
                "total_record_count": 100,
                "current_page_no": 1,
                "products": {
                    "product": [
                        {
                            "product_id": "1234567890",
                            "original_price": "10.99",
                            "original_price_currency": "EUR",
                            "target_sale_price": "9.99",
                            "target_sale_price_currency": "EUR",
                            "discount": "10%",
                            "shop_name": "Mock Jewelry Store",
                            "shop_id": "9876543210",
                            "shop_url": "https://www.aliexpress.com/store/9876543210",
                            "product_detail_url": "https://www.aliexpress.com/item/1234567890.html",
                            "product_main_image_url": "https://example.com/image.jpg",
                            "first_level_category_name": "Jewelry & Accessories",
                            "second_level_category_name": "Fine Jewelry",
                        }
                    ]
                },
            }
        elif "product-info" in endpoint:
            return [
                {
                    "product_id": "1234567890",
                    "product_title": "Mock Jewelry Product",
                    "original_price": "10.99",
                    "original_price_currency": "EUR",
                    "target_sale_price": "9.99",
                    "target_sale_price_currency": "EUR",
                    "discount": "10%",
                    "shop_name": "Mock Jewelry Store",
                    "shop_id": "9876543210",
                    "shop_url": "https://www.aliexpress.com/store/9876543210",
                    "product_detail_url": "https://www.aliexpress.com/item/1234567890.html",
                    "evaluate_rate": "97.5%",
                    "first_level_category_name": "Jewelry & Accessories",
                    "second_level_category_name": "Fine Jewelry",
                }
            ]
        return None

    def search_products(self, keyword, page_no=1, page_size=100):
        """
        Search for products using a keyword.

        Args:
            keyword: Search keyword
            page_no: Page number for pagination
            page_size: Number of products per page (max 100)

        Returns:
            List of products matching the search
        """
        endpoint = "api/v3/products"
        params = {
            "keywords": keyword,
            "page_no": str(page_no),
            "page_size": str(page_size),
            "target_currency": self.target_currency,
            "target_language": self.target_language,
            "ship_to_country": self.target_country,
            "sort": "SALE_PRICE_DESC",  # Default sort by sale price descending
        }

        response = self._call_api(endpoint, params)

        if not response:
            return [], None

        # Extract products from response
        products = response.get("products", {}).get("product", [])
        total_count = response.get("total_record_count", 0)

        logger.info(f"Found {len(products)} products (total: {total_count})")

        # Return products and total count as a tuple
        return products, total_count

    def get_product_info(self, product_id):
        """
        Get detailed information for a specific product.

        Args:
            product_id: ID of the product

        Returns:
            Product details
        """
        endpoint = "api/v3/product-info"
        params = {
            "product_id": str(product_id),
            "target_currency": self.target_currency,
            "ship_to_country": self.target_country,
            "target_language": self.target_language,
        }

        response = self._call_api(endpoint, params)

        if not response or not isinstance(response, list) or len(response) == 0:
            return None

        # Return the first product from the list
        return response[0]

    def search_products_by_category(self, category_ids, page_no=1, page_size=50):
        """
        Search for products using one or more category IDs.

        Args:
            category_ids: Category ID or list of category IDs to search
            page_no: Page number for pagination
            page_size: Number of products per page (max 100, default 50)

        Returns:
            List of products matching the categories
        """
        endpoint = "api/v3/products"

        # Convert category_ids to a comma-separated string if it's a list
        if isinstance(category_ids, list):
            category_ids_str = ",".join([str(cid) for cid in category_ids])
        else:
            category_ids_str = str(category_ids)

        params = {
            "category_ids": category_ids_str,
            "page_no": str(page_no),
            "page_size": str(page_size),
            "target_currency": self.target_currency,
            "target_language": self.target_language,
            "ship_to_country": self.target_country,
            "sort": "LAST_VOLUME_ASC",  # Default sort by relevance
        }

        response = self._call_api(endpoint, params)

        if not response:
            return []

        # Extract products from response
        products = response.get("products", {}).get("product", [])
        total_count = response.get("total_record_count", 0)

        logger.info(
            f"Found {len(products)} products in categories {category_ids} (total: {total_count})"
        )

        # Return products and total count as a tuple
        return products, total_count

    def get_seller_info_from_product(self, product):
        """
        Extract seller information from a product object.

        Args:
            product: Product object from the API

        Returns:
            Seller information
        """
        if not product:
            return None

        # Extract seller data
        shop_id = str(product.get("shop_id", ""))
        shop_url = product.get("shop_url", "")
        shop_name = product.get("shop_name", "")

        if not shop_id or not shop_url:
            logger.warning("Missing seller data in product")
            return None

        # Create a seller-only json object
        seller_json = {"shop_id": shop_id, "shop_url": shop_url, "shop_name": shop_name}

        return {
            "shop_id": shop_id,
            "shop_url": shop_url,
            "shop_name": shop_name,
            "raw_json": seller_json,
        }
