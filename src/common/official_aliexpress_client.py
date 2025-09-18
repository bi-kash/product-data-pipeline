"""
Official AliExpress API client using the iop module.
This implementation uses the official AliExpress API endpoints with session-based authentication.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

# Add the iop module to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'iop'))
import iop

from src.common.config import get_env, get_ignore_categories
from src.session.session_manager import get_latest_valid_tokens

# Configure logging
logging.basicConfig(
    level=getattr(logging, get_env("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class OfficialAliExpressClient:
    """Client for interacting with the official AliExpress API using iop module."""

    def __init__(self):
        """
        Initialize the Official AliExpress API client.
        Will automatically fetch the latest valid tokens as needed.
        """
        # API configuration
        self.api_url = get_env("IOP_URL", "https://api-sg.aliexpress.com/sync")
        self.auth_url = "https://api-sg.aliexpress.com/rest"  # For authentication endpoints
        self.appkey = get_env("IOP_APPKEY")
        self.app_secret = get_env("IOP_APPSECRET")
        
        # Target market configuration
        self.target_currency = get_env("ALIEXPRESS_TARGET_CURRENCY", "USD")
        self.target_language = get_env("ALIEXPRESS_TARGET_LANGUAGE", "en_US")
        self.target_country = get_env("ALIEXPRESS_TARGET_COUNTRY", "US")
        
        # Filtering configuration
        self.min_sale_price = get_env("MIN_SALE_PRICE", None)
        if self.min_sale_price:
            try:
                self.min_sale_price = float(self.min_sale_price)
                logger.info(f"Using minimum sale price filter: {self.min_sale_price} {self.target_currency}")
            except ValueError:
                logger.error(f"Invalid MIN_SALE_PRICE value: {self.min_sale_price}. Must be a number.")
                self.min_sale_price = None
        
        self.max_sale_price = get_env("MAX_SALE_PRICE", None)
        if self.max_sale_price:
            try:
                self.max_sale_price = float(self.max_sale_price)
                logger.info(f"Using maximum sale price filter: {self.max_sale_price} {self.target_currency}")
            except ValueError:
                logger.error(f"Invalid MAX_SALE_PRICE value: {self.max_sale_price}. Must be a number.")
                self.max_sale_price = None

        # Rate limiting configuration
        self.rate_limit_delay = int(get_env("RATE_LIMIT_DELAY", "1"))
        self.max_retries = int(get_env("MAX_RETRIES", "3"))
        
        # Validate configuration
        if not (self.appkey and self.app_secret):
            logger.error("IOP_APPKEY and IOP_APPSECRET must be set in .env file")
            raise ValueError("Missing IOP credentials")
            
        logger.info(f"Initializing Official AliExpress API client")
        logger.info(f"API URL: {self.api_url}")
        logger.info(f"Auth URL: {self.auth_url}")
        logger.info(f"Target Currency: {self.target_currency}")
        logger.info(f"Target Country: {self.target_country}")

    def _get_valid_session_token(self):
        """
        Get a valid session token, automatically refreshing if needed.
        
        Returns:
            str: Valid access token
            
        Raises:
            Exception: If no valid token can be obtained
        """
        token_result = get_latest_valid_tokens()
        
        if not token_result['success']:
            raise Exception(f"Failed to get valid token: {token_result['message']}")
            
        if token_result['refreshed']:
            logger.info("Token was automatically refreshed")
            
        return token_result['token']

    def _make_api_call(self, api_method, params, retry_count=0):
        """
        Make an API call to the official AliExpress API.
        
        Args:
            api_method: The API method to call (e.g., 'aliexpress.ds.text.search')
            params: Dictionary of API parameters
            retry_count: Current retry count
            
        Returns:
            dict: API response
        """
        try:
            # Get valid access token
            access_token = self._get_valid_session_token()
            
            # Create IOP client and request - use sync endpoint for product operations
            client = iop.IopClient(self.api_url, self.appkey, self.app_secret)
            request = iop.IopRequest(api_method)
            
            # Add all parameters (do NOT add session/token as parameter)
            for key, value in params.items():
                if value is not None:  # Only add non-None parameters
                    request.add_api_param(key, str(value))
            
            # Execute request with rate limiting
            if retry_count > 0:
                time.sleep(self.rate_limit_delay * retry_count)
                
            logger.debug(f"Making API call: {api_method} with params: {params}")
            # Pass access token directly to execute() method
            response = client.execute(request, access_token)
            
            # Log response details for debugging
            logger.debug(f"Response code: {response.code}")
            logger.debug(f"Response message: {response.message}")
            logger.debug(f"Response type: {response.type}")
            logger.debug(f"Response body: {response.body}")
            
            # Check response - for sync endpoint, success is when response.code is None
            if response.code is not None and response.code != "0":
                error_msg = response.message or "Unknown error"
                logger.error(f"API error ({response.code}): {error_msg}")
                
                # Handle token errors with retry
                if 'token' in error_msg.lower() or 'session' in error_msg.lower():
                    if retry_count < self.max_retries:
                        logger.info(f"Token error, retrying... (attempt {retry_count + 1}/{self.max_retries})")
                        return self._make_api_call(api_method, params, retry_count + 1)
                        
                raise Exception(f"API error: {error_msg}")
            
            # Check for nested API errors in the response body
            if isinstance(response.body, dict):
                for key, value in response.body.items():
                    if isinstance(value, dict) and 'code' in value:
                        # "00" and codes starting with "SUCCESS" are success codes
                        if value['code'] not in ["0", "00"] and not str(value['code']).startswith('SUCCESS'):
                            error_msg = value.get('message', f"API error code: {value['code']}")
                            logger.error(f"Nested API error ({value['code']}): {error_msg}")
                            raise Exception(f"API error: {error_msg}")
            
            return response.body
            
        except Exception as e:
            if retry_count < self.max_retries and 'token' not in str(e).lower():
                logger.warning(f"API call failed, retrying... (attempt {retry_count + 1}/{self.max_retries}): {e}")
                time.sleep(self.rate_limit_delay * (retry_count + 1))
                return self._make_api_call(api_method, params, retry_count + 1)
            else:
                logger.error(f"API call failed after {retry_count + 1} attempts: {e}")
                raise

    def search_products(self, keyword=None, page_no=1, page_size=20, sort_by=None):
        """
        Search for products using keywords and/or category - compatible with harvester expectations.
        
        Args:
            keyword: Search keyword (if provided, will also include category from config)
            page_no: Page number for pagination
            page_size: Number of products per page
            sort_by: Sort method (defaults to min_price,desc for better results)
            
        Returns:
            tuple: (products_list, total_count, original_count, all_below_threshold) where:
                - products_list: filtered products based on price and categories
                - total_count: total products available from API
                - original_count: number of products returned by API for this page (before filtering)
                - all_below_threshold: whether ALL products on this page are below minimum price threshold
        """
        from src.common.config import get_search_category, get_minimum_pagination_pages
        
        # Get category from config if available (for keyword + category search)
        category_id = get_search_category() if keyword else None
        
        # Get minimum pages to check before stopping pagination
        min_pages = get_minimum_pagination_pages()
        
        # Set default sort order for better results
        if not sort_by:
            sort_by = 'min_price,desc'  # Sort by price descending to get higher priced items first
            
        try:
            response = self.search_products_raw(
                keyword=keyword,
                category_id=category_id,
                page_no=page_no,
                page_size=page_size,
                sort_by=sort_by
            )
            
            # Extract products from nested response
            if 'aliexpress_ds_text_search_response' in response:
                data = response['aliexpress_ds_text_search_response'].get('data', {})
                total_count = data.get('totalCount', 0)
                products_data = data.get('products', {})
                products = products_data.get('selection_search_product', [])
                
                # Track original count from API before filtering
                original_count = len(products)
                
                # Convert to format expected by existing code and check for min price threshold
                converted_products = []
                all_below_threshold = False
                below_min_price_count = 0
                
                for product in products:
                    # Count products below minimum price threshold
                    if self.min_sale_price:
                        try:
                            sale_price = float(product.get('targetSalePrice', 0))
                            if sale_price < self.min_sale_price:
                                below_min_price_count += 1
                        except (ValueError, TypeError):
                            pass  # Continue processing if price can't be parsed
                    
                    if self._should_include_product(product):
                        converted_products.append(self._convert_product_format(product))
                
                # Check if ALL products on this page are below minimum price threshold
                if self.min_sale_price and original_count > 0 and below_min_price_count == original_count:
                    all_below_threshold = True
                    logger.info(f"All {original_count} products on page {page_no} are below minimum price threshold ({self.min_sale_price})")
                else:
                    all_below_threshold = False
                        
                search_info = f"keyword='{keyword}'" if keyword else "category-only"
                if keyword and category_id:
                    search_info += f" + category='{category_id}'"
                elif category_id:
                    search_info = f"category='{category_id}'"
                    
                logger.info(f"Found {len(converted_products)} products for {search_info} (API returned: {original_count}, total available: {total_count}, all below threshold: {all_below_threshold})")
                return converted_products, total_count, original_count, all_below_threshold
            else:
                logger.warning(f"Unexpected response format: {response}")
                return [], 0, 0, False
                
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return [], 0, 0, False

    def get_product_details(self, product_id):
        """
        Get detailed product information using product ID.
        
        Args:
            product_id: The product/item ID
            
        Returns:
            dict: Detailed product information
        """
        params = {
            'ship_to_country': self.target_country,
            'product_id': product_id,
            'target_currency': self.target_currency,
            'target_language': self.target_language,
            'remove_personal_benefit': 'false'
        }
        
        logger.info(f"Getting product details for ID: {product_id}")
        
        response = self._make_api_call('aliexpress.ds.product.get', params)
        return response

    def search_products_by_keyword(self, keyword, page_no=1, page_size=20):
        """
        Search products by keyword (legacy method for compatibility).
        
        Args:
            keyword: Search keyword
            page_no: Page number
            page_size: Products per page
            
        Returns:
            list: List of product data compatible with existing code
        """
        try:
            response = self.search_products(
                keyword=keyword,
                page_no=page_no,
                page_size=page_size,
                sort_by='orders,desc'  # Sort by popularity
            )
            
            # Extract products from nested response
            if 'aliexpress_ds_text_search_response' in response:
                data = response['aliexpress_ds_text_search_response'].get('data', {})
                products_data = data.get('products', {})
                products = products_data.get('selection_search_product', [])
                
                # Convert to format expected by existing code
                converted_products = []
                for product in products:
                    if self._should_include_product(product):
                        converted_products.append(self._convert_product_format(product))
                        
                logger.info(f"Found {len(converted_products)} products for keyword '{keyword}'")
                return converted_products
            else:
                logger.warning(f"Unexpected response format: {response}")
                return []
                
        except Exception as e:
            logger.error(f"Error searching products by keyword '{keyword}': {e}")
            return []

    def get_seller_info_from_product(self, product):
        """
        Extract seller information from product data - compatible with harvester expectations.
        This method calls the product details API to get full seller information.
        
        Args:
            product: Product dictionary
            
        Returns:
            dict: Seller information including shop_id
        """
        result = self.get_seller_and_product_details(product)
        return result['seller_info']
    
    def get_seller_and_product_details(self, product):
        """
        Extract both seller information and product details from product data.
        This method fetches product details once and returns both seller info and the full details.
        
        Args:
            product: Product dictionary
            
        Returns:
            dict: Contains 'seller_info' and 'product_details' keys
        """
        product_id = product.get('product_id')
        if not product_id:
            logger.warning("No product_id found in product data")
            return {
                'seller_info': {
                    'store_id': None,
                    'store_name': None,
                    'store_url': None,
                    'seller_id': None,
                    'seller_name': None,
                    'shop_name': None,
                    'store_info': None,
                    'shop_id': None,
                    'shop_url': None,
                    'raw_json': {},
                    'product_id': None,
                    'item_id': None,
                },
                'product_details': None
            }
        
        try:
            # Get detailed product information which includes seller/shop data
            details = self.get_product_details(product_id)
            
            if not details or 'aliexpress_ds_product_get_response' not in details:
                logger.warning(f"No product details found for product {product_id}")
                return {
                    'seller_info': {
                        'store_id': None,
                        'store_name': None,
                        'store_url': None,
                        'seller_id': None,
                        'seller_name': None,
                        'shop_name': None,
                        'store_info': None,
                        'shop_id': None,
                        'shop_url': None,
                        'raw_json': {},
                        'product_id': product_id,
                        'item_id': product_id,
                    },
                    'product_details': details
                }
            
            result = details['aliexpress_ds_product_get_response'].get('result', {})
            
            # Extract seller/shop information from product details
            # The AliExpress API returns store info in 'ae_store_info' field
            store_info = result.get('ae_store_info', {})
            
            # Map the fields according to what the harvester expects
            seller_info = {
                'store_id': store_info.get('store_id') or store_info.get('storeId'),
                'store_name': store_info.get('store_name') or store_info.get('storeName'),
                'store_url': store_info.get('store_url') or store_info.get('storeUrl'),
                'seller_id': store_info.get('store_id') or store_info.get('storeId'),  # Often same as store_id
                'seller_name': store_info.get('store_name') or store_info.get('storeName'),  # Often same as store_name
                'shop_name': store_info.get('store_name') or store_info.get('storeName'),  # Add shop_name field for harvester
                'store_info': store_info,
                'shop_id': store_info.get('store_id') or store_info.get('storeId'),  # shop_id is often the store_id
                'shop_url': store_info.get('store_url') or store_info.get('storeUrl'),  # Add shop_url field
                'raw_json': store_info,  # Add raw_json field for harvester compatibility
                'product_id': product_id,
                'item_id': product_id,
            }
            
            # Generate store_url if not provided by API
            if not seller_info['shop_url'] and seller_info['shop_id']:
                seller_info['shop_url'] = f"https://www.aliexpress.com/store/{seller_info['shop_id']}"
                seller_info['store_url'] = seller_info['shop_url']
            
            # If store_info is empty or doesn't have the expected fields, try other common field names
            if not seller_info['shop_id']:
                # Try alternative field mappings from the root result
                for field_name in ['shop_id', 'shopId', 'storeId', 'store_id', 'sellerId', 'seller_id']:
                    if field_name in result:
                        seller_info['shop_id'] = result[field_name]
                        seller_info['store_id'] = result[field_name]
                        seller_info['seller_id'] = result[field_name]
                        break
                
                # If still no shop_id, log available fields for debugging
                if not seller_info['shop_id']:
                    logger.warning(f"Could not find shop_id in product details for {product_id}. Available fields: {list(result.keys())}")
                    logger.warning(f"ae_store_info content: {store_info}")
                    # As a fallback, use the product_id as shop identifier
                    seller_info['shop_id'] = f"unknown_shop_{product_id}"
                    seller_info['shop_url'] = f"https://www.aliexpress.com/store/unknown_{product_id}"
                    seller_info['raw_json'] = store_info or result  # Ensure raw_json is always present
            
            return {
                'seller_info': seller_info,
                'product_details': details
            }
            
        except Exception as e:
            logger.error(f"Error getting seller info for product {product_id}: {e}")
            return {
                'seller_info': {
                    'store_id': None,
                    'store_name': None,
                    'store_url': None,
                    'seller_id': None,
                    'seller_name': None,
                    'shop_name': None,
                    'store_info': None,
                    'shop_id': f"error_shop_{product_id}",  # Provide fallback to prevent KeyError
                    'shop_url': f"https://www.aliexpress.com/store/error_{product_id}",  # Provide fallback URL
                    'raw_json': {},
                    'product_id': product_id,
                    'item_id': product_id,
                },
                'product_details': None
            }

    def search_products_raw(self, keyword=None, category_id=None, page_no=1, page_size=20, sort_by=None):
        """
        Search for products using the official API - raw response.
        
        Args:
            keyword: Search keyword
            category_id: Category ID to search in
            page_no: Page number (1-based)
            page_size: Number of products per page (max 50)
            sort_by: Sort method (e.g., 'min_price,asc', 'orders,desc')
            
        Returns:
            dict: Raw API response
        """
        params = {
            'local': self.target_language,
            'countryCode': self.target_country,
            'currency': self.target_currency,
        }
        
        # Add search criteria
        if keyword:
            params['keyWord'] = keyword
        if category_id:
            params['categoryId'] = category_id
        
        # Only add pagination and sorting if they're supported
        # Note: Some basic searches work without these parameters
        if page_no > 1 or page_size != 20:  # Only add if non-default
            params['pageIndex'] = page_no
            params['pageSize'] = min(page_size, 100)  # API limit is 50
        if sort_by:
            params['sortBy'] = sort_by
        
  
        logger.info(f"Searching products: keyword='{keyword or 'N/A'}', category='{category_id or 'N/A'}', page={page_no}")
        
        response = self._make_api_call('aliexpress.ds.text.search', params)
        return response


    def _convert_product_format(self, product):
        """
        Convert official API product format to format expected by existing code.
        
        Args:
            product: Product data from official API
            
        Returns:
            dict: Converted product data
        """
        return {
            'product_id': product.get('itemId'),
            'product_title': product.get('title'),
            'product_main_image_url': product.get('itemMainPic'),
            'product_detail_url': f"https:{product.get('itemUrl')}" if product.get('itemUrl', '').startswith('//') else product.get('itemUrl'),
            'original_price': product.get('salePrice'),  # Original price in USD
            'target_sale_price': product.get('targetSalePrice'),   # Target price in target currency (EUR)
            'original_price_currency': product.get('originalPriceCurrency', 'USD'),  # Original currency
            'target_sale_price_currency': product.get('targetOriginalPriceCurrency', 'EUR'),  # Target currency
            'discount': product.get('discount'),
            'evaluate_rate': product.get('evaluateRate') if product.get('evaluateRate') and product.get('evaluateRate').strip() else None,
            'orders': product.get('orders'),
            'category_id': product.get('cateId'),  # Category ID from API
            'raw_json': product  # Store original response for reference
        }

    def _should_include_product(self, product):
        """
        Check if a product should be included based on filtering criteria.
        
        Args:
            product: Product data from API
            
        Returns:
            bool: True if product should be included
        """
        # Apply category filters first (early return for ignored categories)
        ignore_categories = get_ignore_categories()
        if ignore_categories:
            product_categories = product.get('cateId', '')
            if product_categories:
                # Product categories can be comma-separated string
                if isinstance(product_categories, str):
                    product_cat_list = [cat.strip() for cat in product_categories.split(',') if cat.strip()]
                elif isinstance(product_categories, list):
                    product_cat_list = [str(cat).strip() for cat in product_categories if str(cat).strip()]
                else:
                    product_cat_list = [str(product_categories).strip()]
                
                # Check if any product category matches any ignored category
                for product_cat in product_cat_list:
                    if product_cat in ignore_categories:
                        logger.info(f"Product {product.get('itemId')} ignored because it contains category ID: {product_cat} (in IGNORE_CATEGORIES)")
                        return False
        
        # Apply price filters
        if self.min_sale_price or self.max_sale_price:
            try:
                # Use target sale price (USD) for filtering
                sale_price = float(product.get('targetSalePrice', 0))
                if self.max_sale_price and sale_price > self.max_sale_price:
                    logger.debug(f"Filtering out product {product.get('itemId')} - price {sale_price} above maximum {self.max_sale_price}")
                    return False
                if self.min_sale_price and sale_price < self.min_sale_price:
                    logger.debug(f"Filtering out product {product.get('itemId')} - price {sale_price} below minimum {self.min_sale_price}")
                    return False
            except (ValueError, TypeError):
                logger.debug(f"Could not parse price for product {product.get('itemId')}")
                
        return True

    def query_freight(self, product_id, selected_sku_id, quantity=1, ship_to_country=None):
        """
        Query freight/shipping information for a product using aliexpress.ds.freight.query API.
        
        Args:
            product_id: Product ID to query freight for
            selected_sku_id: Specific SKU ID (required)
            quantity: Quantity to ship (default: 1)
            ship_to_country: Destination country code (e.g., "FR", "DE", "US")
            
        Returns:
            dict: Freight query response with delivery options
        """
        # Use target country if not specified
        if not ship_to_country:
            ship_to_country = self.target_country
            
        # Build the query delivery request payload as dict then convert to JSON string
        query_delivery_req = {
            "quantity": str(quantity),
            "shipToCountry": ship_to_country,
            "productId": str(product_id),
            "selectedSkuId": str(selected_sku_id),
            "language": self.target_language,
            "currency": self.target_currency,
            "locale": "zh_CN"  # Default locale for API
        }
            
        params = {
            'queryDeliveryReq': json.dumps(query_delivery_req)  # Use proper JSON formatting
        }
        
        logger.info(f"Querying freight for product {product_id} to {ship_to_country}")
        logger.debug(f"Freight query params: {params}")
        
        try:
            response = self._make_api_call('aliexpress.ds.freight.query', params)
            return response
        except Exception as e:
            logger.error(f"Error querying freight for product {product_id}: {e}")
            return None