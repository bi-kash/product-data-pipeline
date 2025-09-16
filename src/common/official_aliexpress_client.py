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

from src.common.config import get_env
from src.session.session_manager import get_valid_token_for_code

# Configure logging
logging.basicConfig(
    level=getattr(logging, get_env("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class OfficialAliExpressClient:
    """Client for interacting with the official AliExpress API using iop module."""

    def __init__(self, session_code=None):
        """
        Initialize the Official AliExpress API client.
        
        Args:
            session_code: Session code for authentication. If None, will try to get from env.
        """
        # API configuration
        self.api_url = get_env("IOP_URL", "https://api-sg.aliexpress.com/sync")
        self.auth_url = "https://api-sg.aliexpress.com/rest"  # For authentication endpoints
        self.appkey = get_env("IOP_APPKEY")
        self.app_secret = get_env("IOP_APPSECRET")
        self.session_code = session_code or get_env("ALIEXPRESS_SESSION_CODE")
        
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
            
        if not self.session_code:
            logger.warning("No session code provided. Some API calls may fail.")
            
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
        if not self.session_code:
            raise Exception("No session code configured")
            
        token_result = get_valid_token_for_code(self.session_code)
        
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
            # Get valid session token
            session_token = self._get_valid_session_token()
            
            # Create IOP client and request - use sync endpoint for product operations
            client = iop.IopClient(self.api_url, self.appkey, self.app_secret)
            request = iop.IopRequest(api_method)
            
            # Add session token
            request.add_api_param('session', session_token)
            
            # Add all other parameters
            for key, value in params.items():
                if value is not None:  # Only add non-None parameters
                    request.add_api_param(key, str(value))
            
            # Execute request with rate limiting
            if retry_count > 0:
                time.sleep(self.rate_limit_delay * retry_count)
                
            logger.debug(f"Making API call: {api_method} with params: {params}")
            response = client.execute(request)
            
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

    def search_products(self, keyword=None, category_id=None, page_no=1, page_size=20, sort_by=None):
        """
        Search for products using the official API.
        
        Args:
            keyword: Search keyword
            category_id: Category ID to search in
            page_no: Page number (1-based)
            page_size: Number of products per page (max 50)
            sort_by: Sort method (e.g., 'min_price,asc', 'orders,desc')
            
        Returns:
            dict: API response with products
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
            params['pageSize'] = min(page_size, 50)  # API limit is 50
        if sort_by:
            params['sortBy'] = sort_by
            
        logger.info(f"Searching products: keyword='{keyword}', category='{category_id}', page={page_no}")
        
        response = self._make_api_call('aliexpress.ds.text.search', params)
        return response

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

    def search_products(self, keyword, page_no=1, page_size=20):
        """
        Search for products using a keyword - compatible with harvester expectations.
        
        Args:
            keyword: Search keyword
            page_no: Page number for pagination
            page_size: Number of products per page
            
        Returns:
            tuple: (products_list, total_count, original_count, should_continue) where:
                - products_list: filtered products based on price
                - total_count: total products available from API
                - original_count: number of products returned by API for this page (before filtering)
                - should_continue: whether pagination should continue (False if min price reached)
        """
        try:
            response = self.search_products_raw(
                keyword=keyword,
                page_no=page_no,
                page_size=page_size,
                sort_by='min_price,desc'  # Sort by price descending to get higher priced items first
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
                should_continue = True
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
                    logger.info(f"All {original_count} products on page are below minimum price threshold ({self.min_sale_price}). Stopping pagination.")
                    should_continue = False
                        
                logger.info(f"Found {len(converted_products)} products for keyword '{keyword}' (API returned: {original_count}, total available: {total_count}, continue: {should_continue})")
                return converted_products, total_count, original_count, should_continue
            else:
                logger.warning(f"Unexpected response format: {response}")
                return [], 0, 0, False
                
        except Exception as e:
            logger.error(f"Error searching products by keyword '{keyword}': {e}")
            return [], 0, 0, False

    def search_products_by_category(self, category_ids, page_no=1, page_size=20):
        """
        Search products by category - compatible with harvester expectations.
        
        Args:
            category_ids: List of category IDs or comma-separated string
            page_no: Page number
            page_size: Products per page
            
        Returns:
            tuple: (products_list, total_count, original_count, should_continue) for compatibility with existing harvester
        """
        try:
            # Handle both list and string inputs
            if isinstance(category_ids, list):
                category_id = category_ids[0] if category_ids else None
            else:
                category_id = category_ids.split(',')[0] if category_ids else None
                
            if not category_id:
                logger.warning("No category ID provided")
                return [], 0, 0, False
                
            response = self.search_products_raw(
                category_id=category_id,
                page_no=page_no,
                page_size=page_size,
                sort_by='min_price,desc'  # Sort by price descending to get higher priced items first
            )
            
            # Extract products from nested response
            if 'aliexpress_ds_text_search_response' in response:
                data = response['aliexpress_ds_text_search_response'].get('data', {})
                total_count = data.get('totalCount', 0)
                products_data = data.get('products', {})
                products = products_data.get('selection_search_product', [])
                
                # Track original count before filtering
                original_count = len(products)
                
                # Convert to format expected by existing code
                converted_products = []
                should_continue = True
                below_min_price_count = 0
                
                for product in products:
                    # Count products below minimum price threshold
                    if self.min_sale_price:
                        try:
                            target_price = product.get('target_sale_price', product.get('sale_price', 0))
                            if float(target_price) < self.min_sale_price:
                                below_min_price_count += 1
                        except (ValueError, TypeError):
                            pass  # Continue processing if price can't be parsed
                    
                    if self._should_include_product(product):
                        converted_products.append(self._convert_product_format(product))
                
                # Check if ALL products on this page are below minimum price threshold
                if self.min_sale_price and original_count > 0 and below_min_price_count == original_count:
                    logger.info(f"All {original_count} products on page are below minimum price threshold ({self.min_sale_price}) for category '{category_id}'. Stopping pagination.")
                    should_continue = False
                        
                logger.info(f"Found {len(converted_products)} products for category '{category_id}' (API returned: {original_count}, total available: {total_count}, continue: {should_continue})")
                return converted_products, total_count, original_count, should_continue
            else:
                logger.warning(f"Unexpected response format: {response}")
                return [], 0, 0, False
                
        except Exception as e:
            logger.error(f"Error searching products by category '{category_ids}': {e}")
            return [], 0

    def get_seller_info_from_product(self, product):
        """
        Extract seller information from product data - compatible with harvester expectations.
        This method calls the product details API to get full seller information.
        
        Args:
            product: Product dictionary
            
        Returns:
            dict: Seller information including shop_id
        """
        product_id = product.get('product_id')
        if not product_id:
            logger.warning("No product_id found in product data")
            return {
                'store_id': None,
                'store_name': None,
                'store_url': None,
                'seller_id': None,
                'seller_name': None,
                'shop_name': None,  # Add shop_name field
                'store_info': None,
                'shop_id': None,
                'shop_url': None,
                'raw_json': {},  # Add empty raw_json for consistency
                'product_id': None,
                'item_id': None,
            }
        
        try:
            # Get detailed product information which includes seller/shop data
            details = self.get_product_details(product_id)
            
            if not details or 'aliexpress_ds_product_get_response' not in details:
                logger.warning(f"No product details found for product {product_id}")
                return {
                    'store_id': None,
                    'store_name': None,
                    'store_url': None,
                    'seller_id': None,
                    'seller_name': None,
                    'shop_name': None,  # Add shop_name field
                    'store_info': None,
                    'shop_id': None,
                    'shop_url': None,
                    'raw_json': {},  # Add empty raw_json for consistency
                    'product_id': product_id,
                    'item_id': product_id,
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
            
            return seller_info
            
        except Exception as e:
            logger.error(f"Error getting seller info for product {product_id}: {e}")
            return {
                'store_id': None,
                'store_name': None,
                'store_url': None,
                'seller_id': None,
                'seller_name': None,
                'shop_name': None,  # Add shop_name field
                'store_info': None,
                'shop_id': f"error_shop_{product_id}",  # Provide fallback to prevent KeyError
                'shop_url': f"https://www.aliexpress.com/store/error_{product_id}",  # Provide fallback URL
                'raw_json': {},  # Add empty raw_json for consistency
                'product_id': product_id,
                'item_id': product_id,
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
        
  
        logger.info(f"Searching products: keyword='{keyword}', category='{category_id}', page={page_no}")
        
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