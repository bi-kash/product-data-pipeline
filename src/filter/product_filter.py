"""
Product Filter Engine - Business Rules Implementation
====================================================

This module implements the core business rules for filtering products from whitelisted sellers.

Business Rules:
1. Price Rule: (Most expensive variant + cheapest shipping) <= TOTAL_MAX_PRICE
2. Shipping Rule: delivery_time <= MAX_DELIVERY_DAYS
3. Shipping Badge Detection: Identifies shipping types from API data

The module processes products from whitelisted sellers and stores qualifying
products in the filtered_products table.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from src.common.database import (
    Product, 
    Seller, 
    FilteredProduct,
    ShippingInfo,
    get_db_session
)
from src.common.config import get_env
from src.common.official_aliexpress_client import OfficialAliExpressClient

# Configure logging
logger = logging.getLogger(__name__)


class ProductFilterEngine:
    """Main engine for filtering products based on business rules."""
    
    def __init__(self, max_price_eur: float = None, max_delivery_days: int = None):
        """
        Initialize the filter engine.
        
        Args:
            max_price_eur: Maximum total cost in EUR (variant + shipping)
            max_delivery_days: Maximum delivery time in days
        """
        self.max_price_eur = max_price_eur or float(get_env("TOTAL_MAX_PRICE", "100"))
        self.max_delivery_days = max_delivery_days or int(get_env("MAX_DELIVERY_DAYS", "8"))
        
        # Initialize API client for enrichment
        try:
            self.api_client = OfficialAliExpressClient()
            logger.info("API client initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize API client: {e}")
            self.api_client = None
        
        logger.info(f"Filter engine initialized: max_price_eur={self.max_price_eur}, max_delivery_days={self.max_delivery_days}")

    def process_whitelisted_products(self, limit: int = None, dry_run: bool = False) -> Dict[str, int]:
        """
        Process products from whitelisted sellers through filtering rules.
        
        Args:
            limit: Maximum number of products to process
            dry_run: If True, don't save to database
            
        Returns:
            Dict with processing statistics
        """
        stats = {
            'products_processed': 0,
            'products_passed_filter': 0,
            'products_failed_price_rule': 0,
            'products_failed_shipping_rule': 0,
            'products_api_enriched': 0,
            'errors': 0
        }
        
        db = get_db_session()
        
        try:
            # Get products from whitelisted sellers
            whitelisted_products = self._get_whitelisted_products(db, limit)
            logger.info(f"Found {len(whitelisted_products)} products from whitelisted sellers")
            
            for product in whitelisted_products:
                try:
                    # Use a savepoint for each product to allow partial rollbacks
                    savepoint = db.begin_nested()
                    
                    # Only count as processed if we're actually going to process it
                    stats['products_processed'] += 1
                    logger.info(f"Processing product {product.product_id} from seller {product.shop_id}")
                    
                    # Apply filtering rules
                    filter_result = self._apply_filtering_rules(product, db)
                    
                    # Update statistics
                    if filter_result['passed_price_rule']:
                        if not filter_result['passed_shipping_rule']:
                            stats['products_failed_shipping_rule'] += 1
                    else:
                        stats['products_failed_price_rule'] += 1
                    
                    if filter_result['api_enriched']:
                        stats['products_api_enriched'] += 1
                    
                    # If product passes all rules, create filtered product entry
                    if filter_result['passed_price_rule'] and filter_result['passed_shipping_rule']:
                        if not dry_run:
                            self._create_filtered_product(product, filter_result, db)
                        stats['products_passed_filter'] += 1
                        logger.info(f"✅ Product {product.product_id} passed all filters")
                        savepoint.commit()
                    else:
                        logger.info(f"❌ Product {product.product_id} failed filters: price={filter_result['passed_price_rule']}, shipping={filter_result['passed_shipping_rule']}")
                        savepoint.rollback()
                
                except Exception as e:
                    logger.error(f"Error processing product {product.product_id}: {e}")
                    stats['errors'] += 1
                    # Rollback the savepoint for this product
                    try:
                        savepoint.rollback()
                    except Exception as rollback_error:
                        logger.error(f"Error during savepoint rollback: {rollback_error}")
            
            # Commit all successful changes
            if not dry_run:
                db.commit()
            
        except Exception as e:
            logger.error(f"Error in filter processing: {e}")
            # Rollback the entire transaction
            db.rollback()
            raise
        finally:
            db.close()
        
        return stats

    def _get_whitelisted_products(self, db, limit: int = None) -> List[Product]:
        """Get products from whitelisted sellers that haven't been filtered yet."""
        # Query for products from whitelisted sellers that are NOT already in filtered_products
        query = db.query(Product).join(Seller).outerjoin(
            FilteredProduct, Product.product_id == FilteredProduct.product_id
        ).filter(
            Seller.approval_status == 'WHITELIST',
            FilteredProduct.product_id.is_(None)  # Only products not yet filtered
        )
        
        if limit:
            query = query.limit(limit)
        
        return query.all()

    def _apply_filtering_rules(self, product: Product, db) -> Dict:
        """
        Apply all filtering rules to a product.
        
        Returns:
            Dict with filtering results and extracted data
        """
        result = {
            'passed_price_rule': False,
            'passed_shipping_rule': False,
            'max_variant_price': None,
            'min_shipping_cost': None,
            'total_cost': None,
            'delivery_time': None,
            'shipping_type': 'Standard',
            'shipping_speed_rating': None,
            'api_enriched': False
        }
        
        # Get detailed product information
        product_data = self._get_enriched_product_data(product)
        if product_data:
            result['api_enriched'] = True
        
        # Apply price rule
        price_result = self._apply_price_rule(product, product_data, db)
        result.update(price_result)
        
        # Apply shipping rule
        shipping_result = self._apply_shipping_rule(product, product_data)
        result.update(shipping_result)
        
        return result

    def _get_enriched_product_data(self, product: Product) -> Optional[Dict]:
        """
        Get enriched product data from API or existing raw_json_detail.
        
        Returns:
            Product detail data dict or None
        """
        # First check if we already have detailed data
        if product.raw_json_detail:
            logger.debug(f"Using existing product detail data for {product.product_id}")
            return product.raw_json_detail
        
        # If API client available, fetch fresh data
        if self.api_client:
            try:
                logger.debug(f"Fetching product details from API for {product.product_id}")
                detail_data = self.api_client.get_product_details(product.product_id)
                return detail_data
            except Exception as e:
                logger.warning(f"Failed to fetch product details for {product.product_id}: {e}")
        
        return None

    def _apply_price_rule(self, product: Product, product_data: Optional[Dict], db=None) -> Dict:
        """
        Apply price filtering rule: (max_variant_price + min_shipping_cost) <= max_price_eur
        
        Args:
            product: Product to apply price rule to
            product_data: Product detail data
            db: Database session (optional, for getting shipping costs)
        
        Returns:
            Dict with price rule results
        """
        result = {
            'passed_price_rule': False,
            'max_variant_price': None,
            'min_shipping_cost': 0.0,  # Default to 0 if no shipping data
            'total_cost': None
        }
        
        try:
            # Extract variant prices from product detail data
            max_variant_price = self._extract_max_variant_price(product, product_data)
            
            # Get minimum shipping cost from existing data if available
            min_shipping_cost = self._get_min_shipping_cost(product, db) if db else 0.0
            
            if max_variant_price is not None:
                total_cost = max_variant_price + min_shipping_cost
                
                result.update({
                    'max_variant_price': max_variant_price,
                    'min_shipping_cost': min_shipping_cost,
                    'total_cost': total_cost,
                    'passed_price_rule': total_cost <= self.max_price_eur
                })
                
                logger.debug(f"Price rule for {product.product_id}: max_variant={max_variant_price}, shipping={min_shipping_cost}, total={total_cost}, limit={self.max_price_eur}, passed={result['passed_price_rule']}")
            else:
                # Fallback to existing price data
                if product.target_sale_price:
                    result.update({
                        'max_variant_price': product.target_sale_price,
                        'min_shipping_cost': min_shipping_cost,
                        'total_cost': product.target_sale_price + min_shipping_cost,
                        'passed_price_rule': (product.target_sale_price + min_shipping_cost) <= self.max_price_eur
                    })
                    logger.debug(f"Price rule fallback for {product.product_id}: using target_sale_price={product.target_sale_price}")
        
        except Exception as e:
            logger.error(f"Error applying price rule to {product.product_id}: {e}")
        
        return result

    def _apply_shipping_rule(self, product: Product, product_data: Optional[Dict]) -> Dict:
        """
        Apply shipping filtering rule: delivery_time <= max_delivery_days
        
        Returns:
            Dict with shipping rule results
        """
        result = {
            'passed_shipping_rule': False,
            'delivery_time': None,
            'ship_to_country': None,
            'shipping_type': 'Standard',
            'shipping_speed_rating': None
        }
        
        try:
            # Extract delivery time and shipping info
            delivery_time = self._extract_delivery_time(product_data)
            ship_to_country = self._extract_ship_to_country(product_data)
            shipping_type = self._extract_shipping_type(product_data)
            shipping_speed_rating = self._extract_shipping_speed_rating(product_data)
            
            result.update({
                'delivery_time': delivery_time,
                'ship_to_country': ship_to_country,
                'shipping_type': shipping_type,
                'shipping_speed_rating': shipping_speed_rating
            })
            
            # Apply shipping rule
            if delivery_time is not None:
                result['passed_shipping_rule'] = delivery_time <= self.max_delivery_days
                logger.debug(f"Shipping rule for {product.product_id}: delivery_time={delivery_time}, limit={self.max_delivery_days}, passed={result['passed_shipping_rule']}")
            else:
                # If no delivery time data, pass the rule but log the missing data
                result['passed_shipping_rule'] = True
                logger.warning(f"Shipping rule for {product.product_id}: no delivery time data available, passing by default")
        
        except Exception as e:
            logger.error(f"Error applying shipping rule to {product.product_id}: {e}")
        
        return result

    def _extract_max_variant_price(self, product: Product, product_data: Optional[Dict]) -> Optional[float]:
        """Extract the highest variant price from product data."""
        max_variant_info = self._extract_max_variant_info(product, product_data)
        return max_variant_info['price'] if max_variant_info else None

    def _extract_max_variant_info(self, product: Product, product_data: Optional[Dict]) -> Optional[Dict]:
        """Extract the highest variant price and its associated SKU ID from product data."""
        if not product_data:
            logger.debug(f"No product data for {product.product_id}")
            return None
        
        try:
            # Navigate to SKU information
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            sku_info = result.get('ae_item_sku_info_dtos', {})
            skus = sku_info.get('ae_item_sku_info_d_t_o', [])
            
            logger.debug(f"Found {len(skus)} SKUs for product {product.product_id}")
            
            if not skus:
                return None
            
            max_price = 0.0
            max_price_sku_id = None
            prices_found = []
            
            for i, sku in enumerate(skus):
                sku_id = sku.get('sku_id')
                # Prioritize sale prices over regular prices
                price_fields = ['offer_sale_price', 'offer_bulk_sale_price']
                for field in price_fields:
                    if field in sku:
                        try:
                            price = float(sku[field])
                            prices_found.append(f"SKU{i}({sku_id}):{field}={price}")
                            if price > max_price:
                                max_price = price
                                max_price_sku_id = sku_id
                            break  # Stop after finding first valid price for this SKU
                        except (ValueError, TypeError):
                            continue
            
            logger.debug(f"Sale prices found for {product.product_id}: {prices_found}, max_price={max_price}, max_sku_id={max_price_sku_id}")
            
            if max_price > 0 and max_price_sku_id:
                return {
                    'price': max_price,
                    'sku_id': max_price_sku_id
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting variant prices: {e}")
            return None

    def _extract_delivery_time(self, product_data: Optional[Dict]) -> Optional[int]:
        """Extract delivery time from product data."""
        if not product_data:
            return None
        
        try:
            # Check logistics_info_dto.delivery_time (correct path based on output_product_get.json)
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            logistics_info = result.get('logistics_info_dto', {})
            
            delivery_time = logistics_info.get('delivery_time')
            if delivery_time is not None:
                return int(delivery_time)
            
            logger.debug(f"No delivery_time found in logistics_info_dto")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting delivery time: {e}")
            return None

    def _extract_ship_to_country(self, product_data: Optional[Dict]) -> Optional[str]:
        """Extract ship_to_country from product data."""
        if not product_data:
            return None
        
        try:
            # Check logistics_info_dto.ship_to_country
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            logistics_info = result.get('logistics_info_dto', {})
            
            ship_to_country = logistics_info.get('ship_to_country')
            return ship_to_country
            
        except Exception as e:
            logger.error(f"Error extracting ship_to_country: {e}")
            return None

    def _extract_shipping_type(self, product_data: Optional[Dict]) -> str:
        """Extract shipping type (Choice, Local+, Standard) from product data."""
        if not product_data:
            return 'Standard'
        
        try:
            # This is a simplified implementation
            # In a real scenario, you'd look for specific badges or indicators
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            
            # Look for Choice or Local+ indicators
            # This would need to be expanded based on actual API response structure
            
            return 'Standard'  # Default for now
            
        except Exception as e:
            logger.error(f"Error extracting shipping type: {e}")
            return 'Standard'

    def _extract_shipping_speed_rating(self, product_data: Optional[Dict]) -> Optional[str]:
        """Extract shipping speed rating from product data."""
        if not product_data:
            return None
        
        try:
            response = product_data.get('aliexpress_ds_product_get_response', {})
            result = response.get('result', {})
            
            return result.get('shipping_speed_rating')
            
        except Exception as e:
            logger.error(f"Error extracting shipping speed rating: {e}")
            return None

    def _get_min_shipping_cost(self, product: Product, db) -> float:
        """
        Get the minimum shipping cost for a product from existing data.
        
        Args:
            product: Product to get shipping cost for
            db: Database session
            
        Returns:
            Minimum shipping cost (0.0 if no data available)
        """
        try:
            # First, check if we have it in filtered_products
            from src.common.database import FilteredProduct
            filtered_product = db.query(FilteredProduct).filter(
                FilteredProduct.product_id == product.product_id
            ).first()
            
            if filtered_product and filtered_product.min_shipping_price is not None:
                logger.debug(f"Found existing min_shipping_price for product {product.product_id}: {filtered_product.min_shipping_price}")
                return filtered_product.min_shipping_price
            
            # If not in filtered_products, check shipping_info table
            from src.common.database import ShippingInfo
            shipping_info = db.query(ShippingInfo).filter(
                ShippingInfo.product_id == product.product_id
            ).first()
            
            if shipping_info and shipping_info.shipping_fee is not None:
                logger.debug(f"Found shipping fee in shipping_info for product {product.product_id}: {shipping_info.shipping_fee}")
                return shipping_info.shipping_fee
            
            # Default to 0.0 if no shipping data available
            logger.debug(f"No shipping cost data found for product {product.product_id}, using 0.0")
            return 0.0
            
        except Exception as e:
            logger.error(f"Error getting min shipping cost for product {product.product_id}: {e}")
            return 0.0

    def _create_filtered_product(self, product: Product, filter_result: Dict, db) -> FilteredProduct:
        """Create a new filtered product entry and fetch detailed shipping information."""
        filtered_product = FilteredProduct(
            # Copy all fields from the original product
            product_id=product.product_id,
            shop_id=product.shop_id,
            product_title=product.product_title,
            product_detail_url=product.product_detail_url,
            product_main_image_url=product.product_main_image_url,
            original_price=product.original_price,
            target_sale_price=product.target_sale_price,
            original_price_currency=product.original_price_currency,
            target_sale_price_currency=product.target_sale_price_currency,
            discount=product.discount,
            evaluate_rate=product.evaluate_rate,
            category_id=product.category_id,
            first_seen_at=product.first_seen_at,
            last_seen_at=product.last_seen_at,
            raw_json_detail=product.raw_json_detail,
            
            # Add the three extra fields specific to filtered products
            ship_to_country=filter_result.get('ship_to_country'),
            delivery_time=filter_result.get('delivery_time'),
            max_variant_price=filter_result.get('max_variant_price'),
            min_shipping_price=None  # Will be updated after freight queries
        )
        
        # Add to database
        db.add(filtered_product)
        db.flush()  # Ensure filtered_product is in DB before adding shipping info
        
        # Fetch detailed shipping information using freight query API
        self._fetch_and_store_shipping_info(product, db)
        
        return filtered_product

    def _fetch_and_store_shipping_info(self, product: Product, db):
        """
        Fetch shipping information for the max-priced variant only and store the cheapest valid option.
        
        Args:
            product: Product to fetch shipping info for
            db: Database session
        """
        if not self.api_client:
            logger.warning(f"No API client available for freight query for product {product.product_id}")
            return
            
        try:
            # Get max delivery days threshold from environment
            max_delivery_days = int(get_env("MAX_DELIVERY_DAYS", 30))
            
            # Get ship_to_country from environment or default to target country
            ship_to_country = get_env("ALIEXPRESS_TARGET_COUNTRY", "DE")
            
            # Extract max-priced variant information
            max_variant_info = self._extract_max_variant_info(product, product.raw_json_detail)
            if not max_variant_info:
                logger.warning(f"No max variant info found for product {product.product_id}")
                return
                
            max_sku_id = max_variant_info['sku_id']
            max_price = max_variant_info['price']
            
            logger.info(f"Processing max-priced variant for product {product.product_id}: SKU {max_sku_id}, price {max_price}")
            
            try:
                logger.debug(f"Querying freight for product {product.product_id}, max-priced SKU {max_sku_id}")
                
                # Query freight information for the max-priced SKU only
                freight_response = self.api_client.query_freight(
                    product_id=product.product_id,
                    selected_sku_id=max_sku_id,
                    ship_to_country=ship_to_country,
                    quantity=1
                )
                
                if not freight_response:
                    logger.warning(f"No freight response for product {product.product_id}, SKU {max_sku_id}")
                    return
                
                # Extract delivery options from response
                if isinstance(freight_response, dict) and 'aliexpress_ds_freight_query_response' in freight_response:
                    result = freight_response['aliexpress_ds_freight_query_response'].get('result', {})
                    
                    # Ensure result is a dictionary
                    if not isinstance(result, dict):
                        logger.warning(f"Unexpected result format for product {product.product_id}, SKU {max_sku_id}: result is {type(result)}")
                        return
                        
                    delivery_options = result.get('delivery_options', [])
                    
                    # Handle different possible structures for delivery_options
                    if isinstance(delivery_options, dict):
                        # Check for the actual API response structure
                        if 'delivery_option_d_t_o' in delivery_options:
                            delivery_list = delivery_options['delivery_option_d_t_o']
                        elif 'aeop_logistics_dto' in delivery_options:
                            delivery_list = delivery_options['aeop_logistics_dto']
                        elif 'aeop_ds_logistics_dto' in delivery_options:
                            delivery_list = delivery_options['aeop_ds_logistics_dto']
                        else:
                            # If it's a single delivery option in dict format, wrap it in a list
                            delivery_list = [delivery_options]
                    elif isinstance(delivery_options, list):
                        delivery_list = delivery_options
                    else:
                        logger.warning(f"Unexpected delivery_options format for product {product.product_id}, SKU {max_sku_id}: {type(delivery_options)}")
                        return
                    
                    logger.info(f"Found {len(delivery_list)} shipping options for product {product.product_id}, max-priced SKU {max_sku_id}")
                    
                    # Filter valid shipping options and prioritize free shipping
                    valid_options = []
                    free_shipping_options = []
                    
                    for i, option in enumerate(delivery_list):
                        # Ensure each option is a dictionary
                        if not isinstance(option, dict):
                            logger.warning(f"Skipping delivery option {i} for product {product.product_id}, SKU {max_sku_id}: option is {type(option)}, value: {option}")
                            continue
                        
                        # Check delivery time constraint
                        max_delivery = self._parse_int(option.get('max_delivery_days'))
                        if max_delivery is not None and max_delivery > max_delivery_days:
                            logger.debug(f"Skipping shipping option {i} for product {product.product_id}: delivery time {max_delivery} exceeds limit {max_delivery_days}")
                            continue
                        
                        # Check if this is a free shipping option
                        is_free_shipping = self._parse_bool(option.get('free_shipping'))
                        
                        # Extract shipping fee
                        shipping_fee = self._parse_float(option.get('shipping_fee_cent'))
                        if shipping_fee is None:
                            # Try alternative field names for shipping fee
                            shipping_fee = self._parse_float(option.get('logisticsFee'))
                        
                        # If free shipping is available and meets delivery time, prioritize it
                        if is_free_shipping:
                            free_shipping_options.append({
                                'option': option,
                                'shipping_fee': 0.0,  # Free shipping cost is 0
                                'index': i
                            })
                            logger.debug(f"Found free shipping option {i} for product {product.product_id}: delivery {max_delivery} days")
                        elif shipping_fee is not None:
                            valid_options.append({
                                'option': option,
                                'shipping_fee': shipping_fee,
                                'index': i
                            })
                    
                    # Prioritize free shipping options first, then regular options
                    all_valid_options = free_shipping_options + valid_options
                    
                    if not all_valid_options:
                        logger.warning(f"No valid shipping options found for product {product.product_id} (delivery time <= {max_delivery_days} days)")
                        return
                    
                    # Find the best option (free shipping first, then cheapest paid option)
                    if free_shipping_options:
                        # If we have free shipping options, pick the fastest free option
                        best_free_option = min(free_shipping_options, key=lambda x: self._parse_int(x['option'].get('max_delivery_days', 999)))
                        best_option = best_free_option['option']
                        min_shipping_price = 0.0
                        logger.info(f"Selected free shipping option for product {product.product_id}: delivery {self._parse_int(best_option.get('max_delivery_days'))} days")
                    else:
                        # No free shipping, pick the cheapest paid option
                        cheapest_option = min(valid_options, key=lambda x: x['shipping_fee'])
                        best_option = cheapest_option['option']
                        min_shipping_price = cheapest_option['shipping_fee']
                        logger.info(f"Selected cheapest paid shipping option for product {product.product_id}: fee {min_shipping_price}")
                    
                    logger.info(f"Selected shipping option for product {product.product_id}: fee {min_shipping_price}, delivery {self._parse_int(best_option.get('max_delivery_days'))} days")
                    
                    # Store only the best shipping option (free or cheapest paid)
                    shipping_info = ShippingInfo(
                        product_id=product.product_id,
                        sku_id=max_sku_id,
                        code=best_option.get('code'),
                        company=best_option.get('company'),
                        shipping_fee=min_shipping_price,
                        shipping_fee_currency=best_option.get('shipping_fee_currency'),
                        free_shipping=self._parse_bool(best_option.get('free_shipping')),
                        min_delivery_days=self._parse_int(best_option.get('min_delivery_days')),
                        max_delivery_days=self._parse_int(best_option.get('max_delivery_days')),
                        guaranteed_delivery_days=self._parse_int(best_option.get('guaranteed_delivery_days')),
                        ship_from_country=best_option.get('ship_from_country'),
                        tracking=self._parse_bool(best_option.get('tracking')),
                        raw_freight_response=freight_response
                    )
                    
                    # Add shipping info to database
                    db.add(shipping_info)
                    
                    # Update filtered_products with minimum shipping price
                    filtered_product = db.query(FilteredProduct).filter(
                        FilteredProduct.product_id == product.product_id
                    ).first()
                    
                    if filtered_product:
                        filtered_product.min_shipping_price = min_shipping_price
                        logger.info(f"Updated product {product.product_id} with min shipping price: {min_shipping_price}")
                    
                    logger.info(f"Stored cheapest valid shipping option for product {product.product_id}, min shipping price: {min_shipping_price}")
                        
                else:
                    logger.warning(f"Unexpected freight response format for product {product.product_id}, SKU {max_sku_id}: {freight_response}")
                    
            except Exception as e:
                logger.error(f"Error fetching shipping info for product {product.product_id}, SKU {max_sku_id}: {e}")
                
        except Exception as e:
            logger.error(f"Error fetching shipping info for product {product.product_id}: {e}")

    def _parse_float(self, value) -> Optional[float]:
        """Safely parse float value."""
        if value is None or value == "null":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_int(self, value) -> Optional[int]:
        """Safely parse integer value."""
        if value is None or value == "null":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _parse_bool(self, value) -> Optional[bool]:
        """Safely parse boolean value."""
        if value is None or value == "null":
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes')
        return None


def run_product_filtering(max_price_eur: float = None, max_delivery_days: int = None, 
                         limit: int = None, dry_run: bool = False) -> Dict[str, int]:
    """
    Main function to run product filtering.
    
    Args:
        max_price_eur: Maximum total price in EUR
        max_delivery_days: Maximum delivery time in days  
        limit: Maximum products to process
        dry_run: Don't save to database
        
    Returns:
        Processing statistics
    """
    logger.info("Starting product filtering process")
    
    filter_engine = ProductFilterEngine(max_price_eur, max_delivery_days)
    stats = filter_engine.process_whitelisted_products(limit, dry_run)
    
    logger.info("Product filtering completed")
    logger.info(f"Statistics: {stats}")
    
    # Run image ingestion for filtered products (unless dry run)
    if not dry_run and stats.get('products_passed_filter', 0) > 0:
        logger.info("Starting image ingestion for filtered products")
        try:
            from src.ingestion.image_ingestion import ImageIngestionEngine
            
            image_engine = ImageIngestionEngine()
            image_stats = image_engine.ingest_all_images()
            
            logger.info("Image ingestion completed")
            logger.info(f"Image statistics: {image_stats}")
            
            # Add image stats to the main stats
            stats.update({
                'images_extracted': image_stats.get('images_extracted', 0),
                'hero_images': image_stats.get('hero_images', 0),
                'gallery_images': image_stats.get('gallery_images', 0),
                'variant_images': image_stats.get('variant_images', 0),
                'image_errors': image_stats.get('errors', 0)
            })
            
        except Exception as e:
            logger.error(f"Error during image ingestion: {e}")
            stats['image_errors'] = 1
    else:
        if dry_run:
            logger.info("Skipping image ingestion (dry run mode)")
        else:
            logger.info("Skipping image ingestion (no products passed filter)")
    
    return stats