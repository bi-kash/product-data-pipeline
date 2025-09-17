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
    SessionLocal, 
    Product, 
    Seller, 
    FilteredProduct,
    get_db_session
)
from src.common.config import get_env
from src.common.official_aliexpress_client import OfficialAliExpressClient
from src.session.session_manager import list_sessions

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
        sessions = list_sessions()
        session_code = sessions[0]['code'] if sessions else None
        if not session_code:
            logger.warning("No session code available for API enrichment")
            self.api_client = None
        else:
            self.api_client = OfficialAliExpressClient(session_code=session_code)
        
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
                    stats['products_processed'] += 1
                    logger.info(f"Processing product {product.product_id} from seller {product.shop_id}")
                    
                    # Check if already filtered
                    existing_filter = db.query(FilteredProduct).filter(
                        FilteredProduct.product_id == product.product_id
                    ).first()
                    
                    if existing_filter:
                        logger.debug(f"Product {product.product_id} already filtered, skipping")
                        continue
                    
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
                    else:
                        logger.info(f"❌ Product {product.product_id} failed filters: price={filter_result['passed_price_rule']}, shipping={filter_result['passed_shipping_rule']}")
                
                except Exception as e:
                    logger.error(f"Error processing product {product.product_id}: {e}")
                    stats['errors'] += 1
            
            if not dry_run:
                db.commit()
            
        except Exception as e:
            logger.error(f"Error in filter processing: {e}")
            db.rollback()
            raise
        finally:
            db.close()
        
        return stats

    def _get_whitelisted_products(self, db, limit: int = None) -> List[Product]:
        """Get products from whitelisted sellers."""
        query = db.query(Product).join(Seller).filter(
            Seller.approval_status == 'WHITELIST'
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
        price_result = self._apply_price_rule(product, product_data)
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

    def _apply_price_rule(self, product: Product, product_data: Optional[Dict]) -> Dict:
        """
        Apply price filtering rule: (max_variant_price + min_shipping_cost) <= max_price_eur
        
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
            
            # Extract shipping costs (simplified - using 0 for now)
            # TODO: Implement actual shipping cost extraction if available in API
            min_shipping_cost = 0.0
            
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
            prices_found = []
            for i, sku in enumerate(skus):
                # Prioritize sale prices over regular prices
                # Check offer_sale_price first (discounted/sale price)
                # Then check offer_bulk_sale_price as fallback
                # Skip sku_price as it's usually the original price before discount
                price_fields = ['offer_sale_price', 'offer_bulk_sale_price']
                for field in price_fields:
                    if field in sku:
                        try:
                            price = float(sku[field])
                            prices_found.append(f"SKU{i}:{field}={price}")
                            max_price = max(max_price, price)
                            break  # Stop after finding first valid price for this SKU
                        except (ValueError, TypeError):
                            continue
            
            logger.debug(f"Sale prices found for {product.product_id}: {prices_found}, max_price={max_price}")
            return max_price if max_price > 0 else None
            
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

    def _create_filtered_product(self, product: Product, filter_result: Dict, db) -> FilteredProduct:
        """Create a new filtered product entry."""
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
            max_variant_price=filter_result.get('max_variant_price')
        )
        
        db.add(filtered_product)
        return filtered_product


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
    
    return stats