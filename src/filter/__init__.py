"""
Product Filter Module - Module B
====================================

This module handles product filtering based on business rules for whitelisted sellers.

Key Features:
- Price Rule: Filters products where (most expensive variant + cheapest shipping) <= TOTAL_MAX_PRICE
- Shipping Rule: Filters products with fast delivery (delivery_time <= configurable limit)
- Shipping Badge Detection: Identifies "Choice", "Local+", or "Standard" shipping types
- Creates filtered_products table for qualified products

Workflow:
1. Monitors for whitelisted sellers
2. Processes their products through business rules
3. Stores qualifying products in filtered_products table
4. Enriches with delivery time and shipping badge information
"""