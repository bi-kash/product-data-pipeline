"""
Airtable Integration Module

This module handles synchronization of product data to Airtable for VA workflow.
Implements a two-table system:
- Products table: One row per master/unique product
- Variants table: One row per purchasable variant combination
"""