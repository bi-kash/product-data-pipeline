"""
Airtable Client for product data synchronization using pyairtable.
"""

import hashlib
import logging
from typing import Dict, List, Optional, Any

from pyairtable import Api
from pyairtable.exceptions import PyAirtableError

from ..common.config import get_env

logger = logging.getLogger(__name__)


class AirtableClient:
    """
    Modern Airtable client using pyairtable library and Personal Access Tokens.
    Only handles the Products table.
    """
    
    def __init__(self):
        # Get Personal Access Token (modern authentication)
        self.token = get_env('AIRTABLE_PERSONAL_ACCESS_TOKEN') or get_env('AIRTABLE_API_KEY')
        self.base_id = get_env('AIRTABLE_BASE_ID')
        self.products_table_name = get_env('AIRTABLE_PRODUCTS_TABLE', 'Products')
        
        if not self.token or not self.base_id:
            raise ValueError("Missing Airtable config: AIRTABLE_PERSONAL_ACCESS_TOKEN and AIRTABLE_BASE_ID required")
        
        # Initialize pyairtable
        self.api = Api(self.token)
        self.base = self.api.base(self.base_id)
        self.products_table = self.base.table(self.products_table_name)
        
        logger.info(f"Initialized Airtable client for base {self.base_id}")
    
    def upsert_products_by_anonymous_id(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Upsert products using anonymous ID as the key.
        
        Args:
            records: List of records to upsert
            
        Returns:
            Dict with creation/update statistics and record details
        """
        if not records:
            return {'created': 0, 'updated': 0, 'records': []}
        
        result = {'created': 0, 'updated': 0, 'records': []}
        
        for record in records:
            try:
                # Extract fields (sync.py sends records with 'fields' key)
                fields = record.get('fields', record)
                anon_product_id = fields.get('anon_product_id')
                
                if not anon_product_id:
                    logger.warning("Record missing anon_product_id, skipping")
                    continue
                
                # Search for existing record by anon_product_id
                existing = self.products_table.all(
                    formula=f"{{anon_product_id}} = '{anon_product_id}'", 
                    max_records=1
                )
                
                if existing:
                    # Update existing record
                    updated_record = self.products_table.update(existing[0]['id'], fields)
                    result['updated'] += 1
                    result['records'].append(updated_record)
                    logger.debug(f"Updated product record: {anon_product_id}")
                else:
                    # Create new record
                    created_record = self.products_table.create(fields)
                    result['created'] += 1
                    result['records'].append(created_record)
                    logger.debug(f"Created product record: {anon_product_id}")
                    
            except PyAirtableError as e:
                logger.error(f"Airtable error upserting record {fields.get('anon_product_id', 'unknown')}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error upserting record {fields.get('anon_product_id', 'unknown')}: {e}")
                
        logger.info(f"Products upsert completed: {result['created']} created, {result['updated']} updated")
        return result
    
    def generate_anonymous_id(self, product_id: str) -> str:
        """
        Generate anonymous ID for a product ID.
        
        Args:
            product_id: Real product ID
            
        Returns:
            Anonymous ID (12-character MD5 hash)
        """
        return hashlib.md5(product_id.encode()).hexdigest()[:12]
    
    def reverse_anonymous_id(self, anon_id: str) -> Optional[str]:
        """
        Reverse an anonymous ID to get the real product ID.
        Since MD5 is one-way, this requires a lookup in the ProductMapping table.
        
        Args:
            anon_id: Anonymous ID to reverse
            
        Returns:
            Real product ID if found, None otherwise
        """
        try:
            from ..common.database import ProductMapping, get_db_session
            
            with get_db_session() as db:
                mapping = db.query(ProductMapping).filter(
                    ProductMapping.anon_product_id == anon_id
                ).first()
                
                return mapping.product_id if mapping else None
                
        except Exception as e:
            logger.error(f"Error reversing anonymous ID {anon_id}: {e}")
            return None
    
    def get_all_products(self) -> List[Dict]:
        """
        Get all products from Airtable.
        
        Returns:
            List of product records
        """
        try:
            return self.products_table.all()
        except Exception as e:
            logger.error(f"Error fetching all products: {e}")
            return []
    
    def delete_product_by_anonymous_id(self, anon_product_id: str) -> bool:
        """
        Delete a product by its anonymous ID.
        
        Args:
            anon_product_id: Anonymous product ID
            
        Returns:
            True if deleted, False if not found or error
        """
        try:
            # Find the record
            records = self.products_table.all(
                formula=f"{{anon_product_id}} = '{anon_product_id}'",
                max_records=1
            )
            
            if records:
                self.products_table.delete(records[0]['id'])
                logger.info(f"Deleted product record: {anon_product_id}")
                return True
            else:
                logger.warning(f"Product not found for deletion: {anon_product_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting product {anon_product_id}: {e}")
            return False