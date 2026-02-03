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
    Handles both Products and Variants tables.
    """
    
    def __init__(self):
        # Get Personal Access Token (modern authentication)
        self.token = get_env('AIRTABLE_PERSONAL_ACCESS_TOKEN') or get_env('AIRTABLE_API_KEY')
        self.base_id = get_env('AIRTABLE_BASE_ID')
        self.products_table_name = get_env('AIRTABLE_PRODUCTS_TABLE', 'Products')
        self.variants_table_name = get_env('AIRTABLE_VARIANTS_TABLE', 'Variants')
        
        if not self.token or not self.base_id:
            raise ValueError("Missing Airtable config: AIRTABLE_PERSONAL_ACCESS_TOKEN and AIRTABLE_BASE_ID required")
        
        # Initialize pyairtable
        self.api = Api(self.token)
        self.base = self.api.base(self.base_id)
        self.products_table = self.base.table(self.products_table_name)
        self.variants_table = self.base.table(self.variants_table_name)
        
        logger.info(f"Initialized Airtable client for base {self.base_id} with Products and Variants tables")
    
    def upsert_products_by_anonymous_id(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Batch upsert products using anonymous ID as the key.
        Uses Airtable's native batch_upsert for optimal performance.
        
        Args:
            records: List of records to upsert
            
        Returns:
            Dict with creation/update statistics and record details
        """
        if not records:
            return {'created': 0, 'updated': 0, 'records': []}
        
        try:
            # Prepare records for batch upsert - extract fields and ensure proper format
            batch_records = []
            for record in records:
                fields = record.get('fields', record)
                if not fields.get('anon_product_id'):
                    logger.warning("Record missing anon_product_id, skipping")
                    continue
                batch_records.append({'fields': fields})
            
            if not batch_records:
                return {'created': 0, 'updated': 0, 'records': []}
            
            # Use batch_upsert with anon_product_id as the key field
            # This handles up to 10 records per request automatically
            upsert_result = self.products_table.batch_upsert(
                records=batch_records,
                key_fields=['anon_product_id'],
                replace=False  # Merge fields rather than replace entire record
            )
            
            # Extract statistics from result
            created_records = upsert_result.get('createdRecords', [])
            updated_records = upsert_result.get('updatedRecords', [])
            all_records = upsert_result.get('records', [])
            
            result = {
                'created': len(created_records),
                'updated': len(updated_records),
                'records': all_records
            }
            
            logger.info(f"Products batch upsert completed: {result['created']} created, {result['updated']} updated")
            return result
            
        except PyAirtableError as e:
            logger.error(f"Airtable batch upsert error: {e}")
            return {'created': 0, 'updated': 0, 'records': [], 'error': str(e)}
        except Exception as e:
            logger.error(f"Unexpected error in batch upsert: {e}")
            return {'created': 0, 'updated': 0, 'records': [], 'error': str(e)}
    
    def upsert_variants_by_anonymous_sku_id(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Batch upsert variants using anonymous SKU ID as the key.
        Uses Airtable's native batch_upsert for optimal performance.
        
        Args:
            records: List of variant records to upsert
            
        Returns:
            Dict with creation/update statistics and record details
        """
        if not records:
            return {'created': 0, 'updated': 0, 'records': []}
        
        try:
            # Prepare records for batch upsert
            batch_records = []
            for record in records:
                fields = record.get('fields', record)
                if not fields.get('anon_sku_id'):
                    logger.warning("Variant record missing anon_sku_id, skipping")
                    continue
                batch_records.append({'fields': fields})
            
            if not batch_records:
                return {'created': 0, 'updated': 0, 'records': []}
            
            # Use batch_upsert with anon_sku_id as the key field
            # This handles up to 10 records per request automatically
            upsert_result = self.variants_table.batch_upsert(
                records=batch_records,
                key_fields=['anon_sku_id'],
                replace=False  # Merge fields rather than replace entire record
            )
            
            # Extract statistics from result
            created_records = upsert_result.get('createdRecords', [])
            updated_records = upsert_result.get('updatedRecords', [])
            all_records = upsert_result.get('records', [])
            
            result = {
                'created': len(created_records),
                'updated': len(updated_records),
                'records': all_records
            }
            
            logger.info(f"Variants batch upsert completed: {result['created']} created, {result['updated']} updated")
            return result
            
        except PyAirtableError as e:
            logger.error(f"Airtable variants batch upsert error: {e}")
            return {'created': 0, 'updated': 0, 'records': [], 'error': str(e)}
        except Exception as e:
            logger.error(f"Unexpected error in variants batch upsert: {e}")
            return {'created': 0, 'updated': 0, 'records': [], 'error': str(e)}
    
    def generate_anonymous_id(self, product_id: str) -> str:
        """
        Generate anonymous ID for a product ID.
        
        Args:
            product_id: Real product ID
            
        Returns:
            Anonymous ID (12-character MD5 hash)
        """
        return hashlib.md5(product_id.encode()).hexdigest()[:12]
    
    def generate_anonymous_sku_id(self, sku_id: str) -> str:
        """
        Generate anonymous SKU ID for a variant SKU ID.
        
        Args:
            sku_id: Real SKU ID
            
        Returns:
            Anonymous SKU ID (12-character MD5 hash)
        """
        return hashlib.md5(sku_id.encode()).hexdigest()[:12]
    
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
    
    def reverse_anonymous_sku_id(self, anon_sku_id: str) -> Optional[str]:
        """
        Reverse an anonymous SKU ID to get the real SKU ID.
        Since MD5 is one-way, this requires a lookup in the SKUMapping table.
        
        Args:
            anon_sku_id: Anonymous SKU ID to reverse
            
        Returns:
            Real SKU ID if found, None otherwise
        """
        try:
            from ..common.database import SKUMapping, get_db_session
            
            with get_db_session() as db:
                mapping = db.query(SKUMapping).filter(
                    SKUMapping.anon_sku_id == anon_sku_id
                ).first()
                
                return mapping.sku_id if mapping else None
                
        except Exception as e:
            logger.error(f"Error reversing anonymous SKU ID {anon_sku_id}: {e}")
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