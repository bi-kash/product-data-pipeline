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
        
        logger.info(f"Initialized Airtable client for base {self.base_id}")
    
    def upsert_records(self, table_name: str, records: List[Dict], key_field: str) -> Dict[str, int]:
        """Upsert records using pyairtable."""
        if not records:
            return {'created': 0, 'updated': 0}
        
        table = self.products_table if table_name == self.products_table_name else self.variants_table
        result = {'created': 0, 'updated': 0}
        
        for record in records:
            try:
                # Extract fields (sync.py sends records with 'fields' key)
                fields = record.get('fields', record)
                key_value = fields.get(key_field)
                
                if not key_value:
                    continue
                
                # Search for existing record
                existing = table.all(formula=f"{{{key_field}}} = '{key_value}'", max_records=1)
                
                if existing:
                    # Update
                    table.update(existing[0]['id'], fields)
                    result['updated'] += 1
                else:
                    # Create
                    table.create(fields)
                    result['created'] += 1
                    
            except Exception as e:
                logger.error(f"Error upserting record: {e}")
                
        return result
    
    def generate_anonymous_id(self, product_id: str) -> str:
        """Generate anonymous ID."""
        return hashlib.md5(product_id.encode()).hexdigest()[:12]
