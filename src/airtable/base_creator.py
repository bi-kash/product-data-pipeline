"""
Airtable Base Creation Module
Uses Airtable Meta API to create base and tables programmatically.
"""

import json
import logging
import os
import re
import requests
from typing import Dict, List, Any, Optional
from src.common.config import get_env

logger = logging.getLogger(__name__)

class AirtableBaseCreator:
    """Creates Airtable bases and tables using Meta API."""
    
    def __init__(self):
        self.token = self._get_token()
        self.workspace_id = get_env('AIRTABLE_WORKSPACE_ID')
        self.base_url = "https://api.airtable.com/v0/meta"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
    def _get_token(self) -> str:
        """Get Personal Access Token from environment."""
        token = get_env('AIRTABLE_PERSONAL_ACCESS_TOKEN')
        if not token:
            token = get_env('AIRTABLE_API_KEY')  # Fallback
        if not token:
            raise ValueError("No Airtable token found. Set AIRTABLE_PERSONAL_ACCESS_TOKEN")
        return token

    def _update_env_file(self, new_base_id: str) -> bool:
        """
        Update the .env file with the new base ID.
        
        Args:
            new_base_id: The new Airtable base ID to set
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            env_file_path = ".env"
            
            # Check if .env file exists
            if not os.path.exists(env_file_path):
                logger.warning("No .env file found to update")
                return False
                
            # Read the current .env file content
            with open(env_file_path, 'r') as file:
                content = file.read()
            
            # Pattern to match AIRTABLE_BASE_ID line
            pattern = r'^AIRTABLE_BASE_ID=.*$'
            replacement = f'AIRTABLE_BASE_ID={new_base_id}'
            
            # Replace the line or add it if it doesn't exist
            if re.search(pattern, content, re.MULTILINE):
                updated_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
            else:
                # Add the line at the end if it doesn't exist
                updated_content = content.rstrip() + f'\n{replacement}\n'
            
            # Write the updated content back to the file
            with open(env_file_path, 'w') as file:
                file.write(updated_content)
                
            logger.info(f"Successfully updated .env file with new base ID: {new_base_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update .env file: {e}")
            return False

    def create_product_base(self, base_name: str = "Product Pipeline") -> Dict[str, Any]:
        """
        Create a complete Airtable base with Products and Variants tables.
        
        Args:
            base_name: Name for the new base
            
        Returns:
            Dict containing base_id and table information
        """
        logger.info(f"Creating Airtable base: {base_name}")
        
        # Define the base structure
        base_data = {
            "name": base_name,
            "tables": [
                self._get_products_table_schema(),
                self._get_variants_table_schema(),
                self._get_product_mapping_table_schema(),
                self._get_sku_mapping_table_schema()
            ]
        }
        
        # Add workspace ID if provided
        if self.workspace_id:
            base_data["workspaceId"] = self.workspace_id
        
        # Create the base
        response = requests.post(
            f"{self.base_url}/bases",
            headers=self.headers,
            json=base_data,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            data = response.json()
            base_id = data['id']
            
            print(f"✅ Base created successfully!")
            print(f"Base ID: {base_id}")
            print(f"Base URL: https://airtable.com/{base_id}")
            print()
            print("📊 Created tables:")
            print("   1️⃣  Products - Main product data with S3 URLs")
            print("   2️⃣  Variants - All SKU variations with proper formatting") 
            print("   3️⃣  Product Mapping - Original AliExpress URLs and real IDs")
            print("   4️⃣  SKU Mapping - Anonymous to real SKU mapping with variant URLs")
            print()
            
            # Automatically update .env file
            if self._update_env_file(base_id):
                print("🔧 Automatically updated .env file with new base ID")
            else:
                print("⚠️  Could not update .env file automatically")
                print(f"Please manually add: AIRTABLE_BASE_ID={base_id}")
            
            print()
            print("Next steps:")
            print("1. Run: python main.py airtable:sync")
            return base_id
        elif response.status_code == 422:
            print(f"❌ Base creation failed: Server error (422)")
            print(f"This usually means your token doesn't have the required scopes for base creation.")
            print()
            print("Recommended solutions:")
            print("1. Use manual base creation (see docs/AIRTABLE_SETUP.md)")
            print("2. Or create a new token with schema creation permissions")
            print()
            print("Your token is valid for data operations, so you can still sync to an existing base.")
            return None
        else:
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {"error": response.text}
            print(f"❌ Failed to create base: {response.status_code}")
            print(f"Error: {error_data}")
            return None
    
    def _get_products_table_schema(self) -> Dict[str, Any]:
        """Define Products table schema."""
        return {
            "name": "Products",
            "description": "Master and Unique products (one row per product)",
            "fields": [
                {
                    "name": "anon_product_id",
                    "type": "singleLineText",
                    "description": "Anonymized product ID (primary key for sync)"
                },
                {
                    "name": "title",
                    "type": "singleLineText",
                    "description": "Product title"
                },
                {
                    "name": "description", 
                    "type": "multilineText",
                    "description": "Product description"
                },
                {
                    "name": "specifications",
                    "type": "multilineText",
                    "description": "Product specifications (key-value pairs from ae_item_properties)"
                },
                {
                    "name": "hero_image",
                    "type": "url",
                    "description": "Main product image (S3 URL, is_primary=true)"
                },
                {
                    "name": "gallery_images",
                    "type": "multilineText", 
                    "description": "Gallery images (comma-separated S3 URLs)"
                },
                {
                    "name": "video",
                    "type": "url",
                    "description": "Product video (S3 URL, if available)"
                },
                {
                    "name": "duplicate_status",
                    "type": "singleSelect",
                    "options": {
                        "choices": [
                            {"name": "MASTER", "color": "greenBright"},
                            {"name": "UNIQUE", "color": "blueBright"}
                        ]
                    },
                    "description": "Product duplicate status"
                },
                {
                    "name": "selected_variant",
                    "type": "singleLineText",
                    "description": "Selected variant key (will link after tables created)"
                },
                # Simple fields that will be populated by sync (rollups require table relationships)
                {
                    "name": "price_eur",
                    "type": "currency",
                    "options": {
                        "symbol": "€",
                        "precision": 2
                    },
                    "description": "Price from selected variant"
                },
                {
                    "name": "shipping_eur", 
                    "type": "currency",
                    "options": {
                        "symbol": "€",
                        "precision": 2
                    },
                    "description": "Shipping cost from selected variant"
                },
                {
                    "name": "total_eur",
                    "type": "currency",
                    "options": {
                        "symbol": "€",
                        "precision": 2
                    },
                    "description": "Total cost from selected variant"
                },
                {
                    "name": "delivery_time",
                    "type": "singleLineText",
                    "description": "Delivery time from selected variant"
                },
                {
                    "name": "sync_timestamp",
                    "type": "dateTime",
                    "options": {
                        "dateFormat": {"name": "iso"},
                        "timeFormat": {"name": "24hour"},
                        "timeZone": "utc"
                    },
                    "description": "Last sync timestamp"
                }
            ]
        }

    def _get_variants_table_schema(self) -> Dict[str, Any]:
        """Define Variants table schema.""" 
        return {
            "name": "Variants",
            "description": "Product variants (one row per purchasable combination)",
            "fields": [
                {
                    "name": "variant_key",
                    "type": "singleLineText",
                    "description": "Stable variant key (primary key)"
                },
                {
                    "name": "anon_product_id",
                    "type": "singleLineText",
                    "description": "Link to parent product (anon_product_id)"
                },
                {
                    "name": "anon_sku_id",
                    "type": "singleLineText",
                    "description": "Anonymous SKU ID"
                },
                {
                    "name": "definition_name",
                    "type": "singleLineText",
                    "description": "Property value definition name (e.g. 'Cherry Wood Color', 'blue')"
                },
                {
                    "name": "price_eur",
                    "type": "currency",
                    "options": {
                        "symbol": "€",
                        "precision": 2
                    },
                    "description": "Variant price in EUR"
                },
                {
                    "name": "shipping_eur",
                    "type": "currency", 
                    "options": {
                        "symbol": "€",
                        "precision": 2
                    },
                    "description": "Shipping cost in EUR"
                },
                {
                    "name": "total_eur", 
                    "type": "currency",
                    "options": {
                        "symbol": "€", 
                        "precision": 2
                    },
                    "description": "Total cost (price + shipping) in EUR"
                },
                {
                    "name": "delivery_min_days",
                    "type": "number",
                    "options": {
                        "precision": 0
                    },
                    "description": "Minimum delivery time in days"
                },
                {
                    "name": "delivery_max_days",
                    "type": "number", 
                    "options": {
                        "precision": 0
                    },
                    "description": "Maximum delivery time in days"
                },
                {
                    "name": "delivery_range",
                    "type": "singleLineText",
                    "description": "Delivery time range (e.g. '7-14 days')"
                },
                {
                    "name": "variant_hero_image",
                    "type": "url",
                    "description": "Variant-specific hero image (S3 URL)"
                },
                {
                    "name": "variant_images",
                    "type": "multilineText",
                    "description": "Variant-specific images (S3 URLs, pipe-separated)"
                },
                {
                    "name": "is_recommended",
                    "type": "checkbox",
                    "options": {
                        "icon": "star",
                        "color": "yellowBright"
                    },
                    "description": "Recommended variant flag (e.g. cheapest & fastest)"
                },
                {
                    "name": "sync_timestamp",
                    "type": "dateTime",
                    "options": {
                        "dateFormat": {"name": "iso"},
                        "timeFormat": {"name": "24hour"},
                        "timeZone": "utc"
                    },
                    "description": "Last sync timestamp"
                }
            ]
        }

    def _get_product_mapping_table_schema(self) -> Dict[str, Any]:
        """Define Product Mapping table schema."""
        return {
            "name": "Product Mapping",
            "description": "Maps anonymous product IDs to real AliExpress data and URLs",
            "fields": [
                {
                    "name": "anon_product_id",
                    "type": "singleLineText",
                    "description": "Anonymous product ID (matches Products.anon_product_id)"
                },
                {
                    "name": "real_product_id",
                    "type": "singleLineText", 
                    "description": "Real AliExpress product ID"
                },
                {
                    "name": "aliexpress_product_url",
                    "type": "url",
                    "description": "Direct AliExpress product page URL"
                },
                {
                    "name": "aliexpress_main_image_url",
                    "type": "url",
                    "description": "Original AliExpress main product image URL"
                },
                {
                    "name": "aliexpress_video_url",
                    "type": "url",
                    "description": "Original AliExpress product video URL (if available)"
                },
                {
                    "name": "sync_timestamp",
                    "type": "dateTime",
                    "options": {
                        "dateFormat": {"name": "iso"},
                        "timeFormat": {"name": "24hour"},
                        "timeZone": "utc"
                    },
                    "description": "Last sync timestamp"
                }
            ]
        }

    def get_base_schema(self, base_id: str) -> Dict[str, Any]:
        """Get existing base schema."""
        response = requests.get(
            f"{self.base_url}/bases/{base_id}/tables",
            headers=self.headers,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get base schema: {response.status_code}")
            response.raise_for_status()

    def _get_sku_mapping_table_schema(self) -> Dict[str, Any]:
        """Define SKU Mapping table schema."""
        return {
            "name": "SKU Mapping",
            "description": "Maps anonymous SKU IDs to real AliExpress SKU data and variant URLs",
            "fields": [
                {
                    "name": "anon_sku_id",
                    "type": "singleLineText",
                    "description": "Anonymous SKU ID (matches Variants.anon_sku_id)"
                },
                {
                    "name": "real_sku_id",
                    "type": "singleLineText", 
                    "description": "Real AliExpress SKU ID"
                },
                {
                    "name": "aliexpress_product_url",
                    "type": "url",
                    "description": "Direct AliExpress product page URL"
                },
                {
                    "name": "aliexpress_main_image",
                    "type": "url",
                    "description": "Original AliExpress main product image URL"
                },
                {
                    "name": "aliexpress_variant_image",
                    "type": "url",
                    "description": "Original AliExpress variant-specific image URL"
                },
                {
                    "name": "sync_timestamp",
                    "type": "dateTime",
                    "options": {
                        "dateFormat": {"name": "iso"},
                        "timeFormat": {"name": "24hour"},
                        "timeZone": "utc"
                    },
                    "description": "Last sync timestamp"
                }
            ]
        }


def create_base_command(base_name: str = None, workspace_id: str = None, test_token: bool = False):
    """
    CLI command to create Airtable base.
    
    Args:
        base_name: Name for the new base
        workspace_id: Workspace ID to create base in  
        test_token: Test token configuration
    """
    creator = AirtableBaseCreator()
    
    if test_token:
        print("🔍 Testing Personal Access Token...")
        print(f"Token: {creator.token[:10]}...{creator.token[-4:] if len(creator.token) > 14 else creator.token}")
        try:
            # Test with a simple API call
            response = requests.get(
                "https://api.airtable.com/v0/meta/whoami",
                headers=creator.headers,
                timeout=10
            )
            if response.status_code == 200:
                user_info = response.json()
                print(f"✅ Token is valid!")
                print(f"👤 User: {user_info.get('id', 'Unknown')}")
                if 'scopes' in user_info:
                    print(f"🔐 Scopes: {', '.join(user_info['scopes'])}")
            else:
                print(f"❌ Token test failed: {response.status_code}")
                print(f"Response: {response.text}")
        except Exception as e:
            print(f"❌ Token test error: {e}")
        return
    
    # Override workspace if provided
    if workspace_id:
        creator.workspace_id = workspace_id
    
    base_name = base_name or "Product Pipeline"
    
    try:
        result = creator.create_product_base(base_name)
        
        if result:
            print(f"🎉 Successfully created Airtable base!")
            print(f"📊 Base ID: {result['id']}")
            print(f"📋 Base Name: {result['name']}")
            
            # Display tables
            if "tables" in result:
                print(f"\n📁 Tables created:")
                for table in result["tables"]:
                    print(f"  🗃️  {table['name']} (ID: {table['id']})")
                    print(f"      Fields: {len(table.get('fields', []))} fields")
            
            print(f"\n🔧 Next steps:")
            print(f"1. Update your .env file:")
            print(f"   AIRTABLE_BASE_ID={result['id']}")
            print(f"2. Run sync command:")
            print(f"   python main.py airtable:sync --dry-run")
        else:
            # Base creation failed, error already printed by create_product_base
            return None
        
    except Exception as e:
        logger.error(f"❌ Failed to create base: {e}")
        print(f"❌ Failed to create base: {e}")
        print("\n💡 Troubleshooting:")
        print("1. Make sure your Personal Access Token has these scopes:")
        print("   - schema.bases:write")
        print("   - data.records:read") 
        print("   - data.records:write")
        print("2. Check that your token is complete (starts with 'pat' and is ~87 characters)")
        print("3. Verify the token in your .env file is not truncated")
        print("\n🔄 Alternative: Create base manually at https://airtable.com/create")
        print("   Then use: python main.py airtable:sync --dry-run")
        return None