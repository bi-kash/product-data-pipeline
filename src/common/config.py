"""
Configuration management module.
"""

import os
import csv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def use_keywords():
    """
    Determine if keyword-based search should be used based on the USE_KEYWORDS environment variable.
    
    When USE_KEYWORDS is true:
    - The harvester will search using both keywords AND categories
    - It makes separate API calls for keyword search and category search
    - Both approaches contribute to a single job_run record
    
    When USE_KEYWORDS is false:
    - The harvester will search using ONLY categories
    - This often yields better results by avoiding supply-heavy keyword searches
    - Only category IDs are recorded in the job_run record
    
    Returns:
        bool: True if USE_KEYWORDS is set to "true" (case-insensitive), False otherwise
    """
    return os.getenv("USE_KEYWORDS", "true").lower() == "true"


def get_search_keywords():
    """
    Load search keywords from the KEYWORDS environment variable (comma-separated).
    
    These keywords are used for product search when USE_KEYWORDS=true.
    Each keyword will trigger a separate API call and search.
    
    Recommended keywords are specific jewelry types like:
    - necklace, ring, bracelet, earrings
    
    Avoid supply terms like:
    - beads, findings, chain, wire
    
    Returns:
        list: List of keywords for merchant search
        
    Example:
        With KEYWORDS=necklace,ring,bracelet in .env
        Returns: ['necklace', 'ring', 'bracelet']
    """
    env_val = os.getenv("KEYWORDS", "")
    # Remove any inline comments (anything after a # symbol)
    if '#' in env_val:
        env_val = env_val.split('#')[0]
    return [k.strip() for k in env_val.split(",") if k.strip()]


def get_search_categories():
    """
    Get list of category IDs from the CATEGORIES environment variable (comma-separated).
    
    These categories are used for:
    1. Category-only search when USE_KEYWORDS=false
    2. Additional category search when USE_KEYWORDS=true
    
    Recommended categories for finished jewelry:
    - 200001680: Fine Jewelry
    - 1509: Fashion Jewelry
    - 201239108: Customized Jewelry
    - 200370154: Smart Jewelry
    
    Categories to avoid (primarily contain supplies):
    - 200001479: Jewelry Packaging & Display
    - 200001478: Jewelry Tools & Equipment
    - 201238105: Jewelry Making
    
    Returns:
        list: List of category IDs as strings
        
    Example:
        With CATEGORIES=200001680,1509 in .env
        Returns: ['200001680', '1509']
    """
    env_val = os.getenv("CATEGORIES", "")
    # Remove any inline comments (anything after a # symbol)
    if '#' in env_val:
        env_val = env_val.split('#')[0]
    return [c.strip() for c in env_val.split(",") if c.strip()]


def create_example_env_file():
    """
    Create an example .env file if it doesn't exist.
    """
    env_example_path = ".env.example"

    if not os.path.exists(env_example_path):
        with open(env_example_path, "w", encoding="utf-8") as f:
            f.write(
                """# Database Configuration
USE_SQLITE=true  # Set to false for PostgreSQL
DB_HOST=your_db_host
DB_NAME=your_db_name
DB_USER=your_db_username
DB_PASSWORD=your_db_password
DB_PORT=5432

# RapidAPI Configuration for AliExpress
RAPIDAPI_KEY=your_rapidapi_key
RAPIDAPI_HOST=aliexpress-true-api.p.rapidapi.com

# AliExpress API Region Settings
ALIEXPRESS_TARGET_CURRENCY=EUR
ALIEXPRESS_TARGET_LANGUAGE=EN
ALIEXPRESS_TARGET_COUNTRY=DE

# Application Settings
KEYWORD_FILE=data/keywords.csv
LOG_LEVEL=INFO
RATE_LIMIT_DELAY=1
MAX_RETRIES=3
TIMEOUT=30
USE_MOCK_DATA=false
API_PAGE_SIZE=50  # Maximum number of products per page for API requests
"""
            )


def get_env(key, default=None):
    """
    Get an environment variable with fallback to default.

    Args:
        key: Name of the environment variable
        default: Default value if not set

    Returns:
        Value of the environment variable or default
    """
    return os.getenv(key, default)


def get_api_page_size():
    """
    Get the page size for API requests.
    
    Returns:
        Integer page size (default 50)
    """
    try:
        return int(get_env("API_PAGE_SIZE", "50"))
    except (ValueError, TypeError):
        return 50
