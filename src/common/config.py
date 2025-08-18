"""
Configuration management module.
"""

import os
import csv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_search_keywords():
    """
    Load search keywords from the keywords.csv file.

    Returns:
        List of keywords for merchant search
    """
    keyword_file = os.getenv("KEYWORD_FILE", "data/keywords.csv")

    # Create the file with sample keywords if it doesn't exist
    if not os.path.exists(keyword_file):
        os.makedirs(os.path.dirname(keyword_file), exist_ok=True)
        with open(keyword_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["keyword"])
            writer.writerow(["jewelry"])

    # Read keywords from file
    keywords = []
    with open(keyword_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row and row[0].strip():
                keywords.append(row[0].strip())

    return keywords


def get_search_categories():
    """
    Get list of category IDs to search from file.

    Returns:
        List of tuples (category_id, category_name)
    """
    categories_file = os.getenv("CATEGORIES_FILE", "data/jewelry_categories.csv")

    if not os.path.exists(categories_file):
        return []

    categories = []
    with open(categories_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                category_id = int(row["category_id"])
                category_name = row["category_name"]
                categories.append((category_id, category_name))
            except (KeyError, ValueError):
                continue

    return categories


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
