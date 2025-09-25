# Product Data Pipeline

A modular data pipeline for harvesting and processing jewelry product data from AliExpress using RapidAPI.

## Overview

This project implements a modular pipe- **product_status:** Store duplicate detection results (Module C)

- `id`: Auto-incrementing primary key
- `product_id`: Foreign key to filtered_products table (unique)
- `status`: Product status ("UNIQUE", "MASTER", "DUPLICATE", "REVIEW_SUSPECT")
- `duplicate_master_id`: Points to master product if this is a duplicate
- `total_landed_cost`: Item price + shipping cost for master selection
- `detection_method`: How duplicates were identified ("PHASH", "CLIP", "MANUAL")
- `phash_difference`: Hamming distance between pHash values (0-64)
- `clip_similarity`: CLIP similarity score (0.0-1.0)
- `created_at`: When the analysis was performed
- `updated_at`: When the record was last updated

*Note: Database may contain additional legacy columns from previous versions.*tify jewelry products from AliExpress, qualify them based on rules, and prepare them for processing by virtual assistants.

The current implementation (Module 1) focuses on merchant harvesting and verification:

- **Merchant Harvest:** Collects merchant IDs via category-based search using RapidAPI
- **Database Management:** Stores merchant and product data with approval status
- **VA Workflow:** Supports CSV export/import for merchant verification

## Requirements

- Python 3.10 or newer
- PostgreSQL database (e.g., via Supabase)
- RapidAPI key with access to AliExpress API

## Installation

1. Clone the repository
2. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Create a `.env` file by copying `.env.example`:

```bash
cp .env.example .env
```

5. **Subscribe to the AliExpress True API**:

   - Visit [AliExpress True API on RapidAPI](https://rapidapi.com/georgekhananaev/api/aliexpress-true-api)
   - Sign up for a RapidAPI account if you don't have one
   - Subscribe to the API (they offer a free tier with limited requests)
   - Copy your personal RapidAPI key from the dashboard

6. Edit the `.env` file with your database and RapidAPI credentials. Required variables include:

```
# Database configuration
USE_SQLITE=true  # Set to false for PostgreSQL
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=product_pipeline

# RapidAPI configuration
RAPIDAPI_KEY=your_rapidapi_key  # Replace with your key from step 5
RAPIDAPI_HOST=aliexpress-true-api.p.rapidapi.com

# Region settings (for price and shipping data)
ALIEXPRESS_TARGET_CURRENCY=EUR
ALIEXPRESS_TARGET_LANGUAGE=EN
ALIEXPRESS_TARGET_COUNTRY=DE
```

> **Note**: Never commit your `.env` file to version control as it contains sensitive information like your API key.

## Database Setup

The application automatically creates the required tables when first run. The database schema includes:

- **sellers:** Store merchant information

  - `shop_id`: Primary key, unique identifier from AliExpress
  - `shop_url`: URL to the seller's shop
  - `shop_name`: Name of the seller's shop
  - `approval_status`: One of ["PENDING", "WHITELIST", "BLACKLIST"]
  - `first_seen_at`: Timestamp when first discovered (UTC)
  - `last_seen_at`: Timestamp when last seen (UTC)
  - `raw_json`: Raw data from the API (JSON)
  - `note`: Optional note for review

- **products:** Store product information from AliExpress search

  - `product_id`: Primary key, unique identifier from AliExpress
  - `shop_id`: Foreign key to sellers table
  - `product_title`: Title of the product
  - `product_detail_url`: URL to the product detail page
  - `product_main_image_url`: URL to the main product image
  - `original_price`: Original price (Float)
  - `target_sale_price`: Sale price (Float)
  - `original_price_currency`: Currency for original price
  - `target_sale_price_currency`: Currency for sale price
  - `discount`: Discount percentage
  - `evaluate_rate`: Product rating
  - `category_id`: Comma-separated category IDs
  - `first_seen_at`: Timestamp when first discovered (UTC)
  - `last_seen_at`: Timestamp when last seen (UTC)
  - `raw_json_search`: Raw search result data from the API (JSON)
  - `raw_json_detail`: Raw detailed product data from the API (JSON)

- **filtered_products:** Store products that pass business rule filtering

  - `product_id`: Primary key, foreign key to products table
  - `shop_id`: Foreign key to sellers table
  - All product fields mirrored from products table
  - `ship_to_country`: Destination country for shipping
  - `delivery_time`: Estimated delivery time in days
  - `min_delivery_days`: Minimum delivery time in days
  - `max_delivery_days`: Maximum delivery time in days
  - `max_variant_price`: Highest variant price found
  - `min_shipping_price`: Cheapest shipping price among all SKU variants
  - `filtered_at`: Timestamp when product was filtered

- **shipping_info:** Store detailed shipping options for products

  - `id`: Auto-incrementing primary key
  - `product_id`: Foreign key to filtered_products table
  - `sku_id`: SKU identifier for the specific variant
  - `code`: Shipping method code (e.g., "CAINIAO_FULFILLMENT_STD")
  - `company`: Shipping company name (e.g., "AliExpress Selection Standard")
  - `shipping_fee`: Numeric shipping fee (Float)
  - `shipping_fee_currency`: Currency for shipping fee
  - `free_shipping`: Boolean indicating free shipping
  - `min_delivery_days`: Minimum delivery days for this option
  - `max_delivery_days`: Maximum delivery days for this option
  - `guaranteed_delivery_days`: Guaranteed delivery days
  - `ship_from_country`: Origin country code
  - `tracking`: Boolean indicating tracking availability
  - `raw_freight_response`: Full API response (JSON)
  - `created_at`: Timestamp when shipping info was retrieved

- **product_images:** Store categorized product images with analysis data

  - `id`: Auto-incrementing primary key
  - `product_id`: Foreign key to filtered_products table
  - `image_url`: Full image URL (unique constraint)
  - `image_role`: Type of image ("hero", "gallery", "variant")
  - `sku_id`: SKU ID from ae_item_sku_info_d_t_o
  - `variant_key`: Property name:value format
  - `property_value`: Property value (e.g., "Red", "Blue")
  - `property_name`: Property name (e.g., "Color", "Size")
  - `property_id`: Property ID from sku_property_id
  - `property_value_definition_name`: Full definition name
  - `sort_index`: Order of images within the product
  - `width`: Image width in pixels
  - `height`: Image height in pixels
  - `is_primary`: Boolean indicating primary/hero image
  - `local_file_path`: Local file path after download (relative path)
  - `phash`: Perceptual hash for duplicate detection (64-character hex string)
  - `download_status`: Download status ("pending", "downloaded", "failed")
  - `created_at`: Timestamp when image was processed

- **session_codes:** Store AliExpress API session credentials

  - `id`: Auto-incrementing primary key
  - `code`: Authorization code used to create the session (unique)
  - `access_token`: Current access token for API calls
  - `refresh_token`: Token used to refresh the access token
  - `expire_time`: Access token expiration time (milliseconds string)
  - `refresh_token_valid_time`: Refresh token expiration time
  - `expires_in`: Token lifetime in seconds (string)
  - `refresh_expires_in`: Refresh token lifetime in seconds
  - `havana_id`: Havana ID from AliExpress
  - `locale`: User locale setting
  - `user_nick`: AliExpress user nickname
  - `account_id`: Account identifier
  - `user_id`: User identifier
  - `account_platform`: Account platform
  - `sp`: SP parameter
  - `request_id`: API request identifier
  - `seller_id`: Seller identifier
  - `account`: Associated account information
  - `token_type`: Token type ('original' or 'refreshed')
  - `is_active`: Boolean indicating if session is currently active
  - `response_json`: Full API response (JSON)
  - `created_at`: When the session was created
  - `updated_at`: When the session was last updated

- **product_status:** Store duplicate detection results (Module C)

  - `id`: Auto-incrementing primary key
  - `product_id`: Foreign key to filtered_products table (unique)
  - `status`: Product status ("UNIQUE", "MASTER", "DUPLICATE", "REVIEW_SUSPECT")
  - `duplicate_master_id`: Points to master product if this is a duplicate
  - `total_landed_cost`: Item price + shipping cost for master selection
  - `detection_method`: How duplicates were identified ("PHASH", "CLIP", "MANUAL")
  - `phash_difference`: Hamming distance between pHash values (0-64)
  - `clip_similarity`: CLIP similarity score (0.0-1.0)
  - `created_at`: When the analysis was performed
  - `updated_at`: When the record was last updated

- **job_runs:** Log information about script executions

  - `id`: Auto-incrementing primary key
  - `job_type`: Type of job (e.g., "HARVEST_INIT", "FILTER_PRODUCTS")
  - `start_time`: When the job started (UTC)
  - `end_time`: When the job completed (UTC)
  - `duration_seconds`: Job duration in seconds
  - `found_count`: Number of items found
  - `new_count`: Number of new items added
  - `skipped_count`: Number of items skipped
  - `error_count`: Number of errors encountered
  - `keywords`: Comma-separated or JSON string of keywords used
  - `categories`: Comma-separated or JSON string of categories used
  - `details`: Additional job details as JSON

The database is managed using SQLAlchemy ORM for better code organization and type safety.

## Usage

For a detailed explanation of each command, workflow steps, and troubleshooting, please refer to the [Quality Assurance Guide](./QA.md).

### Merchant Harvesting

**Initialize merchant harvest:**

```bash
python main.py harvest:init
```

Options:

- `--limit N`: Process only the first N products
- `--dry-run`: Simulate without writing to database

**Run incremental (delta) harvest:**

```bash
python main.py harvest:delta
```

Options:

- `--limit N`: Process only the first N products
- `--dry-run`: Simulate without writing to database

**Show harvest status:**

```bash
python main.py harvest:status
```

This displays:

- Seller counts by approval status
- Recent job runs with statistics
- Useful SQL queries for analysis

### Product Filtering

**Filter products from whitelisted sellers:**

```bash
python main.py filter:products
```

Options:

- `--max-price PRICE`: Maximum total price in EUR (variant + shipping)
- `--max-delivery DAYS`: Maximum delivery time in days
- `--limit N`: Process only the first N products
- `--dry-run`: Simulate without writing to database

This command:

- Processes products from WHITELIST approved sellers
- Enriches products with shipping information and delivery estimates
- Applies business rules for price and delivery time filtering
- Stores qualifying products in the `filtered_products` table
- Automatically triggers image ingestion for processed products

### Session Management

The pipeline requires valid AliExpress API sessions for accessing product details and shipping information.

**Create a new session:**

```bash
python main.py create_session --code YOUR_AUTHORIZATION_CODE
```

This command:

- Creates a new API session using an authorization code from AliExpress
- Stores session credentials in the database
- Returns session details including access token

**Refresh an existing session:**

```bash
python main.py refresh_session
```

Options:

- `--token TOKEN`: Specific access token to refresh
- `--refresh-token REFRESH_TOKEN`: Specific refresh token to use

This command:

- Automatically uses stored session credentials from database (preferred)
- Can manually refresh using provided tokens
- Updates database with new session credentials

**List all sessions:**

```bash
python main.py list_sessions
```

This displays:

- All stored sessions with their status
- Session metadata (user, account, dates)
- Active vs inactive session indicators

### Merchant Review Workflow

**Export pending merchants for review:**

```bash
python main.py review:export-pending
```

Options:

- `--output PATH`: Specify output CSV file path (default: data/pending_merchants.csv)

**Import review results:**

```bash
python main.py review:import-results
```

Options:

- `--input PATH`: Specify input CSV file path (default: data/reviewed_merchants.csv)

## Search Configuration

The pipeline can search for products using two methods:

1. **Category-based search**: Searches for products within specific AliExpress categories
2. **Keyword-based search**: Searches for products matching specific keywords

You can configure the pipeline to use:

- Only category-based search (recommended for most cases)
- Both category and keyword-based search together

### Blacklisting by Product Title

The pipeline automatically blacklists products and their sellers if the product title contains certain blacklisted terms. This helps filter out unwanted products during the harvesting process:

```
# Add terms that should automatically blacklist products if found in titles
BLACKLIST_TERMS_IN_TITLE=bead,beads,supply,supplies,making,diy,component,findings
```

When a product title contains any of these terms:

- The product is marked with status="BLACKLIST" in the database
- The seller is marked with approval_status="BLACKLIST" in the database
- These items are counted in the "blacklisted" statistics during harvesting

This automatic blacklisting happens during the harvesting process, not after review import.

For detailed information about configuring search parameters, avoiding jewelry supplies, and best practices, see the [Search Configuration Guide](./SEARCH_CONFIG.md).

### Category IDs

Category IDs are configured directly in the `.env` file:

```
CATEGORIES=200001680,1509,201239108,200370154
```

Recommended categories for finished jewelry:

- `200001680`: Fine Jewelry
- `1509`: Fashion Jewelry
- `201239108`: Customized Jewelry
- `200370154`: Smart Jewelry

Categories to avoid (primarily contain supplies):

- `200001479`: Jewelry Packaging & Display
- `200001478`: Jewelry Tools & Equipment
- `201238105`: Jewelry Making

The system efficiently searches multiple categories at once and implements paging with a default page size of 50.

## Data Analysis

Useful SQL queries for analyzing the data:

```sql
-- Get merchants found in the last 24 hours
SELECT COUNT(*) FROM sellers WHERE first_seen_at >= NOW() - INTERVAL '24 hours';

-- Get distribution of approval status
SELECT approval_status, COUNT(*) FROM sellers GROUP BY approval_status;

-- Get products by category

GROUP BY pc.category_name
ORDER BY COUNT(*) DESC;
```

## Configuration

The pipeline is configured via:

- `.env`: Contains database credentials, API keys, and application settings. This file must be created by copying `.env.example` and updating it with your personal RapidAPI key and other settings.
- `SEARCH_CONFIG.md`: Contains detailed documentation on configuring search parameters to avoid jewelry supplies

## Complete Workflow

The product data pipeline follows this enhanced workflow:

### Phase 1: Data Collection

1. **Session Setup** - Run `create_session` to establish API credentials for detailed product data access
2. **Initial Data Collection** - Run `harvest:init` to collect initial merchant and product data
3. **Regular Updates** - Run `harvest:delta` periodically to update and add new data

### Phase 2: Merchant Review

4. **Export for Review** - Use `review:export-pending` to create a CSV file for expert reviewers
5. **Expert Review** - Reviewers update approval statuses in the CSV file (WHITELIST or BLACKLIST)
6. **Import Results** - Use `review:import-results` to update the database with reviewer decisions

### Phase 3: Product Processing

7. **Product Filtering** - Run `filter:products` to process products from whitelisted sellers
8. **Image Ingestion** - Automatically triggered during filtering to categorize and store product images
9. **Session Maintenance** - Run `refresh_session` as needed to maintain API access

### Phase 4: Monitoring

10. **Monitor & Analyze** - Use `harvest:status` and `list_sessions` to track pipeline performance

**Key Features:**

- **Automated Image Processing**: Products are automatically processed for hero, gallery, and variant images
- **Shipping Cost Integration**: Real shipping costs and delivery times are calculated and stored
- **Business Rule Filtering**: Products are filtered based on total cost (product + shipping) and delivery time
- **Session Management**: Robust API session handling with automatic refresh capabilities
- **Intelligent Duplicate Detection**: Advanced image-based duplicate detection using pHash and CLIP analysis

For a detailed explanation of each step, expected outcomes, and troubleshooting guidance, refer to the [Quality Assurance Guide](./QA.md).

## Module C: Duplicate Detection & Selection

Module C provides intelligent duplicate detection capabilities using advanced image analysis techniques. It identifies duplicate products based on image similarity and selects the best representative from each duplicate group.

### Overview

The duplicate detection system uses a two-stage cascade approach:

1. **pHash Analysis**: Fast perceptual hashing to identify potential duplicates
2. **CLIP Analysis**: Deep learning-based semantic image analysis for ambiguous cases
3. **Master Selection**: Intelligent selection of the best product from each duplicate group

### Database Schema

Module C extends the database with:

- **product_status:** Track duplicate detection results
  - `id`: Auto-incrementing primary key
  - `product_id`: Foreign key to filtered_products table (unique)
  - `status`: One of ["UNIQUE", "DUPLICATE", "MASTER", "REVIEW_SUSPECT"]
  - `duplicate_master_id`: Points to the master product in duplicate groups
  - `total_landed_cost`: Total cost including shipping (for master selection)
  - `detection_method`: How duplicates were identified ("PHASH", "CLIP", "MANUAL")
  - `phash_difference`: Hamming distance between pHash values (0-64)
  - `clip_similarity`: CLIP similarity score (0.0-1.0)
  - `created_at`: When the analysis was performed
  - `updated_at`: When the record was last updated

### Duplicate Detection Commands

**Run duplicate detection:**

```bash
python main.py detect:duplicates
```

Options:

- `--limit N`: Process only the first N products
- `--force`: Reprocess products that already have status

This command:

- Analyzes all filtered products for duplicates using pHash and CLIP
- Uses intelligent cascade: pHash for fast screening, CLIP for ambiguous cases
- Groups duplicates and selects the master (lowest total landed cost)
- Updates the product_status table with results

**Show detection status:**

```bash
python main.py detect:status
```

This displays:

- Count of products by status (UNIQUE, DUPLICATE, MASTER, REVIEW_SUSPECT)
- Detection method statistics (pHash vs CLIP)
- Average similarity scores and confidence levels
- Recent detection job statistics

**Export suspects for review:**

```bash
python main.py detect:export-suspects
```

Options:

- `--output PATH`: Specify output CSV file path (default: data/review_suspects.csv)

This command:

- Exports all REVIEW_SUSPECT cases to CSV for manual review
- Includes product details, similarity scores, and image information
- Allows manual verification of edge cases

### Configuration

Duplicate detection is configured via `.env` variables:

```bash
# pHash thresholds (Hamming distance, 0-64)
PHASH_DUPLICATE_THRESHOLD=2       # ≤2: Definitely duplicate
PHASH_AMBIGUOUS_THRESHOLD=18      # 3-18: Send to CLIP analysis
                                  # >18: Definitely not duplicate

# CLIP threshold (similarity score, 0.0-1.0)
CLIP_DUPLICATE_THRESHOLD=0.95     # ≥0.95: Confirmed duplicate

# CLIP model configuration
CLIP_MODEL=ViT-B/32              # Model type (CPU-friendly)
CLIP_DEVICE=auto                 # Device: 'auto', 'cpu', or 'cuda'
CLIP_IMAGE_ROLES=hero,variant    # Image types to analyze
CLIP_MAX_IMAGES_PER_PRODUCT=5    # Limit for efficiency
```

### Detection Logic

1. **pHash Stage**:

   - Compare perceptual hashes of all product image pairs
   - If distance ≤ `PHASH_DUPLICATE_THRESHOLD`: Mark as duplicate (skip CLIP)
   - If distance ≤ `PHASH_AMBIGUOUS_THRESHOLD`: Send to CLIP analysis
   - If distance > `PHASH_AMBIGUOUS_THRESHOLD`: Mark as unique

2. **CLIP Stage** (for ambiguous cases):

   - Generate semantic embeddings for hero and variant images
   - Calculate cosine similarity between embeddings
   - If similarity ≥ `CLIP_DUPLICATE_THRESHOLD`: Mark as duplicate
   - Otherwise: Mark as unique

3. **Master Selection**:
   - Group all duplicates together
   - Select the product with the lowest `total_landed_cost` as master
   - Mark master as "MASTER", others as "DUPLICATE"
   - Store detection metadata (method, scores, confidence)

### Integration with Pipeline

Module C integrates seamlessly with the existing pipeline:

- Runs after product filtering and image ingestion
- Uses existing `filtered_products` and `product_images` tables
- Maintains referential integrity with foreign keys
- Provides clear status tracking for downstream processes

For detailed testing procedures and troubleshooting, see the [Quality Assurance Guide](./QA.md).

## Documentation

The project includes several documentation files:

- [QA.md](./QA.md): Quality assurance guide with detailed explanations of each command, step-by-step testing procedures, complete workflow documentation, and troubleshooting guidance.

- [SEARCH_CONFIG.md](./SEARCH_CONFIG.md): Comprehensive guide on configuring search parameters, avoiding jewelry supplies, and optimizing category-based searches.

Use these guides for testing the application, understanding the complete data pipeline workflow, and optimizing your search configuration.

## License

All rights reserved. This code is proprietary and confidential.
