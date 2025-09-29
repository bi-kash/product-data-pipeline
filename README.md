# Product Data Pipeline

A modular data pipeline for harvesting and processing jewelry product data from AliExpress using the official AliExpress Dropship API.

## Overview

This project implements a modular pipeline to identify jewelry products from AliExpress, qualify them based on rules, and prepare them for processing by virtual assistants.

### Pipeline Modules

- **Session Management:** API credential handling and automatic token refresh (required first)
- **Module A (Harvesting):** Merchant discovery and product collection via category-based search
- **Module B (Filtering):** Product qualification, enrichment, and image processing
- **Module C (Duplicate Detection):** Advanced image-based duplicate detection and master selection

### Key Features

- **Automated Product Discovery:** Keyword-based search with intelligent filtering
- **Image Processing Pipeline:** Automatic categorization, download, and perceptual hashing
- **Shipping Cost Integration:** Real shipping costs and delivery time calculations
- **Duplicate Detection:** Advanced pHash + CLIP analysis for identifying duplicate products
- **Business Rule Filtering:** Configurable price and delivery time constraints
- **VA Workflow Support:** CSV export/import for merchant verification

## Requirements

- Python 3.10 or newer
- PostgreSQL database (e.g., via Supabase)
- AliExpress Dropship API credentials

## Installation

1. Clone the repository
2. Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Create a `.env` file by copying `.env.example`:

```bash
cp .env.example .env
```

5. **Set up AliExpress Dropship API access**:

   - Obtain AliExpress Dropship API credentials from the official AliExpress developer portal
   - Follow AliExpress documentation for OAuth 2.0 authorization flow
   - Get your authorization code for session creation

6. Edit the `.env` file with your database and API configuration. Required variables include:

```
# Database configuration
USE_SQLITE=true  # Set to false for PostgreSQL
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=product_pipeline

# AliExpress Dropship API configuration
IOP_APPKEY=YOUR_APP_KEY
IOP_APPSECRET=YOUR_APP_SECRET
# (Session will be created via authorization code)

# Region settings (for price and shipping data)
ALIEXPRESS_TARGET_CURRENCY=EUR
ALIEXPRESS_TARGET_LANGUAGE=en_US
ALIEXPRESS_TARGET_COUNTRY=DE
```

> **Note**: Never commit your `.env` file to version control as it contains sensitive information like your API credentials.

## Database Setup

The application automatically creates the required tables when first run. For complete database schema information including all tables, relationships, and field specifications, see [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md).

The database is managed using SQLAlchemy ORM for better code organization and type safety, with support for both SQLite (development) and PostgreSQL (production).

## Usage by Module

For a detailed explanation of each command, workflow steps, and troubleshooting, please refer to the [Quality Assurance Guide](./QA.md).

## Session Management

Session management is **required first** before any module operations, as the pipeline uses the official AliExpress Dropship API which requires authenticated sessions.

### Commands

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

### Troubleshooting Token Issues

If you see repeated token refresh errors during operations:

**Error Pattern:**
```
ERROR:src.session.session_manager:Failed to refresh token: The specified refresh token is invalid or expired
ERROR:src.session.session_manager:Error refreshing token: (sqlite3.OperationalError) database is locked
```

**Solution:**
The pipeline now includes intelligent error handling that will stop repeated attempts and provide clear guidance:

1. **Circuit Breaker:** After 3 failed refresh attempts, the system will stop trying and display instructions
2. **Clear Instructions:** Get a new authorization code and create a fresh session
3. **Automatic Recovery:** Creating a new session clears the error state

**Steps to Fix:**
1. Get a new authorization code from [AliExpress Open Platform](https://open.aliexpress.com/oauth/authorize?response_type=code&force_auth=true&redirect_uri=urn:ietf:wg:oauth:2.0:oob&client_id=YOUR_APP_KEY)
2. Create a new session: `python main.py create_session --code YOUR_NEW_CODE`
3. Resume your operations

## Module A: Merchant Harvesting & Product Discovery

This module handles initial data collection from AliExpress via category-based searches.

### Commands

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

### Configuration

Module A uses category-based search with optional keyword support:

```bash
# Core search configuration
CATEGORIES=200001680,1509,201239108,200370154
IGNORE_CATEGORIES=200001479,200001478,201238105  # Categories to exclude

# Auto-blacklisting by product title
BLACKLIST_TERMS_IN_TITLE=bead,beads,supply,supplies,making,diy,component,findings
```

Recommended categories for finished jewelry:

- `36`: Includes all Jewelry
- `200001680`: Fine Jewelry
- `1509`: Fashion Jewelry
- `201239108`: Customized Jewelry
- `200370154`: Smart Jewelry

For detailed search configuration guidance, see [SEARCH_CONFIG.md](./SEARCH_CONFIG.md).

### Commands

**Filter products from whitelisted sellers:**

```bash
python main.py filter:products
```

Options:

- `--limit N`: Process only the first N products
- `--dry-run`: Simulate without writing to database

This command:

- Processes products from WHITELIST approved sellers
- Enriches products with shipping information and delivery estimates
- Applies business rules for price and delivery time filtering
- Stores qualifying products in the `filtered_products` table
- Automatically triggers image ingestion for processed products

### Image Processing Features

Module B automatically handles:

- **Image Categorization**: Separates hero, gallery, and variant images
- **Property Mapping**: Links variant images to SKU properties (color, size, etc.)
- **Image Download**: Downloads and stores images locally with relative paths
- **Perceptual Hashing**: Calculates pHash for duplicate detection
- **Dimension Extraction**: Determines image width and height
- **Deduplication**: Prevents storing duplicate images
- **S3 Upload**: Uploads images to AWS S3 with anonymized UUID filenames for public access

### S3 Image Storage (Optional)

The pipeline supports uploading downloaded images to AWS S3 for public access:

**Configuration:**

1. Add to your `.env` file:

```bash
# AWS S3 Configuration for Image Upload
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-product-images-bucket
S3_IMAGES_PREFIX=product-images/    # Optional prefix for organizing images
```

2. Configure your S3 bucket for public read access:
   - Go to S3 Console → Your Bucket → Permissions
   - Configure "Block public access" settings as needed
   - Add a bucket policy to allow public read access to uploaded images
   - Run `python test_s3_setup.py` to get the exact policy for your configuration

**Usage:**

```bash
# Download and upload images for all products
python -m src.ingestion.image_ingestion ingest --download --s3

# Download and upload images for a specific product
python -m src.ingestion.image_ingestion product <product_id> --download --s3

# Test S3 configuration and upload
python test_s3_setup.py
```

**Features:**

- **Anonymized Filenames**: Uses UUID-based filenames to protect privacy
- **Public Access**: Uploaded images are publicly accessible via HTTPS URLs
- **Metadata Preservation**: Stores original product ID and image role in S3 metadata
- **Error Handling**: Gracefully handles upload failures and continues processing
- **Original Extension Preservation**: Maintains original file extensions (.jpg, .png, etc.)

### Business Rules

Configurable filtering criteria (set via environment variables):

```bash
# Price and delivery constraints
TOTAL_MAX_PRICE=300
# Maximum delivery time in days for fast shipping filter
MAX_DELIVERY_DAYS=20
```

## Module C: Duplicate Detection & Selection

Module C provides intelligent duplicate detection capabilities using advanced image analysis techniques. It identifies duplicate products based on image similarity and selects the best representative from each duplicate group.

### Overview

The duplicate detection system uses a two-stage cascade approach:

1. **pHash Analysis**: Fast perceptual hashing to identify potential duplicates
2. **CLIP Analysis**: Deep learning-based semantic image analysis for ambiguous cases
3. **Master Selection**: Intelligent selection of the best product from each duplicate group

### Commands

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

- `--output PATH`: Specify output CSV file path (default: data/suspect_duplicates.csv)

This command:

- Exports all REVIEW_SUSPECT cases to CSV for manual review with S3 image URLs
- Includes complete product details (titles, prices, costs) without AliExpress URLs
- Shows CLIP similarity scores with closest matching images identified
- Separates main/hero images from other product images for easy comparison
- Provides direct S3 access to all images for instant browser viewing
- Creates empty status and notes columns for manual input

**Import reviewed suspects:**

```bash
python main.py detect:import-reviewed
```

Options:

- `--input PATH`: Specify input CSV file path (default: data/suspect_duplicates.csv)
- `--dry-run`: Show what would be updated without making changes

This command:

- Imports manually reviewed suspect duplicates from CSV
- Updates product status based on review decisions (DUPLICATE, UNIQUE, UNCERTAIN)
- Automatically performs master reassignment when needed
- If a reviewed suspect should become the new master (e.g., lower cost), the system:
  - Reassigns the entire duplicate group to the new master
  - Updates all related duplicates to point to the new master
  - Converts the previous master to a duplicate
- Provides detailed feedback on all changes made

### Manual Review Workflow

The duplicate detection system includes a comprehensive manual review workflow for ambiguous cases:

#### 1. Export Review Cases

```bash
python main.py detect:export-suspects --output data/review_cases.csv
```

This creates a CSV file with the following columns:

**Product Information:**
- `master_product_id`, `duplicate_product_id`: Product identifiers
- `master_title`, `duplicate_title`: Product titles
- `master_price`, `duplicate_price`, `duplicate_cost`: Pricing information

**S3 Image URLs (Direct Browser Access):**
- `master_image`, `duplicate_image`: **Closest matching images with CLIP similarity scores**
- `master_main_image`, `duplicate_main_image`: Hero/primary images
- `master_images`, `duplicate_images`: All other product images (pipe-separated)

**Analysis Data:**
- `phash_difference`, `clip_similarity`: Similarity metrics
- `status` (empty), `notes` (empty): **Review decision fields**

#### 2. Manual Review Process

Open the CSV in Excel or similar spreadsheet application:

1. **Compare Product Details**: Review `master_title` vs `duplicate_title`, pricing differences
2. **Examine Images**: Click S3 URLs to view images directly in browser:
   - `master_image` vs `duplicate_image`: **Closest matching images** (includes CLIP similarity scores)
   - `master_main_image` vs `duplicate_main_image`: Hero images comparison
   - `master_images` vs `duplicate_images`: All product images (pipe-separated lists)
3. **Analyze Similarity Metrics**: 
   - **pHash difference**: Lower values indicate more similar images (0 = identical)
   - **CLIP similarity**: Higher values indicate more similar content (1.0 = identical)
4. **Make Decision**: Fill the `status` column with:
   - `duplicate`: Products are the same (will be deleted)
   - `keep`: Products are different (will be kept)
   - `manual`: Need further review (keep as review suspect)
5. **Add Notes**: Optional explanations in the `notes` column

#### 3. Import Review Results

```bash
python main.py detect:import-reviewed --input data/review_cases.csv
```

The import process will:

- **Process Review Decisions**: Handle `status` column values (`duplicate`, `keep`, `manual`)
- **Backward Compatibility**: Also supports old format (`review_decision` column with `DUPLICATE`/`UNIQUE`)
- **Update Product Status**: Apply decisions to database automatically
- **Master Reassignment**: Handle duplicate product merging and status updates
- **Detailed Feedback**: Show summary of all changes made during import

#### 4. Master Reassignment Logic

When a REVIEW_SUSPECT is marked as DUPLICATE, the system checks if it should become the new master:

- **Price Comparison**: If the suspect has a lower cost than the current master
- **Automatic Reassignment**: The entire duplicate group is reassigned to the new master
- **Status Updates**: Previous master becomes DUPLICATE, new master becomes MASTER
- **Group Consistency**: All duplicates point to the new master

**Example Scenario:**

```
Current State:  Master A (€120) ← Duplicate B, Suspect C (€100)
Review Result:  C marked as DUPLICATE
Final State:    Master C (€100) ← Duplicate A, Duplicate B
```

#### 5. Dry Run Testing

Always test with dry-run first to preview changes:

```bash
python main.py detect:import-reviewed --input data/review_cases.csv --dry-run
```

This shows exactly what would be updated without making any database changes.

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
   - If similarity in ambiguous range (0.94-0.955): Mark as REVIEW_SUSPECT
   - Otherwise: Mark as unique

3. **Master Selection & Review Assignment**:

   - Group confirmed duplicates together
   - Select the product with the lowest `total_landed_cost` as master
   - Mark master as "MASTER", others as "DUPLICATE"
   - REVIEW_SUSPECT products are assigned to existing masters for manual review
   - All REVIEW_SUSPECT products have a `duplicate_master_id` pointing to their suspected master
   - Store detection metadata (method, scores, confidence)

4. **Three-Tier Decision System**:
   - **DUPLICATE**: High confidence duplicates (automatic processing)
   - **REVIEW_SUSPECT**: Ambiguous cases requiring manual review
   - **UNIQUE**: Confirmed unique products

## Search Configuration

The pipeline supports both category-based and keyword-based search methods. Category-based search is recommended for most use cases as it provides better precision for finished jewelry products.

For comprehensive search configuration guidance including category recommendations and blacklisting strategies, see [SEARCH_CONFIG.md](./SEARCH_CONFIG.md).

## Data Analysis

For complete database schema information and useful SQL queries for analyzing pipeline data, see [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md).

## Configuration

The pipeline is configured via:

- `.env`: Contains database credentials, API keys, and application settings. This file must be created by copying `.env.example` and updating it with your AliExpress Dropship API credentials and other settings.
- `SEARCH_CONFIG.md`: Contains detailed documentation on configuring search parameters to avoid jewelry supplies

## Complete Pipeline Workflow

The product data pipeline follows this modular workflow across all three modules:

### Phase 1: Module A - Data Collection & Merchant Review

1. **Session Setup** - Run `create_session` to establish API credentials
2. **Initial Harvest** - Run `harvest:init` to collect merchants and products
3. **Merchant Review** - Export pending merchants, review, and import decisions
4. **Delta Updates** - Run `harvest:delta` periodically for new data

### Phase 2: Module B - Product Processing & Enrichment

5. **Product Filtering** - Run `filter:products` on whitelisted sellers
6. **Image Processing** - Automatically categorizes, downloads, and analyzes images
7. **Shipping Integration** - Calculates real shipping costs and delivery times
8. **Business Rules** - Applies price and delivery constraints

### Phase 3: Module C - Duplicate Detection & Master Selection

9. **Duplicate Detection** - Run `detect:duplicates` using pHash and CLIP analysis
10. **Master Selection** - Automatically selects best product from duplicate groups
11. **Export Review Cases** - Run `detect:export-suspects` to get ambiguous cases
12. **Manual Review** - Review CSV file and make DUPLICATE/UNIQUE decisions
13. **Import Decisions** - Run `detect:import-reviewed` to apply manual decisions
14. **Master Reassignment** - System automatically handles master changes when needed
15. **Status Tracking** - Monitor detection results and pipeline performance

### Phase 4: Monitoring & Maintenance

13. **Session Management** - Refresh API sessions as needed
14. **Status Monitoring** - Use status commands to track each module
15. **Data Analysis** - Query database for insights and optimization

**Pipeline Benefits:**

- **Modular Design**: Each module can run independently
- **Automated Processing**: Minimal manual intervention required
- **Quality Assurance**: Built-in review workflows for edge cases
- **Scalable Architecture**: Handles large product catalogs efficiently
- **Intelligent Deduplication**: Advanced ML techniques for accurate duplicate detection

For detailed step-by-step procedures and troubleshooting, see [QA.md](./QA.md).

## Documentation

The project includes comprehensive documentation organized by topic:

- **[DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md)**: Complete database schema reference with all tables, relationships, field specifications, and useful SQL queries for data analysis.

- **[QA.md](./QA.md)**: Quality assurance guide with detailed explanations of each command, step-by-step testing procedures, complete workflow documentation, and troubleshooting guidance.

- **[SEARCH_CONFIG.md](./SEARCH_CONFIG.md)**: Comprehensive guide on configuring search parameters, avoiding jewelry supplies, and optimizing category-based searches.

Use these guides for understanding the database structure, testing the application, understanding the complete data pipeline workflow, and optimizing your search configuration.

## License

All rights reserved. This code is proprietary and confidential.
