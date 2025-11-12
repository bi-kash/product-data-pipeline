# Product Data Pipeline

A modular data pipeline for harvesting and processing jewelry product data from AliExpress using the official AliExpress Dropship API.

## Overview

This project implements a modular pipeline to identify jewelry products from AliExpress, qualify them based on rules, and prepare them for processing by virtual assistants.

### Pipeline Modules

- **Session Management:** API credential handling and automatic token refresh (required first)
- **Module A (Harvesting):** Merchant discovery and product collection via category-based search
- **Module B (Filtering):** Product qualification, enrichment, and image processing
- **Module C (Duplicate Detection):** Advanced image-based duplicate detection and master selection
- **Module D (Airtable Integration):** Data export to Airtable with anonymization and structured schema

### Key Features

- **Automated Product Discovery:** Keyword-based search with intelligent filtering
- **Image Processing Pipeline:** Automatic categorization, download, and perceptual hashing
- **Shipping Cost Integration:** Real shipping costs and delivery time calculations
- **Duplicate Detection:** Advanced pHash + CLIP analysis for identifying duplicate products
- **Business Rule Filtering:** Configurable price and delivery time constraints
- **Airtable Integration:** Automated export with data anonymization and structured three-table schema
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

## Quick Start Demo

For a complete demonstration of the entire pipeline, use the automated demo script:

### Automated Pipeline Demo

```bash
python demo_pipeline.py
```

**⚠️ WARNING: This script will DELETE ALL DATABASE DATA!**

This comprehensive demo script demonstrates the complete pipeline workflow by:

1. **🗑️ Database Cleanup**: Deletes all existing data (with safety confirmation)
2. **🔑 API Session Setup**: Guides you through creating a fresh API session
3. **🌾 Merchant Harvest**: Harvests 100 products from AliExpress
4. **👥 Seller Approval**: Randomly assigns seller statuses (80% whitelist, 10% blacklist, 10% pending)
5. **🔍 Product Filtering**: Filters products based on business rules
6. **🔍 Duplicate Detection**: Runs advanced duplicate detection using pHash + CLIP
7. **📊 Airtable Integration**: Creates Airtable base and syncs all processed data

**Features:**

- **Interactive Setup**: Provides authorization URL and guides you through API setup
- **Safety Confirmations**: Multiple confirmations before deleting data
- **Progress Tracking**: Clear progress indicators and status messages
- **Error Handling**: Graceful error handling with helpful messages
- **Final Statistics**: Complete summary of pipeline results

**Options:**

```bash
python demo_pipeline.py --skip-confirmation  # Skip safety prompts (use with caution)
python demo_pipeline.py --help              # Show help
```

**Demo Workflow:**

1. **Run the demo**: `python demo_pipeline.py`
2. **Confirm data deletion** (script will ask for confirmation)
3. **Get authorization code**:
   - Script provides AliExpress authorization URL
   - Visit the URL in your browser
   - Log in and authorize the application
   - Copy the authorization code from the redirect URL
   - Paste it into the script
4. **Watch the pipeline run** through all modules automatically
5. **Review results** in terminal and check your Airtable base

**Expected Results:**

- Complete product catalog with images and variants
- Duplicate detection results with master selection
- Structured Airtable base with anonymized data
- Statistics showing seller approval distribution and processing results

This demo is perfect for:

- **New users** wanting to see the complete pipeline in action
- **Testing** the pipeline with fresh data
- **Demonstrations** to stakeholders or team members
- **Development** testing after making changes

**Demo Files:**

- `demo_pipeline.py`: Main automated demo script
- `demo_usage_examples.sh`: Usage examples and help

**Alternative: Individual Module Testing**

If you prefer to run modules individually, follow the detailed usage instructions in each module section below.

## Usage by Module

For a detailed explanation of each command, workflow steps, and troubleshooting, please refer to the [Quality Assurance Guide](./QA.md).

## Session Management

Session management is **required first** before any module operations, as the pipeline uses the official AliExpress Dropship API which requires authenticated sessions.

### Commands

**Get a Link to code:**

```bash
python main.py get_code_link
```

This command:

- Gives you the URL to get the code.
- Open the URL above in your browser
- Log in to your AliExpress account
- Authorize the application
- Copy the authorization code from the callback URL
- Use the code with: python main.py create_session --code YOUR_CODE

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

## Module B: Product Filtering & Enrichment

This module handles product qualification, enrichment, image/video processing, and autonomous product discovery.

### Commands

**Scrape product IDs from seller stores:**

```bash
python main.py filter:scraper
```

Options:

- `--limit N`: Process only the first N sellers
- `--dry-run`: Simulate without writing to database

This command:

- Uses Selenium to scrape product IDs from whitelisted seller store pages
- Populates `scraped_products` table with product IDs only (no API calls)
- Updates `scraper_progress` table to track which sellers have been scraped
- Marks sellers as scraped (status='scraped') or failed (status='failed')
- Does **not** fetch product details or populate the `products` table (that's done by `filter:products`)

**Filter products from whitelisted sellers:**

```bash
python main.py filter:products
```

Options:

- `--limit N`: Process only the first N products
- `--dry-run`: Simulate without writing to database

This command performs a comprehensive end-to-end product processing workflow:

**1. Product Extraction & Enrichment:**

- Processes products from `scraped_products` table (populated by `filter:scraper`)
- Fetches complete product details from AliExpress API
- Enriches products with real shipping costs and delivery time estimates
- Applies business rules for price and delivery time filtering
- Stores qualifying products in the `filtered_products` table

**2. Automatic Image & Video Processing:**

- **Image Ingestion**: Downloads and categorizes all product images (hero, gallery, variant)
- **Image Upload**: Uploads images to S3 with anonymized UUID filenames
- **Video Processing**: Extracts video URLs from API response, downloads videos, and uploads to S3
- **Perceptual Hashing**: Calculates pHash for duplicate detection
- **Database Storage**: Saves image/video metadata with S3 URLs to `product_images` and `product_videos` tables

**3. Autonomous Workflow:**

- Processes products **one at a time** to ensure complete processing
- When no more products remain in `scraped_products`:
  - Automatically identifies unscraped or incomplete sellers from `scraper_progress`
  - **Retries any incomplete sellers** (failed, in_progress, etc.) before moving to new sellers
  - Scrapes the next seller's store page using Selenium
  - Populates `scraped_products` with new product IDs
  - Continues filtering automatically
- This creates a **self-sustaining pipeline** that continuously discovers and processes products
- **Automatic retry logic**: Any incomplete scraping attempts are automatically retried on subsequent runs

### Two-Step Workflow

Module B uses a efficient two-step approach:

**Step 1: ID Collection (`filter:scraper`)**

- Fast Selenium-based scraping of product IDs from seller store pages
- No API calls, no product detail fetching
- Populates `scraped_products` with IDs for later processing
- Can scrape multiple sellers quickly without API rate limits

**Step 2: Product Processing (`filter:products`)**

- Fetches complete product details from AliExpress API for each ID
- Applies business rules and enriches with shipping data
- Downloads and uploads images/videos to S3
- Stores qualifying products in `filtered_products`
- Automatically triggers Step 1 when no IDs remain (autonomous mode)

**Benefits:**

- **Decoupled Processing**: Scraping and API fetching are separated
- **API Efficiency**: Only fetch details for products from whitelisted sellers
- **Autonomous Operation**: `filter:products` runs continuously without manual intervention
- **Progress Tracking**: `scraper_progress` table tracks which sellers have been processed

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
S3_IMAGES_PREFIX=product-images/
S3_VIDEOS_PREFIX=product-images/
```

2. Configure your S3 bucket for public read access:
   - Go to S3 Console → Your Bucket → Permissions
   - Configure "Block public access" settings as needed
   - Add a bucket policy to allow public read access to uploaded images and videos
     ```bash
     {
        "Version": "2012-10-17",
        "Statement": [
           {
                 "Sid": "PublicReadGetObject",
                 "Effect": "Allow",
                 "Principal": "*",
                 "Action": "s3:GetObject",
                 "Resource": "arn:aws:s3:::productpipeline/product-images/*"
           }
        ]
     }
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

The duplicate detection system uses a two-stage cascade approach with comprehensive verification capabilities:

1. **pHash Analysis**: Fast perceptual hashing to identify potential duplicates
2. **CLIP Analysis**: Deep learning-based semantic image analysis for ambiguous cases
3. **Master Selection**: Intelligent selection of the best product from each duplicate group
4. **Manual Verification**: Export confirmed duplicates and suspects for accuracy verification and parameter tuning

**Key Benefits**:

- **Intelligent Cascade**: Fast pHash screening reduces expensive CLIP computations by ~85%
- **Tunable Parameters**: Comprehensive threshold configuration for different accuracy requirements
- **Quality Assurance**: Export functionality enables manual verification of algorithm decisions
- **Continuous Improvement**: Analysis tools support iterative parameter optimization

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

**Export confirmed duplicates for analysis:**

```bash
python main.py detect:export-duplicates
```

Options:

- `--output PATH`: Specify output CSV file path (default: data/confirmed_duplicates.csv)

This command:

- **Purpose**: Verify accuracy of automatic duplicate detection decisions
- **Content**: All products with status=DUPLICATE and their master products
- **Data Included**:
  - Master-duplicate product relationships with titles and prices
  - S3 URLs for direct image/video comparison in browser
  - Detection scores (pHash differences, CLIP similarities)
  - All images organized as: closest match, main/hero, and other images
- **Use Cases**:
  - **Accuracy Verification**: Manually verify that detected duplicates are truly duplicates
  - **Parameter Tuning**: Analyze score distributions to optimize thresholds
  - **Quality Assurance**: Review algorithm performance before production deployment
  - **Data Analysis**: Understand duplicate patterns in your product catalog

**CSV Format** (for both export commands):

| Column                 | Description                                | Example                                            |
| ---------------------- | ------------------------------------------ | -------------------------------------------------- |
| `master_product_id`    | ID of the master product                   | `1005010018511535`                                 |
| `duplicate_product_id` | ID of the duplicate product                | `1005010018511536`                                 |
| `master_title`         | Title of master product                    | `Gold Plated Chain Necklace...`                    |
| `duplicate_title`      | Title of duplicate product                 | `18K Gold Chain Necklace...`                       |
| `master_image`         | **Closest matching image** with CLIP score | `https://s3.../image.jpg (CLIP: 0.9756)`           |
| `duplicate_image`      | **Closest matching image** with CLIP score | `https://s3.../image.jpg (CLIP: 0.9756)`           |
| `master_main_image`    | Hero/primary image                         | `https://s3.../hero.jpg`                           |
| `duplicate_main_image` | Hero/primary image                         | `https://s3.../hero.jpg`                           |
| `master_images`        | All other images (pipe-separated)          | `https://s3.../img1.jpg \| https://s3.../img2.jpg` |
| `duplicate_images`     | All other images (pipe-separated)          | `https://s3.../img3.jpg \| https://s3.../img4.jpg` |
| `phash_difference`     | Perceptual hash difference (0-64)          | `3`                                                |
| `clip_similarity`      | CLIP similarity score (0.0-1.0)            | `0.9756`                                           |
| `status`               | Current status                             | `DUPLICATE` or `REVIEW_SUSPECT`                    |
| `notes`                | Manual review notes (empty)                | _(for your notes)_                                 |

**Key Features**:

- **Direct S3 Access**: All image URLs are clickable for instant browser viewing
- **Closest Match Detection**: `master_image` and `duplicate_image` show the most similar images with CLIP scores
- **Complete Image Sets**: Access to all product images for comprehensive comparison
- **Score Analysis**: Both pHash and CLIP scores for understanding detection logic

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

### Configuration & Parameter Tuning

Duplicate detection is configured via `.env` variables. These parameters can be tuned based on manual verification of results:

```bash
# pHash thresholds (Hamming distance, 0-64)
PHASH_DUPLICATE_THRESHOLD=2       # ≤2: Definitely duplicate
PHASH_AMBIGUOUS_THRESHOLD=18      # 3-18: Send to CLIP analysis
                                  # >18: Definitely not duplicate

# CLIP thresholds (similarity score, 0.0-1.0)
CLIP_DUPLICATE_THRESHOLD=0.955    # ≥0.955: Confirmed duplicate
CLIP_AMBIGUOUS_THRESHOLD=0.94     # 0.94-0.955: Send to manual review
                                  # <0.94: Definitely not duplicate

# CLIP model configuration
CLIP_MODEL=ViT-B/32              # Model type (CPU-friendly)
CLIP_DEVICE=auto                 # Device: 'auto', 'cpu', or 'cuda'
CLIP_MAX_IMAGES_PER_PRODUCT=5    # Limit for efficiency
```

### Parameter Verification & Tuning Workflow

To verify and optimize detection parameters, use this systematic approach:

#### 1. Export Analysis Data

```bash
# Export confirmed duplicates for accuracy verification
python main.py detect:export-duplicates --output data/duplicates_analysis.csv

# Export suspects for threshold tuning
python main.py detect:export-suspects --output data/suspects_analysis.csv
```

#### 2. Manual Comparison & Analysis

**Analyze Confirmed Duplicates:**

1. **Open duplicates_analysis.csv** in Excel or spreadsheet application
2. **Review accuracy** by comparing:
   - **Product titles**: Check if `master_title` vs `duplicate_title` are actually the same
   - **Images**: Click S3 URLs to visually compare `master_image` vs `duplicate_image`
   - **Scores**: Analyze `phash_difference` and `clip_similarity` values
3. **Identify false positives**:
   - Products marked as duplicates that are actually different
   - Note the score ranges where false positives occur
4. **Check threshold effectiveness**:
   - Very low pHash differences (0-2) should be true duplicates
   - High CLIP similarities (>0.955) should be true duplicates

**Analyze Suspect Cases:**

1. **Open suspects_analysis.csv** for threshold tuning
2. **Review borderline cases** that fall in ambiguous ranges:
   - pHash differences between 3-18
   - CLIP similarities between 0.94-0.955
3. **Manual classification**:
   - True duplicates that should be auto-detected
   - True uniques that should be auto-rejected
   - Genuine ambiguous cases requiring human review

#### 3. Parameter Adjustment Guidelines

**pHash Threshold Tuning:**

- **Lower PHASH_DUPLICATE_THRESHOLD** (e.g., 1): More conservative, fewer false positives
- **Raise PHASH_DUPLICATE_THRESHOLD** (e.g., 5): More aggressive, catch more variations
- **Adjust PHASH_AMBIGUOUS_THRESHOLD** (e.g., 15): Reduce CLIP workload if too many ambiguous cases

**CLIP Threshold Tuning:**

- **Raise CLIP_DUPLICATE_THRESHOLD** (e.g., 0.97): More conservative, fewer false positives
- **Lower CLIP_DUPLICATE_THRESHOLD** (e.g., 0.93): More aggressive, catch subtle duplicates
- **Adjust CLIP_AMBIGUOUS_THRESHOLD**: Fine-tune the manual review boundary

**Example Tuning Process:**

```bash
# 1. Initial analysis
python main.py detect:duplicates --limit 100
python main.py detect:export-duplicates --output analysis_v1.csv

# 2. Review analysis_v1.csv, identify issues
# 3. Adjust parameters in .env file

# 4. Re-run with new parameters
python main.py detect:duplicates --limit 100 --force
python main.py detect:export-duplicates --output analysis_v2.csv

# 5. Compare results and iterate
```

#### 4. Quality Metrics to Track

**Accuracy Indicators:**

- **False Positive Rate**: Confirmed duplicates that are actually different products
- **False Negative Rate**: Products marked as unique that are actually duplicates
- **Manual Review Load**: Percentage of products requiring human review
- **Consistency**: Similar products receiving consistent classifications

**Performance Indicators:**

- **CLIP Usage**: Percentage of comparisons requiring CLIP analysis
- **Processing Speed**: Detection time per product pair
- **Resource Usage**: Memory and compute requirements

#### 5. Validation Workflow

```bash
# Complete validation cycle
python main.py detect:duplicates              # Run detection
python main.py detect:status                  # Check statistics
python main.py detect:export-duplicates       # Export for verification
python main.py detect:export-suspects         # Export ambiguous cases

# Manual review and parameter adjustment
# Repeat until satisfactory accuracy achieved
```

This systematic approach ensures optimal detection accuracy while minimizing manual review overhead.

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

## Module D: Airtable Integration & Data Export

Module D provides comprehensive Airtable integration for the product data pipeline, enabling seamless export of processed product data to Airtable bases with proper anonymization and data organization.

### Overview

The Airtable integration system provides:

1. **Automated Base Creation**: Creates complete Airtable bases with proper schema
2. **Three-Table Architecture**: Separates data into Products, Variants, and Product Mapping tables
3. **Data Anonymization**: Uses anonymous product IDs while maintaining mapping to real AliExpress data
4. **URL-Based Image Storage**: Stores S3 image URLs instead of uploading attachments
5. **Multi-Property Variant Support**: Handles complex variants (Color + Size combinations)
6. **Automatic Environment Updates**: Updates `.env` file with new base IDs

### Three-Table System

The module creates a structured three-table system in Airtable:

#### 1. Products Table

- **Purpose**: Main product data with anonymous IDs for public access
- **Key Fields**: `anon_product_id`, `title`, `description`, `hero_image`, `gallery_images`, `price_eur`, `selected_variant`
- **Data Privacy**: Uses anonymous IDs to protect sensitive product information

#### 2. Variants Table

- **Purpose**: All SKU variations with proper multi-property formatting
- **Key Fields**: `anon_product_id`, `variant_key`, `variant_label`, `price_eur`, `is_recommended`
- **Variant Format**: "Color: Red + Size: L" (properly spaced formatting)

#### 3. Product Mapping Table

- **Purpose**: Secure mapping between anonymous and real AliExpress product data
- **Key Fields**: `anon_product_id`, `real_product_id`, `aliexpress_product_url`, `aliexpress_main_image_url`
- **Access Control**: Contains sensitive real product IDs and original AliExpress URLs

### Commands

**Create Airtable base:**

```bash
python main.py airtable:create-base
```

Options:

- `--name NAME`: Specify base name (default: "Product Pipeline")
- `--workspace-id ID`: Target workspace ID (optional)
- `--test-token`: Test token validity

This command:

- Creates a new Airtable base with complete three-table schema
- Sets up proper field types for all data (URLs, numbers, text, etc.)
- Automatically updates `.env` file with new base ID
- Provides direct links to the created base

**Sync data to Airtable:**

```bash
python main.py airtable:sync
```

Options:

- `--limit N`: Sync only first N products (useful for testing)
- `--filter STATUS`: Sync only products with specific status (MASTER, UNIQUE)
- `--dry-run`: Show what would be synced without making changes

This command:

- Syncs Products table with anonymous product data and S3 image URLs
- Syncs Variants table with properly formatted multi-property variants
- Syncs Product Mapping table with real AliExpress URLs and product IDs
- Uses anonymous IDs consistently across all tables for data linking
- Provides detailed sync statistics and error reporting

### Configuration

Airtable integration is configured via `.env` variables:

```bash
# Airtable Configuration
AIRTABLE_PERSONAL_ACCESS_TOKEN=pat...    # Personal Access Token (recommended)
AIRTABLE_API_KEY=pat...                  # Fallback API key
AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX        # Target base ID (auto-updated by create-base)
AIRTABLE_PRODUCTS_TABLE=Products          # Products table name
AIRTABLE_VARIANTS_TABLE=Variants          # Variants table name
AIRTABLE_WORKSPACE_ID=wspXXXXXXXXXXXXX     # Optional workspace ID
```

### Data Anonymization

The module implements a comprehensive anonymization system:

**Anonymous ID Generation:**

- **Method**: 12-character MD5 hash of real product ID
- **Example**: Real ID `1005010018511535` → Anonymous ID `0fb823156563`
- **Consistency**: Same anonymous ID used across all three tables

**Data Separation:**

- **Public Tables** (Products, Variants): Use anonymous IDs only
- **Mapping Table**: Contains the secure mapping between anonymous and real IDs
- **Access Control**: Mapping table provides controlled access to original AliExpress data

### Multi-Property Variant Support

The system handles complex product variants with multiple properties:

**Variant Key Format:**

- **Single Property**: "Color: Red"
- **Multiple Properties**: "Color: Red + Size: L"
- **Proper Spacing**: Ensures readable formatting with spaces around colons and plus signs

**Variant Processing:**

- Extracts all property combinations from AliExpress data
- Creates properly formatted variant keys for each SKU
- Links variants to product using consistent anonymous IDs
- Identifies recommended variants (typically cheapest option)

### Image URL Management

The module stores image references without uploading actual files:

**Gallery Images:**

- **Format**: Comma-separated S3 URLs stored as text
- **Field Type**: Single line text (not attachment)
- **Example**: `https://s3.amazonaws.com/bucket/image1.jpg, https://s3.amazonaws.com/bucket/image2.jpg`

**Hero Images:**

- **Format**: Single S3 URL stored as text
- **Field Type**: URL field for direct linking
- **Purpose**: Primary product image for display

### Automatic Environment Management

The base creator includes intelligent environment file updates:

**Automatic Updates:**

- **Base ID Detection**: Extracts new base ID from API response
- **Environment File Update**: Automatically updates `AIRTABLE_BASE_ID` in `.env`
- **Backup Handling**: Preserves existing environment variables
- **Error Recovery**: Provides manual instructions if automatic update fails

**Benefits:**

- **No Manual Copying**: Eliminates need to manually copy base IDs
- **Reduced Errors**: Prevents copy-paste mistakes
- **Streamlined Workflow**: Base creation immediately ready for sync

### Error Handling & Recovery

The module includes comprehensive error handling:

**Field Detection:**

- **Dynamic Schema**: Detects available fields in existing bases
- **Graceful Degradation**: Skips unknown fields without failing
- **Field Filtering**: Only syncs fields that exist in target tables

**Connection Management:**

- **Token Validation**: Tests token validity before operations
- **Base Verification**: Confirms base accessibility before sync
- **Table Detection**: Handles missing tables gracefully

**Sync Recovery:**

- **Partial Sync Support**: Continues if individual records fail
- **Detailed Logging**: Provides specific error information
- **Resume Capability**: Can resume interrupted syncs

### Base Creation Features

**Schema Management:**

- **Complete Field Definitions**: Creates all necessary fields with proper types
- **Table Relationships**: Sets up linking between tables via anonymous IDs
- **Field Descriptions**: Includes helpful descriptions for all fields
- **Data Type Optimization**: Uses appropriate field types (URL, number, text, etc.)

**Success Detection:**

- **Status Code Handling**: Recognizes both 200 and 201 as success
- **Response Parsing**: Extracts base ID and table information
- **Validation**: Confirms base creation before proceeding

### Sync Performance

**Optimized Processing:**

- **Batch Operations**: Processes records in efficient batches
- **Selective Sync**: Only syncs MASTER and UNIQUE products
- **Status Filtering**: Supports filtering by product status
- **Limit Controls**: Allows limiting sync size for testing

**Progress Tracking:**

- **Detailed Statistics**: Reports created/updated counts per table
- **Total Summaries**: Provides overall sync performance metrics
- **Error Reporting**: Identifies and reports any sync failures

### Usage Examples

**Complete Setup Workflow:**

```bash
# 1. Create new base with proper schema
python main.py airtable:create-base --name "My Product Base"

# 2. Test sync with small dataset
python main.py airtable:sync --limit 5 --dry-run

# 3. Sync MASTER products only
python main.py airtable:sync --filter MASTER

# 4. Full sync of all qualifying products
python main.py airtable:sync
```

**Development & Testing:**

```bash
# Test with dry run
python main.py airtable:sync --limit 1 --dry-run

# Check token validity
python main.py airtable:create-base --test-token
```

### Integration Benefits

**Data Organization:**

- **Structured Schema**: Consistent data structure across all bases
- **Relationship Integrity**: Proper linking between products and variants
- **Data Privacy**: Anonymous IDs protect sensitive information

**Operational Efficiency:**

- **Automated Setup**: One command creates complete base structure
- **Consistent Formatting**: Standardized variant and field formats
- **Error Prevention**: Validates data before sync operations

**Scalability:**

- **Large Dataset Support**: Handles thousands of products efficiently
- **Incremental Updates**: Supports ongoing sync operations
- **Performance Monitoring**: Tracks sync performance and success rates

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

The product data pipeline follows this modular workflow across all four modules:

### Phase 1: Module A - Data Collection & Merchant Review

1. **Session Setup** - Run `create_session` to establish API credentials
2. **Initial Harvest** - Run `harvest:init` to collect merchants and products
3. **Merchant Review** - Export pending merchants, review, and import decisions
4. **Delta Updates** - Run `harvest:delta` periodically for new data

### Phase 2: Module B - Product Processing & Enrichment

5. **Product ID Scraping** - Run `filter:scraper` to collect product IDs from whitelisted sellers
6. **Product Filtering** - Run `filter:products` to fetch details and apply business rules
7. **Image/Video Processing** - Automatically downloads, uploads to S3, and stores metadata
8. **Autonomous Discovery** - `filter:products` automatically scrapes new sellers when needed
9. **Business Rules** - Applies price and delivery constraints throughout

### Phase 3: Module C - Duplicate Detection & Master Selection

9. **Duplicate Detection** - Run `detect:duplicates` using pHash and CLIP analysis
10. **Master Selection** - Automatically selects best product from duplicate groups
11. **Export Review Cases** - Run `detect:export-suspects` to get ambiguous cases
12. **Manual Review** - Review CSV file and make DUPLICATE/UNIQUE decisions
13. **Import Decisions** - Run `detect:import-reviewed` to apply manual decisions
14. **Master Reassignment** - System automatically handles master changes when needed
15. **Status Tracking** - Monitor detection results and pipeline performance

### Phase 4: Module D - Airtable Integration & Export

16. **Base Creation** - Run `airtable:create-base` to create structured Airtable base
17. **Data Export** - Run `airtable:sync` to export processed products to Airtable
18. **Anonymous ID Management** - Automatically handles data anonymization and mapping
19. **Multi-Property Variants** - Exports complex variant combinations with proper formatting
20. **Image URL Storage** - Stores S3 image URLs for efficient access

### Phase 5: Monitoring & Maintenance

21. **Session Management** - Refresh API sessions as needed
22. **Status Monitoring** - Use status commands to track each module
23. **Data Analysis** - Query database for insights and optimization
24. **Airtable Updates** - Periodic sync updates to keep Airtable data current

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
