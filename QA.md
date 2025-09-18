# Product Data Pipeline Quality Assurance Guide

This guide explains each command in the Product Data Pipeline, what it does, and how to verify it's working correctly. It also outlines the complete workflow from data harvesting to product filtering and image processing.

## Table of Contents

1. [Command Overview](#1-command-overview)
2. [Complete Workflow](#2-complete-workflow)
3. [Step-by-Step Testing](#3-step-by-step-testing)
4. [Troubleshooting](#4-troubleshooting)

## 1. Command Overview

The Product Data Pipeline offers several command-line operations to manage the entire data pipeline. Here's an overview of each command:

### Data Harvesting Commands

| Command          | Description              | What It Does                                                                                                                                                |
| ---------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `harvest:init`   | Initial data harvest     | Performs the first data collection from AliExpress API, creates new seller and product records. Use this when starting fresh or need complete data refresh. |
| `harvest:delta`  | Incremental data harvest | Updates existing data and adds new sellers/products that weren't found in previous runs. Use this for regular updates without duplicating effort.           |
| `harvest:status` | Show harvesting status   | Displays statistics about harvesting jobs, seller approval counts, and category distributions. Use this to monitor progress and verify results.             |

### Session Management Commands

| Command           | Description                    | What It Does                                                                                                              |
| ----------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `create_session`  | Create new API session         | Creates a new AliExpress API session using authorization code. Required for accessing detailed product and shipping data. |
| `refresh_session` | Refresh existing session token | Refreshes API session tokens to maintain access. Can use database session or manual tokens.                               |
| `list_sessions`   | List all stored sessions       | Displays all API sessions with their status, metadata, and activity indicators. Use to monitor session health.            |

### Product Processing Commands

| Command           | Description                 | What It Does                                                                                                                              |
| ----------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `filter:products` | Filter and process products | Processes products from whitelisted sellers, applies business rules, enriches with shipping data, and triggers automatic image ingestion. |

### Review Process Commands

| Command                 | Description                 | What It Does                                                                                                    |
| ----------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `review:export-pending` | Export merchants for review | Creates a CSV file with all pending merchants that need review. This file will be shared with expert reviewers. |
| `review:import-results` | Import review results       | Reads the reviewed CSV file where experts have updated approval statuses and updates the database accordingly.  |

## 2. Complete Workflow

The complete product data pipeline consists of the following enhanced workflow:

### Phase 1: Setup and Data Collection

1. **API Session Setup**

   - Run `create_session --code YOUR_CODE` to establish API credentials
   - This enables access to detailed product data and shipping information
   - Session credentials are stored in the database for reuse

2. **Initial Data Collection**

   - Run `harvest:init` to collect the initial set of merchants and products
   - This creates records in the database with all merchants marked as "PENDING"
   - Products include basic information from search results

3. **Regular Data Updates**
   - Run `harvest:delta` on a scheduled basis (e.g., daily) to:
     - Update information for existing merchants
     - Add new merchants that weren't found before
   - All new merchants are marked as "PENDING"

### Phase 2: Merchant Review Process

4. **Export for Expert Review**

   - Run `review:export-pending` to generate a CSV file with all pending merchants
   - This creates a file (default: `data/pending_merchants.csv`) containing merchant details

5. **Expert Review Process (Manual)**

   - Experts open the exported CSV file
   - For each merchant, they update the `approval_status` column:
     - `PENDING` → Keep as is if still under review
     - `WHITELIST` or `WHITE_LIST` → Approved merchants
     - `BLACKLIST` or `BLACK_LIST` → Rejected merchants
   - They can add notes in the `note` column explaining their decision
   - Save the updated file (e.g., as `data/reviewed_merchants.csv`)

6. **Import Review Results**
   - Run `review:import-results` to update the database with expert decisions
   - The system applies all status changes and notes from the CSV file
   - Only merchants with changed status (not PENDING) are updated

### Phase 3: Product Processing and Enrichment

7. **Product Filtering and Enrichment**

   - Run `filter:products` to process products from WHITELIST approved sellers
   - This command performs multiple operations:
     - Fetches detailed product information using API sessions
     - Retrieves shipping costs and delivery estimates for all product variants
     - Applies business rules (max price, max delivery time)
     - Stores qualifying products in `filtered_products` table
     - Automatically triggers image ingestion for each processed product

8. **Image Processing (Automatic)**

   - Triggered automatically during product filtering
   - Categorizes product images into three types:
     - **Hero Images**: First image from the product's image gallery
     - **Variant Images**: Images associated with specific product variants (color, size, etc.)
     - **Gallery Images**: Additional product photos (excluding hero and variant images)
   - Stores image metadata including property associations and sort order

9. **Session Maintenance**
   - Run `refresh_session` periodically to maintain API access
   - The system can automatically refresh using stored credentials
   - Manual token refresh is available if needed

### Phase 4: Monitoring and Analysis

10. **Monitor and Analyze**
    - Run `harvest:status` regularly to check:
      - Job history and performance
      - Current approval status counts
      - Product and category statistics
    - Run `list_sessions` to monitor API session health
    - Use database queries to analyze filtered products and images

## 3. Step-by-Step Testing

This section walks through testing each command and verifying its results.

### 3.1 Session Management Testing

#### 3.1.1 Create Session Test

```bash
# Create a new session (you'll need an authorization code from AliExpress)
python main.py create_session --code YOUR_AUTHORIZATION_CODE
```

**What Happens:**

- Sends authorization code to AliExpress API
- Receives access token, refresh token, and session metadata
- Stores session credentials in the database
- Returns session details

**How to Verify Success:**

```bash
# List sessions to verify creation
python main.py list_sessions

# Expected output should show:
# - A new session with "Active" status
# - Session details including user information
```

#### 3.1.2 Session Refresh Test

```bash
# Refresh using stored session (automatic)
python main.py refresh_session

# Or refresh using specific tokens (manual)
python main.py refresh_session --token ACCESS_TOKEN --refresh-token REFRESH_TOKEN
```

**What Happens:**

- Uses stored session credentials or provided tokens
- Calls AliExpress refresh endpoint
- Updates database with new tokens and expiration

**How to Verify Success:**

```bash
# List sessions to verify refresh
python main.py list_sessions

# Expected output should show:
# - Updated session with new timestamps
# - Session remains "Active"
```

#### 3.1.3 List Sessions Test

```bash
# Display all stored sessions
python main.py list_sessions
```

**What Happens:**

- Queries database for all session records
- Displays session metadata and status

**Expected Output:**

```
📋 Found 1 session(s):

- Code: your_code_here
  Status: 🟢 Active
  Type: Bearer
  User: username
  Account: account_info
  Created: 2025-01-15 10:30:00
  Updated: 2025-01-15 10:30:00
```

### 3.2 Data Harvesting Testing

#### 3.2.1 Initial Harvest Test

```bash
# Run a small initial harvest
python main.py harvest:init --limit 10
```

**What Happens:**

- Connects to AliExpress API
- Searches for products using configured categories or keywords
- For each product found:
  - Creates a new seller record if not already in database
  - Creates a new product record
  - Tracks job progress and statistics

**How to Verify Success:**

```bash
# Check harvest status to verify job ran successfully
python main.py harvest:status

# Expected output should show:
# - A HARVEST_INIT job with found_count and new_count values
# - Seller counts showing all or mostly PENDING status
```

#### 3.2.2 Delta Harvest Test

```bash
# Run a delta harvest with a larger limit
python main.py harvest:delta --limit 20
```

**What Happens:**

- Connects to AliExpress API like initial harvest
- For products/sellers already in database:
  - Updates their information (last_seen_at timestamp, etc.)
- For new products/sellers:
  - Creates new records like the initial harvest
- Records are marked as "found" vs "new" in job statistics

**How to Verify Success:**

```bash
# Check harvest status to see both jobs
python main.py harvest:status

# Expected output should show:
# - A HARVEST_DELTA job with found_count ≥ new_count
# - More total sellers than after initial harvest
```

### 3.3 Merchant Review Testing

#### 3.3.1 Export Pending Merchants Test

```bash
# Export pending merchants
python main.py review:export-pending
```

**What Happens:**

- Queries database for all sellers with PENDING approval status
- Creates CSV file at `data/pending_merchants.csv` with:
  - shop_id
  - shop_url
  - shop_name
  - approval_status (all set to "PENDING")
  - note

**How to Verify Success:**

```bash
# Check that file was created
ls -la data/pending_merchants.csv

# Preview the file contents
head data/pending_merchants.csv

# Count lines to verify number of records
wc -l data/pending_merchants.csv
```

#### 3.3.2 Review and Update CSV File (Manual Step)

This step would be performed by expert reviewers. For testing purposes:

```bash
# Create a test file with some modified statuses
head -n 5 data/pending_merchants.csv > data/reviewed_merchants.csv
sed -i '' '2s/PENDING/WHITELIST/' data/reviewed_merchants.csv
sed -i '' '3s/PENDING/BLACKLIST/' data/reviewed_merchants.csv

# View the modified file
cat data/reviewed_merchants.csv
```

**What an Expert Would Do:**

1. Open the CSV file in Excel, Google Sheets, or similar
2. Review each seller (checking their shop URL if needed)
3. Update approval_status to "WHITELIST" for approved sellers
4. Update approval_status to "BLACKLIST" for rejected sellers
5. Add notes explaining decisions in the note column
6. Save the file as `data/reviewed_merchants.csv`

#### 3.3.3 Import Review Results Test

```bash
# First test with dry run
python main.py review:import-results --dry-run

# Then perform actual import
python main.py review:import-results
```

**What Happens:**

- Reads the reviewed CSV file
- Normalizes status values (handling "WHITE_LIST" and "BLACK_LIST" formats)
- For each row:
  - Skips sellers still marked as "PENDING"
  - Updates database with new status and notes for WHITELIST/BLACKLIST entries
  - Counts various outcomes (updated, skipped, errors, etc.)
- Displays summary statistics

**How to Verify Success:**

```bash
# Check seller approval counts
python main.py harvest:status

# Expected output should show:
# - Fewer PENDING sellers
# - Some WHITELIST and BLACKLIST sellers
```

### 3.4 Product Filtering Testing

#### 3.4.1 Product Filtering Test

```bash
# Filter products with business rules
python main.py filter:products --max-price 50.00 --max-delivery 14 --limit 5
```

**What Happens:**

- Queries database for products from WHITELIST approved sellers
- For each product:
  - Fetches detailed product information using API session
  - Retrieves shipping information for all product variants
  - Calculates total cost (product price + shipping)
  - Checks delivery time estimates
  - Applies business rule filters (max price, max delivery)
  - Stores qualifying products in `filtered_products` table
  - Automatically triggers image ingestion for the product

**How to Verify Success:**

```bash
# Check if filtered products were created
python -c "
from src.common.database import get_db_session, FilteredProduct
with get_db_session() as db:
    count = db.query(FilteredProduct).count()
    print(f'Filtered products: {count}')

    # Show sample records
    products = db.query(FilteredProduct).limit(3).all()
    for p in products:
        print(f'Product {p.product_id}: €{p.target_sale_price} - {p.min_delivery_days}-{p.max_delivery_days} days')
"
```

#### 3.4.2 Image Ingestion Verification

```bash
# Check if images were automatically ingested
python -c "
from src.common.database import get_db_session, ProductImage
with get_db_session() as db:
    count = db.query(ProductImage).count()
    print(f'Total images: {count}')

    # Show image breakdown by role
    from sqlalchemy import func
    breakdown = db.query(ProductImage.image_role, func.count(ProductImage.id)).group_by(ProductImage.image_role).all()
    for role, count in breakdown:
        print(f'{role} images: {count}')
"
```

#### 3.4.3 Shipping Information Verification

```bash
# Check shipping information was stored
python -c "
from src.common.database import get_db_session, ShippingInfo
with get_db_session() as db:
    count = db.query(ShippingInfo).count()
    print(f'Shipping records: {count}')

    # Show sample shipping info
    shipping = db.query(ShippingInfo).limit(3).all()
    for s in shipping:
        print(f'Product {s.product_id}, SKU {s.sku_id}: €{s.shipping_fee} - {s.max_delivery_days} days ({s.company})')
"
```

### 3.5 Advanced Testing

#### 3.5.1 End-to-End Workflow Test

```bash
# Complete workflow test
echo "=== Phase 1: Session Setup ==="
python main.py list_sessions

echo "=== Phase 2: Data Collection ==="
python main.py harvest:init --limit 5 --dry-run

echo "=== Phase 3: Status Check ==="
python main.py harvest:status

echo "=== Phase 4: Product Processing ==="
python main.py filter:products --limit 2 --dry-run

echo "=== Workflow test completed ==="
```

#### 3.5.2 Database Integrity Test

```bash
# Verify database relationships and data integrity
python -c "
from src.common.database import get_db_session, Product, FilteredProduct, ProductImage, ShippingInfo
with get_db_session() as db:
    print('=== Database Integrity Check ===')

    # Check products have sellers
    products_without_sellers = db.query(Product).filter(Product.shop_id.is_(None)).count()
    print(f'Products without sellers: {products_without_sellers}')

    # Check filtered products exist in products table
    orphaned_filtered = db.query(FilteredProduct).filter(
        ~FilteredProduct.product_id.in_(db.query(Product.product_id))
    ).count()
    print(f'Orphaned filtered products: {orphaned_filtered}')

    # Check images have valid products
    orphaned_images = db.query(ProductImage).filter(
        ~ProductImage.product_id.in_(db.query(Product.product_id))
    ).count()
    print(f'Orphaned images: {orphaned_images}')

    print('=== Integrity check completed ===')
"
```

python main.py harvest:status

# Expected output should show:

# - A HARVEST_INIT job with found_count and new_count values

# - Seller counts showing all or mostly PENDING status

````

### 3.2 Delta Harvest Test

```bash
# Run a delta harvest with a larger limit
python main.py harvest:delta --limit 20
````

**What Happens:**

- Connects to AliExpress API like initial harvest
- For products/sellers already in database:
  - Updates their information (last_seen_at timestamp, etc.)
- For new products/sellers:
  - Creates new records like the initial harvest
- Records are marked as "found" vs "new" in job statistics

**How to Verify Success:**

```bash
# Check harvest status to see both jobs
python main.py harvest:status

# Expected output should show:
# - A HARVEST_DELTA job with found_count ≥ new_count
# - More total sellers than after initial harvest
```

### 3.3 Export Pending Merchants Test

```bash
# Export pending merchants
python main.py review:export-pending
```

**What Happens:**

- Queries database for all sellers with PENDING approval status
- Creates CSV file at `data/pending_merchants.csv` with:
  - shop_id
  - shop_url
  - shop_name
  - approval_status (all set to "PENDING")
  - note

**How to Verify Success:**

```bash
# Check that file was created
ls -la data/pending_merchants.csv

# Preview the file contents
head data/pending_merchants.csv

# Count lines to verify number of records
wc -l data/pending_merchants.csv
```

### 3.4 Review and Update CSV File (Manual Step)

This step would be performed by expert reviewers. For testing purposes:

```bash
# Create a test file with some modified statuses
head -n 5 data/pending_merchants.csv > data/reviewed_merchants.csv
sed -i '' '2s/PENDING/WHITELIST/' data/reviewed_merchants.csv
sed -i '' '3s/PENDING/BLACKLIST/' data/reviewed_merchants.csv

# View the modified file
cat data/reviewed_merchants.csv
```

**What an Expert Would Do:**

1. Open the CSV file in Excel, Google Sheets, or similar
2. Review each seller (checking their shop URL if needed)
3. Update approval_status to "WHITELIST" for approved sellers
4. Update approval_status to "BLACKLIST" for rejected sellers
5. Add notes explaining decisions in the note column
6. Save the file as `data/reviewed_merchants.csv`

### 3.5 Import Review Results Test

```bash
# First test with dry run
python main.py review:import-results --dry-run

# Then perform actual import
python main.py review:import-results
```

**What Happens:**

- Reads the reviewed CSV file
- Normalizes status values (handling "WHITE_LIST" and "BLACK_LIST" formats)
- For each row:
  - Skips sellers still marked as "PENDING"
  - Updates database with new status and notes for WHITELIST/BLACKLIST entries
  - Counts various outcomes (updated, skipped, errors, etc.)
- Displays summary statistics

**How to Verify Success:**

```bash
# Check seller approval counts
python main.py harvest:status

# Expected output should show:
# - Fewer PENDING sellers
# - Some WHITELIST and BLACKLIST sellers
```

## 4. Troubleshooting Common Issues

### 4.1 Session Management Issues

#### 4.1.1 Authorization Code Invalid

**Problem:** `create_session` fails with authorization error

**Solution:**

1. Verify the authorization code was copied correctly from AliExpress
2. Check if the code has expired (codes typically have short lifespans)
3. Ensure your AliExpress app credentials are configured correctly in `.env`

#### 4.1.2 Token Refresh Fails

**Problem:** `refresh_session` returns authentication error

**Solution:**

1. Check if tokens in database are corrupted:
   ```bash
   python main.py list_sessions
   ```
2. If all sessions show as expired, create a new session:
   ```bash
   python main.py create_session --code NEW_CODE
   ```

### 4.2 Harvest Issues

#### 4.2.1 No Products Found

**Problem:** Harvest completes but `found_count` is 0

**Possible Causes & Solutions:**

1. **Network Issues:** Check internet connection and AliExpress API status
2. **API Rate Limits:** Wait and retry, or reduce the `--limit` parameter
3. **Search Configuration:** Verify `CATEGORY` and search terms in `.env` file
4. **Session Expired:** Run `python main.py refresh_session` first

#### 4.2.2 Database Connection Errors

**Problem:** "Cannot connect to database" errors during harvest

**Solution:**

1. Check database file permissions (for SQLite)
2. Verify `DATABASE_URL` in `.env` file
3. For PostgreSQL, ensure server is running and credentials are correct

### 4.3 Product Filtering Issues

#### 4.3.1 No Products Pass Filters

**Problem:** `filter:products` completes but creates 0 filtered products

**Possible Causes:**

1. **Price Filters Too Restrictive:** Increase `--max-price` parameter
2. **Delivery Filters Too Restrictive:** Increase `--max-delivery` parameter
3. **No Whitelisted Sellers:** Check that some sellers have WHITELIST approval status
4. **Shipping API Issues:** Products might lack shipping information

**Debugging Steps:**

```bash
# Check seller approval status
python main.py harvest:status

# Try with relaxed filters
python main.py filter:products --max-price 100 --max-delivery 30 --limit 1

# Check raw product data
python -c "
from src.common.database import get_db_session, Product, Seller
with get_db_session() as db:
    products = db.query(Product).join(Seller).filter(Seller.approval_status == 'WHITELIST').limit(3).all()
    print(f'Available products from whitelisted sellers: {len(products)}')
"
```

#### 4.3.2 Image Ingestion Fails

**Problem:** Products are filtered but no images are stored

**Solution:**

1. Check if products have image URLs in the raw data
2. Verify network connectivity for image downloads
3. Check image ingestion logs for specific error messages

### 4.4 Review Process Issues

#### 4.4.1 Export Creates Empty File

**Problem:** `review:export-pending` creates CSV with only headers

**Solution:**

- This means no sellers have PENDING status. Check status distribution:
  ```bash
  python main.py harvest:status
  ```

#### 4.4.2 Import Fails with CSV Errors

**Problem:** `review:import-results` fails to read the CSV file

**Possible Causes & Solutions:**

1. **File Format Issues:** Ensure CSV uses proper encoding (UTF-8) and commas as delimiters
2. **Missing Columns:** Verify the CSV has required columns: `shop_id`, `approval_status`, `note`
3. **File Path Issues:** Ensure the file is saved as `data/reviewed_merchants.csv`

### 4.5 General Debugging Tips

#### 4.5.1 Enable Verbose Logging

Add detailed logging to see what's happening:

```python
# Add to any Python script for debugging
import logging
logging.basicConfig(level=logging.DEBUG)
```

#### 4.5.2 Database Inspection Commands

Use these commands to inspect database state:

```bash
# Count records in each table
python -c "
from src.common.database import get_db_session, Product, Seller, FilteredProduct, ProductImage
with get_db_session() as db:
    print(f'Products: {db.query(Product).count()}')
    print(f'Sellers: {db.query(Seller).count()}')
    print(f'Filtered Products: {db.query(FilteredProduct).count()}')
    print(f'Images: {db.query(ProductImage).count()}')
"

# Check recent activity
python -c "
from src.common.database import get_db_session, Product
from datetime import datetime, timedelta
with get_db_session() as db:
    recent = datetime.now() - timedelta(hours=24)
    recent_count = db.query(Product).filter(Product.created_at >= recent).count()
    print(f'Products created in last 24h: {recent_count}')
"
```

#### 4.5.3 Configuration Verification

Verify your configuration is loaded correctly:

```bash
python -c "
from src.common.config import get_search_category, get_ignore_categories
print(f'Search category: {get_search_category()}')
print(f'Ignore categories: {get_ignore_categories()}')
"
```

### 4.6 Performance Troubleshooting

#### 4.6.1 Slow API Calls

**Problem:** Commands take very long to complete

**Solutions:**

1. **Reduce Batch Sizes:** Use smaller `--limit` values
2. **Check Network:** Verify stable internet connection
3. **API Rate Limits:** Add delays between requests if needed

#### 4.6.2 Database Performance

**Problem:** Database queries are slow

**Solutions:**

1. **Add Indexes:** Critical indexes should already exist, but verify with database tools
2. **Database Size:** Consider archiving old data if database grows very large
3. **Connection Pooling:** For high-volume usage, consider PostgreSQL over SQLite
