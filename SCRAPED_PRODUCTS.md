# Scraped Products Table Integration

## Overview

The `scraped_products` table acts as a central tracking system for all products discovered via the scraper. It tracks which products have been scraped and whether they've been processed (extracted) through the filtering workflow.

## Database Schema

```sql
CREATE TABLE scraped_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    seller_id VARCHAR(255) REFERENCES sellers(shop_id) NOT NULL,
    is_extracted BOOLEAN NOT NULL DEFAULT FALSE,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    extracted_at TIMESTAMP WITH TIME ZONE,
    
    INDEX idx_product_id (product_id),
    INDEX idx_seller_id (seller_id),
    INDEX idx_is_extracted (is_extracted)
);
```

## Workflow Integration

### Two-Stage Processing

#### Stage 1: Scraping (filter:scraper)
```bash
python main.py filter:scraper --seller-ids 2663214
```

**What it does:**
1. Scrapes product IDs from seller store pages
2. Saves product IDs to `scraped_products` with `is_extracted=False`
3. Fetches product details from API → saves to `products` table
4. Applies filters → saves qualifying products to `filtered_products`
5. Marks products as extracted: `is_extracted=True`, sets `extracted_at`

#### Stage 2: Extraction (filter:products)
```bash
python main.py filter:products
```

**What it does:**
1. Queries `scraped_products` for products where `is_extracted=False`
2. Joins with `products` table to get product details
3. Only processes products from whitelisted sellers
4. Applies filtering rules
5. Saves qualifying products to `filtered_products`
6. Marks processed products: `is_extracted=True`, sets `extracted_at`

### Legacy Mode

To process all products from whitelisted sellers (old behavior):
```bash
python main.py filter:products --no-scraped-products
```

This bypasses the `scraped_products` table entirely.

## Benefits

### 1. **Separation of Concerns**
- Scraping is independent from filtering
- Can scrape products now, filter later
- Can re-filter products without re-scraping

### 2. **Progress Tracking**
- Know exactly which products have been scraped
- Track which products are pending extraction
- Monitor extraction progress per seller

### 3. **Incremental Processing**
- Process scraped products in batches
- Resume interrupted filtering sessions
- Process only new products since last run

### 4. **Audit Trail**
- `scraped_at`: When product was discovered
- `extracted_at`: When product was processed
- Full history of scraping and filtering operations

## Usage Examples

### Full Workflow

```bash
# Step 1: Scrape products from sellers
python main.py filter:scraper --seller-ids "2663214,1234567"

# Step 2: Extract and filter scraped products
python main.py filter:products

# Step 3: Check remaining unextracted products
python -c "
from src.common.database import get_db_session, ScrapedProduct
db = get_db_session()
count = db.query(ScrapedProduct).filter(ScrapedProduct.is_extracted == False).count()
print(f'Unextracted products: {count}')
db.close()
"
```

### Incremental Processing

```bash
# Process in batches of 100
python main.py filter:products --limit 100

# Run again to process next 100
python main.py filter:products --limit 100
```

### Re-scraping

If you want to re-scrape and re-process:

```sql
-- Reset extraction status for a seller
UPDATE scraped_products 
SET is_extracted = FALSE, extracted_at = NULL 
WHERE seller_id = '2663214';

-- Then run filter:products again
```

## Queries

### Check scraping status

```sql
-- Overall statistics
SELECT 
    seller_id,
    COUNT(*) as total_products,
    SUM(CASE WHEN is_extracted THEN 1 ELSE 0 END) as extracted,
    SUM(CASE WHEN NOT is_extracted THEN 1 ELSE 0 END) as pending
FROM scraped_products
GROUP BY seller_id
ORDER BY seller_id;
```

### Find unextracted products

```sql
-- Unextracted products from whitelisted sellers
SELECT 
    sp.product_id,
    sp.seller_id,
    s.shop_name,
    sp.scraped_at
FROM scraped_products sp
JOIN sellers s ON sp.seller_id = s.shop_id
WHERE sp.is_extracted = FALSE
AND s.approval_status = 'WHITELIST'
ORDER BY sp.scraped_at DESC
LIMIT 100;
```

### Processing history

```sql
-- Products extracted in the last 24 hours
SELECT 
    COUNT(*) as products_extracted,
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction
FROM scraped_products
WHERE extracted_at >= NOW() - INTERVAL '24 hours';
```

### Extraction rate

```sql
-- Extraction completion percentage by seller
SELECT 
    sp.seller_id,
    s.shop_name,
    COUNT(*) as total_scraped,
    SUM(CASE WHEN sp.is_extracted THEN 1 ELSE 0 END) as extracted,
    ROUND(100.0 * SUM(CASE WHEN sp.is_extracted THEN 1 ELSE 0 END) / COUNT(*), 2) as completion_pct
FROM scraped_products sp
JOIN sellers s ON sp.seller_id = s.shop_id
GROUP BY sp.seller_id, s.shop_name
ORDER BY completion_pct ASC;
```

## Comparison: Old vs New Workflow

### Old Workflow (--no-scraped-products)
```
products table (from harvest) → filter:products → filtered_products
```

**Issues:**
- Products must already be in products table
- No tracking of which products were scraped vs harvested
- Hard to know which products are pending processing
- Can't separate scraping from filtering

### New Workflow (default)
```
Scraper → scraped_products (is_extracted=False)
           ↓
       products table
           ↓
    filter:products → Check scraped_products
           ↓              where is_extracted=False
    filtered_products
           ↓
    Mark is_extracted=True
```

**Advantages:**
- Clear separation between discovery and processing
- Track processing status per product
- Process in stages: scrape all, then filter incrementally
- Can re-process products without re-scraping
- Better progress visibility

## Error Handling

### Product Not Found

If a product in `scraped_products` is not found in the `products` table:
- Product is still marked as `is_extracted=True`
- Warning is logged
- Processing continues with next product

### Failed Filtering

If filtering fails for a product:
- Error is logged
- Product is still marked as `is_extracted=True`
- Processing continues with next product
- Error count is tracked in stats

### Interrupted Processing

If `filter:products` is interrupted:
- Already processed products remain marked as extracted
- Unprocessed products remain `is_extracted=False`
- Can resume by running `filter:products` again

## Best Practices

### 1. Scrape First, Filter Later

```bash
# Scrape all sellers
python main.py filter:scraper

# Filter in batches during off-peak hours
python main.py filter:products --limit 1000
```

### 2. Monitor Progress

Create a monitoring query:
```sql
-- Save as monitor_extraction.sql
SELECT 
    CURRENT_TIMESTAMP as checked_at,
    COUNT(*) as total_scraped,
    SUM(CASE WHEN is_extracted THEN 1 ELSE 0 END) as extracted,
    SUM(CASE WHEN NOT is_extracted THEN 1 ELSE 0 END) as remaining,
    ROUND(100.0 * SUM(CASE WHEN is_extracted THEN 1 ELSE 0 END) / COUNT(*), 2) as progress_pct
FROM scraped_products;
```

### 3. Periodic Re-extraction

Products can change over time. Re-extract periodically:
```sql
-- Reset products older than 30 days
UPDATE scraped_products 
SET is_extracted = FALSE, extracted_at = NULL
WHERE extracted_at < NOW() - INTERVAL '30 days';
```

### 4. Clean Up Old Entries

Remove scraped products that are no longer needed:
```sql
-- Remove products from blacklisted sellers
DELETE FROM scraped_products sp
WHERE seller_id IN (
    SELECT shop_id FROM sellers WHERE approval_status = 'BLACKLIST'
);
```

## Troubleshooting

### Products not being processed

Check if products exist in products table:
```sql
SELECT 
    sp.product_id,
    CASE WHEN p.product_id IS NOT NULL THEN 'YES' ELSE 'NO' END as in_products_table
FROM scraped_products sp
LEFT JOIN products p ON sp.product_id = p.product_id
WHERE sp.is_extracted = FALSE
LIMIT 10;
```

### Verify seller whitelist status

```sql
SELECT 
    sp.seller_id,
    s.approval_status,
    COUNT(*) as unextracted_products
FROM scraped_products sp
JOIN sellers s ON sp.seller_id = s.shop_id
WHERE sp.is_extracted = FALSE
GROUP BY sp.seller_id, s.approval_status;
```

### Check for duplicates

```sql
-- Find duplicate entries
SELECT product_id, seller_id, COUNT(*)
FROM scraped_products
GROUP BY product_id, seller_id
HAVING COUNT(*) > 1;
```
