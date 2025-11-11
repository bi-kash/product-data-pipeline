# Scraper-Based Product Filtering Workflow

## Overview

The scraper-based filtering workflow provides a new way to populate `filtered_products` by scraping product IDs directly from seller store pages, then fetching and filtering those products via the API.

### Workflow Comparison

**Old Workflow (filter:products)**

```
products table (existing) → Apply filters → filtered_products
```

**New Workflow (filter:scraper)**

```
Scrape seller store → Get product_ids → Fetch from API → Apply filters → filtered_products
                                                                        ↓
                                                          Track in scraper_progress
```

## Architecture

### Components

1. **SellerStoreScraper** (`src/scraper/seller_scraper.py`)

   - Selenium-based scraper for extracting product IDs from seller store pages
   - Handles browser automation, country/currency selection, pagination
   - Returns list of product IDs found on seller's store

2. **ScraperBasedFilter** (`src/filter/scraper_filter.py`)

   - Orchestrates the complete workflow
   - Processes sellers one at a time sequentially
   - Tracks progress in `scraper_progress` table

3. **ScraperProgress** (Database table)
   - Tracks scraping progress per seller
   - Fields: seller_id, total_products_found, products_scraped, products_fetched, products_filtered, status, timestamps

### Database Schema

The new `scraper_progress` table:

```sql
CREATE TABLE scraper_progress (
    id SERIAL PRIMARY KEY,
    seller_id VARCHAR(255) REFERENCES sellers(shop_id),
    total_products_found INTEGER,
    products_scraped INTEGER,
    products_fetched INTEGER,
    products_filtered INTEGER,
    status VARCHAR(50) DEFAULT 'not_started',  -- 'not_started', 'in_progress', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Configuration

Add to your `.env` file:

```bash
# Selenium Scraper Configuration
SELENIUM_HEADLESS=true          # Run browser in headless mode
SELENIUM_TIMEOUT=10             # Timeout for page elements (seconds)
TARGET_COUNTRY=Germany          # Target country for shipping/pricing
TARGET_CURRENCY=EUR             # Target currency
```

## Installation

1. Install Selenium and WebDriver Manager:

```bash
pip install selenium>=4.0.0 webdriver-manager>=3.8.0
```

2. Create database tables:

```bash
python -c "from src.common.database import create_tables_if_not_exist; create_tables_if_not_exist()"
```

## Usage

### Basic Usage

Process all whitelisted sellers:

```bash
python main.py filter:scraper
```

### Process Specific Sellers

Process one or more specific sellers:

```bash
# Single seller
python main.py filter:scraper --seller-ids 2663214

# Multiple sellers (comma-separated)
python main.py filter:scraper --seller-ids "2663214,1234567,9876543"
```

### Limit Number of Sellers

Process only the first N whitelisted sellers:

```bash
python main.py filter:scraper --limit 5
```

## Workflow Details

### Step-by-Step Process

For each seller, the workflow executes these steps:

1. **Initialize Progress**

   - Creates/resets `scraper_progress` entry
   - Sets status to `in_progress`

2. **Scrape Product IDs**

   - Launches Chrome browser (headless by default)
   - Navigates to seller's store page
   - Sets country to Germany and currency to EUR
   - Paginate through all products
   - Extracts product IDs from links
   - Updates `total_products_found` and `products_scraped`

3. **Fetch Product Details**

   - For each product ID, calls `aliexpress.ds.product.get` API
   - Saves product data to `products` table
   - Updates `products_fetched` count

4. **Apply Filters**

   - Uses existing `ProductFilterEngine` to apply business rules:
     - Price rule: (max_variant_price + min_shipping) ≤ TOTAL_MAX_PRICE
     - Shipping rule: delivery_time ≤ MAX_DELIVERY_DAYS
   - Saves qualifying products to `filtered_products`
   - Extracts and stores variants in `product_variants`
   - Fetches shipping info and stores in `shipping_info`
   - Updates `products_filtered` count

5. **Complete**
   - Sets status to `completed` or `failed`
   - Records `completed_at` timestamp
   - Moves to next seller

## Progress Tracking

View scraping progress in the database:

```sql
SELECT
    seller_id,
    status,
    total_products_found,
    products_scraped,
    products_fetched,
    products_filtered,
    started_at,
    completed_at
FROM scraper_progress
ORDER BY started_at DESC;
```

## Error Handling

- If scraping fails for a seller, status is set to `failed`
- Error message is stored in `error_message` column
- Processing continues with next seller
- Overall stats include `sellers_failed` count

## Example Output

```
================================================================================
Processing seller: 2663214 (Test Store)
================================================================================

Step 1: Initializing progress tracking for seller 2663214
Step 2: Scraping product IDs from seller 2663214 store page
Page loaded: All Items - Test Store
Opening ship-to menu...
✓ Selected Germany
✓ Selected EUR currency
✓ Country and currency settings saved
✓ Accepted cookies
Starting product extraction...
Page 1: Found 48 products (total: 48)
✓ Reached last page
✓ Extraction complete. Total products: 48

Step 3: Fetching product details from API
Fetching product 1/48: 1005007129046363
✓ Successfully fetched and saved product 1005007129046363
...
✓ Fetched 48 products from API

Step 4: Applying filters to products
✓ Product 1005007129046363 passed filters
...
✓ 12 products passed filters and saved to filtered_products

Step 5: Marking seller 2663214 as completed
✅ Successfully completed processing seller 2663214
   Scraped: 48, Fetched: 48, Filtered: 12

================================================================================
Overall Processing Complete
================================================================================
Sellers processed: 1
Sellers completed: 1
Sellers failed: 0
Total products scraped: 48
Total products fetched: 48
Total products filtered: 12
```

## Advantages Over Old Workflow

1. **Direct Discovery**: Finds products directly from seller stores, not dependent on existing `products` table
2. **Fresh Data**: Always fetches latest product information from API
3. **Progress Tracking**: Detailed progress tracking per seller in database
4. **Sequential Processing**: Processes one seller at a time, easier to monitor and debug
5. **Targeted**: Can process specific sellers on demand

## Testing

Test with a single whitelisted seller:

```bash
# First, whitelist a seller
# Then run scraper-based filter
python main.py filter:scraper --seller-ids 2663214
```

Verify results:

```sql
-- Check scraper progress
SELECT * FROM scraper_progress WHERE seller_id = '2663214';

-- Check filtered products
SELECT COUNT(*) FROM filtered_products WHERE shop_id = '2663214';

-- Check product variants
SELECT COUNT(*) FROM product_variants pv
JOIN filtered_products fp ON pv.product_id = fp.product_id
WHERE fp.shop_id = '2663214';
```

## Troubleshooting

### ChromeDriver Issues

If ChromeDriver fails to install automatically:

```bash
# Install ChromeDriver manually
brew install chromedriver  # macOS
# or
apt-get install chromium-chromedriver  # Linux
```

### Headless Mode Issues

If headless mode causes problems, disable it:

```bash
# In .env
SELENIUM_HEADLESS=false
```

### Timeout Issues

If pages load slowly, increase timeout:

```bash
# In .env
SELENIUM_TIMEOUT=20
```

### Database Connection Issues

Ensure your database connection is working:

```bash
# Test database connection
python -c "from src.common.database import get_db_session; db = get_db_session(); print('✅ Database connected'); db.close()"
```

## Integration with Existing Workflow

The scraper-based filter can be used alongside the old filter workflow:

```bash
# Scrape and filter products from whitelisted sellers
python main.py filter:scraper

# Run duplicate detection
python main.py detect:duplicates

# Sync to Airtable
python main.py airtable:sync
```

## Future Enhancements

Potential improvements:

1. **Parallel Processing**: Process multiple sellers concurrently
2. **Resume Capability**: Resume interrupted scraping sessions
3. **Incremental Updates**: Only scrape new products since last run
4. **Rate Limiting**: Add delays between API calls to avoid rate limits
5. **Retry Logic**: Retry failed scrapes with exponential backoff
