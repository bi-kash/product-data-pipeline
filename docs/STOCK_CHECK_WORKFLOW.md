# Stock Check Workflow

This document explains the stock check functionality for managing product and variant inventory status.

## Overview

The stock check feature allows you to:
- Mark products with a listing status (Online, Todo, Offline, etc.)
- Automatically check stock availability for products marked as "Online"
- Update variant stock status based on AliExpress API data
- Sync stock information to Airtable

## Database Schema Changes

### Products Table (filtered_products)

Added column:
- **`status`** (VARCHAR(20), nullable): Listing status for the product
  - Possible values: `"Online"`, `"Todo"`, `"Offline"`, or custom values
  - Default: `NULL`
  - Controls whether the product's variants are checked for stock

### Variants Table (product_variants)

Added column:
- **`stock_status`** (VARCHAR(20), nullable): Result of the stock check
  - Possible values: `"available"`, `"out_of_stock"`, `"unknown"`
  - Default: `NULL`
  - Automatically updated by the stock check process

## Workflow

### 1. Initial Setup (One-time)

Run the database migration to add the new columns:

```bash
python migrations/add_stock_check_columns.py
```

This will add:
- `status` column to the `filtered_products` table
- `stock_status` column to the `product_variants` table

### 2. Mark Products as Online

Before running stock checks, you need to mark products as "Online" in your database. You can do this:

**Option A: Directly in the database**
```sql
-- Mark all products as Online
UPDATE filtered_products SET status = 'Online';

-- Mark specific products as Online
UPDATE filtered_products 
SET status = 'Online' 
WHERE product_id IN ('1234567890', '0987654321');
```

**Option B: Via Airtable**
1. Add a `status` field to your Products table in Airtable
2. Set products to "Online" status in Airtable
3. Sync back to the database (requires custom sync script)

**Option C: Via Python script**
```python
from src.common.database import get_db_session, FilteredProduct

with get_db_session() as db:
    # Mark all products as Online
    db.query(FilteredProduct).update({FilteredProduct.status: 'Online'})
    db.commit()
```

### 3. Run Stock Check

The stock check command will:
1. Query all products with `status = "Online"`
2. For each product, call the AliExpress API (`product_get`) to fetch fresh data
3. Update variant information in the local database:
   - `sku_available_stock` (from API)
   - `stock_status` (calculated: "available" if stock > 0, else "out_of_stock")
4. Automatically sync the updates to Airtable

**Command syntax:**
```bash
python main.py check_stock [options]
```

**Options:**
- `--limit N`: Check only the first N products (useful for testing)
- `--dry-run`: Simulate the check without updating the database

**Examples:**
```bash
# Check all Online products
python main.py check_stock

# Check only 10 products (for testing)
python main.py check_stock --limit 10

# Dry run to see what would be checked
python main.py check_stock --dry-run
```

### 4. View Results

The command will output statistics:
```
✅ Stock check completed!
📊 Statistics:
   Products checked: 50
   Products updated: 50
   Variants checked: 237
   Variants updated: 189
   Variants available: 180
   Variants out of stock: 9
```

### 5. Check Airtable

After the stock check completes, the data is automatically synced to Airtable. You should see:

**In Products table:**
- `status` field showing "Online", "Todo", "Offline", etc.

**In Variants table:**
- `stock` field showing the available quantity
- `stock_status` field showing "available" or "out_of_stock"

## Stock Status Logic

The `stock_status` for each variant is determined as follows:

```python
if sku_available_stock is None:
    stock_status = "unknown"
elif sku_available_stock > 0:
    stock_status = "available"
else:
    stock_status = "out_of_stock"
```

## Best Practices

### Managing Product Status

1. **Online**: Products actively being sold, stock should be checked regularly
2. **Todo**: Products queued for review before going online
3. **Offline**: Products temporarily removed from sale or permanently discontinued

### Regular Stock Checks

Set up a cron job to run stock checks regularly:

```bash
# Check stock every 6 hours
0 */6 * * * cd /path/to/project && python main.py check_stock >> logs/stock_check.log 2>&1
```

### Monitoring

- Check the logs in `logs/` directory for detailed information
- Monitor the error count in the output
- Set up alerts for high error rates

## Airtable Integration

### Products Table Schema

The following fields are synced to Airtable:
- `anon_product_id`: Anonymous product identifier
- `title`: Product title
- `status`: Listing status (Online, Todo, Offline)
- `duplicate_status`: Duplicate detection status
- `price_eur`, `shipping_eur`, `total_eur`: Pricing information
- `delivery_time`: Delivery time range
- Other fields (images, videos, description, etc.)

### Variants Table Schema

The following fields are synced to Airtable:
- `anon_sku_id`: Anonymous SKU identifier
- `anon_product_id`: Link to parent product
- `variant_key`: Variant identifier (e.g., "Color: Red + Size: L")
- `price_eur`, `shipping_eur`, `total_eur`: Variant pricing
- `stock`: Available quantity from API
- `stock_status`: Stock status (available, out_of_stock, unknown)
- `delivery_time`: Delivery time for this variant
- Other fields (images, etc.)

## Troubleshooting

### No products found with status='Online'

**Problem:** The command reports no products to check.

**Solution:** Mark products as Online first:
```sql
UPDATE filtered_products SET status = 'Online' WHERE product_id = 'YOUR_PRODUCT_ID';
```

### Variants not found in database

**Problem:** Stock check reports variants not found.

**Solution:** Run the product filter first to populate variants:
```bash
python main.py filter:products
```

### API errors

**Problem:** Errors when calling the AliExpress API.

**Solution:** 
1. Check your API credentials in `.env`
2. Ensure your session is valid:
   ```bash
   python main.py list_sessions
   python main.py refresh_session
   ```
3. Check rate limits - add delays between requests if needed

### Airtable sync fails

**Problem:** Data doesn't appear in Airtable after stock check.

**Solution:**
1. Verify Airtable configuration in `.env`:
   - `AIRTABLE_API_KEY` or `AIRTABLE_PAT`
   - `AIRTABLE_BASE_ID`
2. Ensure the `status` and `stock_status` fields exist in Airtable
3. Run sync manually:
   ```bash
   python main.py airtable:sync
   ```

## Advanced Usage

### Filter by Status

You can query products by status in your database:

```python
from src.common.database import get_db_session, FilteredProduct

with get_db_session() as db:
    # Get all Online products
    online_products = db.query(FilteredProduct).filter(
        FilteredProduct.status == 'Online'
    ).all()
    
    # Get products that need review
    todo_products = db.query(FilteredProduct).filter(
        FilteredProduct.status == 'Todo'
    ).all()
```

### Batch Status Updates

```python
from src.common.database import get_db_session, FilteredProduct

with get_db_session() as db:
    # Mark all products in a category as Online
    db.query(FilteredProduct).filter(
        FilteredProduct.category_id.like('%123456%')
    ).update({FilteredProduct.status: 'Online'})
    db.commit()
```

### Custom Stock Check Logic

You can extend the `StockChecker` class to implement custom logic:

```python
from src.stock.stock_checker import StockChecker

class CustomStockChecker(StockChecker):
    def _determine_stock_status(self, sku_available_stock):
        # Custom logic for stock status
        if sku_available_stock is None:
            return 'unknown'
        elif sku_available_stock > 10:
            return 'in_stock_high'
        elif sku_available_stock > 0:
            return 'in_stock_low'
        else:
            return 'out_of_stock'
```

## Migration Rollback

If you need to remove the new columns (not recommended):

```bash
# PostgreSQL only - SQLite doesn't support DROP COLUMN
python migrations/add_stock_check_columns.py --rollback
```

**Note:** This will remove the `status` and `stock_status` columns from the database.

## See Also

- [README.md](../README.md) - Main project documentation
- [DATABASE_SCHEMA.md](../DATABASE_SCHEMA.md) - Complete database schema reference
- [SCRAPER_WORKFLOW.md](../SCRAPER_WORKFLOW.md) - Product filtering workflow
