# Product Data Pipeline

A modular data pipeline for harvesting and processing jewelry product data from AliExpress using RapidAPI.

## Overview

This project implements a modular pipeline to identify jewelry products from AliExpress, qualify them based on rules, and prepare them for processing by virtual assistants.

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

5. Edit the `.env` file with your database and RapidAPI credentials. Required variables include:

```
# Database configuration
USE_SQLITE=true  # Set to false for PostgreSQL
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=product_pipeline

# RapidAPI configuration
RAPIDAPI_KEY=your_rapidapi_key
RAPIDAPI_HOST=aliexpress-true-api.p.rapidapi.com

# Region settings (for price and shipping data)
ALIEXPRESS_TARGET_CURRENCY=EUR
ALIEXPRESS_TARGET_LANGUAGE=EN
ALIEXPRESS_TARGET_COUNTRY=DE
```

## Database Setup

The application automatically creates the required tables when first run. The database schema includes:

- **sellers:** Store merchant information

  - `shop_id`: Primary key, unique identifier from AliExpress
  - `shop_url`: URL to the seller's shop
  - `shop_name`: Name of the seller's shop
  - `approval_status`: One of ["PENDING", "WHITELIST", "BLACKLIST"]
  - `first_seen_at`: Timestamp when first discovered (UTC)
  - `last_seen_at`: Timestamp when last seen (UTC)
  - `raw_json`: Raw data from the API
  - `note`: Optional note for review

- **products:** Store product information

  - `product_id`: Primary key, unique identifier from AliExpress
  - `shop_id`: Foreign key to sellers table
  - `product_title`: Title of the product
  - `product_detail_url`: URL to the product detail page
  - `product_main_image_url`: URL to the main product image
  - Various price fields (original_price, target_sale_price, etc.)
  - `status`: Product status
  - `first_seen_at`: Timestamp when first discovered (UTC)
  - `last_seen_at`: Timestamp when last seen (UTC)
  - `raw_json`: Raw data from the API

- **product_categories:** Track product-category associations

  - `id`: Auto-incrementing primary key
  - `product_id`: Foreign key to products table
  - `category_id`: Category ID from AliExpress
  - `category_name`: Name of the category
  - `search_page`: Page number where the product was found
  - `position_in_results`: Position in search results
  - `search_timestamp`: When the association was created

- **job_runs:** Log information about script executions
  - `id`: Auto-incrementing primary key
  - `job_type`: Type of job (e.g., "HARVEST_INIT", "HARVEST_DELTA")
  - `start_time`: When the job started (UTC)
  - `end_time`: When the job completed (UTC)
  - `duration_seconds`: Job duration in seconds
  - `found_count`: Number of items found
  - `new_count`: Number of new items added
  - `skipped_count`: Number of items skipped
  - `error_count`: Number of errors encountered
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
- Category search statistics
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

## Search Configuration

The pipeline can search for products using two methods:
1. **Category-based search**: Searches for products within specific AliExpress categories
2. **Keyword-based search**: Searches for products matching specific keywords

You can configure the pipeline to use:
- Only category-based search (recommended for most cases)
- Both category and keyword-based search together

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
SELECT pc.category_name, COUNT(*) FROM product_categories pc
GROUP BY pc.category_name
ORDER BY COUNT(*) DESC;
```

## Configuration

The pipeline is configured via:

- `.env`: Contains database credentials, API keys, and application settings
- `data/CATEGORIES.csv`: Contains categories for product search

## Complete Workflow

The product data pipeline follows this workflow:

1. **Initial Data Collection** - Run `harvest:init` to collect initial merchant and product data
2. **Regular Updates** - Run `harvest:delta` periodically to update and add new data
3. **Export for Review** - Use `review:export-pending` to create a CSV file for expert reviewers
4. **Expert Review** - Reviewers update approval statuses in the CSV file (WHITELIST or BLACKLIST)
5. **Import Results** - Use `review:import-results` to update the database with reviewer decisions
6. **Monitor & Analyze** - Use `harvest:status` and `export:tables` to track pipeline performance

For a detailed explanation of each step, expected outcomes, and troubleshooting guidance, refer to the [Quality Assurance Guide](./QA.md).

## Quality Assurance

The [QA.md](./QA.md) document provides:

- Detailed explanation of what each command does
- Step-by-step testing procedures for all features
- Complete workflow documentation
- Troubleshooting guidance for common issues

Use this guide both for testing the application and for understanding the complete data pipeline workflow.

## License

All rights reserved. This code is proprietary and confidential.
