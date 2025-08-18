# Product Data Pipeline Quality Assurance Guide

This guide explains each command in the Product Data Pipeline, what it does, and how to verify it's working correctly. It also outlines the complete workflow from data harvesting to merchant review.

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

### Review Process Commands

| Command                 | Description                 | What It Does                                                                                                    |
| ----------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `review:export-pending` | Export merchants for review | Creates a CSV file with all pending merchants that need review. This file will be shared with expert reviewers. |
| `review:import-results` | Import review results       | Reads the reviewed CSV file where experts have updated approval statuses and updates the database accordingly.  |

### Database Utilities

| Command         | Description                   | What It Does                                                                            |
| --------------- | ----------------------------- | --------------------------------------------------------------------------------------- |
| `export:tables` | Export database tables to CSV | Creates CSV files for each database table (or specified tables) for backup or analysis. |

## 2. Complete Workflow

The complete merchant data pipeline consists of the following workflow:

1. **Initial Data Collection**

   - Run `harvest:init` to collect the initial set of merchants and products
   - This creates records in the database with all merchants marked as "PENDING"

2. **Regular Data Updates**

   - Run `harvest:delta` on a scheduled basis (e.g., daily) to:
     - Update information for existing merchants
     - Add new merchants that weren't found before
   - All new merchants are marked as "PENDING"

3. **Export for Expert Review**

   - Run `review:export-pending` to generate a CSV file with all pending merchants
   - This creates a file (default: `data/pending_merchants.csv`) containing merchant details

4. **Expert Review Process (Manual)**

   - Experts open the exported CSV file
   - For each merchant, they update the `approval_status` column:
     - `PENDING` → Keep as is if still under review
     - `WHITELIST` or `WHITE_LIST` → Approved merchants
     - `BLACKLIST` or `BLACK_LIST` → Rejected merchants
   - They can add notes in the `note` column explaining their decision
   - Save the updated file (e.g., as `data/reviewed_merchants.csv`)

5. **Import Review Results**

   - Run `review:import-results` to update the database with expert decisions
   - The system applies all status changes and notes from the CSV file
   - Only merchants with changed status (not PENDING) are updated

6. **Monitor and Analyze**

   - Run `harvest:status` regularly to check:
     - Job history and performance
     - Current approval status counts
     - Product and category statistics

7. **Data Export (As Needed)**
   - Run `export:tables` to export data for backup or external analysis

## 3. Step-by-Step Testing

This section walks through testing each command and verifying its results.

### 3.1 Initial Harvest Test

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
  - Creates product-category associations
- Tracks job progress and statistics

**How to Verify Success:**

```bash
# Check harvest status to verify job ran successfully
python main.py harvest:status

# Expected output should show:
# - A HARVEST_INIT job with found_count and new_count values
# - Seller counts showing all or mostly PENDING status
```

### 3.2 Delta Harvest Test

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

### 3.6 Export Tables Test

```bash
# Export all tables
python main.py export:tables

# Export specific tables
python main.py export:tables --tables sellers products
```

**What Happens:**

- Creates CSV files for each table in `data/exports/` directory
- Files are named with table name and timestamp (e.g., `sellers_20250818_173510.csv`)
- All database records for each table are exported

**How to Verify Success:**

```bash
# Check that files were created
ls -la data/exports/

# Preview a file
head data/exports/sellers_*.csv
```

## 4. Troubleshooting

Here are common issues and their solutions:

### 4.1 API Connection Problems

**Symptoms:**

- Error messages about API connection, authentication, or rate limits
- Zero products/sellers found despite valid categories/keywords

**Solutions:**

```bash
# Check API key and host are correctly set
grep -E "API_KEY|API_HOST" .env

# Test API connection
python test_api.py

# Look for API errors in logs
grep -i "api" logs/merchant_harvester.log | grep -i "error"
```

### 4.2 Database Issues

**Symptoms:**

- SQL errors in logs or console output
- Missing tables or columns
- Unexpected counts or missing records

**Solutions:**

```bash
# Verify tables exist and are properly structured
python -c "from src.common.database import create_tables_if_not_exist; create_tables_if_not_exist(); print('Tables verified')"

# Check database file exists (for SQLite)
ls -la test.db

# Back up and reset database if needed
cp test.db test.db.backup
rm test.db
python -c "from src.common.database import create_tables_if_not_exist; create_tables_if_not_exist(); print('Fresh database created')"
```

### 4.3 CSV File Issues

**Symptoms:**

- Error messages when importing CSV file
- Missing or unexpected columns
- Missing records

**Solutions:**

```bash
# Check CSV file exists
ls -la data/reviewed_merchants.csv

# Preview CSV structure
head -n 2 data/reviewed_merchants.csv

# Verify required columns exist
head -n 1 data/reviewed_merchants.csv | tr ',' '\n' | grep -E "shop_id|approval_status"

# Check for invalid approval status values
grep -v -E "PENDING|WHITELIST|BLACKLIST|WHITE_LIST|BLACK_LIST" data/reviewed_merchants.csv
```

### 4.4 Log Inspection

When troubleshooting, always check the logs:

```bash
# View merchant harvester logs
tail -n 50 logs/merchant_harvester.log

# View merchant review logs
tail -n 50 logs/merchant_review.log

# Search for errors across all logs
grep -i "error" logs/*.log
```
