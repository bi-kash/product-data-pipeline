# Product Data Pipeline: Search Configuration Guide

This document explains how to effectively use and configure the search parameters in the product data pipeline, specifically focusing on keywords and categories.

## Overview

The product data pipeline can search for products using two methods:

1. **Category-based search**: Searches for products within specific AliExpress categories
2. **Keyword-based search**: Searches for products matching specific keywords

You can configure the pipeline to use:

- Only category-based search
- Both category and keyword-based search together

## Configuration Options

The search behavior is controlled through the `.env` file with the following key parameters:

```
# Search Method Configuration
USE_KEYWORDS=false
KEYWORDS=necklace,ring,bracelet,earrings,pendant necklace,brooch,anklet

# Blacklist terms in product titles
BLACKLIST_TERMS_IN_TITLE=beads,findings,jump rings,clasps,wire,chain by the foot

# Jewelry categories (AliExpress category IDs)
CATEGORIES=200001680,1509,201239108,200370154
```

### USE_KEYWORDS Flag

This flag determines the search method:

- `USE_KEYWORDS=false`: The pipeline will search **only using categories** defined in the `CATEGORIES` parameter.
- `USE_KEYWORDS=true`: The pipeline will search using **both keywords and categories**, making separate API calls for each.

### KEYWORDS Parameter

A comma-separated list of keywords to search for when `USE_KEYWORDS=true`. For example:

```
KEYWORDS=necklace,ring,bracelet,earrings,pendant necklace,brooch,anklet
```

Best practices for keywords:

- Use specific jewelry item types (like "necklace", "earrings")
- Be precise with multi-word phrases (like "pendant necklace")
- Avoid generic terms that might return supplies (like "beads", "chain")
- Separate each keyword with a comma
- Remove spaces after commas for consistency

### BLACKLIST_TERMS_IN_TITLE Parameter

A comma-separated list of terms that, when found in product titles, will cause those products and their sellers to be automatically blacklisted during harvesting. For example:

```
BLACKLIST_TERMS_IN_TITLE=beads,findings,jump rings,clasps,wire,chain by the foot,jewelry making,diy jewelry
```

This is a powerful way to filter out unwanted items, particularly jewelry supplies and components, without having to modify your category selection.

When a product title contains any of these terms:

- The product is marked with status="BLACKLIST" in the database
- The seller is marked with approval_status="BLACKLIST" in the database
- These items are counted in the "blacklisted" statistics during harvesting

Best practices for blacklist terms:

- Include common jewelry supply terms like "beads", "findings", "clasps"
- Include jewelry-making phrases like "diy jewelry", "jewelry making"
- Use lowercase (matching is case-insensitive)
- Separate terms with commas (no spaces after commas)
- Multi-word terms work as expected (e.g., "chain by the foot")
- If left empty, no title-based filtering will occur

The harvester will log how many products were blacklisted due to their titles in the harvest summary. The blacklisting happens during the harvesting process, not after review import.

### CATEGORIES Parameter

A comma-separated list of AliExpress category IDs to use in category-based searches. For example:

```
CATEGORIES=200001680,1509,201239108,200370154
```

The included categories are:

- `200001680`: Fine Jewelry
- `1509`: Fashion Jewelry
- `201239108`: Customized Jewelry
- `200370154`: Smart Jewelry

Categories to avoid (they primarily contain supplies, not finished jewelry):

- `200001479`: Jewelry Packaging & Display
- `200001478`: Jewelry Tools & Equipment
- `201238105`: Jewelry Making

## How Search Works

### Category-Only Search (`USE_KEYWORDS=false`)

When `USE_KEYWORDS=false`:

1. The harvester searches **only** using the categories specified in `CATEGORIES`
2. A single job run is created in the database
3. The categories column in the job_run table reflects the categories used
4. The keywords column in the job_run table remains NULL

This approach is recommended for most cases because:

- It provides broader coverage of jewelry products
- It avoids potential bias from keyword limitations
- It's more efficient (fewer API calls)
- Well-chosen categories naturally filter out supplies

### Combined Search (`USE_KEYWORDS=true`)

When `USE_KEYWORDS=true`:

1. The harvester first searches using each keyword specified in `KEYWORDS`
2. Then it searches using the categories specified in `CATEGORIES`
3. A single job run is created for the entire process
4. The keywords and categories columns in the job_run table reflect exactly which keywords and categories were actually used

**Important**: The AliExpress API doesn't support combined keyword+category searches in a single call. The pipeline makes separate API calls for keywords and categories.

## Avoiding Jewelry Supplies

One common challenge is filtering out jewelry supplies (beads, findings, chains, etc.) to focus only on finished jewelry products. Here are strategies to avoid supplies:

1. **Use category-only search** (`USE_KEYWORDS=false`) with carefully selected finished-jewelry categories:

   - `200001680` (Fine Jewelry)
   - `1509` (Fashion Jewelry)
   - `201239108` (Customized Jewelry)
   - `200370154` (Smart Jewelry)

2. **Avoid supply-heavy categories**:

   - `200001479` (Jewelry Packaging & Display)
   - `200001478` (Jewelry Tools & Equipment)
   - `201238105` (Jewelry Making)

3. **If using keywords** (`USE_KEYWORDS=true`):
   - Use specific finished jewelry terms: "necklace", "earrings", "ring", "bracelet"
   - Avoid supply terms: "beads", "findings", "wire", "chain by the foot", etc.
   - Use qualifying adjectives: "finished", "ready to wear", etc.

## Job Run Tracking

The pipeline tracks which keywords and categories were actually used during each harvest in the `job_run` table:

- **keywords column**: Comma-separated list of the keywords that were actually searched for
- **categories column**: Comma-separated list of the category IDs that were actually searched for

These columns are updated after each page of results is processed, ensuring that even if the harvest is interrupted, the job_run record will accurately reflect which search parameters were used.

## Best Practices

1. **Start with category-only search** (`USE_KEYWORDS=false`) using the recommended finished jewelry categories.

2. **Only enable keyword search** (`USE_KEYWORDS=true`) if you need to further filter or target specific types of jewelry.

3. **Monitor job run records** to see which search parameters are being used and their effectiveness:

   ```bash
   python main.py harvest:status
   ```

4. **Adjust your search strategy** based on the results:

   - If getting too many supplies: remove broader categories, add more specific ones
   - If missing certain product types: consider adding targeted keywords

5. **Use limits during testing** to avoid processing too much data while tuning your search parameters:
   ```bash
   python main.py harvest:delta --limit 100
   ```

## Common Issues and Solutions

### Issue: Too many jewelry supplies in results

**Solutions:**

- Set `USE_KEYWORDS=false` and use only the recommended finished jewelry categories
- Remove any supply-heavy categories from the `CATEGORIES` list
- Add supply-related terms to `BLACKLIST_TERMS_IN_TITLE` to automatically skip products containing those terms
- If using keywords, make them more specific to finished jewelry items

### Issue: Using the title blacklist effectively

**Best practices:**

- Add common supply terms like "beads", "findings", "wire", "jump rings"
- Include multi-word phrases without quotes, e.g., chain by the foot, jewelry making supplies
- Use lowercase terms (matching is case-insensitive)
- Review harvester logs to see how many products are being blacklisted
- Periodically review and refine your blacklist terms based on results

**Understanding automatic blacklisting:**

- Products with blacklisted terms in their titles are stored in the database with status="BLACKLIST"
- Sellers of these products are stored with approval_status="BLACKLIST"
- This happens automatically during the harvest process, not after review
- The automatic blacklisting is permanent and takes effect immediately
- You can see the blacklisting counts in the harvest status summary

### Issue: Missing certain types of jewelry

**Solutions:**

- Add relevant categories that may contain those items
- Set `USE_KEYWORDS=true` and add specific keywords for those jewelry types
- Consider running a test harvest with a high limit to verify results

### Issue: Interrupted harvests showing incomplete data

**Solution:**
The pipeline is designed to update the job_run record after each page of results, so even if a harvest is interrupted:

- The keywords column will show all keywords that were actually used before the interruption
- The categories column will show all categories that were configured, as they're initialized at the start of the harvest
- You can safely restart the harvest and it will continue building your database
