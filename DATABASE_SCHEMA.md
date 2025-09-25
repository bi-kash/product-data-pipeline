# Database Schema Reference

This document provides a complete reference for all database tables and their relationships in the Product Data Pipeline.

## Overview

The database uses SQLAlchemy ORM with support for both SQLite (testing) and PostgreSQL (production). All tables use UTC timestamps and include proper foreign key relationships.

## Core Tables

### sellers

Stores merchant information from AliExpress.

| Field             | Type                    | Constraints                 | Description                           |
| ----------------- | ----------------------- | --------------------------- | ------------------------------------- |
| `shop_id`         | String(255)             | PRIMARY KEY                 | Unique identifier from AliExpress     |
| `shop_url`        | String(255)             | NOT NULL                    | URL to the seller's shop              |
| `shop_name`       | String(255)             | NULL                        | Name of the seller's shop             |
| `approval_status` | String(20)              | NOT NULL, DEFAULT 'PENDING' | Status: PENDING, WHITELIST, BLACKLIST |
| `first_seen_at`   | DateTime(timezone=True) | NOT NULL                    | When first discovered (UTC)           |
| `last_seen_at`    | DateTime(timezone=True) | NOT NULL                    | When last seen (UTC)                  |
| `raw_json`        | JSON                    | NULL                        | Raw API response data                 |
| `note`            | Text                    | NULL                        | Optional note for review              |

**Relationships:**

- One-to-many with `products`
- One-to-many with `filtered_products`

### products

Stores product information from AliExpress search results.

| Field                        | Type                    | Constraints                   | Description                       |
| ---------------------------- | ----------------------- | ----------------------------- | --------------------------------- |
| `product_id`                 | String(255)             | PRIMARY KEY                   | Unique identifier from AliExpress |
| `shop_id`                    | String(255)             | FK(sellers.shop_id), NOT NULL | Reference to seller               |
| `product_title`              | String(500)             | NULL                          | Product title                     |
| `product_detail_url`         | String(500)             | NULL                          | URL to product detail page        |
| `product_main_image_url`     | String(500)             | NULL                          | Main product image URL            |
| `original_price`             | Float                   | NULL                          | Original price                    |
| `target_sale_price`          | Float                   | NULL                          | Sale price                        |
| `original_price_currency`    | String(10)              | NULL                          | Currency for original price       |
| `target_sale_price_currency` | String(10)              | NULL                          | Currency for sale price           |
| `discount`                   | String(20)              | NULL                          | Discount percentage               |
| `evaluate_rate`              | String(20)              | NULL                          | Product rating                    |
| `category_id`                | String(100)             | NULL                          | Comma-separated category IDs      |
| `first_seen_at`              | DateTime(timezone=True) | NOT NULL                      | When first discovered (UTC)       |
| `last_seen_at`               | DateTime(timezone=True) | NOT NULL                      | When last seen (UTC)              |
| `raw_json_search`            | JSON                    | NULL                          | Raw search result data            |
| `raw_json_detail`            | JSON                    | NULL                          | Raw detailed product data         |

**Relationships:**

- Many-to-one with `sellers`
- One-to-many with `filtered_products`

## Processing Tables

### filtered_products

Stores products that pass business rule filtering with enrichment data.

| Field                           | Type                    | Constraints                          | Description                      |
| ------------------------------- | ----------------------- | ------------------------------------ | -------------------------------- |
| `product_id`                    | String(255)             | PRIMARY KEY, FK(products.product_id) | Reference to original product    |
| `shop_id`                       | String(255)             | FK(sellers.shop_id), NOT NULL        | Reference to seller              |
| _(All product fields mirrored)_ | Various                 |                                      | Same as products table           |
| `ship_to_country`               | String(10)              | NULL                                 | Destination country code         |
| `delivery_time`                 | Integer                 | NULL                                 | Estimated delivery time in days  |
| `min_delivery_days`             | Integer                 | NULL                                 | Minimum delivery time            |
| `max_delivery_days`             | Integer                 | NULL                                 | Maximum delivery time            |
| `max_variant_price`             | Float                   | NULL                                 | Highest variant price found      |
| `min_shipping_price`            | Float                   | NULL                                 | Cheapest shipping among variants |
| `filtered_at`                   | DateTime(timezone=True) | DEFAULT now()                        | When product was filtered        |

**Relationships:**

- Many-to-one with `products`
- Many-to-one with `sellers`
- One-to-many with `shipping_info`
- One-to-many with `product_images`
- One-to-one with `product_status`

### shipping_info

Stores detailed shipping options for filtered products.

| Field                      | Type                    | Constraints                                | Description                   |
| -------------------------- | ----------------------- | ------------------------------------------ | ----------------------------- |
| `id`                       | Integer                 | PRIMARY KEY, AUTO_INCREMENT                | Unique identifier             |
| `product_id`               | String(255)             | FK(filtered_products.product_id), NOT NULL | Reference to filtered product |
| `sku_id`                   | String(255)             | NOT NULL                                   | SKU identifier for variant    |
| `code`                     | String(100)             | NULL                                       | Shipping method code          |
| `company`                  | String(200)             | NULL                                       | Shipping company name         |
| `shipping_fee`             | Float                   | NULL                                       | Numeric shipping fee          |
| `shipping_fee_currency`    | String(10)              | NULL                                       | Currency for shipping fee     |
| `free_shipping`            | Boolean                 | NULL                                       | Free shipping indicator       |
| `min_delivery_days`        | Integer                 | NULL                                       | Minimum delivery days         |
| `max_delivery_days`        | Integer                 | NULL                                       | Maximum delivery days         |
| `guaranteed_delivery_days` | Integer                 | NULL                                       | Guaranteed delivery days      |
| `ship_from_country`        | String(10)              | NULL                                       | Origin country code           |
| `tracking`                 | Boolean                 | NULL                                       | Tracking availability         |
| `raw_freight_response`     | JSON                    | NULL                                       | Full API response             |
| `created_at`               | DateTime(timezone=True) | DEFAULT now()                              | When info was retrieved       |

**Relationships:**

- Many-to-one with `filtered_products`
- Related to `product_images` via `sku_id`

### product_images

Stores categorized product images with analysis data.

| Field                            | Type                    | Constraints                                | Description                         |
| -------------------------------- | ----------------------- | ------------------------------------------ | ----------------------------------- |
| `id`                             | Integer                 | PRIMARY KEY, AUTO_INCREMENT                | Unique identifier                   |
| `product_id`                     | String(255)             | FK(filtered_products.product_id), NOT NULL | Reference to filtered product       |
| `image_url`                      | Text                    | NOT NULL, UNIQUE                           | Full image URL                      |
| `image_role`                     | String(20)              | NOT NULL                                   | Image type: hero, gallery, variant  |
| `sku_id`                         | String(255)             | NULL                                       | SKU identifier from API             |
| `variant_key`                    | String(300)             | NULL                                       | Property name:value format          |
| `property_value`                 | String(100)             | NULL                                       | Property value (e.g., "Red")        |
| `property_name`                  | String(100)             | NULL                                       | Property name (e.g., "Color")       |
| `property_id`                    | String(100)             | NULL                                       | Property ID from API                |
| `property_value_definition_name` | String(200)             | NULL                                       | Full definition name                |
| `sort_index`                     | Integer                 | NOT NULL, DEFAULT 0                        | Image ordering                      |
| `width`                          | Integer                 | NULL                                       | Image width in pixels               |
| `height`                         | Integer                 | NULL                                       | Image height in pixels              |
| `is_primary`                     | Boolean                 | DEFAULT FALSE                              | Primary/hero image flag             |
| `local_file_path`                | String(500)             | NULL                                       | Local file path (relative)          |
| `phash`                          | String(64)              | NULL                                       | Perceptual hash (hex string)        |
| `download_status`                | String(20)              | NULL                                       | Status: pending, downloaded, failed |
| `created_at`                     | DateTime(timezone=True) | DEFAULT now()                              | When image was processed            |

**Relationships:**

- Many-to-one with `filtered_products`
- Related to `shipping_info` via `sku_id`

## Session Management

### session_codes

Stores AliExpress API session credentials.

| Field                      | Type                    | Constraints                 | Description                     |
| -------------------------- | ----------------------- | --------------------------- | ------------------------------- |
| `id`                       | Integer                 | PRIMARY KEY, AUTO_INCREMENT | Unique identifier               |
| `code`                     | String(255)             | UNIQUE, NOT NULL, INDEXED   | Authorization code              |
| `access_token`             | Text                    | NOT NULL                    | Current access token            |
| `refresh_token`            | Text                    | NOT NULL                    | Refresh token                   |
| `expire_time`              | String(50)              | NOT NULL                    | Access token expiration (ms)    |
| `refresh_token_valid_time` | String(50)              | NOT NULL                    | Refresh token expiration        |
| `expires_in`               | String(10)              | NOT NULL                    | Token lifetime (seconds)        |
| `refresh_expires_in`       | String(10)              | NOT NULL                    | Refresh lifetime (seconds)      |
| `havana_id`                | String(50)              | NULL                        | Havana ID from AliExpress       |
| `locale`                   | String(10)              | NULL                        | User locale                     |
| `user_nick`                | String(255)             | NULL                        | AliExpress username             |
| `account_id`               | String(50)              | NULL                        | Account identifier              |
| `user_id`                  | String(50)              | NULL                        | User identifier                 |
| `account_platform`         | String(50)              | NULL                        | Account platform                |
| `sp`                       | String(10)              | NULL                        | SP parameter                    |
| `request_id`               | String(255)             | NULL                        | API request identifier          |
| `seller_id`                | String(50)              | NULL                        | Seller identifier               |
| `account`                  | String(255)             | NULL                        | Account information             |
| `token_type`               | String(20)              | DEFAULT 'original'          | Token type: original, refreshed |
| `is_active`                | Boolean                 | DEFAULT TRUE                | Session status                  |
| `response_json`            | JSON                    | NULL                        | Full API response               |
| `created_at`               | DateTime(timezone=True) | DEFAULT now()               | Session creation time           |
| `updated_at`               | DateTime(timezone=True) | DEFAULT now(), AUTO_UPDATE  | Last update time                |

## Module C: Duplicate Detection

### product_status

Stores duplicate detection results and product status.

| Field                 | Type                    | Constraints                                        | Description                                       |
| --------------------- | ----------------------- | -------------------------------------------------- | ------------------------------------------------- |
| `id`                  | Integer                 | PRIMARY KEY, AUTO_INCREMENT                        | Unique identifier                                 |
| `product_id`          | String(255)             | FK(filtered_products.product_id), UNIQUE, NOT NULL | Reference to filtered product                     |
| `status`              | String(50)              | NOT NULL                                           | Status: UNIQUE, MASTER, DUPLICATE, REVIEW_SUSPECT |
| `duplicate_master_id` | String(255)             | NULL                                               | Points to master product if duplicate             |
| `total_landed_cost`   | Float                   | NULL                                               | Item + shipping cost for master selection         |
| `detection_method`    | String(50)              | NULL                                               | Method: PHASH, CLIP, MANUAL                       |
| `phash_difference`    | Integer                 | NULL                                               | Hamming distance (0-64)                           |
| `clip_similarity`     | Float                   | NULL                                               | CLIP similarity score (0.0-1.0)                   |
| `created_at`          | DateTime(timezone=True) | DEFAULT now()                                      | Analysis timestamp                                |
| `updated_at`          | DateTime(timezone=True) | DEFAULT now(), AUTO_UPDATE                         | Last update time                                  |

**Relationships:**

- One-to-one with `filtered_products`

**Note:** Existing databases may contain additional legacy columns (confidence_score, images_processed, processing_duration) that are no longer used by the current codebase.

## Logging & Monitoring

### job_runs

Logs execution information for all pipeline operations.

| Field              | Type                    | Constraints                 | Description                               |
| ------------------ | ----------------------- | --------------------------- | ----------------------------------------- |
| `id`               | Integer                 | PRIMARY KEY, AUTO_INCREMENT | Unique identifier                         |
| `job_type`         | String(50)              | NOT NULL                    | Job type (e.g., HARVEST_INIT)             |
| `start_time`       | DateTime(timezone=True) | NOT NULL                    | Job start time (UTC)                      |
| `end_time`         | DateTime(timezone=True) | NULL                        | Job completion time                       |
| `duration_seconds` | Integer                 | NULL                        | Job duration                              |
| `found_count`      | Integer                 | DEFAULT 0                   | Items found                               |
| `new_count`        | Integer                 | DEFAULT 0                   | New items added                           |
| `skipped_count`    | Integer                 | DEFAULT 0                   | Items skipped                             |
| `error_count`      | Integer                 | DEFAULT 0                   | Errors encountered                        |
| `keywords`         | Text                    | NULL                        | Keywords used (comma-separated or JSON)   |
| `categories`       | Text                    | NULL                        | Categories used (comma-separated or JSON) |
| `details`          | JSON                    | NULL                        | Additional job details                    |

## Indexes and Constraints

### Primary Keys

- All tables have proper primary keys (single column or composite)

### Foreign Keys

- `products.shop_id` → `sellers.shop_id`
- `filtered_products.product_id` → `products.product_id`
- `filtered_products.shop_id` → `sellers.shop_id`
- `shipping_info.product_id` → `filtered_products.product_id`
- `product_images.product_id` → `filtered_products.product_id`
- `product_status.product_id` → `filtered_products.product_id`

### Unique Constraints

- `session_codes.code` (with index)
- `product_images.image_url`
- `product_status.product_id`

### Recommended Indexes

```sql
-- Performance indexes (should be added manually if needed)
CREATE INDEX idx_products_shop_id ON products(shop_id);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_shipping_info_sku_id ON shipping_info(sku_id);
CREATE INDEX idx_product_images_sku_id ON product_images(sku_id);
CREATE INDEX idx_product_images_phash ON product_images(phash);
CREATE INDEX idx_product_status_status ON product_status(status);
CREATE INDEX idx_job_runs_job_type ON job_runs(job_type);
```

## Data Flow

1. **Harvest Phase**: `sellers` → `products` → `job_runs`
2. **Filtering Phase**: `products` → `filtered_products` → `shipping_info` + `product_images`
3. **Duplicate Detection**: `filtered_products` + `product_images` → `product_status`
4. **Session Management**: `session_codes` (independent, used throughout)

## Configuration

- **SQLite**: Used for development/testing (`USE_SQLITE=true`)
- **PostgreSQL**: Used for production (`USE_SQLITE=false`)
- **Timezone**: All timestamps stored in UTC
- **JSON Fields**: Native JSON support in both databases
- **Character Encoding**: UTF-8 for all text fields
