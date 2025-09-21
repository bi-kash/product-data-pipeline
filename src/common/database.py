"""
Database models and helper functions using SQLAlchemy ORM.
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    Text,
    Boolean,
    ForeignKey,
    JSON,
    Float,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check if we're in test mode (use SQLite) or production mode (use PostgreSQL)
USE_SQLITE = os.getenv("USE_SQLITE", "true").lower() == "true"

if USE_SQLITE:
    # Use SQLite for testing
    DATABASE_URL = "sqlite:///./test.db"
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    # Use PostgreSQL for production
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create declarative base model
Base = declarative_base()


def get_db_session():
    """
    Create a new database session.

    Returns:
        SQLAlchemy session object
    """
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.close()
        raise


# Define models
class Seller(Base):
    """Model representing a seller/merchant from AliExpress."""

    __tablename__ = "sellers"

    shop_id = Column(String(255), primary_key=True)
    shop_url = Column(String(255), nullable=False)
    shop_name = Column(String(255), nullable=True)
    approval_status = Column(String(20), nullable=False, default="PENDING")
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    raw_json = Column(JSON)
    note = Column(Text)

    # Define the relationship to products
    products = relationship("Product", back_populates="seller")

    def __repr__(self):
        return f"<Seller(shop_id='{self.shop_id}', approval_status='{self.approval_status}')>"


class Product(Base):
    """Model representing a product from AliExpress."""

    __tablename__ = "products"

    product_id = Column(String(255), primary_key=True)
    shop_id = Column(String(255), ForeignKey("sellers.shop_id"), nullable=False)
    product_title = Column(String(500), nullable=True)
    product_detail_url = Column(String(500), nullable=True)
    product_main_image_url = Column(String(500), nullable=True)
    original_price = Column(Float, nullable=True)
    target_sale_price = Column(Float, nullable=True)
    original_price_currency = Column(String(10), nullable=True)
    target_sale_price_currency = Column(String(10), nullable=True)
    discount = Column(String(20), nullable=True)
    evaluate_rate = Column(String(20), nullable=True)
    category_id = Column(String(100), nullable=True)  # Store comma-separated category IDs like "15,3710,100001404,200042147"
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    raw_json_search = Column(JSON)  # Store text search API response data for individual product
    raw_json_detail = Column(JSON)  # Store aliexpress.ds.product.get API response data

    # Define the relationship to sellers
    seller = relationship("Seller", back_populates="products")

    def __repr__(self):
        return f"<Product(product_id='{self.product_id}', title='{self.product_title[:30]}...')>"


class JobRun(Base):
    """Model representing a job execution."""

    __tablename__ = "job_runs"

    id = Column(Integer, primary_key=True)
    job_type = Column(String(50), nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True))
    duration_seconds = Column(Integer)
    found_count = Column(Integer, default=0)
    new_count = Column(Integer, default=0)
    skipped_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    keywords = Column(
        Text, nullable=True
    )  # Comma-separated or JSON string of keywords used
    categories = Column(
        Text, nullable=True
    )  # Comma-separated or JSON string of categories used
    details = Column(JSON)

    def __repr__(self):
        return f"<JobRun(id={self.id}, job_type='{self.job_type}')>"


class SessionCode(Base):
    """Model representing an AliExpress API session token."""

    __tablename__ = "session_codes"

    id = Column(Integer, primary_key=True)
    code = Column(String(255), unique=True, nullable=False, index=True)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    expire_time = Column(String(50), nullable=False)  # Store as string since API returns milliseconds
    refresh_token_valid_time = Column(String(50), nullable=False)
    expires_in = Column(String(10), nullable=False)  # Store as string to match API
    refresh_expires_in = Column(String(10), nullable=False)
    havana_id = Column(String(50))
    locale = Column(String(10))
    user_nick = Column(String(255))
    account_id = Column(String(50))
    user_id = Column(String(50))
    account_platform = Column(String(50))
    sp = Column(String(10))
    request_id = Column(String(255))
    seller_id = Column(String(50))
    account = Column(String(255))
    token_type = Column(String(20), default='original')  # 'original' or 'refreshed'
    is_active = Column(Boolean, default=True)
    response_json = Column(JSON)  # Store full response for reference
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<SessionCode(code='{self.code}', is_active={self.is_active}, token_type='{self.token_type}')>"


class FilteredProduct(Base):
    """
    Model representing a filtered product.
    
    This table mirrors the products table structure and adds only:
    - ship_to_country: Destination country for shipping
    - delivery_time: Estimated delivery time in days  
    - max_variant_price: Highest variant price found
    """

    __tablename__ = "filtered_products"

    # Primary key - use product_id as primary key
    product_id = Column(String(255), ForeignKey("products.product_id"), primary_key=True)
    
    # Mirror all fields from products table
    shop_id = Column(String(255), ForeignKey("sellers.shop_id"), nullable=False)
    product_title = Column(String(500), nullable=True)
    product_detail_url = Column(String(500), nullable=True)
    product_main_image_url = Column(String(500), nullable=True)
    original_price = Column(Float, nullable=True)
    target_sale_price = Column(Float, nullable=True)
    original_price_currency = Column(String(10), nullable=True)
    target_sale_price_currency = Column(String(10), nullable=True)
    discount = Column(String(20), nullable=True)
    evaluate_rate = Column(String(20), nullable=True)
    category_id = Column(String(100), nullable=True)  # Store comma-separated category IDs
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    raw_json_detail = Column(JSON)  # Store aliexpress.ds.product.get API response data
    
    # Additional fields specific to filtered products
    ship_to_country = Column(String(10), nullable=True)  # Country code (e.g., "US", "DE")
    delivery_time = Column(Integer, nullable=True)  # Estimated delivery time in days
    min_delivery_days = Column(Integer, nullable=True)  # Minimum delivery time in days
    max_delivery_days = Column(Integer, nullable=True)  # Maximum delivery time in days
    max_variant_price = Column(Float, nullable=True)  # Highest variant price found
    min_shipping_price = Column(Float, nullable=True)  # Cheapest shipping price among all SKU variants
    
    # Timestamps for filtering
    filtered_at = Column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    product = relationship("Product", backref="filtered_entries")
    seller = relationship("Seller", backref="filtered_products")

    def __repr__(self):
        return f"<FilteredProduct(product_id='{self.product_id}', delivery_time={self.delivery_time}, min_days={self.min_delivery_days}, max_days={self.max_delivery_days}, max_variant_price={self.max_variant_price})>"


class ShippingInfo(Base):
    """
    Model representing detailed shipping information for filtered products.
    
    This table stores shipping options retrieved from aliexpress.ds.freight.query API
    for each filtered product, including multiple delivery options with pricing.
    """

    __tablename__ = "shipping_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(255), ForeignKey("filtered_products.product_id"), nullable=False)
    sku_id = Column(String(255), nullable=False)  # SKU ID for which shipping info was queried
    
    # Shipping option details
    code = Column(String(100), nullable=True)  # e.g., "CAINIAO_FULFILLMENT_STD"
    company = Column(String(200), nullable=True)  # e.g., "AliExpress Selection Standard"
    
    # Pricing information
    shipping_fee = Column(Float, nullable=True)  # Numeric shipping fee
    shipping_fee_currency = Column(String(10), nullable=True)  # e.g., "USD"
    free_shipping = Column(Boolean, nullable=True)
    
    # Delivery timing
    min_delivery_days = Column(Integer, nullable=True)
    max_delivery_days = Column(Integer, nullable=True)
    guaranteed_delivery_days = Column(Integer, nullable=True)
    
    # Shipping attributes
    ship_from_country = Column(String(10), nullable=True)  # e.g., "CN"
    tracking = Column(Boolean, nullable=True)
    
    # API response metadata
    raw_freight_response = Column(JSON)  # Store full API response
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    filtered_product = relationship("FilteredProduct", backref="shipping_options")

    def get_related_product_images(self, db_session):
        """
        Get product images that share the same SKU ID.
        
        Args:
            db_session: Database session
            
        Returns:
            List of ProductImage objects with matching sku_id
        """
        if not self.sku_id:
            return []
            
        return db_session.query(ProductImage).filter(
            ProductImage.product_id == self.product_id,
            ProductImage.sku_id == self.sku_id
        ).all()

    def __repr__(self):
        return f"<ShippingInfo(product_id='{self.product_id}', sku_id='{self.sku_id}', company='{self.company}', fee={self.shipping_fee}, days={self.min_delivery_days}-{self.max_delivery_days})>"


class ProductImage(Base):
    """
    Model representing product images for filtered products.
    
    Stores all available image URLs (hero, gallery, and variant) for each product.
    """

    __tablename__ = "product_images"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(255), ForeignKey("filtered_products.product_id"), nullable=False)
    
    # Image details
    image_url = Column(Text, nullable=False)  # Full image URL
    image_role = Column(String(20), nullable=False)  # 'hero', 'gallery', 'variant'
    
    # SKU and variant details
    sku_id = Column(String(255), nullable=True)  # SKU ID from ae_item_sku_info_d_t_o
    variant_key = Column(String(300), nullable=True)  # property_name:property_value format
    
    # Property details (replaces variant_key)
    property_value = Column(String(100), nullable=True)  # The property value (e.g., "Red", "Blue")
    property_name = Column(String(100), nullable=True)  # The property name (e.g., "Color", "Size")
    property_id = Column(String(100), nullable=True)  # The property ID from sku_property_id
    property_value_definition_name = Column(String(200), nullable=True)  # Full definition name (e.g., "Cherry Wood Color")
    
    sort_index = Column(Integer, nullable=False, default=0)  # For ordering images
    
    # Optional image dimensions
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    
    # Metadata
    is_primary = Column(Boolean, default=False)  # Mark primary/hero image
    
    # Image download and analysis
    local_file_path = Column(String(500), nullable=True)  # Local file path after download
    phash = Column(String(64), nullable=True)  # Perceptual hash for deduplication
    download_status = Column(String(20), nullable=True)  # 'pending', 'downloaded', 'failed'
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    filtered_product = relationship("FilteredProduct", backref="product_images")

    def get_related_shipping_info(self, db_session):
        """
        Get shipping information that shares the same SKU ID.
        
        Args:
            db_session: Database session
            
        Returns:
            List of ShippingInfo objects with matching sku_id
        """
        if not self.sku_id:
            return []
            
        return db_session.query(ShippingInfo).filter(
            ShippingInfo.product_id == self.product_id,
            ShippingInfo.sku_id == self.sku_id
        ).all()

    def __repr__(self):
        return f"<ProductImage(product_id='{self.product_id}', role='{self.image_role}', property='{self.property_value}', url='{self.image_url[:50]}...')>"


def create_tables_if_not_exist():
    """
    Create the necessary tables if they don't exist.
    """
    Base.metadata.create_all(bind=engine)


def get_utc_now():
    """
    Get current UTC datetime.

    Returns:
        Current UTC datetime object.
    """
    return datetime.now(timezone.utc)


def start_job_run(job_type, keywords=None, categories=None):
    """
    Create a new job run entry with start time.

    Args:
        job_type: Type of the job being run
        keywords: List of keywords used (optional)
        categories: List of categories used (optional)

    Returns:
        ID of the created job run
    """
    db = get_db_session()

    # Convert lists to comma-separated strings if needed
    keywords_str = None
    categories_str = None
    if keywords:
        keywords_str = (
            ",".join(keywords) if isinstance(keywords, list) else str(keywords)
        )
    if categories:
        categories_str = (
            ",".join([str(c) for c in categories])
            if isinstance(categories, list)
            else str(categories)
        )

    try:
        job_run = JobRun(
            job_type=job_type,
            start_time=get_utc_now(),
            keywords=keywords_str,
            categories=categories_str,
        )
        db.add(job_run)
        db.commit()
        db.refresh(job_run)
        return job_run.id
    finally:
        db.close()


def update_job_run_progress(
    job_id,
    found=0,
    new=0,
    skipped=0,
    errors=0,
    details=None,
    keywords=None,
    categories=None,
):
    """
    Update job run entry with progress data without marking it as complete.
    Also updates end_time and duration_seconds to current values so they are available
    even if the process is interrupted.

    Args:
        job_id: ID of the job run to update
        found: Number of records found
        new: Number of new records added
        skipped: Number of records skipped
        errors: Number of errors encountered
        details: Additional details as a dict (will be stored as JSON)
        keywords: String of comma-separated keywords used in the search so far
        categories: String of comma-separated category IDs used in the search so far
    """
    if not job_id:
        return

    db = get_db_session()
    try:
        job_run = db.query(JobRun).filter(JobRun.id == job_id).first()
        if job_run:
            # Update counts
            job_run.found_count = found
            job_run.new_count = new
            job_run.skipped_count = skipped
            job_run.error_count = errors

            # Update keywords and categories if provided
            if keywords is not None:
                job_run.keywords = keywords

            if categories is not None:
                job_run.categories = categories

            # Always update end_time and duration so we have something if interrupted
            end_time = get_utc_now()

            # Ensure start_time is aware of timezone if it's not already
            start_time = job_run.start_time
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)

            duration_seconds = int((end_time - start_time).total_seconds())

            job_run.end_time = end_time
            job_run.duration_seconds = duration_seconds

            # Update details if provided
            if details:
                job_run.details = details

            db.commit()
    finally:
        db.close()


def complete_job_run(
    job_id,
    found=0,
    new=0,
    skipped=0,
    errors=0,
    details=None,
    keywords=None,
    categories=None,
):
    """
    Update job run entry with completion data.

    Args:
        job_id: ID of the job run to update
        found: Number of records found
        new: Number of new records added
        skipped: Number of records skipped
        errors: Number of errors encountered
        details: Additional details as a dict (will be stored as JSON)
        keywords: String of comma-separated keywords used in the search
        categories: String of comma-separated category IDs used in the search
    """
    db = get_db_session()

    try:
        job_run = db.query(JobRun).filter(JobRun.id == job_id).first()

        if job_run:
            end_time = get_utc_now()
            # Ensure start_time is aware of timezone if it's not already
            start_time = job_run.start_time
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            duration_seconds = int((end_time - start_time).total_seconds())

            job_run.end_time = end_time
            job_run.duration_seconds = duration_seconds
            job_run.found_count = found
            job_run.new_count = new
            job_run.skipped_count = skipped
            job_run.error_count = errors
            job_run.details = details

            # Update keywords and categories if provided
            if keywords is not None:
                job_run.keywords = keywords

            if categories is not None:
                job_run.categories = categories

            db.commit()
    finally:
        db.close()


def upsert_seller(shop_id, shop_url, shop_name=None, raw_json=None, note=None):
    """
    Insert or update a seller record.

    If the seller already exists, this function will update certain fields but will
    NOT modify the approval_status. This ensures that once a seller has been
    whitelisted or blacklisted, their status remains unchanged by automated updates.

    Args:
        shop_id: Unique shop/seller ID
        shop_url: URL of the seller's shop
        shop_name: Name of the seller's shop
        raw_json: Original JSON data from API
        note: Optional note about the seller

    Returns:
        Boolean indicating if this was a new record (True) or an update (False)
    """
    db = get_db_session()
    now = get_utc_now()
    is_new = False

    try:
        # Check if seller exists
        seller = db.query(Seller).filter(Seller.shop_id == shop_id).first()

        if seller:
            # Update existing seller
            # NOTE: We never update approval_status here to preserve manual whitelisting/blacklisting
            seller.last_seen_at = now
            if shop_name:
                seller.shop_name = shop_name
            if raw_json:
                seller.raw_json = raw_json
            if note:
                seller.note = note
        else:
            # Insert new seller
            seller = Seller(
                shop_id=shop_id,
                shop_url=shop_url,
                shop_name=shop_name,
                approval_status="PENDING",
                first_seen_at=now,
                last_seen_at=now,
                raw_json=raw_json,
                note=note,
            )
            db.add(seller)
            is_new = True

        db.commit()
        return is_new
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def get_seller_approval_counts():
    """
    Get counts of sellers by approval status.

    Returns:
        Dict with counts of sellers by status
    """
    db = get_db_session()

    try:
        counts = {"PENDING": 0, "WHITELIST": 0, "BLACKLIST": 0, "TOTAL": 0}

        # Get counts for each status
        results = (
            db.query(Seller.approval_status, func.count(Seller.shop_id))
            .group_by(Seller.approval_status)
            .all()
        )

        # Convert to the expected format
        for status, count in results:
            counts[status] = count
            counts["TOTAL"] += count

        return counts
    finally:
        db.close()


def get_recent_job_runs(limit=10):
    """
    Get recent job runs from the database.

    Args:
        limit: Maximum number of job runs to return

    Returns:
        List of job run records as dictionaries
    """
    db = get_db_session()

    try:
        job_runs = (
            db.query(JobRun).order_by(JobRun.start_time.desc()).limit(limit).all()
        )

        # Convert to dictionaries
        results = [
            {
                "id": job.id,
                "job_type": job.job_type,
                "start_time": job.start_time,
                "end_time": job.end_time,
                "duration_seconds": job.duration_seconds,
                "found_count": job.found_count,
                "new_count": job.new_count,
                "skipped_count": job.skipped_count,
                "error_count": job.error_count,
            }
            for job in job_runs
        ]

        return results
    finally:
        db.close()


def get_pending_sellers():
    """
    Get all sellers with PENDING approval status.

    Returns:
        List of pending seller records as dictionaries
    """
    db = get_db_session()

    try:
        sellers = (
            db.query(Seller)
            .filter(Seller.approval_status == "PENDING")
            .order_by(Seller.first_seen_at.desc())
            .all()
        )

        # Convert to dictionaries
        results = [
            {
                "shop_id": seller.shop_id,
                "shop_url": seller.shop_url,
                "shop_name": seller.shop_name,
                "approval_status": seller.approval_status,
                "note": seller.note,
            }
            for seller in sellers
        ]

        return results
    finally:
        db.close()


def update_seller_approval(shop_id, approval_status, note=None):
    """
    Update a seller's approval status.

    Args:
        shop_id: ID of the seller to update
        approval_status: New approval status (PENDING, WHITELIST, BLACKLIST)
        note: Optional note about the approval decision

    Returns:
        Boolean indicating if the update was successful
    """
    db = get_db_session()

    try:
        # Check if seller exists
        seller = db.query(Seller).filter(Seller.shop_id == shop_id).first()

        if not seller:
            return False

        # Update approval status
        seller.approval_status = approval_status

        # Update note if provided
        if note is not None:
            seller.note = note

        db.commit()
        return True
    finally:
        db.close()


def upsert_product(
    product_id,
    shop_id,
    product_title=None,
    product_detail_url=None,
    product_main_image_url=None,
    original_price=None,
    target_sale_price=None,
    original_price_currency=None,
    target_sale_price_currency=None,
    discount=None,
    evaluate_rate=None,
    category_id=None,
    raw_json_search=None,  # For text search API response data
    raw_json_detail=None,  # For aliexpress.ds.product.get API response data
    raw_json=None,         # Backward compatibility - will be stored as raw_json_search
):
    """
    Insert or update a product record.

    Args:
        product_id: Unique product ID
        shop_id: ID of the shop/seller
        product_title: Title of the product
        product_detail_url: URL to the product detail page
        product_main_image_url: URL to the main product image
        original_price: Original price of the product
        target_sale_price: Sale price of the product
        original_price_currency: Currency of the original price
        target_sale_price_currency: Currency of the sale price
        discount: Discount information
        evaluate_rate: Evaluation rate (e.g. 97.5%)
        category_id: Comma-separated category IDs (e.g. "15,3710,100001404,200042147")
        raw_json_search: JSON data from text search API (individual product from search results)
        raw_json_detail: JSON data from aliexpress.ds.product.get API
        raw_json: Backward compatibility - will be stored as raw_json_search

    Returns:
        Boolean indicating if this was a new record (True) or an update (False)
    """
    db = get_db_session()
    now = get_utc_now()
    is_new = False

    # Handle backward compatibility for raw_json parameter
    if raw_json and not raw_json_search:
        raw_json_search = raw_json

    try:
        # Check if product exists
        product = db.query(Product).filter(Product.product_id == product_id).first()

        if product:
            # Update existing product
            product.last_seen_at = now
            if product_title:
                product.product_title = product_title
            if product_detail_url:
                product.product_detail_url = product_detail_url
            if product_main_image_url:
                product.product_main_image_url = product_main_image_url
            if original_price is not None:
                product.original_price = original_price
            if target_sale_price is not None:
                product.target_sale_price = target_sale_price
            if original_price_currency:
                product.original_price_currency = original_price_currency
            if target_sale_price_currency:
                product.target_sale_price_currency = target_sale_price_currency
            if discount:
                product.discount = discount
            # Always update evaluate_rate, including None values to set it to NULL
            product.evaluate_rate = evaluate_rate
            if category_id:
                product.category_id = category_id
            if raw_json_search:
                product.raw_json_search = raw_json_search
            if raw_json_detail:
                product.raw_json_detail = raw_json_detail
        else:
            # Insert new product
            product = Product(
                product_id=product_id,
                shop_id=shop_id,
                product_title=product_title,
                product_detail_url=product_detail_url,
                product_main_image_url=product_main_image_url,
                original_price=original_price,
                target_sale_price=target_sale_price,
                original_price_currency=original_price_currency,
                target_sale_price_currency=target_sale_price_currency,
                discount=discount,
                evaluate_rate=evaluate_rate,
                category_id=category_id,
                first_seen_at=now,
                last_seen_at=now,
                raw_json_search=raw_json_search,
                raw_json_detail=raw_json_detail,
            )
            db.add(product)
            is_new = True

        db.commit()
        return is_new
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def create_session_code(code, api_response, token_type='original'):
    """
    Create or update a session code with the full API response.
    
    Args:
        code: The session code
        api_response: Full response from the AliExpress API
        token_type: 'original' or 'refreshed'
    
    Returns:
        SessionCode: The created or updated session object
    """
    db = get_db_session()
    try:
        # Check if session already exists
        existing_session = db.query(SessionCode).filter_by(code=code).first()
        
        if existing_session:
            # Update existing session
            existing_session.access_token = api_response.get('access_token')
            existing_session.refresh_token = api_response.get('refresh_token')
            existing_session.expire_time = api_response.get('expire_time')
            existing_session.refresh_token_valid_time = api_response.get('refresh_token_valid_time')
            existing_session.expires_in = api_response.get('expires_in')
            existing_session.refresh_expires_in = api_response.get('refresh_expires_in')
            existing_session.havana_id = api_response.get('havana_id')
            existing_session.locale = api_response.get('locale')
            existing_session.user_nick = api_response.get('user_nick')
            existing_session.account_id = api_response.get('account_id')
            existing_session.user_id = api_response.get('user_id')
            existing_session.account_platform = api_response.get('account_platform')
            existing_session.sp = api_response.get('sp')
            existing_session.request_id = api_response.get('request_id')
            existing_session.seller_id = api_response.get('seller_id')
            existing_session.account = api_response.get('account')
            existing_session.token_type = token_type
            existing_session.is_active = True
            existing_session.response_json = api_response
            existing_session.updated_at = func.now()
            session_obj = existing_session
        else:
            # Create new session
            session_obj = SessionCode(
                code=code,
                access_token=api_response.get('access_token'),
                refresh_token=api_response.get('refresh_token'),
                expire_time=api_response.get('expire_time'),
                refresh_token_valid_time=api_response.get('refresh_token_valid_time'),
                expires_in=api_response.get('expires_in'),
                refresh_expires_in=api_response.get('refresh_expires_in'),
                havana_id=api_response.get('havana_id'),
                locale=api_response.get('locale'),
                user_nick=api_response.get('user_nick'),
                account_id=api_response.get('account_id'),
                user_id=api_response.get('user_id'),
                account_platform=api_response.get('account_platform'),
                sp=api_response.get('sp'),
                request_id=api_response.get('request_id'),
                seller_id=api_response.get('seller_id'),
                account=api_response.get('account'),
                token_type=token_type,
                is_active=True,
                response_json=api_response
            )
            db.add(session_obj)
        
        db.commit()
        db.refresh(session_obj)
        return session_obj
        
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def get_active_session_by_code(code):
    """
    Get the active session for a given code.
    
    Args:
        code: The session code
    
    Returns:
        SessionCode: The active session object or None
    """
    db = get_db_session()
    try:
        session = db.query(SessionCode).filter_by(code=code, is_active=True).first()
        return session
    finally:
        db.close()


def deactivate_session(code):
    """
    Mark a session as inactive.
    
    Args:
        code: The session code to deactivate
    
    Returns:
        bool: True if session was found and deactivated, False otherwise
    """
    db = get_db_session()
    try:
        session = db.query(SessionCode).filter_by(code=code).first()
        if session:
            session.is_active = False
            session.updated_at = func.now()
            db.commit()
            return True
        return False
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
