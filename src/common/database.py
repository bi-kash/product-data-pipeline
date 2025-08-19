"""
Database models and helper functions using SQLAlchemy ORM.
"""

import os
import json
import sqlite3
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


class ProductCategory(Base):
    """Model representing the relationship between products and search categories."""

    __tablename__ = "product_categories"

    id = Column(Integer, primary_key=True)
    product_id = Column(String(255), ForeignKey("products.product_id"), nullable=False)
    category_id = Column(Integer, nullable=False)
    category_name = Column(String(255), nullable=True)
    search_page = Column(Integer, nullable=False)
    position_in_results = Column(Integer, nullable=True)
    search_timestamp = Column(DateTime(timezone=True), nullable=False)

    # Define the relationship to products
    product = relationship("Product", back_populates="categories")

    def __repr__(self):
        return f"<ProductCategory(product_id='{self.product_id}', category_id='{self.category_id}')>"


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
    first_level_category_name = Column(String(255), nullable=True)
    second_level_category_name = Column(String(255), nullable=True)
    status = Column(String(20), nullable=False, default="PENDING")
    first_seen_at = Column(DateTime(timezone=True), nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=False)
    raw_json = Column(JSON)

    # Define the relationship to sellers and categories
    seller = relationship("Seller", back_populates="products")
    categories = relationship(
        "ProductCategory", back_populates="product", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Product(product_id='{self.product_id}', title='{self.product_title[:30]}...', status='{self.status}')>"


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
    keywords = Column(Text, nullable=True)  # Comma-separated or JSON string of keywords used
    categories = Column(Text, nullable=True)  # Comma-separated or JSON string of categories used
    details = Column(JSON)

    def __repr__(self):
        return f"<JobRun(id={self.id}, job_type='{self.job_type}')>"


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
        keywords_str = ",".join(keywords) if isinstance(keywords, list) else str(keywords)
    if categories:
        categories_str = ",".join([str(c) for c in categories]) if isinstance(categories, list) else str(categories)

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


def update_job_run_progress(job_id, found=0, new=0, skipped=0, errors=0, details=None, keywords=None, categories=None):
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


def complete_job_run(job_id, found=0, new=0, skipped=0, errors=0, details=None, keywords=None, categories=None):
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


def get_product_categories_stats():
    """
    Get statistics about product-category associations.

    Returns:
        Dictionary with statistics
    """
    db = get_db_session()

    try:
        # Get total count
        total_count = db.query(func.count(ProductCategory.id)).scalar() or 0

        # Get top categories by product count
        top_categories = (
            db.query(
                ProductCategory.category_id,
                ProductCategory.category_name,
                func.count(ProductCategory.product_id).label("product_count"),
            )
            .group_by(ProductCategory.category_id)
            .order_by(func.count(ProductCategory.product_id).desc())
            .limit(10)
            .all()
        )

        # Format results
        return {
            "total_associations": total_count,
            "top_categories": [
                {
                    "category_id": cat_id,
                    "category_name": cat_name or f"Category {cat_id}",
                    "product_count": count,
                }
                for cat_id, cat_name, count in top_categories
            ],
        }
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


def associate_product_category(
    product_id, category_id, category_name=None, search_page=1, position_in_results=None
):
    """
    Associate a product with a search category.

    Args:
        product_id: ID of the product
        category_id: Category ID that produced this product
        category_name: Category name (optional)
        search_page: Page number in search results
        position_in_results: Position in search results (optional)

    Returns:
        ID of the created association
    """
    db = get_db_session()
    now = get_utc_now()

    try:
        # Create the association
        association = ProductCategory(
            product_id=product_id,
            category_id=category_id,
            category_name=category_name,
            search_page=search_page,
            position_in_results=position_in_results,
            search_timestamp=now,
        )
        db.add(association)
        db.commit()
        db.refresh(association)
        return association.id
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
    first_level_category_name=None,
    second_level_category_name=None,
    raw_json=None,
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
        first_level_category_name: First level category
        second_level_category_name: Second level category
        raw_json: Original JSON data from API

    Returns:
        Boolean indicating if this was a new record (True) or an update (False)
    """
    db = get_db_session()
    now = get_utc_now()
    is_new = False

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
            if evaluate_rate:
                product.evaluate_rate = evaluate_rate
            if first_level_category_name:
                product.first_level_category_name = first_level_category_name
            if second_level_category_name:
                product.second_level_category_name = second_level_category_name
            if raw_json:
                product.raw_json = raw_json
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
                first_level_category_name=first_level_category_name,
                second_level_category_name=second_level_category_name,
                status="PENDING",
                first_seen_at=now,
                last_seen_at=now,
                raw_json=raw_json,
            )
            db.add(product)
            is_new = True

        db.commit()
        return is_new
    finally:
        db.close()
