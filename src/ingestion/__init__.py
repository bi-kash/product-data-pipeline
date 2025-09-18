"""
Ingestion package for processing and storing various types of product data.

This package contains modules for:
- Image ingestion: Extracting and storing product images
- Future modules for other data ingestion tasks
"""

from .image_ingestion import ImageIngestionEngine

__all__ = ['ImageIngestionEngine']