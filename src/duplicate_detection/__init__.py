"""
Module C: Duplicate Detection & Selection

This module implements intelligent visual duplicate detection using a two-stage approach:
1. Fast pHash comparison for initial filtering
2. Semantic CLIP analysis for refined duplicate detection
3. Master product selection based on total_landed_cost

The module is designed to be independent and only depends on the common folder.
"""

__version__ = "1.0.0"

from .duplicate_detector import DuplicateDetector

__all__ = [
    'DuplicateDetector'
]