"""Utility functions and helpers."""

from .normalization import normalize_customer_name
from .uuid import generate_uuid

__all__ = [
    'normalize_customer_name',
    'generate_uuid'
]
