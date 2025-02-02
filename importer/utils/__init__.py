"""Utility functions and helpers."""

from .normalization import normalize_customer_name, normalize_domain
from .uuid import generate_uuid

__all__ = [
    'normalize_customer_name',
    'normalize_domain',
    'generate_uuid'
]
