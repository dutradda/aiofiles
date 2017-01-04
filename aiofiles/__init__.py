"""Utilities for asyncio-friendly file handling."""
from .threadpool import open, gzip_open, zip_open

__version__ = '0.4.0'

__all__ = (open, gzip_open, zip_open)
