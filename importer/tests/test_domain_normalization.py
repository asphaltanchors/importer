"""Tests for domain normalization utilities."""
import pytest
from ..utils.normalization import normalize_domain

def test_normalize_domain():
    """Test domain normalization for various cases."""
    # Regular domains
    assert normalize_domain('example.com') == 'example.com'
    assert normalize_domain('test.org') == 'test.org'
    
    # Subdomains
    assert normalize_domain('foo.bar.com') == 'bar.com'
    assert normalize_domain('app.staging.company.com') == 'company.com'
    assert normalize_domain('a.b.c.domain.com') == 'domain.com'
    
    # Country-specific TLDs
    assert normalize_domain('example.co.uk') == 'example.co.uk'
    assert normalize_domain('foo.example.co.uk') == 'example.co.uk'
    assert normalize_domain('app.site.example.com.au') == 'example.com.au'
    
    # Invalid domains
    assert normalize_domain('') is None
    assert normalize_domain('not-a-domain') is None
    assert normalize_domain('foo@bar.com') is None
    assert normalize_domain('http://invalid') is None
    
    # Edge cases
    assert normalize_domain('  example.com  ') == 'example.com'  # Extra whitespace
    assert normalize_domain('EXAMPLE.COM') == 'example.com'  # Case insensitive
    assert normalize_domain('example.com.') == 'example.com'  # Trailing dot
