"""Integration tests for product processing phase."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pandas as pd

from ..processors.product import ProductProcessor
from ..db.models import Product, Base

@pytest.fixture
def session():
    """Create a test database session."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        yield session

def test_product_creation_basic():
    """Test basic product creation."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Create test data
    data = pd.DataFrame([{
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Test Product',
        'Product/Service  Amount': '100.00'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert result['success']
    assert result['stats']['products_created'] == 1
    assert result['stats']['errors'] == 0

def test_product_system_products():
    """Test creation of system-defined products."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Process empty data to trigger system product creation
    data = pd.DataFrame([])
    result = processor.process(data)
    
    # Verify system products were created
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        products = session.query(Product).all()
        codes = [p.productCode for p in products]
        # Check for required system products
        assert 'SHIPPING' in codes
        assert 'TAX' in codes
        assert 'DISCOUNT' in codes

def test_product_code_mapping():
    """Test product code mapping and normalization."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Create test data with various product code formats
    data = pd.DataFrame([
        {
            'Product/Service': 'TEST-001',
            'Product/Service Description': 'Test Product 1',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'TEST_001',  # Different separator
            'Product/Service Description': 'Test Product 1',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'test001',  # Different case
            'Product/Service Description': 'Test Product 1',
            'Product/Service  Amount': '100.00'
        }
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert result['success']
    assert result['stats']['products_created'] == 1  # All map to same product
    assert result['stats']['errors'] == 0

def test_product_idempotent_processing():
    """Test idempotent product processing."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Create test data
    data = pd.DataFrame([{
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Test Product',
        'Product/Service  Amount': '100.00'
    }])
    
    # Process data twice
    result1 = processor.process(data)
    result2 = processor.process(data)
    
    # Verify results
    assert result1['success'] and result2['success']
    assert result1['stats']['products_created'] == 1
    assert result2['stats']['products_created'] == 0  # No new products created
    assert result2['stats'].get('products_updated', 0) == 0  # No updates needed

def test_product_description_update():
    """Test updating product description."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Create initial product
    data1 = pd.DataFrame([{
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Initial Description',
        'Product/Service  Amount': '100.00'
    }])
    result1 = processor.process(data1)
    
    # Update product with new description
    data2 = pd.DataFrame([{
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Updated Description',
        'Product/Service  Amount': '100.00'
    }])
    result2 = processor.process(data2)
    
    # Verify results
    assert result1['success'] and result2['success']
    assert result2['stats'].get('products_updated', 0) == 1
    
    # Verify description was updated
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        product = session.query(Product).filter_by(productCode='TEST001').first()
        assert product.description == 'Updated Description'

def test_product_batch_processing():
    """Test product batch processing."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 2,  # Small batch size for testing
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Create test data with multiple records
    data = pd.DataFrame([
        {'Product/Service': 'PROD1', 'Product/Service Description': 'Product 1', 'Product/Service  Amount': '100.00'},
        {'Product/Service': 'PROD2', 'Product/Service Description': 'Product 2', 'Product/Service  Amount': '200.00'},
        {'Product/Service': 'PROD3', 'Product/Service Description': 'Product 3', 'Product/Service  Amount': '300.00'},
        {'Product/Service': 'PROD4', 'Product/Service Description': 'Product 4', 'Product/Service  Amount': '400.00'}
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert result['success']
    assert result['stats']['products_created'] == 4
    assert result['stats']['total_batches'] == 2  # Should be processed in 2 batches

def test_product_validation():
    """Test product validation rules."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = ProductProcessor(config)
    
    # Create test data with validation issues
    data = pd.DataFrame([
        {
            'Product/Service': '',  # Empty code
            'Product/Service Description': 'Invalid Product 1',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'TEST',  # Invalid code format
            'Product/Service Description': 'Invalid Product 2',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'TEST001',  # Valid product
            'Product/Service Description': 'Valid Product',
            'Product/Service  Amount': '100.00'
        }
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert not result['success']  # Should fail validation
    assert result['stats']['products_created'] == 1  # Valid product still created
    assert result['stats']['errors'] == 2  # Two validation errors
    assert len(result['error_details']) == 2  # Error details recorded

def test_product_error_tracking():
    """Test error tracking with ErrorTracker integration."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 2  # Low error limit for testing
    }
    processor = ProductProcessor(config)
    
    # Create test data with multiple issues
    data = pd.DataFrame([
        {
            'Product/Service': 'TEST001',
            'Product/Service Description': None,  # Missing description
            'Product/Service  Amount': 'invalid'  # Invalid amount
        },
        {
            'Product/Service': '',  # Empty code
            'Product/Service Description': 'Invalid Product',
            'Product/Service  Amount': '100.00'
        }
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify error tracking
    assert not result['success']
    assert result['stats']['errors'] == 2
    assert len(result['error_details']) == 2
    
    # Verify error details
    error_messages = [e['message'] for e in result['error_details']]
    assert any('description' in msg.lower() for msg in error_messages)
    assert any('amount' in msg.lower() for msg in error_messages)
