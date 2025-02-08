"""Integration tests for product processing phase."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd

from ..processors.product import ProductProcessor
from ..db.models import Product, Base

@pytest.fixture
def engine():
    """Create a test database engine."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture
def session_manager(engine):
    """Create a session manager for testing."""
    return sessionmaker(bind=engine)

@pytest.fixture
def session(engine):
    """Create a test database session."""
    with Session(engine) as session:
        yield session

def test_validate_data(session_manager):
    """Test product data validation."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Test missing required columns
    data1 = pd.DataFrame([{'Description': 'Test'}])
    critical1, warnings1 = processor.validate_data(data1)
    assert len(critical1) == 1  # Missing Product/Service column
    
    # Test empty product codes
    data2 = pd.DataFrame([
        {'Product/Service': None, 'Product/Service Description': 'Test'},
        {'Product/Service': 'PROD001', 'Product/Service Description': 'Test'}
    ])
    critical2, warnings2 = processor.validate_data(data2)
    assert len(critical2) == 0
    assert len(warnings2) == 1  # Warning about empty product code
    
    # Test test products
    data3 = pd.DataFrame([{
        'Product/Service': 'TEST-001',
        'Product/Service Description': 'Test'
    }])
    critical3, warnings3 = processor.validate_data(data3)
    assert len(critical3) == 0
    assert len(warnings3) == 1  # Warning about test product
    
    # Test deprecated products
    data4 = pd.DataFrame([{
        'Product/Service': 'PROD001',
        'Product/Service Description': 'DEPRECATED: Old Product'
    }])
    critical4, warnings4 = processor.validate_data(data4)
    assert len(critical4) == 0
    assert len(warnings4) == 1  # Warning about deprecated product

def test_product_creation_basic(session_manager):
    """Test basic product creation."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Product/Service': 'PROD001',
        'Product/Service Description': 'Test Product',
        'Product/Service  Amount': '100.00'
    }])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] == 1
    assert stats['total_errors'] == 0
    assert stats['total_products'] == 1

def test_product_system_products(session_manager):
    """Test creation of system-defined products."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Process empty data to trigger system product creation
    data = pd.DataFrame([])
    result = processor.process(data)
    
    # Verify system products were created
    with Session(create_engine('sqlite:///:memory:')) as session:
        products = session.query(Product).all()
        codes = [p.productCode for p in products]
        # Check for required system products
        assert 'SHIPPING' in codes
        assert 'TAX' in codes
        assert 'DISCOUNT' in codes

def test_product_code_mapping(session_manager):
    """Test product code mapping and normalization."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Create test data with various formats
    data = pd.DataFrame([
        {
            'Product/Service': 'PROD-001',
            'Product/Service Description': 'Test Product 1',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'PROD_001',  # Different separator
            'Product/Service Description': 'Test Product 1',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'prod001',  # Different case
            'Product/Service Description': 'Test Product 1',
            'Product/Service  Amount': '100.00'
        }
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] == 1  # All normalize to same code
    assert stats['total_errors'] == 0
    assert stats['skipped'] == 2  # Duplicates skipped

def test_product_idempotent_processing(session_manager):
    """Test idempotent product processing."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Product/Service': 'PROD001',
        'Product/Service Description': 'Test Product',
        'Product/Service  Amount': '100.00'
    }])
    
    # Process data twice
    result1 = processor.process(data)
    stats1 = processor.get_stats()
    
    # Reset processor for second run
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    result2 = processor.process(data)
    stats2 = processor.get_stats()
    
    # Verify results
    assert stats1['created'] == 1
    assert stats2['created'] == 0  # No new products created
    assert stats2['skipped'] > 0  # Product skipped on second run

def test_product_description_update(session_manager):
    """Test updating product description."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Create initial product
    data1 = pd.DataFrame([{
        'Product/Service': 'PROD001',
        'Product/Service Description': 'Initial Description',
        'Product/Service  Amount': '100.00'
    }])
    result1 = processor.process(data1)
    stats1 = processor.get_stats()
    
    # Update product with new description
    data2 = pd.DataFrame([{
        'Product/Service': 'PROD001',
        'Product/Service Description': 'Updated Description',
        'Product/Service  Amount': '100.00'
    }])
    result2 = processor.process(data2)
    stats2 = processor.get_stats()
    
    # Verify results
    assert stats1['created'] == 1
    assert stats2['updated'] == 1
    
    # Verify description was updated
    with Session(create_engine('sqlite:///:memory:')) as session:
        product = session.query(Product).filter_by(productCode='PROD001').first()
        assert product.description == 'Updated Description'

def test_product_batch_processing(session_manager):
    """Test product batch processing."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=2,  # Small batch size for testing
        error_limit=10
    )
    
    # Create test data with multiple records
    data = pd.DataFrame([
        {'Product/Service': 'PROD1', 'Product/Service Description': 'Product 1', 'Product/Service  Amount': '100.00'},
        {'Product/Service': 'PROD2', 'Product/Service Description': 'Product 2', 'Product/Service  Amount': '200.00'},
        {'Product/Service': 'PROD3', 'Product/Service Description': 'Product 3', 'Product/Service  Amount': '300.00'},
        {'Product/Service': 'PROD4', 'Product/Service Description': 'Product 4', 'Product/Service  Amount': '400.00'}
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] == 4
    assert stats['successful_batches'] == 2  # Should be processed in 2 batches
    assert stats['total_errors'] == 0

def test_product_error_handling(session_manager):
    """Test error handling and validation."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=2  # Low error limit for testing
    )
    
    # Create test data with validation issues
    data = pd.DataFrame([
        {
            'Product/Service': '',  # Empty code
            'Product/Service Description': 'Invalid Product 1',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'TEST-PROD',  # Test product not allowed
            'Product/Service Description': 'Invalid Product 2',
            'Product/Service  Amount': '100.00'
        },
        {
            'Product/Service': 'PROD001',  # Valid product
            'Product/Service Description': 'Valid Product',
            'Product/Service  Amount': '100.00'
        }
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] == 1  # Valid product created
    assert stats['validation_errors'] > 0  # Validation errors recorded
    assert stats['total_errors'] > 0

def test_debug_logging(session_manager):
    """Test debug logging functionality."""
    processor = ProductProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Product/Service': 'PROD001',
        'Product/Service Description': 'Test Product',
        'Product/Service  Amount': '100.00'
    }])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify timing stats are present
    assert 'processing_time' in stats
    assert 'db_operation_time' in stats
    assert stats['started_at'] is not None
    assert stats['completed_at'] is not None
