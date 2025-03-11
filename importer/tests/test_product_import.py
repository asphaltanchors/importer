"""Integration tests for product import processor."""

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ..processors.product_import import ProductImportProcessor
from ..db.models import Product, ProductPriceHistory

def test_validate_data(session_manager):
    """Test product import data validation."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10
    )
    
    # Test missing required columns
    data1 = pd.DataFrame([{'Description': 'Test'}])
    critical1, warnings1 = processor.validate_data(data1)
    assert len(critical1) == 1  # Missing Item Name column
    
    # Test empty product codes
    data2 = pd.DataFrame([
        {'Item Name': None, 'Purchase Cost': '10.00', 'Sales Price': '20.00'},
        {'Item Name': 'PROD001', 'Purchase Cost': '10.00', 'Sales Price': '20.00'}
    ])
    critical2, warnings2 = processor.validate_data(data2)
    assert len(critical2) == 0
    assert len(warnings2) == 1  # Warning about empty product code
    
    # Test non-numeric cost and price
    data3 = pd.DataFrame([{
        'Item Name': 'PROD001',
        'Purchase Cost': 'invalid',
        'Sales Price': '20.00'
    }])
    critical3, warnings3 = processor.validate_data(data3)
    assert len(critical3) == 0
    assert len(warnings3) == 1  # Warning about non-numeric cost

def test_product_creation_with_prices(session_manager):
    """Test product creation with cost and list price."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Item Name': 'PROD001',
        'Purchase Cost': '10.50',
        'Sales Price': '25.99',
        'Sales Description': 'Test Product'
    }])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] == 1
    assert stats['total_errors'] == 0
    assert stats['total_products'] == 1
    
    # Verify product was created with correct prices
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        product = session.query(Product).filter_by(productCode='PROD001').first()
        assert product is not None
        assert float(product.cost) == 10.50
        assert float(product.listPrice) == 25.99
        assert product.description == 'Test Product'

def test_price_history_tracking(session_manager):
    """Test price history tracking."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        track_price_history=True
    )
    
    # Create initial product
    data1 = pd.DataFrame([{
        'Item Name': 'PROD002',
        'Purchase Cost': '15.00',
        'Sales Price': '30.00',
        'Sales Description': 'Test Product'
    }])
    result1 = processor.process(data1)
    stats1 = processor.get_stats()
    
    # Verify initial history entry
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        product = session.query(Product).filter_by(productCode='PROD002').first()
        history = session.query(ProductPriceHistory).filter_by(productId=product.id).all()
        assert len(history) == 1
        assert float(history[0].cost) == 15.00
        assert float(history[0].listPrice) == 30.00
        assert history[0].notes == 'Initial import'
    
    # Update product with new prices
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        track_price_history=True
    )
    
    data2 = pd.DataFrame([{
        'Item Name': 'PROD002',
        'Purchase Cost': '18.00',
        'Sales Price': '35.00',
        'Sales Description': 'Test Product'
    }])
    result2 = processor.process(data2)
    stats2 = processor.get_stats()
    
    # Verify updated history entries
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        product = session.query(Product).filter_by(productCode='PROD002').first()
        history = session.query(ProductPriceHistory).filter_by(productId=product.id).order_by(
            ProductPriceHistory.effectiveDate
        ).all()
        assert len(history) == 2
        assert float(history[1].cost) == 18.00
        assert float(history[1].listPrice) == 35.00
        assert history[1].notes == 'Updated via product import'
        
        # Verify product has updated prices
        assert float(product.cost) == 18.00
        assert float(product.listPrice) == 35.00

def test_disable_price_history(session_manager):
    """Test disabling price history tracking."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        track_price_history=False
    )
    
    # Create product
    data = pd.DataFrame([{
        'Item Name': 'PROD003',
        'Purchase Cost': '12.00',
        'Sales Price': '24.00',
        'Sales Description': 'Test Product'
    }])
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify no history entries
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        product = session.query(Product).filter_by(productCode='PROD003').first()
        history = session.query(ProductPriceHistory).filter_by(productId=product.id).all()
        assert len(history) == 0
        
        # Verify product has correct prices
        assert float(product.cost) == 12.00
        assert float(product.listPrice) == 24.00

def test_partial_price_update(session_manager):
    """Test updating only one price field."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10
    )
    
    # Create initial product
    data1 = pd.DataFrame([{
        'Item Name': 'PROD004',
        'Purchase Cost': '10.00',
        'Sales Price': '20.00',
        'Sales Description': 'Test Product'
    }])
    result1 = processor.process(data1)
    
    # Update only cost
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10
    )
    
    data2 = pd.DataFrame([{
        'Item Name': 'PROD004',
        'Purchase Cost': '12.00',
        'Sales Description': 'Test Product'
    }])
    result2 = processor.process(data2)
    stats2 = processor.get_stats()
    
    # Verify price update
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        product = session.query(Product).filter_by(productCode='PROD004').first()
        assert float(product.cost) == 12.00
        assert float(product.listPrice) == 20.00  # Unchanged
        
    assert stats2['price_updated'] == 1

def test_batch_processing(session_manager):
    """Test batch processing of products."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=2,  # Small batch size for testing
        error_limit=10
    )
    
    # Create test data with multiple records
    data = pd.DataFrame([
        {'Item Name': 'BATCH1', 'Purchase Cost': '10.00', 'Sales Price': '20.00'},
        {'Item Name': 'BATCH2', 'Purchase Cost': '11.00', 'Sales Price': '22.00'},
        {'Item Name': 'BATCH3', 'Purchase Cost': '12.00', 'Sales Price': '24.00'},
        {'Item Name': 'BATCH4', 'Purchase Cost': '13.00', 'Sales Price': '26.00'}
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] == 4
    assert stats['successful_batches'] == 2  # Should be processed in 2 batches
    assert stats['total_errors'] == 0
    
    # Verify all products were created
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        products = session.query(Product).filter(
            Product.productCode.in_(['BATCH1', 'BATCH2', 'BATCH3', 'BATCH4'])
        ).all()
        assert len(products) == 4

def test_error_handling(session_manager):
    """Test error handling for invalid data."""
    processor = ProductImportProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10
    )
    
    # Create test data with invalid values
    data = pd.DataFrame([
        {'Item Name': 'ERROR1', 'Purchase Cost': 'invalid', 'Sales Price': '20.00'},
        {'Item Name': 'ERROR2', 'Purchase Cost': '10.00', 'Sales Price': 'invalid'},
        {'Item Name': 'ERROR3', 'Purchase Cost': '12.00', 'Sales Price': '24.00'}  # Valid
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['created'] >= 1  # At least the valid product should be created
    assert stats['validation_errors'] > 0  # Should have validation errors
    
    # Verify valid product was created
    with Session(create_engine(os.getenv('TEST_DATABASE_URL'))) as session:
        product = session.query(Product).filter_by(productCode='ERROR3').first()
        assert product is not None
        assert float(product.cost) == 12.00
        assert float(product.listPrice) == 24.00
