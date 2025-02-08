"""Integration tests for company processing phase."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd

from ..processors.company import CompanyProcessor
from ..db.models import Company, Base

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

def test_company_creation_basic(session_manager):
    """Test basic company creation from domain."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Customer': 'Example Corp',
        'Main Email': 'contact@example.com'
    }])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['companies_created'] == 1
    assert stats['total_errors'] == 0
    assert stats['domains_extracted'] == 1
    assert stats['rows_with_domain'] == 1

def test_company_domain_normalization(session_manager):
    """Test company domain normalization."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Create test data with various email formats
    data = pd.DataFrame([
        {'Customer': 'Test 1', 'Main Email': 'test@Example.Com'},
        {'Customer': 'Test 2', 'Main Email': 'test@sub.EXAMPLE.com'},
        {'Customer': 'Test 3', 'Main Email': 'test@www.example.com'}
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['companies_created'] == 1  # All normalize to same domain
    assert stats['total_errors'] == 0
    assert stats['domains_extracted'] == 1
    assert stats['rows_with_domain'] == 3

def test_company_idempotent_processing(session_manager):
    """Test idempotent company processing."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Customer': 'Example Corp',
        'Main Email': 'contact@example.com'
    }])
    
    # Process data twice
    result1 = processor.process(data)
    stats1 = processor.get_stats()
    
    # Reset processor for second run
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    result2 = processor.process(data)
    stats2 = processor.get_stats()
    
    # Verify results
    assert stats1['companies_created'] == 1
    assert stats2['companies_created'] == 0  # No new companies created
    assert stats1['total_errors'] == 0
    assert stats2['total_errors'] == 0

def test_company_batch_processing(session_manager):
    """Test company batch processing."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=2,  # Small batch size for testing
        error_limit=10
    )
    
    # Create test data with multiple records
    data = pd.DataFrame([
        {'Customer': 'Company 1', 'Main Email': 'contact@company1.com'},
        {'Customer': 'Company 2', 'Main Email': 'contact@company2.com'},
        {'Customer': 'Company 3', 'Main Email': 'contact@company3.com'},
        {'Customer': 'Company 4', 'Main Email': 'contact@company4.com'}
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['companies_created'] == 4
    assert stats['successful_batches'] == 2  # Should be processed in 2 batches
    assert stats['total_errors'] == 0
    assert stats['domains_extracted'] == 4
    assert stats['rows_with_domain'] == 4

def test_company_error_handling(session_manager):
    """Test company processing error handling."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=2  # Low error limit for testing
    )
    
    # Create test data with missing customer names
    data = pd.DataFrame([
        {'Customer': None, 'Main Email': 'test@example.com'},
        {'Customer': '', 'Main Email': 'test@example.com'},
        {'Customer': 'Valid Company', 'Main Email': 'test@valid.com'}
    ])
    
    # Process data
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['total_errors'] > 0  # Should have validation errors
    assert stats['companies_created'] >= 1  # Valid company should be created
    assert stats['failed_batches'] > 0

def test_validate_data(session_manager):
    """Test data validation."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Test missing required columns
    data1 = pd.DataFrame([{'Email': 'test@example.com'}])
    critical1, warnings1 = processor.validate_data(data1)
    assert len(critical1) == 1  # Missing Customer column
    
    # Test empty customer names
    data2 = pd.DataFrame([
        {'Customer': None, 'Main Email': 'test@example.com'},
        {'Customer': 'Valid', 'Main Email': 'test@example.com'}
    ])
    critical2, warnings2 = processor.validate_data(data2)
    assert len(critical2) == 0
    assert len(warnings2) == 1  # Warning about empty customer
    
    # Test missing email fields
    data3 = pd.DataFrame([{'Customer': 'Test Company'}])
    critical3, warnings3 = processor.validate_data(data3)
    assert len(critical3) == 0
    assert len(warnings3) == 1  # Warning about no email fields

def test_extract_email_domain(session_manager):
    """Test domain extraction from various fields."""
    processor = CompanyProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Test various email locations
    test_cases = [
        {
            'Customer': 'Test Company',
            'Main Email': 'test@example.com',
            'expected_domain': 'example.com'
        },
        {
            'Customer': 'Test Company',
            'Billing Address Email': 'billing@company.com',
            'expected_domain': 'company.com'
        },
        {
            'Customer': 'Test Company',
            'Notes': 'Contact at: support@business.com',
            'expected_domain': 'business.com'
        },
        {
            'Customer': 'acme.com',  # Domain-like company name
            'expected_domain': 'acme.com'
        }
    ]
    
    for case in test_cases:
        expected = case.pop('expected_domain')
        row = pd.Series(case)
        domain = processor.extract_email_domain(row)
        assert domain == expected, f"Failed to extract domain from {case}"
