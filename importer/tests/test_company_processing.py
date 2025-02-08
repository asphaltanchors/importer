"""Integration tests for company processing phase."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pandas as pd

from ..processors.company import CompanyProcessor
from ..db.models import Company, Base

@pytest.fixture
def session():
    """Create a test database session."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        yield session

def test_company_creation_basic():
    """Test basic company creation from domain."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = CompanyProcessor(config)
    
    # Create test data
    data = pd.DataFrame([{
        'company_domain': 'example.com',
        'Customer Name': 'Example Corp'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert result['success']
    assert result['stats']['companies_created'] == 1
    assert result['stats']['errors'] == 0

def test_company_required_domains():
    """Test creation of required company domains."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = CompanyProcessor(config)
    
    # Process empty data to trigger required company creation
    data = pd.DataFrame([])
    result = processor.process(data)
    
    # Verify required companies were created
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        companies = session.query(Company).all()
        domains = [c.domain for c in companies]
        assert 'amazon-fba.com' in domains
        assert 'unknown-domain.com' in domains

def test_company_domain_normalization():
    """Test company domain normalization."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = CompanyProcessor(config)
    
    # Create test data with various domain formats
    data = pd.DataFrame([
        {'company_domain': 'Example.Com', 'Customer Name': 'Test 1'},
        {'company_domain': 'sub.EXAMPLE.com', 'Customer Name': 'Test 2'},
        {'company_domain': 'www.example.com', 'Customer Name': 'Test 3'}
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert result['success']
    assert result['stats']['companies_created'] == 1  # All normalize to same domain
    assert result['stats']['errors'] == 0

def test_company_idempotent_processing():
    """Test idempotent company processing."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    processor = CompanyProcessor(config)
    
    # Create test data
    data = pd.DataFrame([{
        'company_domain': 'example.com',
        'Customer Name': 'Example Corp'
    }])
    
    # Process data twice
    result1 = processor.process(data)
    result2 = processor.process(data)
    
    # Verify results
    assert result1['success'] and result2['success']
    assert result1['stats']['companies_created'] == 1
    assert result2['stats']['companies_created'] == 0  # No new companies created
    assert result2['stats'].get('companies_updated', 0) == 0  # No updates needed

def test_company_batch_processing():
    """Test company batch processing."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 2,  # Small batch size for testing
        'error_limit': 10
    }
    processor = CompanyProcessor(config)
    
    # Create test data with multiple records
    data = pd.DataFrame([
        {'company_domain': 'company1.com', 'Customer Name': 'Company 1'},
        {'company_domain': 'company2.com', 'Customer Name': 'Company 2'},
        {'company_domain': 'company3.com', 'Customer Name': 'Company 3'},
        {'company_domain': 'company4.com', 'Customer Name': 'Company 4'}
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert result['success']
    assert result['stats']['companies_created'] == 4
    assert result['stats']['total_batches'] == 2  # Should be processed in 2 batches

def test_company_error_handling():
    """Test company processing error handling."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 2  # Low error limit for testing
    }
    processor = CompanyProcessor(config)
    
    # Create test data with invalid domains
    data = pd.DataFrame([
        {'company_domain': '', 'Customer Name': 'Invalid 1'},
        {'company_domain': None, 'Customer Name': 'Invalid 2'},
        {'company_domain': 'valid.com', 'Customer Name': 'Valid Company'}
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify results
    assert not result['success']  # Should fail due to error limit
    assert result['stats']['companies_created'] == 1  # Valid company still created
    assert result['stats']['errors'] == 2  # Two invalid domain errors
    assert len(result['error_details']) == 2  # Error details recorded
