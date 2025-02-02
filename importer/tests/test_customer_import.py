"""Integration tests for customer import with name normalization."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pandas as pd

from ..processors.customer import CustomerProcessor
from ..db.models import Customer, Company, Base

@pytest.fixture
def session():
    """Create a test database session."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        # Create test company
        company = Company(
            id='test-company',
            name='Test Company',
            domain='example.com'
        )
        session.add(company)
        session.commit()
        
        yield session

def test_customer_import_basic(session):
    """Test basic customer import with exact name match."""
    processor = CustomerProcessor(session)
    
    # Create test data
    data = pd.DataFrame([{
        'Customer Name': 'Acme Corp',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was created
    customer = session.query(Customer).first()
    assert customer is not None
    assert customer.customerName == 'Acme Corp'
    assert processor.stats['customers_created'] == 1
    assert processor.stats.get('normalized_matches', 0) == 0

def test_customer_import_normalized_name(session):
    """Test customer import with normalized name matching."""
    processor = CustomerProcessor(session)
    
    # Create existing customer
    existing = Customer.create(
        name='ACME CORP',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(existing)
    session.commit()
    
    # Create test data with different name format
    data = pd.DataFrame([{
        'Customer Name': 'Acme Corp LLC',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was updated, not created
    customers = session.query(Customer).all()
    assert len(customers) == 1
    assert customers[0].customerName == 'Acme Corp LLC'  # Name updated to new format
    assert processor.stats['customers_updated'] == 1
    assert processor.stats.get('normalized_matches', 0) == 1

def test_customer_import_comma_name(session):
    """Test customer import with comma-separated name."""
    processor = CustomerProcessor(session)
    
    # Create existing customer
    existing = Customer.create(
        name='John Smith',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(existing)
    session.commit()
    
    # Create test data with comma format
    data = pd.DataFrame([{
        'Customer Name': 'Smith, John',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was updated, not created
    customers = session.query(Customer).all()
    assert len(customers) == 1
    assert customers[0].customerName == 'Smith, John'  # Name updated to new format
    assert processor.stats['customers_updated'] == 1
    assert processor.stats.get('normalized_matches', 0) == 1

def test_customer_import_percentage_notation(session):
    """Test customer import with percentage notation."""
    processor = CustomerProcessor(session)
    
    # Create existing customer with percentage notation
    existing = Customer.create(
        name='White Cap 30%:Whitecap Edmonton Canada',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(existing)
    session.commit()
    
    # Create test data with same notation but different case
    data = pd.DataFrame([{
        'Customer Name': 'WHITE CAP 30%:WHITECAP EDMONTON CANADA',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was updated, not created
    customers = session.query(Customer).all()
    assert len(customers) == 1
    assert customers[0].customerName == 'WHITE CAP 30%:WHITECAP EDMONTON CANADA'
    assert processor.stats['customers_updated'] == 1
    assert processor.stats.get('normalized_matches', 0) == 1

def test_customer_import_business_suffixes(session):
    """Test customer import with various business suffixes."""
    processor = CustomerProcessor(session)
    
    # Create test data with different suffix variations
    data = pd.DataFrame([
        {
            'Customer Name': 'Test Company LLC',
            'QuickBooks Internal Id': '1',
            'company_domain': 'example.com'
        },
        {
            'Customer Name': 'Test Company Inc.',
            'QuickBooks Internal Id': '2',
            'company_domain': 'example.com'
        },
        {
            'Customer Name': 'Test Company Corp',
            'QuickBooks Internal Id': '3',
            'company_domain': 'example.com'
        }
    ])
    
    # Process data
    result = processor.process(data)
    
    # Verify all variations were normalized and created as one customer
    customers = session.query(Customer).all()
    assert len(customers) == 3  # Each has unique QuickBooks ID
    assert processor.stats['customers_created'] == 3
    assert processor.stats.get('normalized_matches', 0) == 0  # No matches since all new
