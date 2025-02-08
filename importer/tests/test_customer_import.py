"""Integration tests for customer import with name normalization."""

import os
import pytest
import pandas as pd
from sqlalchemy.orm import Session

from ..processors.customer import CustomerProcessor
from ..db.models import Customer, Company

def test_customer_import_basic(populated_session, caplog):
    """Test basic customer import with exact name match."""
    import logging
    caplog.set_level(logging.DEBUG)
    
    print("\n=== Test Start ===")
    
    # Verify company exists from populated_session fixture
    company = populated_session.query(Company).filter(Company.domain == 'example.com').first()
    print(f"Using company from fixture: id={company.id}, domain={company.domain}")
    
    # Create company in a fresh session outside the transaction
    engine = populated_session.get_bind().engine
    with Session(engine) as fresh_session:
        fresh_session.add(Company(
            id='test-company',
            name='Test Company',
            domain='example.com'
        ))
        fresh_session.commit()
    
    # Initialize processor after company is committed
    processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Customer Name': 'Acme Corp',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'  # Match the company domain
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was created
    customer = populated_session.query(Customer).first()
    assert customer is not None
    assert customer.customerName == 'Acme Corp'
    assert processor.stats['customers_created'] == 1
    assert processor.stats.get('normalized_matches', 0) == 0

def test_customer_import_normalized_name(populated_session):
    """Test customer import with normalized name matching."""
    processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create existing customer
    existing = Customer.create(
        name='ACME CORP',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    populated_session.add(existing)
    populated_session.commit()
    
    # Create test data with different name format
    data = pd.DataFrame([{
        'Customer Name': 'Acme Corp LLC',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was updated, not created
    customers = populated_session.query(Customer).all()
    assert len(customers) == 1
    assert customers[0].customerName == 'Acme Corp LLC'  # Name updated to new format
    assert processor.stats['customers_updated'] == 1
    assert processor.stats.get('normalized_matches', 0) == 1

def test_customer_import_comma_name(populated_session):
    """Test customer import with comma-separated name."""
    processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create existing customer
    existing = Customer.create(
        name='John Smith',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    populated_session.add(existing)
    populated_session.commit()
    
    # Create test data with comma format
    data = pd.DataFrame([{
        'Customer Name': 'Smith, John',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was updated, not created
    customers = populated_session.query(Customer).all()
    assert len(customers) == 1
    assert customers[0].customerName == 'Smith, John'  # Name updated to new format
    assert processor.stats['customers_updated'] == 1
    assert processor.stats.get('normalized_matches', 0) == 1

def test_customer_import_percentage_notation(populated_session):
    """Test customer import with percentage notation."""
    processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create existing customer with percentage notation
    existing = Customer.create(
        name='White Cap 30%:Whitecap Edmonton Canada',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    populated_session.add(existing)
    populated_session.commit()
    
    # Create test data with same notation but different case
    data = pd.DataFrame([{
        'Customer Name': 'WHITE CAP 30%:WHITECAP EDMONTON CANADA',
        'QuickBooks Internal Id': '12345',
        'company_domain': 'example.com'
    }])
    
    # Process data
    result = processor.process(data)
    
    # Verify customer was updated, not created
    customers = populated_session.query(Customer).all()
    assert len(customers) == 1
    assert customers[0].customerName == 'WHITE CAP 30%:WHITECAP EDMONTON CANADA'
    assert processor.stats['customers_updated'] == 1
    assert processor.stats.get('normalized_matches', 0) == 1

def test_customer_import_business_suffixes(populated_session):
    """Test customer import with various business suffixes."""
    processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
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
    customers = populated_session.query(Customer).all()
    assert len(customers) == 3  # Each has unique QuickBooks ID
    assert processor.stats['customers_created'] == 3
    assert processor.stats.get('normalized_matches', 0) == 0  # No matches since all new
