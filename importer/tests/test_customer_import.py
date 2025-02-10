"""Integration tests for customer import with name normalization."""

import os
import pandas as pd
from sqlalchemy.orm import Session

from ..processors.company import CompanyProcessor
from ..processors.customer import CustomerProcessor
from ..db.models import Customer, Company

def test_customer_creation_basic(session_manager):
    """Test basic customer creation with company relationship."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Customer': 'Acme Corp',
        'Main Email': 'contact@example.com',
        'Customer Name': 'Acme Corp',
        'QuickBooks Internal Id': '12345'
    }])
    
    # Process company first (required for customer creation)
    company_result = company_processor.process(data)
    company_stats = company_processor.get_stats()
    
    # Initialize customer processor after company exists
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Process customer data
    customer_result = customer_processor.process(data)
    customer_stats = customer_processor.get_stats()
    
    # Verify results
    assert company_stats['companies_created'] == 1
    assert company_stats['total_errors'] == 0
    assert customer_stats['customers_created'] == 1
    assert customer_stats['total_errors'] == 0

def test_customer_name_normalization(session_manager):
    """Test customer name normalization with various formats."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data with various name formats
    data = pd.DataFrame([
        {
            'Customer': 'Valid Company',
            'Main Email': 'contact@example.com',
            'Customer Name': None,  # Invalid - missing name
            'QuickBooks Internal Id': '1'
        },
        {
            'Customer': 'Valid Company',
            'Main Email': 'contact@example.com',
            'Customer Name': '',  # Invalid - empty name
            'QuickBooks Internal Id': '2'
        },
        {
            'Customer': 'Valid Company',
            'Main Email': 'contact@example.com',
            'Customer Name': 'Valid Customer',  # Valid record
            'QuickBooks Internal Id': '3'
        }
    ])
    
    # Process company data first
    company_processor.process(data)
    
    # Initialize customer processor after companies exist
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Process customer data
    customer_processor.process(data)
    
    # Verify results
    customer_stats = customer_processor.get_stats()
    assert customer_stats['customers_created'] == 3
    assert customer_stats['total_errors'] == 0

def test_customer_idempotent_processing(session_manager):
    """Test idempotent customer processing."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data
    data = pd.DataFrame([{
        'Customer': 'Test Corp',
        'Main Email': 'contact@example.com',
        'Customer Name': 'Test Corp',
        'QuickBooks Internal Id': '12345'
    }])
    
    # Process company first
    company_processor.process(data)
    
    # Initialize customer processor after company exists
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Process customer data first time
    result1 = customer_processor.process(data)
    stats1 = customer_processor.get_stats()
    
    # Reset processor for second run
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    result2 = customer_processor.process(data)
    stats2 = customer_processor.get_stats()
    
    # Verify results
    assert stats1['customers_created'] == 1
    assert stats2['customers_created'] == 0  # No new customers created
    assert stats1['total_errors'] == 0
    assert stats2['total_errors'] == 0

def test_customer_batch_processing(session_manager):
    """Test customer batch processing."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=2,  # Small batch size for testing
        error_limit=10,
        debug=True
    )
    
    # Create test data with multiple records
    data = pd.DataFrame([
        {
            'Customer': 'Company 1',
            'Main Email': 'contact@company1.com',
            'Customer Name': 'Company 1',
            'QuickBooks Internal Id': '1'
        },
        {
            'Customer': 'Company 2',
            'Main Email': 'contact@company2.com',
            'Customer Name': 'Company 2',
            'QuickBooks Internal Id': '2'
        },
        {
            'Customer': 'Company 3',
            'Main Email': 'contact@company3.com',
            'Customer Name': 'Company 3',
            'QuickBooks Internal Id': '3'
        },
        {
            'Customer': 'Company 4',
            'Main Email': 'contact@company4.com',
            'Customer Name': 'Company 4',
            'QuickBooks Internal Id': '4'
        }
    ])
    
    # Process company data first
    company_processor.process(data)
    
    # Initialize customer processor after companies exist
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=2,  # Small batch size for testing
        error_limit=10,
        debug=True
    )
    
    # Process customer data
    customer_processor.process(data)
    
    # Verify results
    customer_stats = customer_processor.get_stats()
    assert customer_stats['customers_created'] == 4
    assert customer_stats['successful_batches'] == 2  # Should be processed in 2 batches
    assert customer_stats['total_errors'] == 0

def test_customer_error_handling(session_manager):
    """Test customer processing error handling."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=2,  # Low error limit for testing
        debug=True
    )
    
    # Create test data with invalid records
    data = pd.DataFrame([
        {
            'Customer': 'Valid Company',
            'Main Email': 'contact@example.com',
            'Customer Name': None,  # Invalid - missing name
            'QuickBooks Internal Id': '1'
        },
        {
            'Customer': 'Valid Company',
            'Main Email': 'contact@example.com',
            'Customer Name': '',  # Invalid - empty name
            'QuickBooks Internal Id': '2'
        },
        {
            'Customer': 'Valid Company',
            'Main Email': 'contact@example.com',
            'Customer Name': 'Valid Customer',  # Valid record
            'QuickBooks Internal Id': '3'
        }
    ])
    
    # Process company data first
    company_processor.process(data)
    
    # Initialize customer processor after company exists
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=2,  # Low error limit for testing
        debug=True
    )
    
    # Process customer data
    customer_processor.process(data)
    
    # Verify results
    stats = customer_processor.get_stats()
    assert stats['total_errors'] > 0  # Should have validation errors
    assert stats['customers_created'] >= 1  # Valid customer should be created
    assert stats['failed_batches'] > 0

def test_validate_data(session_manager):
    """Test customer data validation."""
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Test missing required columns
    data1 = pd.DataFrame([{'Email': 'test@example.com'}])
    critical1, warnings1 = customer_processor.validate_data(data1)
    assert len(critical1) >= 1  # Missing required columns
    
    # Test empty customer names
    data2 = pd.DataFrame([
        {
            'Customer Name': None,
            'QuickBooks Internal Id': '1',
            'company_domain': 'example.com'
        },
        {
            'Customer Name': 'Valid',
            'QuickBooks Internal Id': '2',
            'company_domain': 'example.com'
        }
    ])
    critical2, warnings2 = customer_processor.validate_data(data2)
    assert len(warnings2) >= 1  # Warning about empty customer name
    
    # Test missing company domain
    data3 = pd.DataFrame([{
        'Customer Name': 'Test Company',
        'QuickBooks Internal Id': '1'
    }])
    critical3, warnings3 = customer_processor.validate_data(data3)
    assert len(warnings3) >= 1  # Warning about missing company domain

def test_customer_created_date(session_manager):
    """Test customer created date is set from CSV."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data with created date
    data = pd.DataFrame([{
        'Customer': 'Test Corp',
        'Main Email': 'contact@example.com',
        'Customer Name': 'Test Corp',
        'QuickBooks Internal Id': '12345',
        'Created Date': '01-15-2024'  # MM-DD-YYYY format
    }])
    
    # Process company first
    company_processor.process(data)
    
    # Initialize customer processor after company exists
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Process customer data
    customer_processor.process(data)
    
    # Verify created date was set correctly
    with Session(session_manager()) as session:
        customer = session.query(Customer).first()
        assert customer is not None
        expected_date = pd.to_datetime('01-15-2024', format='%m-%d-%Y')
        assert customer.createdAt.date() == expected_date.date()

def test_customer_company_relationship(session_manager):
    """Test customer-company relationship handling."""
    # Initialize company processor
    company_processor = CompanyProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Create test data with company relationship
    data = pd.DataFrame([{
        'Customer': 'Test Corp',
        'Main Email': 'contact@example.com',
        'Customer Name': 'Test Corp',
        'QuickBooks Internal Id': '12345'
    }])
    
    # Process company data first
    company_processor.process(data)
    
    # Initialize customer processor after company exists
    customer_processor = CustomerProcessor(
        config={'database_url': os.getenv('TEST_DATABASE_URL')},
        batch_size=100,
        error_limit=10,
        debug=True
    )
    
    # Verify company exists and process customer
    with Session(session_manager()) as session:
        company = session.query(Company).filter(Company.domain == 'example.com').first()
        assert company is not None
        
        # Process customer data
        customer_processor.process(data)
        
        # Verify customer-company relationship
        customer = session.query(Customer).first()
        assert customer is not None
        assert customer.companyDomain == 'example.com'
