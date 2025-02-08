"""Integration tests for invoice import with full processing sequence."""

import pytest
from pathlib import Path
import tempfile
import csv
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..processors.invoice import InvoiceProcessor
from ..processors.company import CompanyProcessor
from ..processors.product import ProductProcessor
from ..utils import generate_uuid
from ..db.models import (
    Customer, Company, Product, Base,
    Order, OrderStatus, PaymentStatus, OrderItem
)

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
        # Create test company
        company = Company(
            id='test-company',
            name='Test Company',
            domain='example.com'
        )
        session.add(company)
        
        # Create test product
        product = Product(
            id='test-product',
            productCode='TEST001',
            name='Test Product',
            description='Test product for invoice import',
            unitPrice=100.00
        )
        session.add(product)
        
        session.commit()
        
        yield session

def create_test_csv(rows):
    """Create a temporary CSV file with test data."""
    fd, path = tempfile.mkstemp(suffix='.csv')
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Invoice No',
            'Invoice Date',
            'Customer',
            'Terms',
            'Due Date',
            'Status',
            'Product/Service',
            'Product/Service Description',
            'Qty',
            'Product/Service  Amount',
            'Product/Service Sales Tax'
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return Path(path)

def test_validate_data(session_manager):
    """Test invoice data validation."""
    processor = InvoiceProcessor(
        config={'database_url': 'sqlite:///:memory:'},
        batch_size=100,
        error_limit=10
    )
    
    # Test missing required columns
    data1 = pd.DataFrame([{'Terms': 'Net 30'}])
    critical1, warnings1 = processor.validate_data(data1)
    assert len(critical1) == 1  # Missing required columns
    
    # Test empty invoice numbers
    data2 = pd.DataFrame([
        {'Invoice No': None, 'Invoice Date': '01-15-2025', 'Customer': 'Test'},
        {'Invoice No': 'INV001', 'Invoice Date': '01-15-2025', 'Customer': 'Test'}
    ])
    critical2, warnings2 = processor.validate_data(data2)
    assert len(critical2) == 0
    assert len(warnings2) == 1  # Warning about empty invoice number
    
    # Test invalid dates
    data3 = pd.DataFrame([{
        'Invoice No': 'INV001',
        'Invoice Date': 'invalid-date',
        'Customer': 'Test'
    }])
    critical3, warnings3 = processor.validate_data(data3)
    assert len(critical3) == 0
    assert len(warnings3) == 1  # Warning about invalid date

def test_invoice_full_processing_sequence(session_manager):
    """Test the complete invoice processing sequence."""
    config = {
        'database_url': 'sqlite:///:memory:',
        'batch_size': 100,
        'error_limit': 10
    }
    
    # Create test data
    data = pd.DataFrame([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'New Customer LLC',
        'Terms': 'Net 30',
        'Due Date': '02-14-2025',
        'Status': 'Open',
        'Product/Service': 'NEW001',
        'Product/Service Description': 'New Product',
        'Qty': '1',
        'Product/Service  Amount': '100.00',
        'Product/Service Sales Tax': '0.00'
    }])
    
    # 1. Process companies
    company_processor = CompanyProcessor(config)
    company_processor.process(pd.DataFrame([{
        'Customer': 'New Customer LLC',
        'Main Email': 'contact@newcustomer.com'
    }]))
    company_stats = company_processor.get_stats()
    assert company_stats['companies_created'] == 1
    
    # 2. Process products
    product_processor = ProductProcessor(config)
    product_processor.process(pd.DataFrame([{
        'Product/Service': 'NEW001',
        'Product/Service Description': 'New Product',
        'Product/Service  Amount': '100.00'
    }]))
    product_stats = product_processor.get_stats()
    assert product_stats['products_created'] == 1
    
    # 3. Process invoice
    invoice_processor = InvoiceProcessor(config)
    result = invoice_processor.process(data)
    stats = invoice_processor.get_stats()
    
    # Verify results
    assert stats['total_invoices'] == 1
    assert stats['created'] == 1
    assert stats['total_errors'] == 0
    
    # Verify relationships
    with Session(create_engine('sqlite:///:memory:')) as session:
        # Verify company was created
        company = session.query(Company).filter_by(domain='newcustomer.com').first()
        assert company is not None
        
        # Verify customer was created and linked to company
        customer = session.query(Customer).filter_by(customerName='New Customer LLC').first()
        assert customer is not None
        assert customer.company_domain == company.domain
        
        # Verify product was created
        product = session.query(Product).filter_by(productCode='NEW001').first()
        assert product is not None
        assert product.description == 'New Product'
        
        # Verify order was created and linked to customer
        order = session.query(Order).filter_by(orderNumber='INV001').first()
        assert order is not None
        assert order.customerId == customer.id

def test_invoice_import_exact_customer_match(session):
    """Test invoice import with exact customer name match."""
    # Create test customer
    customer = Customer.create(
        name='Acme Corp',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(customer)
    session.commit()
    
    # Create test data
    data = pd.DataFrame([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Acme Corp',
        'Terms': 'Net 30',
        'Due Date': '02-14-2025',
        'Status': 'Open',
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Test product',
        'Qty': '1',
        'Product/Service  Amount': '100.00',
        'Product/Service Sales Tax': '0.00'
    }])
    
    # Process invoice
    processor = InvoiceProcessor(
        config={'database_url': str(session.get_bind().url)},
        batch_size=100,
        error_limit=10
    )
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['total_invoices'] == 1
    assert stats['created'] == 1
    assert stats['total_errors'] == 0

def test_invoice_import_normalized_customer_match(session):
    """Test invoice import with normalized customer name match."""
    # Create test customer
    customer = Customer.create(
        name='ACME CORPORATION',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(customer)
    session.commit()
    
    # Create test data
    data = pd.DataFrame([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Acme Corp LLC',  # Different format
        'Terms': 'Net 30',
        'Due Date': '02-14-2025',
        'Status': 'Open',
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Test product',
        'Qty': '1',
        'Product/Service  Amount': '100.00',
        'Product/Service Sales Tax': '0.00'
    }])
    
    # Process invoice
    processor = InvoiceProcessor(
        config={'database_url': str(session.get_bind().url)},
        batch_size=100,
        error_limit=10
    )
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['total_invoices'] == 1
    assert stats['created'] == 1
    assert stats['total_errors'] == 0

def test_invoice_import_update_order(session):
    """Test updating an existing order with changed data."""
    # Create test customer
    customer = Customer.create(
        name='Acme Corp',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(customer)
    
    # Create initial order
    order = Order(
        id=generate_uuid(),
        orderNumber='INV001',
        customerId=customer.id,
        orderDate=datetime.strptime('01-15-2025', '%m-%d-%Y'),
        status=OrderStatus.OPEN,
        paymentStatus=PaymentStatus.UNPAID,
        subtotal=100.00,
        taxAmount=0.00,
        totalAmount=100.00,
        terms='Net 30',
        class_='Retail'
    )
    session.add(order)
    session.commit()
    
    # Create test data with updates
    data = pd.DataFrame([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Acme Corp',
        'Terms': 'Net 15',  # Changed terms
        'Due Date': '01-30-2025',
        'Status': 'Paid',  # Changed status
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Updated product',
        'Qty': '2',
        'Product/Service  Amount': '200.00',
        'Product/Service Sales Tax': '20.00'
    }])
    
    # Process invoice
    processor = InvoiceProcessor(
        config={'database_url': str(session.get_bind().url)},
        batch_size=100,
        error_limit=10
    )
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['total_invoices'] == 1
    assert stats['updated'] == 1
    assert stats['created'] == 0
    
    # Verify order updates
    updated_order = session.query(Order).filter_by(orderNumber='INV001').first()
    assert updated_order.status == OrderStatus.CLOSED
    assert updated_order.paymentStatus == PaymentStatus.PAID
    assert updated_order.terms == 'Net 15'

def test_invoice_import_customer_not_found(session):
    """Test invoice import with non-existent customer."""
    data = pd.DataFrame([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Non Existent Corp',
        'Terms': 'Net 30',
        'Due Date': '02-14-2025',
        'Status': 'Open',
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Test product',
        'Qty': '1',
        'Product/Service  Amount': '100.00',
        'Product/Service Sales Tax': '0.00'
    }])
    
    # Process invoice
    processor = InvoiceProcessor(
        config={'database_url': str(session.get_bind().url)},
        batch_size=100,
        error_limit=10
    )
    result = processor.process(data)
    stats = processor.get_stats()
    
    # Verify results
    assert stats['total_invoices'] == 0  # No invoices created
    assert stats['total_errors'] > 0  # Error for customer not found
