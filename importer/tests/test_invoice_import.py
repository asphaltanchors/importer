"""Integration tests for invoice import with customer name normalization."""

import pytest
from pathlib import Path
import tempfile
import csv
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ..processors.invoice import InvoiceProcessor
from ..utils import generate_uuid
from ..db.models import (
    Customer, Company, Product, Base,
    Order, OrderStatus, PaymentStatus, OrderItem
)

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
    
    # Create test invoice data
    csv_path = create_test_csv([{
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
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']
    assert result['summary']['stats']['total_invoices'] == 1
    assert result['summary']['stats']['created'] == 1
    assert result['summary']['stats']['errors'] == 0

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
    
    # Create test invoice data with different name format
    csv_path = create_test_csv([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Acme Corp LLC',
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
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']
    assert result['summary']['stats']['total_invoices'] == 1
    assert result['summary']['stats']['created'] == 1
    assert result['summary']['stats']['errors'] == 0

def test_invoice_import_comma_customer_name(session):
    """Test invoice import with comma-separated customer name."""
    # Create test customer
    customer = Customer.create(
        name='John Smith',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(customer)
    session.commit()
    
    # Create test invoice data with comma format
    csv_path = create_test_csv([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Smith, John',
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
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']
    assert result['summary']['stats']['total_invoices'] == 1
    assert result['summary']['stats']['created'] == 1
    assert result['summary']['stats']['errors'] == 0

def test_invoice_import_percentage_notation(session):
    """Test invoice import with percentage notation in customer name."""
    # Create test customer
    customer = Customer.create(
        name='White Cap 30%:Whitecap Edmonton Canada',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    session.add(customer)
    session.commit()
    
    # Create test invoice data with different case
    csv_path = create_test_csv([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'WHITE CAP 30%:WHITECAP EDMONTON CANADA',
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
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']
    assert result['summary']['stats']['total_invoices'] == 1
    assert result['summary']['stats']['created'] == 1
    assert result['summary']['stats']['errors'] == 0

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
    
    # Add initial line item
    line_item = OrderItem(
        id=generate_uuid(),
        orderId=order.id,
        productCode='TEST001',
        description='Initial product',
        quantity=1,
        unitPrice=100.00,
        amount=100.00
    )
    session.add(line_item)
    session.commit()
    
    # Create test invoice data with updated values
    csv_path = create_test_csv([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Acme Corp',
        'Terms': 'Net 15',  # Changed terms
        'Due Date': '01-30-2025',
        'Status': 'Paid',  # Changed status
        'Product/Service': 'TEST001',
        'Product/Service Description': 'Updated product',  # Changed description
        'Qty': '2',  # Changed quantity
        'Product/Service  Amount': '200.00',  # Changed amount
        'Product/Service Sales Tax': '20.00'  # Added tax
    }])
    
    # Process invoice
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']
    assert result['summary']['stats']['total_invoices'] == 1
    assert result['summary']['stats']['updated'] == 1
    assert result['summary']['stats']['created'] == 0
    
    # Verify order updates
    updated_order = session.query(Order).filter_by(orderNumber='INV001').first()
    assert updated_order.status == OrderStatus.CLOSED
    assert updated_order.paymentStatus == PaymentStatus.PAID
    assert updated_order.terms == 'Net 15'
    assert updated_order.subtotal == 200.00
    assert updated_order.taxAmount == 20.00
    assert updated_order.totalAmount == 220.00
    
    # Verify line item updates
    line_items = session.query(OrderItem).filter_by(orderId=updated_order.id).all()
    assert len(line_items) == 1
    assert line_items[0].description == 'Updated product'
    assert line_items[0].quantity == 2
    assert line_items[0].unitPrice == 100.00
    assert line_items[0].amount == 200.00

def test_invoice_import_update_customer(session):
    """Test updating an order when customer details have changed."""
    # Create initial customer and order
    customer1 = Customer.create(
        name='ACME CORPORATION',
        quickbooks_id='12345',
        company_domain='example.com'
    )
    customer2 = Customer.create(
        name='Acme Corp LLC',  # Different format but normalizes to same name
        quickbooks_id='67890',
        company_domain='example.com'
    )
    session.add_all([customer1, customer2])
    
    order = Order(
        id=generate_uuid(),
        orderNumber='INV001',
        customerId=customer1.id,
        orderDate=datetime.strptime('01-15-2025', '%m-%d-%Y'),
        status=OrderStatus.OPEN,
        paymentStatus=PaymentStatus.UNPAID,
        subtotal=100.00,
        totalAmount=100.00
    )
    session.add(order)
    session.commit()
    
    # Create test invoice data with second customer
    csv_path = create_test_csv([{
        'Invoice No': 'INV001',
        'Invoice Date': '01-15-2025',
        'Customer': 'Acme Corp LLC',  # Different customer format
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
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']
    assert result['summary']['stats']['total_invoices'] == 1
    assert result['summary']['stats']['updated'] == 1
    
    # Verify order was updated with new customer
    updated_order = session.query(Order).filter_by(orderNumber='INV001').first()
    assert updated_order.customerId == customer2.id

def test_invoice_import_customer_not_found(session):
    """Test invoice import with non-existent customer."""
    # Create test invoice data
    csv_path = create_test_csv([{
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
    processor = InvoiceProcessor(str(session.get_bind().url))
    result = processor.process(csv_path)
    
    # Verify results
    assert result['success']  # Overall process succeeds
    assert result['summary']['stats']['total_invoices'] == 0  # No invoices created
    assert result['summary']['stats']['errors'] == 1  # One error for customer not found
    assert any('Customer not found' in error['message'] for error in result['summary']['errors'])
