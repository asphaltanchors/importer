"""Shared test fixtures and utilities."""

import os
import pytest
import tempfile
import csv
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from ..db.models import Base, Company, Product, Customer

# Load test environment variables
load_dotenv('.env.test')

@pytest.fixture(scope="session")
def engine():
    """Create test database engine."""
    database_url = os.getenv('TEST_DATABASE_URL')
    if not database_url:
        raise ValueError("TEST_DATABASE_URL not set in .env.test")
    
    print(f"\nConnecting to database...")
    engine = create_engine(database_url)
    
    # Test connection and print user info
    with engine.connect() as conn:
        result = conn.execute(text('SELECT current_user, current_database()'))
        user, db = result.first()
        print(f"Connected as: {user}")
        print(f"Database: {db}")
        
        # Test schema permissions
        result = conn.execute(text(
            "SELECT has_schema_privilege('public', 'usage') as usage, "
            "has_schema_privilege('public', 'create') as create"
        ))
        perms = result.first()
        print(f"Schema permissions - usage: {perms.usage}, create: {perms.create}")
        conn.commit()
        
    # Create tables if they don't exist
    with engine.connect() as conn:
        # Execute schema.sql if tables don't exist
        result = conn.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'Company')"
        ))
        tables_exist = result.scalar()
        
        if not tables_exist:
            print("Creating database schema...")
            with open('docs/schema.sql') as f:
                schema = f.read()
                conn.execute(text(schema))
                conn.commit()
        else:
            print("Tables already exist, truncating...")
            # Truncate all tables in the correct order
            conn.execute(text('TRUNCATE TABLE "OrderItem" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "Order" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "CustomerEmail" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "CustomerPhone" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "Customer" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "Product" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "Company" CASCADE'))
            conn.execute(text('TRUNCATE TABLE "Address" CASCADE'))
            conn.commit()
    
    yield engine
    
    # Clean up by truncating all tables
    with engine.connect() as conn:
        conn.execute(text('TRUNCATE TABLE "OrderItem" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Order" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "CustomerEmail" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "CustomerPhone" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Customer" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Product" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Company" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Address" CASCADE'))
        conn.commit()

@pytest.fixture(autouse=True)
def clean_tables(engine):
    """Automatically truncate tables before each test."""
    # Clean up by truncating all tables
    with engine.connect() as conn:
        conn.execute(text('TRUNCATE TABLE "OrderItem" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Order" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "CustomerEmail" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "CustomerPhone" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Customer" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Product" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Company" CASCADE'))
        conn.execute(text('TRUNCATE TABLE "Address" CASCADE'))
        conn.commit()
    yield

@pytest.fixture
def session_manager(engine):
    """Create a session manager for testing."""
    return sessionmaker(bind=engine)

@pytest.fixture
def session(engine, clean_tables):
    """Create a test database session with automatic rollback."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture
def populated_session(session, clean_tables):
    """Create a session with common test data."""
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
        description='Test product for testing',
        createdAt=datetime.utcnow(),
        modifiedAt=datetime.utcnow()
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
