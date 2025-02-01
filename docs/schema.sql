-- Database Schema

-- Prisma Migrations table
CREATE TABLE "_prisma_migrations" (
    id VARCHAR PRIMARY KEY,
    checksum VARCHAR NOT NULL,
    finished_at TIMESTAMPTZ,
    migration_name VARCHAR NOT NULL,
    logs TEXT,
    rolled_back_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL,
    applied_steps_count INTEGER NOT NULL
);

-- Company table - Stores company information with enrichment data
CREATE TABLE "Company" (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT UNIQUE NOT NULL,
    enriched BOOLEAN DEFAULT FALSE,
    enrichedAt TIMESTAMP,
    enrichedSource TEXT,
    enrichmentError TEXT,
    enrichmentData JSONB,
    createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Customer table - Stores customer information with billing and shipping details
CREATE TABLE "Customer" (
    id TEXT PRIMARY KEY,
    customerName TEXT NOT NULL,
    companyDomain TEXT REFERENCES "Company"(domain),
    quickbooksId TEXT,
    status TEXT,
    terms TEXT,
    taxCode TEXT,
    taxItem TEXT,
    resaleNumber TEXT,
    creditLimit NUMERIC,
    billingAddressId TEXT REFERENCES "Address"(id),
    shippingAddressId TEXT REFERENCES "Address"(id),
    sourceData JSONB,
    createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modifiedAt TIMESTAMP
);

-- Address table - Stores address information for billing and shipping
CREATE TABLE "Address" (
    id TEXT PRIMARY KEY,
    line1 TEXT NOT NULL,
    line2 TEXT,
    line3 TEXT,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postalCode TEXT NOT NULL,
    country TEXT NOT NULL
);

-- CustomerEmail table - Stores customer email addresses
CREATE TYPE email_type AS ENUM ('MAIN', 'CC');
CREATE TABLE "CustomerEmail" (
    id TEXT PRIMARY KEY,
    customerId TEXT REFERENCES "Customer"(id),
    email TEXT NOT NULL,
    type email_type NOT NULL,
    isPrimary BOOLEAN DEFAULT FALSE
);

-- CustomerPhone table - Stores customer phone numbers
CREATE TYPE phone_type AS ENUM ('MAIN', 'MOBILE', 'WORK', 'OTHER');
CREATE TABLE "CustomerPhone" (
    id TEXT PRIMARY KEY,
    customerId TEXT REFERENCES "Customer"(id),
    phone TEXT NOT NULL,
    type phone_type NOT NULL,
    isPrimary BOOLEAN DEFAULT FALSE
);

-- Product table - Stores product catalog information
CREATE TABLE "Product" (
    id TEXT PRIMARY KEY,
    productCode TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modifiedAt TIMESTAMP
);

-- Order table - Stores order header information
CREATE TYPE order_status AS ENUM ('DRAFT', 'PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED');
CREATE TYPE payment_status AS ENUM ('PENDING', 'PARTIAL', 'PAID', 'OVERDUE', 'VOID');
CREATE TABLE "Order" (
    id TEXT PRIMARY KEY,
    orderNumber TEXT UNIQUE NOT NULL,
    customerId TEXT REFERENCES "Customer"(id),
    orderDate TIMESTAMP NOT NULL,
    status order_status NOT NULL,
    paymentStatus payment_status NOT NULL,
    subtotal NUMERIC NOT NULL,
    taxPercent NUMERIC,
    taxAmount NUMERIC,
    totalAmount NUMERIC NOT NULL,
    billingAddressId TEXT REFERENCES "Address"(id),
    shippingAddressId TEXT REFERENCES "Address"(id),
    paymentMethod TEXT,
    paymentDate TIMESTAMP,
    terms TEXT,
    dueDate TIMESTAMP,
    poNumber TEXT,
    class TEXT,
    shippingMethod TEXT,
    shipDate TIMESTAMP,
    quickbooksId TEXT,
    sourceData JSONB,
    createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modifiedAt TIMESTAMP
);

-- OrderItem table - Stores order line items
CREATE TABLE "OrderItem" (
    id TEXT PRIMARY KEY,
    orderId TEXT REFERENCES "Order"(id),
    productCode TEXT REFERENCES "Product"(productCode),
    description TEXT,
    quantity NUMERIC NOT NULL,
    unitPrice NUMERIC NOT NULL,
    amount NUMERIC NOT NULL,
    serviceDate TIMESTAMP,
    sourceData JSONB
);

-- CompanyStats table - Stores aggregated company statistics
CREATE TABLE "CompanyStats" (
    id TEXT PRIMARY KEY,
    customerCount BIGINT NOT NULL,
    totalOrders NUMERIC NOT NULL
);

-- Indexes
CREATE INDEX idx_company_domain ON "Company"(domain);
CREATE INDEX idx_customer_company_domain ON "Customer"(companyDomain);
CREATE INDEX idx_customer_email_customer_id ON "CustomerEmail"(customerId);
CREATE INDEX idx_customer_phone_customer_id ON "CustomerPhone"(customerId);
CREATE INDEX idx_order_customer_id ON "Order"(customerId);
CREATE INDEX idx_order_item_order_id ON "OrderItem"(orderId);
CREATE INDEX idx_order_item_product_code ON "OrderItem"(productCode);
