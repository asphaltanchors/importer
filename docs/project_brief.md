# MQI (Meltano Quickbooks Importer)

## Overview
This project imports QuickBooks CSV files into a PostgreSQL database using Meltano for ETL orchestration and dbt for transformations. The primary goal is to create a normalized database structure from QuickBooks' relatively unstructured CSV exports.

## Core Requirements

### Data Sources
The system imports four types of CSV files from QuickBooks:

1. **Sales Receipts**
   - Multi-line entries per order
   - Common order ID across lines
   - Line item types:
     * Products (with quantity and SKU)
     * Shipping charges
     * Handling fees
     * Tax entries
     * Special instructions
   - Payment methods (e.g., Amazon, Credit Card)
   - Must maintain QuickBooks IDs

2. **Invoice Receipts**
   - Similar structure to Sales Receipts
   - Additional fields:
     * Payment terms (e.g., Net 30)
     * Status tracking (Open/Closed)
     * PO numbers
   - Business-focused metadata
   - Must maintain QuickBooks IDs

3. **Customers**
   - Contact information
   - Multiple address types
   - Company determination:
     * Use explicit company name when available
     * Extract from email domain as fallback
     * Treat different TLDs as separate companies
   - Must maintain QuickBooks IDs

4. **Products**
   - Product descriptions and SKUs
   - Cost tracking (with history)
   - Inventory tracking:
     * On-hand quantity
     * In-route quantity

### Data Processing Modes

The system operates in two modes:

1. **Bulk Historical Import**
   - Handles large datasets (~100,000 records)
   - One-time initial data load
   - Full historical data import

2. **Daily Updates**
   - Processes recent changes (~100 records per file)
   - Typically covers last 7 days of data
   - Same processing pipeline as bulk import

### Historical Tracking

**Fields Requiring History:**
- Product costs (COGS)
- Inventory levels (on-hand and in-route)
- Product pricing changes
- Customer address changes

**Fields Without History:**
- Invoice status (only latest state kept)
- Most other fields (latest value only)

### Data Quality Management

**Handling Rules:**
- Accept all international addresses without validation
- Maintain all original QuickBooks IDs and references
- Drop malformed records with warning logs
- Log quality issues for manual review

**Processing Requirements:**
- Idempotent operations
- Duplicate detection
- Complete audit trails
- Batch processing (no real-time requirements)

## Technical Implementation

### Core Technologies
- PostgreSQL database
- Meltano ETL pipeline
- dbt transformations

### Operational Specifications
- Expected daily volume: ~100 records per file type
- Batch processing approach
- Historical data retained indefinitely
- Error logging and reporting
- Performance monitoring for large imports

### Sample Data Structures
```csv
# Sales Receipt Example (simplified from actual format)
Sales Receipt No,Customer,Sales Receipt Date,Payment Method,Product/Service,Product/Service Description,Product/Service Quantity,Product/Service Rate,Product/Service Amount,Shipping Method,Status,Total Amount
3D-8075,NC,02-04-2025,Credit Card,01-7013,"BoltHold eAK-4 Asphalt Anchor Kit",1,70.00,70.00,FedEx Ground,false,95.77
3D-8075,NC,02-04-2025,Credit Card,Shipping,"FEDEX - FedEx Home Delivery",,25.77,25.77,FedEx Ground,false,95.77

# Invoice Example (simplified from actual format)
Invoice No,Customer,Invoice Date,Product/Service,Product/Service Description,Product/Service Quantity,Product/Service Rate,Product/Service Amount,Terms,Status,Total Amount
A6846,"Fasteners Plus",02-07-2025,01-6358.58K,"SP58 Asphalt Anchor 5/8\" thread",18,82.8,1490.40,N30,Open,1503.9
A6846,"Fasteners Plus",02-07-2025,Shipping,"FedEx ground account 341184894",,0.00,0.00,N30,Open,1503.9

Note: Actual CSV files contain additional fields for addresses, metadata, and internal references. Above samples show key fields only.
```

## Future Considerations
- Additional QuickBooks export format support
- Data archival strategy if volume grows significantly
- Reporting and analytics (separate project)
