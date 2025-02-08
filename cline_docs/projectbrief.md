# Project Brief

## Overview
A Python-based data import system that processes QuickBooks sales data (invoices and receipts) into a structured PostgreSQL database. The system handles complex data relationships, maintains data quality, and provides robust error handling and validation.

## Core Requirements

### Data Import
- Import sales data from QuickBooks CSV exports
- Support both invoice and sales receipt formats
- Maintain data relationships and integrity
- Process data in efficient batches
- Handle special cases (Amazon FBA, system products)

### Data Quality
- Ensure idempotent processing
- Validate data before processing
- Track and report errors
- Maintain consistent product catalog
- Handle customer/company relationships

### System Design
- Modular processing pipeline
- Clear separation of concerns
- Consistent error handling
- Robust logging and monitoring
- Configurable processing options

## Goals

### Primary Goals
1. Reliable Data Import
   - Consistent processing of sales data
   - Accurate relationship mapping
   - Error detection and handling
   - Data validation

2. Data Quality
   - Clean customer records
   - Accurate product mapping
   - Consistent naming
   - Relationship integrity

3. System Reliability
   - Idempotent operations
   - Transaction safety
   - Error recovery
   - Performance optimization

### Technical Goals
1. Code Quality
   - Clear architecture
   - Consistent patterns
   - Strong typing
   - Comprehensive testing

2. Maintainability
   - Clear documentation
   - Standard patterns
   - Error tracking
   - Performance monitoring

3. Extensibility
   - Modular design
   - Clear interfaces
   - Configuration options
   - Processing hooks

## Success Criteria
- Reliable processing of QuickBooks data
- Accurate customer and product relationships
- Clear error reporting and validation
- Consistent processing patterns
- Maintainable codebase
- Comprehensive documentation
- Performance at scale
