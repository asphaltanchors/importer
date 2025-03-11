# Product Importer

This document describes how to use the product importer to import products with cost and list price information.

## Overview

The product importer allows you to import products from a CSV file, including cost and list price information. It can create new products or update existing ones, and optionally track price history changes.

## CSV Format

The importer expects a CSV file with the following columns:

- `Item Name`: The product code (required)
- `Purchase Cost`: The cost of the product
- `Sales Price`: The list price of the product
- `Purchase Description` or `Sales Description`: The product description

Example CSV format:

```csv
"S.No","Item Name","Item Type","Item Subtype","Purchase Cost","Purchase Description","Sales Price","Sales Description"
"1","PROD001","Service","ItemService","10.50","Product description","25.99","Product sales description"
```

## Command Line Usage

### Basic Usage

```bash
python -m importer import-products inputs/sample_products.csv
```

### Options

- `--output FILE`: Save processing results to a file
- `--batch-size SIZE`: Number of records to process per batch (default: 100)
- `--track-history/--no-track-history`: Enable/disable price history tracking (default: enabled)
- `--debug`: Enable detailed debug output

### Examples

Import products with default settings:
```bash
python -m importer import-products inputs/sample_products.csv
```

Import products with larger batch size and disable price history tracking:
```bash
python -m importer import-products inputs/sample_products.csv --batch-size 500 --no-track-history
```

Save processing results to a file:
```bash
python -m importer import-products inputs/sample_products.csv --output results.json
```

## Features

### Product Creation and Updates

The importer will:
- Create new products if they don't exist
- Update existing products with new information
- Update cost and list price if provided

### Price History Tracking

When price history tracking is enabled (default), the importer will:
- Create an initial price history entry for new products
- Create a new price history entry when a product's cost or list price changes
- Track the effective date of price changes

To disable price history tracking:
```bash
python -m importer import-products inputs/sample_products.csv --no-track-history
```

### Batch Processing

Products are processed in batches to improve performance and reduce memory usage. You can adjust the batch size with the `--batch-size` option.

### Error Handling

The importer provides:
- Validation of required fields
- Warnings for non-numeric cost and price values
- Detailed error reporting
- Batch-level error recovery

## Integration with Other Commands

The product importer is part of the larger importer system and follows the same patterns as other commands. It can be used alongside other import commands like `process-invoices` and `process-receipts`.
