"""Sales processing commands."""

import click

from .receipt_customers import process_receipt_customers
from .import_products import import_products

@click.group()
def sales():
    """Specialized sales data operations."""
    pass

# Register specialized commands
sales.add_command(process_receipt_customers)
sales.add_command(import_products)

# Export top-level process commands
from .process_invoices import process_invoices
from .process_receipts import process_receipts
