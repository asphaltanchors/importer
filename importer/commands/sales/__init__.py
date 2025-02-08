"""Sales processing commands."""

import click

from .receipt_customers import process_receipt_customers

@click.group()
def sales():
    """Specialized sales data operations."""
    pass

# Register specialized commands
sales.add_command(process_receipt_customers)

# Export top-level process commands
from .process_invoices import process_invoices
from .process_receipts import process_receipts
