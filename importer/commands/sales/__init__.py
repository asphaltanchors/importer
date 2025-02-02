"""Sales processing commands."""

import click

from .process_invoices import process_invoices
from .process_receipts import process_receipts

@click.group()
def sales():
    """Sales data processing commands."""
    pass

# Register commands
sales.add_command(process_invoices)
sales.add_command(process_receipts)
