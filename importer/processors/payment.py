"""Payment processor for sales data."""

from datetime import datetime
from typing import Dict, Any, Optional, List, Set
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Order, OrderStatus, PaymentStatus
from ..utils import generate_uuid
from ..utils.csv_normalization import (
    normalize_dataframe_columns, 
    validate_required_columns,
    validate_json_data
)
from .base import BaseProcessor

class PaymentProcessor(BaseProcessor):
    """Process payment information from sales data."""
    
    def __init__(self, session_manager, batch_size: int = 100):
        """Initialize the processor.
        
        Args:
            session_manager: Database session manager
            batch_size: Number of orders to process per batch
        """
        self.session_manager = session_manager
        super().__init__(None, batch_size)  # We'll manage sessions ourselves
        
        # Track processed payments
        self.processed_orders: Set[str] = set()
        
        # Additional stats
        self.stats.update({
            'total_payments': 0,
            'orders_processed': 0,
            'orders_not_found': 0,
            'invalid_amounts': 0
        })
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'invoice_number': ['Invoice No', 'Sales Receipt No'],
            'payment_terms': ['Terms'],
            'payment_method': ['Payment Method'],
            'payment_status': ['Status'],
            'payment_amount': ['Total Amount'],
            'due_date': ['Due Date']
        }
    
    def get_mapped_field(self, row: pd.Series, field: str) -> Optional[str]:
        """Get value for a mapped field from the row.
        
        Args:
            row: CSV row data
            field: Field name to look up
            
        Returns:
            Field value if found, None otherwise
        """
        if field not in self.field_mappings:
            return None
            
        for possible_name in self.field_mappings[field]:
            if possible_name in row:
                return row[possible_name].strip()
        
        return None
    
    def process_file(self, file_path: Path, is_sales_receipt: bool = False) -> Dict[str, Any]:
        """Process payments from a CSV file.
        
        Args:
            file_path: Path to CSV file
            is_sales_receipt: Whether this is a sales receipt vs invoice
            
        Returns:
            Dict containing processing results
        """
        try:
            # Read CSV into DataFrame and normalize column names
            df = pd.read_csv(
                file_path,
                encoding='cp1252',
                dtype=str,  # Read all columns as strings to preserve IDs
                skipinitialspace=True
            )
            df = normalize_dataframe_columns(df)
            
            # Validate required columns
            required_columns = ['Invoice No', 'Sales Receipt No']
            if not any(col in df.columns for col in required_columns):
                raise ValueError("Missing required invoice number column in CSV file")
            
            # Map CSV headers to our standardized field names
            header_mapping = {}
            for std_field, possible_names in self.field_mappings.items():
                for name in possible_names:
                    if name in df.columns:
                        header_mapping[std_field] = name
                        break
            
            if not all(field in header_mapping for field in ['invoice_number']):
                raise ValueError("Missing required columns in CSV file")
            
            # Group by invoice number
            invoice_groups = df.groupby(header_mapping['invoice_number'])
            total_invoices = len(invoice_groups)
            
            print(f"\nProcessing payments for {total_invoices} invoices in batches of {self.batch_size}", flush=True)
            
            # Process in batches
            current_batch = []
            batch_num = 1
            
            for invoice_number, invoice_df in invoice_groups:
                if invoice_number in self.processed_orders:
                    continue
                    
                current_batch.append((invoice_number, invoice_df.iloc[0]))  # Use first row for payment info
                
                if len(current_batch) >= self.batch_size:
                    self._process_batch(current_batch, is_sales_receipt)
                    print(f"Batch {batch_num} complete ({len(current_batch)} invoices)", flush=True)
                    current_batch = []
                    batch_num += 1
            
            # Process final batch if any
            if current_batch:
                self._process_batch(current_batch, is_sales_receipt)
                print(f"Final batch complete ({len(current_batch)} invoices)", flush=True)
            
            return {
                'success': self.stats['failed_batches'] == 0,
                'summary': {
                    'stats': self.stats
                }
            }
            
        except Exception as e:
            logging.error(f"Failed to process file: {str(e)}")
            return {
                'success': False,
                'summary': {
                    'stats': self.stats
                }
            }
    
    def _process_batch(self, batch: List[tuple[str, pd.Series]], is_sales_receipt: bool) -> None:
        """Process a batch of payments.
        
        Args:
            batch: List of (invoice_number, row) tuples
            is_sales_receipt: Whether these are sales receipts
        """
        try:
            with self.session_manager as session:
                for invoice_number, row in batch:
                    try:
                        result = self._process_payment(row, is_sales_receipt, session)
                        if result['success'] and result['order']:
                            self.processed_orders.add(invoice_number)
                            self.stats['orders_processed'] += 1
                            self.stats['total_payments'] += 1
                    except Exception as e:
                        logging.error(f"Error processing invoice {invoice_number}: {str(e)}")
                        continue
                
                # Commit batch
                session.commit()
                self.stats['successful_batches'] += 1
                
        except Exception as e:
            self.stats['failed_batches'] += 1
            self.stats['total_errors'] += 1
            logging.error(f"Error processing batch: {str(e)}")
    
    def _process_payment(self, row: pd.Series, is_sales_receipt: bool, session: Session) -> Dict[str, Any]:
        """Process a single payment."""
        result = {
            'success': True,
            'order': None,
            'error': None
        }
        
        try:
            # Get invoice number
            invoice_number = self.get_mapped_field(row, 'invoice_number')
            if not invoice_number:
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': 'Missing invoice number'
                }
                return result
            
            # Find order
            order = session.query(Order).filter(
                Order.orderNumber == invoice_number
            ).first()
            
            if not order:
                self.stats['orders_not_found'] += 1
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Order not found: {invoice_number}"
                }
                return result
            
            # Get payment amount
            payment_amount = 0.0
            amount_str = self.get_mapped_field(row, 'payment_amount')
            if amount_str:
                try:
                    payment_amount = float(amount_str.replace('$', '').replace(',', ''))
                except ValueError:
                    self.stats['invalid_amounts'] += 1
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid payment amount: {amount_str}"
                    }
                    return result
            
            # Get due date if present
            due_date = None
            due_date_str = self.get_mapped_field(row, 'due_date')
            if due_date_str:
                try:
                    due_date = datetime.strptime(due_date_str, '%m-%d-%Y')
                except ValueError:
                    logging.warning(f"Invalid due date format: {due_date_str}")
            
            # Determine payment status
            is_paid = is_sales_receipt or self.get_mapped_field(row, 'payment_status') == 'Paid'
            status = OrderStatus.CLOSED if is_paid else OrderStatus.OPEN
            payment_status = PaymentStatus.PAID if is_paid else PaymentStatus.UNPAID
            
            # Update order payment info
            order.status = status
            order.paymentStatus = payment_status
            order.totalAmount = payment_amount
            order.terms = self.get_mapped_field(row, 'payment_terms') or ''
            order.dueDate = due_date
            order.paymentMethod = self.get_mapped_field(row, 'payment_method') or 'Invoice'
            order.modifiedAt = datetime.utcnow()
            order.sourceData = validate_json_data(row.to_dict())
            
            result['order'] = order
            return result
            
        except Exception as e:
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': f"Failed to process payment: {str(e)}"
            }
            return result
