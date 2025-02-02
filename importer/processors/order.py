"""Order processor for sales data."""

from datetime import datetime
from typing import Dict, Any, Optional, List, Set
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Order, OrderStatus, PaymentStatus, Customer
from ..utils import generate_uuid
from ..utils.normalization import normalize_customer_name
from .address import AddressProcessor
from .base import BaseProcessor

class OrderProcessor(BaseProcessor):
    """Process orders from sales data."""
    
    def __init__(self, session_manager, batch_size: int = 100):
        """Initialize the processor.
        
        Args:
            session_manager: Database session manager
            batch_size: Number of orders to process per batch
        """
        self.session_manager = session_manager
        super().__init__(None, batch_size)  # We'll manage sessions ourselves
        
        # Track processed orders
        self.processed_orders: Set[str] = set()
        self.successful_order_ids: List[str] = []
        
        # Additional stats
        self.stats.update({
            'total_orders': 0,
            'created': 0,
            'updated': 0,
            'customers_not_found': 0,
            'invalid_addresses': 0
        })
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'invoice_number': ['Invoice No', 'Sales Receipt No'],
            'invoice_date': ['Invoice Date', 'Sales Receipt Date'],
            'customer_id': ['Customer'],  # QuickBooks ID we map to our customer
            'payment_terms': ['Terms'],
            'due_date': ['Due Date'],
            'po_number': ['PO Number'],
            'shipping_method': ['Ship Via'],
            'class': ['Class'],
            'payment_method': ['Payment Method']
        }
        
        # Cache for customer lookups
        self._customer_cache: Dict[str, Optional[Customer]] = {}
    
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
    
    def find_customer(self, customer_name: str, session: Session) -> Optional[Customer]:
        """Find customer by name with normalization fallback and caching.
        
        Args:
            customer_name: Customer name to look up
            session: Database session
            
        Returns:
            Customer if found, None otherwise
        """
        # Check cache first
        if customer_name in self._customer_cache:
            return self._customer_cache[customer_name]
            
        # Try exact match first
        customer = session.query(Customer).filter(
            Customer.customerName == customer_name
        ).first()
        
        # If not found, try normalized match
        if not customer:
            normalized_name = normalize_customer_name(customer_name)
            for existing in session.query(Customer).all():
                if normalize_customer_name(existing.customerName) == normalized_name:
                    customer = existing
                    logging.info(f"Found normalized name match: '{customer_name}' -> '{existing.customerName}'")
                    break
        
        # Cache result (even if None)
        self._customer_cache[customer_name] = customer
        return customer
    
    def process_file(self, file_path: Path, is_sales_receipt: bool = False) -> Dict[str, Any]:
        """Process orders from a CSV file.
        
        Args:
            file_path: Path to CSV file
            is_sales_receipt: Whether this is a sales receipt vs invoice
            
        Returns:
            Dict containing processing results
        """
        try:
            # Read CSV into DataFrame
            df = pd.read_csv(file_path)
            
            # Map CSV headers to our standardized field names
            header_mapping = {}
            for std_field, possible_names in self.field_mappings.items():
                for name in possible_names:
                    if name in df.columns:
                        header_mapping[std_field] = name
                        break
            
            if not all(field in header_mapping for field in ['invoice_number', 'customer_id']):
                raise ValueError("Missing required columns in CSV file")
            
            # Group by invoice number
            invoice_groups = df.groupby(header_mapping['invoice_number'])
            total_invoices = len(invoice_groups)
            
            print(f"\nProcessing {total_invoices} invoices in batches of {self.batch_size}", flush=True)
            
            # Collect all invoices first
            invoices_to_process = []
            for invoice_number, invoice_df in invoice_groups:
                if invoice_number not in self.processed_orders:
                    invoices_to_process.append((invoice_number, invoice_df.iloc[0]))  # Use first row for order info
            
            # Process in actual batches
            total_batches = (len(invoices_to_process) + self.batch_size - 1) // self.batch_size
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(invoices_to_process))
                current_batch = invoices_to_process[start_idx:end_idx]
                
                print(f"Processing batch {batch_num + 1}/{total_batches} ({len(current_batch)} invoices)...", flush=True)
                self._process_batch(current_batch, is_sales_receipt)
            
            return {
                'success': self.stats['failed_batches'] == 0,
                'summary': {
                    'stats': self.stats,
                    'successful_order_ids': self.successful_order_ids
                }
            }
            
        except Exception as e:
            logging.error(f"Failed to process file: {str(e)}")
            return {
                'success': False,
                'summary': {
                    'stats': self.stats,
                    'successful_order_ids': self.successful_order_ids
                }
            }
    
    def _process_batch(self, batch: List[tuple[str, pd.Series]], is_sales_receipt: bool) -> None:
        """Process a batch of orders.
        
        Args:
            batch: List of (invoice_number, row) tuples
            is_sales_receipt: Whether these are sales receipts
        """
        try:
            with self.session_manager as session:
                # Create AddressProcessor for this batch
                address_processor = AddressProcessor(session)
                
                for invoice_number, row in batch:
                    try:
                        self.logger.debug(f"Processing invoice {invoice_number}")
                        result = self._process_order(row, is_sales_receipt, session, address_processor)
                        if result['success'] and result['order']:
                            self.successful_order_ids.append(result['order'].id)
                            self.processed_orders.add(invoice_number)
                            self.stats['total_orders'] += 1
                            if not hasattr(result['order'], 'id'):
                                self.stats['created'] += 1
                            else:
                                self.stats['updated'] += 1
                    except Exception as e:
                        logging.error(f"Error processing invoice {invoice_number}: {str(e)}")
                        continue
                
                # Commit batch
                session.commit()
                self.stats['successful_batches'] += 1
                self.logger.debug(f"Committed batch of {len(batch)} invoices")
                
        except Exception as e:
            self.stats['failed_batches'] += 1
            self.stats['total_errors'] += 1
            logging.error(f"Error processing batch: {str(e)}")
    
    def _process_order(self, row: pd.Series, is_sales_receipt: bool, session: Session, 
                      address_processor: AddressProcessor) -> Dict[str, Any]:
        """Process a single order."""
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
            
            # Check if order already exists
            existing_order = session.query(Order).filter(
                Order.orderNumber == invoice_number
            ).first()
            
            # Get customer
            customer_name = self.get_mapped_field(row, 'customer_id')
            if not customer_name:
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Missing customer name for invoice {invoice_number}"
                }
                return result
            
            customer = self.find_customer(customer_name, session)
            if not customer:
                self.stats['customers_not_found'] += 1
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Customer not found: {customer_name}"
                }
                return result
            
            # Process addresses
            billing_id = customer.billingAddressId
            shipping_id = customer.shippingAddressId
            
            # Create addresses if provided in row
            # Check if we have any address fields
            address_fields = [f"Billing Address {field}" for field in ['Line 1', 'City', 'State']]
            self.logger.debug(f"Checking address fields for invoice {invoice_number}:")
            for field in address_fields:
                self.logger.debug(f"  {field}: {row[field] if field in row else 'Not found'}")
                
            if any(field in row and not pd.isna(row[field]) for field in address_fields):
                self.logger.debug("Found address fields, processing addresses")
                # Create a new DataFrame with just the address fields
                address_data = {}
                # Map the address fields correctly from the CSV format
                billing_data = {
                    'Billing Address Line 1': row['Billing Address Line1'] if 'Billing Address Line1' in row else None,
                    'Billing Address Line 2': row['Billing Address Line2'] if 'Billing Address Line2' in row else None,
                    'Billing Address Line 3': row['Billing Address Line3'] if 'Billing Address Line3' in row else None,
                    'Billing Address City': row['Billing Address City'] if 'Billing Address City' in row else None,
                    'Billing Address State': row['Billing Address State'] if 'Billing Address State' in row else None,
                    'Billing Address Postal Code': row['Billing Address Postal Code'] if 'Billing Address Postal Code' in row else None,
                    'Billing Address Country': row['Billing Address Country'] if 'Billing Address Country' in row else None
                }
                
                shipping_data = {
                    'Shipping Address Line 1': row['Shipping Address Line1'] if 'Shipping Address Line1' in row else None,
                    'Shipping Address Line 2': row['Shipping Address Line2'] if 'Shipping Address Line2' in row else None,
                    'Shipping Address Line 3': row['Shipping Address Line3'] if 'Shipping Address Line3' in row else None,
                    'Shipping Address City': row['Shipping Address City'] if 'Shipping Address City' in row else None,
                    'Shipping Address State': row['Shipping Address State'] if 'Shipping Address State' in row else None,
                    'Shipping Address Postal Code': row['Shipping Address Postal Code'] if 'Shipping Address Postal Code' in row else None,
                    'Shipping Address Country': row['Shipping Address Country'] if 'Shipping Address Country' in row else None
                }
                
                address_data = {**billing_data, **shipping_data}
                
                address_row = pd.Series(address_data)
                
                # Process addresses using DataFrame
                df = pd.DataFrame([address_row])
                self.logger.debug(f"Address data to process:\n{address_row.to_dict()}")
                df = address_processor.process(df)
                
                new_billing_id = df.at[0, 'billing_address_id']
                new_shipping_id = df.at[0, 'shipping_address_id']
                
                self.logger.debug(f"Address processing results - billing_id: {new_billing_id}, shipping_id: {new_shipping_id}")
                
                if new_billing_id is None or new_shipping_id is None:
                    self.stats['invalid_addresses'] += 1
                    result['success'] = False
                    error_msg = f"Failed to process addresses for invoice {invoice_number}"
                    self.logger.error(error_msg)
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': error_msg
                    }
                    return result
                
                billing_id = new_billing_id
                shipping_id = new_shipping_id
            
            now = datetime.utcnow()
            
            # Get order date
            order_date = None
            order_date_str = self.get_mapped_field(row, 'invoice_date')
            if order_date_str:
                try:
                    order_date = datetime.strptime(order_date_str, '%m-%d-%Y')
                except ValueError:
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid order date format: {order_date_str}"
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
            
            # Determine order status
            is_paid = is_sales_receipt or ('Status' in row and row['Status'] == 'Paid')
            status = OrderStatus.CLOSED if is_paid else OrderStatus.OPEN
            payment_status = PaymentStatus.PAID if is_paid else PaymentStatus.UNPAID
            
            if existing_order:
                # Update existing order
                existing_order.status = status
                existing_order.paymentStatus = payment_status
                existing_order.customerId = customer.id
                existing_order.billingAddressId = billing_id
                existing_order.shippingAddressId = shipping_id
                existing_order.terms = self.get_mapped_field(row, 'payment_terms') or ''
                existing_order.dueDate = due_date
                existing_order.poNumber = self.get_mapped_field(row, 'po_number') or ''
                existing_order.class_ = self.get_mapped_field(row, 'class') or ''
                existing_order.shippingMethod = self.get_mapped_field(row, 'shipping_method') or ''
                existing_order.paymentMethod = self.get_mapped_field(row, 'payment_method') or 'Invoice'
                existing_order.quickbooksId = row['QuickBooks Internal Id'] if 'QuickBooks Internal Id' in row else ''
                existing_order.modifiedAt = now
                # Convert row to dict and replace NaN with null for JSON compatibility
                source_data = row.to_dict()
                source_data = {k: (None if pd.isna(v) else v) for k, v in source_data.items()}
                existing_order.sourceData = source_data
                
                result['order'] = existing_order
                
            else:
                # Create new order
                order = Order(
                    id=generate_uuid(),
                    orderNumber=invoice_number,
                    customerId=customer.id,
                    orderDate=order_date,
                    status=status,
                    paymentStatus=payment_status,
                    subtotal=0,  # Will be updated when line items are linked
                    taxPercent=None,
                    taxAmount=0,  # Will be updated when line items are linked
                    totalAmount=0,  # Will be updated when line items are linked
                    billingAddressId=billing_id,
                    shippingAddressId=shipping_id,
                    terms=self.get_mapped_field(row, 'payment_terms') or '',
                    dueDate=due_date,
                    poNumber=self.get_mapped_field(row, 'po_number') or '',
                    class_=self.get_mapped_field(row, 'class') or '',
                    shippingMethod=self.get_mapped_field(row, 'shipping_method') or '',
                    paymentMethod=self.get_mapped_field(row, 'payment_method') or 'Invoice',
                    quickbooksId=row['QuickBooks Internal Id'] if 'QuickBooks Internal Id' in row else '',
                    createdAt=now,
                    modifiedAt=now,
                    # Convert row to dict and replace NaN with null for JSON compatibility
                    sourceData={k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                )
                
                session.add(order)
                result['order'] = order
            
            return result
            
        except Exception as e:
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': f"Failed to process order: {str(e)}"
            }
            return result
