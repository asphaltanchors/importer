"""Invoice data processor."""

from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import csv
import logging
import pandas as pd
from datetime import datetime

from ..db.session import SessionManager
from ..utils import generate_uuid
from ..utils.normalization import normalize_customer_name
from ..db.models import Order, OrderStatus, PaymentStatus, Customer, Product, OrderItem
from .address import AddressProcessor

class InvoiceProcessor:
    """Process invoices from sales data."""
    
    def __init__(self, database_url: str):
        """Initialize the processor.
        
        Args:
            database_url: Database connection URL
        """
        self.session_manager = SessionManager(database_url)
        self.address_processor = None  # Will be initialized per session
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'invoice_number': ['Invoice No', 'Sales Receipt No'],
            'invoice_date': ['Invoice Date', 'Sales Receipt Date'],
            'customer_id': ['Customer'],  # This will be the QuickBooks ID we map to our customer
            'payment_terms': ['Terms'],
            'due_date': ['Due Date'],
            'po_number': ['PO Number'],
            'shipping_method': ['Ship Via'],  # Changed back to Ship Via
            'class': ['Class'],
            'payment_method': ['Payment Method']  # Added payment method mapping
        }
        
    def process(self, file_path: Path) -> Dict[str, Any]:
        """Process invoices from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dict containing processing results with structure:
            {
                'success': bool,
                'summary': {
                    'stats': {
                        'total_invoices': int,  # Total number of unique invoices processed
                        'created': int,     # New orders created
                        'updated': int,     # Existing orders updated
                        'line_items': int,  # Total line items processed
                        'errors': int       # Number of errors encountered
                    },
                    'errors': List[Dict]
                }
            }
        """
        results = {
            'success': True,
            'summary': {
                'stats': {
                    'total_invoices': 0,
                    'created': 0,
                    'updated': 0,
                    'line_items': 0,
                    'errors': 0
                },
                'errors': []
            }
        }
        
        try:
            # Initialize system products
            with self.session_manager as session:
                system_products = [
                    ('SYS-SHIPPING', 'Shipping', 'System product for shipping charges'),
                    ('SYS-HANDLING', 'Handling', 'System product for handling fees'),
                    ('SYS-TAX', 'Tax', 'System product for sales tax'),
                    ('SYS-NJ-TAX', 'NJ Sales Tax', 'System product for New Jersey sales tax'),
                    ('SYS-DISCOUNT', 'Discount', 'System product for discounts')
                ]
                
                for code, name, description in system_products:
                    product = session.query(Product).filter(
                        Product.productCode == code
                    ).first()
                    
                    if not product:
                        product = Product(
                            id=generate_uuid(),
                            productCode=code,
                            name=name,
                            description=description,
                            createdAt=datetime.utcnow(),
                            modifiedAt=datetime.utcnow()
                        )
                        session.add(product)
                
                session.commit()
            
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                
                # Determine if this is a sales receipt
                is_sales_receipt = 'Sales Receipt No' in reader.fieldnames
                
                # Map CSV headers to our standardized field names
                header_mapping = {}
                for std_field, possible_names in self.field_mappings.items():
                    for name in possible_names:
                        if name in reader.fieldnames:
                            header_mapping[std_field] = name
                            break
                
                # Read all rows into memory
                all_rows = list(reader)
                
                # Group rows by invoice number
                invoices = {}
                for row in all_rows:
                    invoice_number = row[header_mapping['invoice_number']].strip()
                    if invoice_number:
                        if invoice_number not in invoices:
                            invoices[invoice_number] = []
                        invoices[invoice_number].append(row)
                
                # Process each invoice
                for invoice_number, invoice_rows in invoices.items():
                    with self.session_manager as session:
                        try:
                            row = invoice_rows[0]  # Use first row for header info
                            row_num = all_rows.index(row) + 1
                            # Check if invoice already exists
                            existing_order = session.query(Order).filter(
                                Order.orderNumber == invoice_number
                            ).with_for_update().first()
                            
                            # Get customer by name
                            customer_name = row[header_mapping['customer_id']].strip()
                            
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
                            
                            # Validate customer
                            if not customer:
                                results['summary']['errors'].append({
                                    'row': row_num,
                                    'severity': 'ERROR',
                                    'message': f"Customer not found: {customer_name}"
                                })
                                results['summary']['stats']['errors'] += 1
                                continue
                            
                            # Calculate totals and validate products
                            subtotal = 0
                            tax_amount = 0
                            validation_errors = []
                            
                            for inv_row in invoice_rows:
                                product_code = inv_row.get('Product/Service', '').strip()
                                if not product_code:
                                    continue
                                
                                # Map special items to system product codes
                                product_code_lower = product_code.lower()
                                if product_code_lower == 'shipping':
                                    mapped_code = 'SYS-SHIPPING'
                                elif product_code_lower == 'handling fee':
                                    mapped_code = 'SYS-HANDLING'
                                elif product_code_lower == 'tax':
                                    mapped_code = 'SYS-TAX'
                                elif product_code_lower == 'nj sales tax':
                                    mapped_code = 'SYS-NJ-TAX'
                                elif product_code_lower == 'discount':
                                    mapped_code = 'SYS-DISCOUNT'
                                else:
                                    mapped_code = product_code.upper()
                                
                                product = session.query(Product).filter(
                                    Product.productCode == mapped_code
                                ).first()
                                
                                if not product:
                                    validation_errors.append({
                                        'row': row_num,
                                        'severity': 'ERROR',
                                        'message': f"Product not found: {product_code} in invoice {invoice_number}"
                                    })
                                    continue
                                
                                # Calculate amounts
                                amount = 0.0
                                amount_str = inv_row.get('Product/Service Amount', '').strip()
                                if amount_str:
                                    try:
                                        amount = float(amount_str.replace('$', '').replace(',', ''))
                                        # Add to subtotal if it's not a tax item
                                        if not mapped_code in ['SYS-TAX', 'SYS-NJ-TAX']:
                                            subtotal += amount
                                    except ValueError:
                                        validation_errors.append({
                                            'row': row_num,
                                            'severity': 'ERROR',
                                            'message': f"Invalid amount for product {product_code}"
                                        })
                                        continue
                                
                                # Add to tax amount if it's a tax item
                                if mapped_code in ['SYS-TAX', 'SYS-NJ-TAX']:
                                    tax_amount += amount
                            
                            # Add validation errors to results
                            for error in validation_errors:
                                results['summary']['errors'].append(error)
                                if error['severity'] == 'ERROR':
                                    results['summary']['stats']['errors'] += 1
                            
                            # Skip if validation failed
                            if validation_errors:
                                continue
                            
                            # Get total amount from CSV
                            total_amount_str = row.get('Total Amount', '0').strip()
                            try:
                                total_amount = float(total_amount_str.replace('$', '').replace(',', ''))
                            except ValueError:
                                total_amount = subtotal + tax_amount  # Fallback to calculated total
                            now = datetime.utcnow()
                            
                            # Create addresses if provided
                            if not self.address_processor:
                                self.address_processor = AddressProcessor(session)
                            
                            # Create addresses if provided
                            address_row = pd.Series({
                                'Billing Address Line 1': row.get('Billing Address Line 1', ''),
                                'Billing Address Line 2': row.get('Billing Address Line 2', ''),
                                'Billing Address Line 3': row.get('Billing Address Line 3', ''),
                                'Billing Address City': row.get('Billing Address City', ''),
                                'Billing Address State': row.get('Billing Address State', ''),
                                'Billing Address Postal Code': row.get('Billing Address Postal Code', ''),
                                'Billing Address Country': row.get('Billing Address Country', ''),
                                'Shipping Address Line 1': row.get('Shipping Address Line 1', ''),
                                'Shipping Address Line 2': row.get('Shipping Address Line 2', ''),
                                'Shipping Address Line 3': row.get('Shipping Address Line 3', ''),
                                'Shipping Address City': row.get('Shipping Address City', ''),
                                'Shipping Address State': row.get('Shipping Address State', ''),
                                'Shipping Address Postal Code': row.get('Shipping Address Postal Code', ''),
                                'Shipping Address Country': row.get('Shipping Address Country', '')
                            })
                            
                            # Process addresses using DataFrame to ensure commit
                            df = pd.DataFrame([address_row])
                            df = self.address_processor.process(df)
                            billing_id = df.at[0, 'billing_address_id']
                            shipping_id = df.at[0, 'shipping_address_id']
                            
                            # Use customer's addresses as fallback
                            billing_id = billing_id if billing_id else customer.billingAddressId
                            shipping_id = shipping_id if shipping_id else customer.shippingAddressId
                            
                            if existing_order:
                                # Update existing order
                                logging.info(f"Updating existing order: {invoice_number}")
                                
                                # Delete existing line items first
                                session.query(OrderItem).filter(
                                    OrderItem.orderId == existing_order.id
                                ).delete(synchronize_session=False)
                                
                                # Update all fields
                                is_sales_receipt = 'Sales Receipt No' in reader.fieldnames
                                existing_order.status = OrderStatus.CLOSED if is_sales_receipt or row.get('Status') == 'Paid' else OrderStatus.OPEN
                                existing_order.paymentStatus = PaymentStatus.PAID if is_sales_receipt or row.get('Status') == 'Paid' else PaymentStatus.UNPAID
                                existing_order.subtotal = subtotal
                                existing_order.taxAmount = tax_amount
                                existing_order.totalAmount = total_amount
                                existing_order.customerId = customer.id
                                existing_order.billingAddressId = billing_id
                                existing_order.shippingAddressId = shipping_id
                                existing_order.terms = row.get(header_mapping.get('payment_terms', ''), '')
                                existing_order.dueDate = datetime.strptime(row[header_mapping['due_date']], '%m-%d-%Y') if header_mapping.get('due_date') and row.get(header_mapping['due_date']) else None
                                existing_order.poNumber = row.get(header_mapping.get('po_number', ''), '')
                                existing_order.class_ = row.get(header_mapping.get('class', ''), '')
                                existing_order.shippingMethod = row.get(header_mapping.get('shipping_method', ''), '')
                                existing_order.paymentMethod = row.get(header_mapping.get('payment_method', ''), 'Invoice')
                                existing_order.quickbooksId = row.get('QuickBooks Internal Id', '')
                                existing_order.modifiedAt = now
                                existing_order.sourceData = row
                                
                                order = existing_order
                                results['summary']['stats']['updated'] += 1
                            else:
                                # Create new order
                                logging.info(f"Creating new order: {invoice_number}")
                                order = Order(
                                    id=generate_uuid(),
                                    orderNumber=invoice_number,
                                    customerId=customer.id,
                                    orderDate=datetime.strptime(row[header_mapping['invoice_date']], '%m-%d-%Y'),
                                    # Sales receipts are always paid/closed, invoices use Status field
                                    status=OrderStatus.CLOSED if is_sales_receipt or row.get('Status') == 'Paid' else OrderStatus.OPEN,
                                    paymentStatus=PaymentStatus.PAID if is_sales_receipt or row.get('Status') == 'Paid' else PaymentStatus.UNPAID,
                                    subtotal=subtotal,
                                    taxPercent=None,
                                    taxAmount=tax_amount,
                                    totalAmount=total_amount,
                                    billingAddressId=billing_id,
                                    shippingAddressId=shipping_id,
                                    terms=row.get(header_mapping.get('payment_terms', ''), ''),
                                    dueDate=datetime.strptime(row[header_mapping['due_date']], '%m-%d-%Y') if header_mapping.get('due_date') and row.get(header_mapping['due_date']) else None,
                                    poNumber=row.get(header_mapping.get('po_number', ''), ''),
                                    class_=row.get(header_mapping.get('class', ''), ''),
                                    shippingMethod=row.get(header_mapping.get('shipping_method', ''), ''),
                                    paymentMethod=row.get(header_mapping.get('payment_method', ''), 'Invoice'),  # Default to 'Invoice' for invoices
                                    quickbooksId=row.get('QuickBooks Internal Id', ''),
                                    createdAt=now,
                                    modifiedAt=now,
                                    sourceData=row
                                )
                                session.add(order)
                                results['summary']['stats']['created'] += 1
                                existing_items = {}
                            
                            
                            # Process line items
                            line_items_processed = 0
                            for inv_row in invoice_rows:
                                product_code = inv_row.get('Product/Service', '').strip()
                                
                                # Map special items to system product codes
                                if not product_code:
                                    continue
                                
                                # Map special items to system product codes
                                product_code_lower = product_code.lower()
                                if product_code_lower == 'shipping':
                                    mapped_code = 'SYS-SHIPPING'
                                elif product_code_lower == 'handling fee':
                                    mapped_code = 'SYS-HANDLING'
                                elif product_code_lower == 'tax':
                                    mapped_code = 'SYS-TAX'
                                elif product_code_lower == 'nj sales tax':
                                    mapped_code = 'SYS-NJ-TAX'
                                elif product_code_lower == 'discount':
                                    mapped_code = 'SYS-DISCOUNT'
                                else:
                                    mapped_code = product_code.upper()
                                
                                # Look up product
                                product = session.query(Product).filter(
                                    Product.productCode == mapped_code
                                ).first()
                                
                                if not product:
                                    results['summary']['errors'].append({
                                        'row': row_num,
                                        'severity': 'ERROR',
                                        'message': f"Product not found: {product_code} in invoice {invoice_number}"
                                    })
                                    continue
                                
                                # Parse quantity and amount
                                quantity = float(inv_row.get('Product/Service Quantity', '1').strip() or '1')
                                amount = 0.0
                                amount_str = inv_row.get('Product/Service Amount', '0').strip()
                                if amount_str:
                                    try:
                                        amount = float(amount_str.replace('$', '').replace(',', ''))
                                    except ValueError:
                                        results['summary']['errors'].append({
                                            'row': row_num,
                                            'severity': 'ERROR',
                                            'message': f"Invalid amount for {product_code}"
                                        })
                                        continue
                                
                                unit_price = amount / quantity if quantity != 0 else 0
                                
                                # Parse service date if present
                                service_date = None
                                service_date_str = inv_row.get('Service Date', '').strip()
                                if service_date_str:
                                    try:
                                        service_date = datetime.strptime(service_date_str, '%m-%d-%Y')
                                    except ValueError:
                                        results['summary']['errors'].append({
                                            'row': row_num,
                                            'severity': 'WARNING',
                                            'message': f"Invalid service date format for {product_code}"
                                        })
                                
                                # Create new line item
                                order_item = OrderItem(
                                    id=generate_uuid(),
                                    orderId=order.id,
                                    productCode=product.productCode,
                                    description=inv_row.get('Product/Service Description', '').strip(),
                                    quantity=quantity,
                                    unitPrice=unit_price,
                                    amount=amount,
                                    serviceDate=service_date,
                                    sourceData=inv_row
                                )
                                session.add(order_item)
                                line_items_processed += 1
                            
                            results['summary']['stats']['line_items'] += line_items_processed
                            session.commit()
                            
                        except Exception as e:
                            session.rollback()
                            results['success'] = False
                            results['summary']['errors'].append({
                                'row': row_num,
                                'severity': 'ERROR',
                                'message': f"Failed to process invoice {invoice_number}: {str(e)}"
                            })
                            results['summary']['stats']['errors'] += 1
                            
                results['summary']['stats']['total_invoices'] = len(invoices)
        except Exception as e:
            results['success'] = False
            results['summary']['errors'].append({
                'row': 0,
                'severity': 'CRITICAL',
                'message': f"Failed to process file: {str(e)}"
            })
            
        return results
