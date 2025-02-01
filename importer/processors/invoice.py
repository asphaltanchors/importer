"""Invoice data processor."""

from pathlib import Path
from typing import Dict, Any, List
import csv
from datetime import datetime

from ..db.session import SessionManager
from ..utils import generate_uuid
from ..db.models import Order, OrderStatus, PaymentStatus, Customer, Product, OrderItem

class InvoiceProcessor:
    """Process invoices from sales data."""
    
    def __init__(self, database_url: str):
        """Initialize the processor.
        
        Args:
            database_url: Database connection URL
        """
        self.session_manager = SessionManager(database_url)
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'invoice_number': ['Invoice No'],
            'invoice_date': ['Invoice Date'],
            'customer_id': ['Customer'],  # This will be the QuickBooks ID we map to our customer
            'payment_terms': ['Terms'],
            'due_date': ['Due Date'],
            'po_number': ['PO Number'],
            'shipping_method': ['Ship Via'],  # Changed back to Ship Via
            'class': ['Class']
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
                        'total_invoices': int,
                        'created': int,
                        'updated': int,
                        'line_items': int,
                        'errors': int
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
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                
                # Map CSV headers to our standardized field names
                header_mapping = {}
                for std_field, possible_names in self.field_mappings.items():
                    for name in possible_names:
                        if name in reader.fieldnames:
                            header_mapping[std_field] = name
                            break
                
                # Read all rows into memory
                all_rows = list(reader)
                
                # Track processed invoice numbers to avoid duplicates
                processed_invoices = set()
                
                with self.session_manager as session:
                    current_invoice = None
                    
                    for row_num, row in enumerate(all_rows, start=1):
                        invoice_number = row[header_mapping['invoice_number']].strip()
                        if not invoice_number:
                            continue
                            
                        # Skip if we've already processed this invoice
                        if invoice_number in processed_invoices:
                            continue
                            
                        try:
                            # Check if invoice already exists
                            existing_order = session.query(Order).filter(
                                Order.orderNumber == invoice_number
                            ).first()
                            
                            # Find all rows for this invoice
                            invoice_rows = [r for r in all_rows if r['Invoice No'].strip() == invoice_number]
                            
                            # Get customer by name
                            customer_name = row[header_mapping['customer_id']].strip()
                            customer = session.query(Customer).filter(
                                Customer.customerName == customer_name
                            ).first()
                            
                            # Validate all data before processing
                            validation_errors = []
                            
                            if not customer:
                                validation_errors.append({
                                    'row': row_num,
                                    'severity': 'ERROR',
                                    'message': f"Customer not found: {customer_name}"
                                })
                            
                            # Validate products and calculate totals
                            subtotal = 0
                            tax_amount = 0
                            
                            for inv_row in invoice_rows:
                                product_code = inv_row.get('Product/Service', '').strip()
                                if not product_code or product_code.lower() in ['shipping', 'handling fee', 'tax', 'discount']:
                                    continue
                                
                                product = session.query(Product).filter(
                                    Product.productCode == product_code.upper()
                                ).first()
                                
                                if not product:
                                    validation_errors.append({
                                        'row': row_num,
                                        'severity': 'ERROR',
                                        'message': f"Product not found: {product_code} in invoice {invoice_number}"
                                    })
                                    continue
                                
                                # Calculate amounts
                                amount_str = inv_row.get('Product/Service  Amount', '').strip()
                                if amount_str:
                                    try:
                                        amount = float(amount_str.replace('$', '').replace(',', ''))
                                        subtotal += amount
                                    except ValueError:
                                        validation_errors.append({
                                            'row': row_num,
                                            'severity': 'ERROR',
                                            'message': f"Invalid amount for product {product_code}"
                                        })
                                
                                # Add tax if present
                                tax_str = inv_row.get('Product/Service Sales Tax', '').strip()
                                if tax_str:
                                    try:
                                        tax = float(tax_str.replace('$', '').replace(',', ''))
                                        tax_amount += tax
                                    except ValueError:
                                        results['summary']['errors'].append({
                                            'row': row_num,
                                            'severity': 'WARNING',
                                            'message': f"Invalid tax amount for product {product_code}, defaulting to 0"
                                        })
                            
                            # Add all validation errors to results
                            for error in validation_errors:
                                results['summary']['errors'].append(error)
                                if error['severity'] == 'ERROR':
                                    results['summary']['stats']['errors'] += 1
                            
                            # Skip processing if there are any errors
                            if validation_errors:
                                continue
                            
                            total_amount = subtotal + tax_amount
                            now = datetime.utcnow()
                            
                            if existing_order:
                                # Update existing order
                                existing_order.status = OrderStatus.CLOSED if row['Status'] == 'Paid' else OrderStatus.OPEN
                                existing_order.paymentStatus = (PaymentStatus.PAID if row['Status'] == 'Paid'
                                                            else PaymentStatus.UNPAID)
                                existing_order.subtotal = subtotal
                                existing_order.taxAmount = tax_amount
                                existing_order.totalAmount = total_amount
                                existing_order.modifiedAt = now
                                existing_order.sourceData = row
                                
                                # Get existing line items
                                existing_items = {item.productCode: item for item in session.query(OrderItem).filter(
                                    OrderItem.orderId == existing_order.id
                                ).all()}
                                
                                order = existing_order
                                results['summary']['stats']['updated'] += 1
                            else:
                                # Create new order
                                order = Order(
                                    id=generate_uuid(),
                                    orderNumber=invoice_number,
                                    customerId=customer.id,
                                    orderDate=datetime.strptime(row[header_mapping['invoice_date']], '%m-%d-%Y'),
                                    status=OrderStatus.CLOSED if row['Status'] == 'Paid' else OrderStatus.OPEN,
                                    paymentStatus=(PaymentStatus.PAID if row['Status'] == 'Paid'
                                                else PaymentStatus.UNPAID),
                                    subtotal=subtotal,
                                    taxPercent=None,
                                    taxAmount=tax_amount,
                                    totalAmount=total_amount,
                                    billingAddressId=customer.billingAddressId,
                                    shippingAddressId=customer.shippingAddressId,
                                    terms=row.get(header_mapping.get('payment_terms', ''), ''),
                                    dueDate=datetime.strptime(row[header_mapping['due_date']], '%m-%d-%Y') if header_mapping.get('due_date') and row.get(header_mapping['due_date']) else None,
                                    poNumber=row.get(header_mapping.get('po_number', ''), ''),
                                    class_=row.get(header_mapping.get('class', ''), ''),
                                    shippingMethod=row.get(header_mapping.get('shipping_method', ''), ''),
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
                                
                                # Skip non-product rows
                                if not product_code or product_code.lower() in ['shipping', 'handling fee', 'tax', 'discount']:
                                    continue
                                
                                # Get product from database
                                product = session.query(Product).filter(
                                    Product.productCode == product_code.upper()
                                ).first()
                                
                                # Parse quantity and amount
                                try:
                                    quantity = float(inv_row.get('Qty', '1').strip() or '1')
                                    amount_str = inv_row.get('Product/Service  Amount', '0').strip()
                                    amount = float(amount_str.replace('$', '').replace(',', ''))
                                    unit_price = amount / quantity
                                except (ValueError, ZeroDivisionError):
                                    results['summary']['errors'].append({
                                        'row': row_num,
                                        'severity': 'ERROR',
                                        'message': f"Invalid quantity or amount for product {product_code}"
                                    })
                                    continue
                                
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
                                            'message': f"Invalid service date format for product {product_code}"
                                        })
                                
                                # Update or create order item
                                if product_code.upper() in existing_items:
                                    # Update existing item
                                    item = existing_items[product_code.upper()]
                                    item.description = inv_row.get('Product/Service Description', '').strip()
                                    item.quantity = quantity
                                    item.unitPrice = unit_price
                                    item.amount = amount
                                    item.serviceDate = service_date
                                    item.sourceData = inv_row
                                else:
                                    # Create new item
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
                            
                            processed_invoices.add(invoice_number)
                            results['summary']['stats']['line_items'] += line_items_processed
                            
                        except Exception as e:
                            results['success'] = False
                            results['summary']['errors'].append({
                                'row': row_num,
                                'severity': 'ERROR',
                                'message': f"Failed to process invoice {invoice_number}: {str(e)}"
                            })
                            results['summary']['stats']['errors'] += 1
                    
                    results['summary']['stats']['total_invoices'] = len(processed_invoices)
                    
        except Exception as e:
            results['success'] = False
            results['summary']['errors'].append({
                'row': 0,
                'severity': 'CRITICAL',
                'message': f"Failed to process file: {str(e)}"
            })
            
        return results
