"""Sales receipt data processor."""

from typing import Dict, Any, List, Optional, Tuple
import logging
import pandas as pd
from datetime import datetime
import time
import click
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..utils import generate_uuid
from ..utils.normalization import normalize_customer_name, find_customer_by_name
from ..utils.product_mapping import map_product_code
from ..utils.csv_normalization import (
    normalize_dataframe_columns, 
    validate_required_columns,
    validate_json_data
)
from ..utils.system_products import is_tax_product, is_shipping_product
from ..db.models import Order, OrderStatus, PaymentStatus, Customer, Product, OrderItem
from .address import AddressProcessor
from .base import BaseProcessor
from .error_tracker import ErrorTracker

class SalesReceiptProcessor(BaseProcessor):
    """Process sales receipts from sales data."""
    
    def __init__(self, session_manager, batch_size: int = 50, error_limit: int = 1000):
        """Initialize the processor."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session_manager = session_manager
        super().__init__(None, batch_size)  # We'll manage sessions ourselves
        self.address_processor = None  # Will be initialized per session
        self.error_tracker = ErrorTracker()
        self.error_limit = error_limit
        
        # Get debug status from click context
        ctx = click.get_current_context(silent=True)
        self.debug = bool(ctx and ctx.obj.get('debug'))
        if self.debug:
            self.logger.debug("Debug mode enabled for SalesReceiptProcessor")
        
        self.stats.update({
            'total_receipts': 0,
            'created': 0,
            'updated': 0,
            'errors': 0,
            'customers_not_found': 0,  # Keep this to track lookup failures
            'processing_time': 0.0,
            'db_operation_time': 0.0
        })
        
        # Field mappings specific to sales receipts
        self.field_mappings = {
            'receipt_number': ['Sales Receipt No'],
            'receipt_date': ['Sales Receipt Date'],
            'customer_id': ['Customer'],
            'payment_method': ['Payment Method'],
            'class': ['Class'],
            'shipping_method': ['Ship Via']
        }
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing."""
        if self.debug:
            self.logger.debug(f"Starting data validation for {len(df)} rows")
            start_time = time.time()
            
        critical_issues = []
        warnings = []
        
        # Check required columns
        required_columns = ['Sales Receipt No', 'Sales Receipt Date', 'Customer']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            msg = f"Missing required columns: {', '.join(missing_columns)}"
            if self.debug:
                self.logger.debug(f"Critical issue found: {msg}")
            critical_issues.append(msg)
            return critical_issues, warnings
        
        # Check for empty rows
        empty_rows = df[df[required_columns].isna().all(axis=1)]
        if not empty_rows.empty:
            msg = (f"Found {len(empty_rows)} empty rows that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_rows.index[:3]))}")
            if self.debug:
                self.logger.debug(f"Warning: {msg}")
            warnings.append(msg)
        
        # Check for missing data
        for col in required_columns:
            nan_rows = df[df[col].isna() & ~df[required_columns].isna().all(axis=1)]
            if not nan_rows.empty:
                msg = (f"Found {len(nan_rows)} rows with missing {col} that will be skipped. "
                      f"First few: {', '.join(map(str, nan_rows.index[:3]))}")
                if self.debug:
                    self.logger.debug(f"Warning: {msg}")
                warnings.append(msg)
        
        # Check date format
        if 'Sales Receipt Date' in df.columns:
            invalid_dates = []
            for idx, date_str in df['Sales Receipt Date'].items():
                if pd.notna(date_str):
                    try:
                        datetime.strptime(str(date_str), '%m-%d-%Y')
                    except ValueError:
                        invalid_dates.append(f"Row {idx}: {date_str}")
                        if len(invalid_dates) >= 3:
                            break
            if invalid_dates:
                msg = (f"Found rows with invalid date formats (these will be skipped). "
                      f"Examples: {', '.join(invalid_dates)}")
                if self.debug:
                    self.logger.debug(f"Warning: {msg}")
                warnings.append(msg)
        
        if self.debug:
            self.logger.debug(f"Validation completed in {time.time() - start_time:.3f}s")
            self.logger.debug(f"Found {len(critical_issues)} critical issues and {len(warnings)} warnings")
            
        return critical_issues, warnings
        
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the data in batches with error handling and progress tracking."""
        if self.debug:
            start_time = time.time()
            self.logger.debug(f"Starting processing of {len(data)} rows")
        
        # Validate data first
        critical_issues, warnings = self.validate_data(data)
        
        # Log warnings but continue
        if warnings:
            self.logger.warning("\nValidation warnings:")
            for warning in warnings:
                self.logger.warning(f"  - {warning}")
            self.logger.warning("Continuing with processing...")
        
        # Only stop on actual critical issues
        if critical_issues:
            self.logger.error("\nData validation failed:")
            for issue in critical_issues:
                self.logger.error(f"  - {issue}")
            self.stats['errors'] += len(critical_issues)
            return pd.DataFrame()
            
        total_rows = len(data)
        total_batches = (total_rows + self.batch_size - 1) // self.batch_size
        
        if self.debug:
            self.logger.debug(f"Processing {total_rows} rows in {total_batches} batches of {self.batch_size}")
            
        result_dfs = []
        
        with click.progressbar(
            length=total_rows,
            label='Processing receipts',
            item_show_func=lambda x: f"Processed {self.stats['total_receipts']}/{total_rows} receipts"
        ) as bar:
            for batch_num, start_idx in enumerate(range(0, total_rows, self.batch_size), 1):
                if self.debug:
                    batch_start = time.time()
                    self.logger.debug(f"\nStarting batch {batch_num}/{total_batches}")
                
                batch_df = data.iloc[start_idx:start_idx + self.batch_size].copy()
                
                try:
                    # Process batch
                    processed_batch = self._process_batch(batch_df)
                    result_dfs.append(processed_batch)
                    self.stats['successful_batches'] += 1
                    
                    if self.debug:
                        batch_time = time.time() - batch_start
                        self.stats['processing_time'] += batch_time
                        self.logger.debug(f"Batch {batch_num} completed in {batch_time:.3f}s")
                        self.logger.debug(f"Memory usage: {batch_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
                    
                    # Update progress
                    bar.update(len(batch_df))
                    
                    # Show periodic stats
                    if batch_num % 5 == 0:
                        self._show_progress_stats(total_rows)
                    
                    # Check error limit
                    if self.stats['errors'] >= self.error_limit:
                        self.logger.error(f"\nStopping: Error limit ({self.error_limit}) reached")
                        break
                    
                except Exception as e:
                    error_context = {
                        'batch_number': batch_num,
                        'start_index': start_idx,
                        'batch_size': len(batch_df),
                        'error': str(e)
                    }
                    self.error_tracker.add_error(
                        'BATCH_PROCESSING_ERROR',
                        f"Error in batch {batch_num}",
                        error_context
                    )
                    if self.debug:
                        self.logger.debug(f"Batch {batch_num} failed: {error_context}", exc_info=True)
                    self.stats['failed_batches'] += 1
                    self.stats['errors'] += 1
                    continue
                
                self.stats['total_processed'] += len(batch_df)
        
        # Log error summary at the end
        self.error_tracker.log_summary(self.logger)
        
        if self.debug:
            total_time = time.time() - start_time
            self.logger.debug("\nProcessing Summary:")
            self.logger.debug(f"Total time: {total_time:.3f}s")
            self.logger.debug(f"Processing time: {self.stats['processing_time']:.3f}s")
            self.logger.debug(f"Database operation time: {self.stats['db_operation_time']:.3f}s")
        
        # Combine all results
        return pd.concat(result_dfs, ignore_index=True) if result_dfs else pd.DataFrame()
    
    def _show_progress_stats(self, total_rows: int) -> None:
        """Show current processing statistics."""
        self.logger.info("\nCurrent Progress:")
        self.logger.info(f"  Processed: {self.stats['total_receipts']}/{total_rows} receipts")
        self.logger.info(f"  Created: {self.stats['created']}")
        self.logger.info(f"  Updated: {self.stats['updated']}")
        self.logger.info(f"  Errors: {self.stats['errors']}")
        self.logger.info(f"  Customer Lookup Failures: {self.stats['customers_not_found']}")
        
        if self.debug:
            self.logger.debug("Performance Stats:")
            self.logger.debug(f"  Processing time: {self.stats['processing_time']:.3f}s")
            self.logger.debug(f"  Database operation time: {self.stats['db_operation_time']:.3f}s")
    
    def _process_batch(self, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of sales receipt rows."""
        if self.debug:
            batch_start = time.time()
            self.logger.debug(f"Starting batch processing for {len(batch_df)} rows")
            
        batch_df['order_id'] = None
        
        try:
            with self.session_manager as session:
                session_id = id(session)
                if self.debug:
                    self.logger.debug(f"Created session {session_id}")
                
                # Map CSV headers to standardized fields
                header_mapping = {}
                for std_field, possible_names in self.field_mappings.items():
                    for name in possible_names:
                        if name in batch_df.columns:
                            header_mapping[std_field] = name
                            if self.debug:
                                self.logger.debug(f"Mapped field {std_field} -> {name}")
                            break
                
                if 'receipt_number' not in header_mapping:
                    raise ValueError("Could not find sales receipt number column")
                
                # Group rows by receipt number
                if self.debug:
                    self.logger.debug("Grouping rows by receipt number")
                    group_start = time.time()
                    
                receipts = {}
                for idx, row in batch_df.iterrows():
                    receipt_number = str(row[header_mapping['receipt_number']]).strip()
                    if receipt_number:
                        if receipt_number not in receipts:
                            receipts[receipt_number] = []
                        receipts[receipt_number].append((idx, row))
                
                if self.debug:
                    self.logger.debug(f"Grouped {len(batch_df)} rows into {len(receipts)} receipts in {time.time() - group_start:.3f}s")
                
                # Process each receipt
                for receipt_number, receipt_rows in receipts.items():
                    try:
                        if self.debug:
                            receipt_start = time.time()
                            self.logger.debug(f"\nProcessing receipt {receipt_number}")
                        
                        idx, row = receipt_rows[0]  # Use first row for header info
                        
                        # Check if receipt exists
                        if self.debug:
                            self.logger.debug(f"Checking for existing receipt {receipt_number}")
                            query_start = time.time()
                            
                        existing_order = session.query(Order).filter(
                            Order.orderNumber == receipt_number
                        ).with_for_update().first()
                        
                        if self.debug:
                            self.stats['db_operation_time'] += time.time() - query_start
                            if existing_order:
                                self.logger.debug(f"Found existing receipt {receipt_number}")
                            else:
                                self.logger.debug(f"Receipt {receipt_number} is new")
                        
                        # Get customer name
                        customer_name = row[header_mapping['customer_id']]
                        
                        # Skip rows with missing required data
                        if pd.isna(customer_name) or pd.isna(row[header_mapping['receipt_date']]):
                            if self.debug:
                                self.logger.debug(f"Skipping receipt {receipt_number} - missing required data")
                            continue
                        
                        # Look up customer - should exist since we processed customers first
                        customer_name = str(customer_name).strip()
                        customer = session.query(Customer).filter(
                            Customer.customerName == customer_name
                        ).first()
                        
                        if not customer:
                            # Try normalized name as fallback
                            normalized_name = normalize_customer_name(customer_name)
                            customer = session.query(Customer).filter(
                                Customer.customerName == normalized_name
                            ).first()
                            
                        if not customer:
                            # Special case for Amazon FBA - try with city
                            if customer_name.upper() == 'AMAZON FBA':
                                city = row.get('Billing Address City', '')
                                if not pd.isna(city):
                                    full_name = f"Amazon FBA - {str(city).strip()}"
                                    customer = session.query(Customer).filter(
                                        Customer.customerName == full_name
                                    ).first()
                            
                        if not customer:
                            error_context = {
                                'receipt_number': receipt_number,
                                'customer_name': customer_name
                            }
                            self.error_tracker.add_error(
                                'CUSTOMER_NOT_FOUND',
                                f"Customer not found after customer processing phase: {customer_name}",
                                error_context
                            )
                            if self.debug:
                                self.logger.debug(f"Customer lookup failed: {error_context}")
                            self.stats['customers_not_found'] += 1
                            self.stats['errors'] += 1
                            continue
                        
                        # Parse receipt date
                        try:
                            receipt_date = datetime.strptime(row[header_mapping['receipt_date']], '%m-%d-%Y')
                        except (ValueError, KeyError) as e:
                            error_context = {
                                'receipt_number': receipt_number,
                                'date_value': row.get(header_mapping.get('receipt_date', ''), 'missing'),
                                'error': str(e)
                            }
                            self.error_tracker.add_error(
                                'INVALID_DATE',
                                f"Invalid receipt date for {receipt_number}",
                                error_context
                            )
                            if self.debug:
                                self.logger.debug(f"Date parsing error: {error_context}")
                            self.stats['errors'] += 1
                            continue
                        
                        # Initialize totals to 0 - will be calculated by LineItemProcessor
                        subtotal = 0
                        tax_amount = 0
                        total_amount = 0
                        now = datetime.utcnow()
                        
                        # Initialize address processor if needed
                        if not self.address_processor:
                            if self.debug:
                                self.logger.debug("Initializing address processor")
                            self.address_processor = AddressProcessor(session)
                        
                        # Create addresses if provided
                        if self.debug:
                            self.logger.debug("Processing addresses")
                            addr_start = time.time()
                            
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
                        
                        if self.debug:
                            self.logger.debug(f"Address processing completed in {time.time() - addr_start:.3f}s")
                        
                        if existing_order:
                            if self.debug:
                                self.logger.debug(f"Updating existing order {receipt_number}")
                                update_start = time.time()
                            
                            # Update existing order
                            existing_order.status = OrderStatus.CLOSED
                            existing_order.paymentStatus = PaymentStatus.PAID
                            existing_order.subtotal = subtotal
                            existing_order.taxAmount = tax_amount
                            existing_order.totalAmount = total_amount
                            existing_order.customerId = customer.id
                            existing_order.billingAddressId = billing_id
                            existing_order.shippingAddressId = shipping_id
                            existing_order.class_ = row.get(header_mapping.get('class', ''), '')
                            existing_order.shippingMethod = row.get(header_mapping.get('shipping_method', ''), '')
                            existing_order.paymentMethod = row.get(header_mapping.get('payment_method', ''), 'Cash')
                            existing_order.quickbooksId = row.get('QuickBooks Internal Id', '')
                            existing_order.modifiedAt = now
                            existing_order.sourceData = validate_json_data(row.to_dict())
                            
                            order = existing_order
                            self.stats['updated'] += 1
                            
                            if self.debug:
                                self.logger.debug(f"Order update completed in {time.time() - update_start:.3f}s")
                        else:
                            if self.debug:
                                self.logger.debug(f"Creating new order {receipt_number}")
                                create_start = time.time()
                            
                            # Create new order
                            order = Order(
                                id=generate_uuid(),
                                orderNumber=receipt_number,
                                customerId=customer.id,
                                orderDate=receipt_date,
                                status=OrderStatus.CLOSED,
                                paymentStatus=PaymentStatus.PAID,
                                subtotal=subtotal,
                                taxPercent=None,
                                taxAmount=tax_amount,
                                totalAmount=total_amount,
                                billingAddressId=billing_id,
                                shippingAddressId=shipping_id,
                                class_=row.get(header_mapping.get('class', ''), ''),
                                shippingMethod=row.get(header_mapping.get('shipping_method', ''), ''),
                                paymentMethod=row.get(header_mapping.get('payment_method', ''), 'Cash'),
                                quickbooksId=row.get('QuickBooks Internal Id', ''),
                                createdAt=now,
                                modifiedAt=now,
                                sourceData=validate_json_data(row.to_dict())
                            )
                            session.add(order)
                            self.stats['created'] += 1
                            
                            if self.debug:
                                self.logger.debug(f"Order creation completed in {time.time() - create_start:.3f}s")
                        
                        # Update order ID in DataFrame
                        for idx, _ in receipt_rows:
                            batch_df.at[idx, 'order_id'] = order.id
                        
                        self.stats['total_receipts'] += 1
                        
                        if self.debug:
                            receipt_time = time.time() - receipt_start
                            self.logger.debug(f"Receipt {receipt_number} processed in {receipt_time:.3f}s")
                        
                    except Exception as e:
                        error_context = {
                            'receipt_number': receipt_number,
                            'error': str(e)
                        }
                        self.error_tracker.add_error(
                            'RECEIPT_PROCESSING_ERROR',
                            f"Failed to process sales receipt {receipt_number}",
                            error_context
                        )
                        if self.debug:
                            self.logger.debug(f"Receipt processing error: {error_context}", exc_info=True)
                        self.stats['errors'] += 1
                        continue
                
                if self.debug:
                    batch_time = time.time() - batch_start
                    self.logger.debug(f"Batch processing completed in {batch_time:.3f}s")
                    self.stats['processing_time'] += batch_time
                
                return batch_df
                
        except Exception as e:
            if self.debug:
                self.logger.debug(f"Batch failed with error: {str(e)}", exc_info=True)
            raise
