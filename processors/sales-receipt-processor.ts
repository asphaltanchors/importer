import { OrderStatus, PaymentStatus } from '@prisma/client';
import { ImportContext } from '../shared/types';
import { BaseOrderProcessor } from '../shared/order-processor';
import { OrderItemData } from '../shared/order-types';
import { parseDate, parseDecimal, processAddress } from '../shared/utils';

interface SalesReceiptRow {
  'QuickBooks Internal Id': string;
  'Sales Receipt No': string;
  'Customer': string;
  'Sales Receipt Date': string;
  'Payment Method': string;
  'Product/Service': string;
  'Product/Service Description': string;
  'Product/Service Quantity': string;
  'Product/Service Rate': string;
  'Product/Service Amount': string;
  'Product/Service Service Date': string;
  'Due Date': string;
  'Ship Date': string;
  'Shipping Method': string;
  'Total Tax': string;
  'Total Amount': string;
  'Created Date': string;
  'Modified Date': string;
  'Billing Address Line 1': string;
  'Billing Address Line 2': string;
  'Billing Address Line 3': string;
  'Billing Address City': string;
  'Billing Address State': string;
  'Billing Address Postal Code': string;
  'Billing Address Country': string;
  'Shipping Address Line 1': string;
  'Shipping Address Line 2': string;
  'Shipping Address Line 3': string;
  'Shipping Address City': string;
  'Shipping Address State': string;
  'Shipping Address Postal Code': string;
  'Shipping Address Country': string;
  [key: string]: string;
}

export class SalesReceiptProcessor extends BaseOrderProcessor {
  private currentReceipt: { quickbooksId: string; rows: SalesReceiptRow[] } | null = null;

  constructor(ctx: ImportContext) {
    super(ctx);
  }

  async processRow(row: SalesReceiptRow): Promise<void> {
    try {
      const stats = this.ctx.stats;
      const quickbooksId = row['QuickBooks Internal Id'];
      
      if (!quickbooksId) {
        stats.warnings.push(`Skipping row: Missing QuickBooks ID`);
        return;
      }

      // If this is a new receipt, process the previous one first
      if (this.currentReceipt && this.currentReceipt.quickbooksId !== quickbooksId) {
        await this.processReceipt(this.currentReceipt.quickbooksId, this.currentReceipt.rows);
        this.currentReceipt = null;
      }

      // Initialize or add to current receipt
      if (!this.currentReceipt) {
        this.currentReceipt = {
          quickbooksId,
          rows: []
        };
      }
      this.currentReceipt.rows.push(row);

      if (this.ctx.debug && quickbooksId === '3A6FF-1715898545') {
        console.log(`Receipt ${quickbooksId}:`);
        console.log(`  - Product/Service: ${row['Product/Service']}`);
        console.log(`  - Current rows: ${this.currentReceipt.rows.length}`);
      }

    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.ctx.stats.warnings.push(`Unexpected error processing row: ${errorMessage}`);
      // Clear current receipt on error
      this.currentReceipt = null;
    }
  }

  async finalize(): Promise<void> {
    try {
      // Process the last receipt if there is one
      if (this.currentReceipt) {
        if (this.ctx.debug) {
          console.log('\nProcessing final receipt in finalize():');
          console.log(`  - QuickBooks ID: ${this.currentReceipt.quickbooksId}`);
          console.log(`  - Row count: ${this.currentReceipt.rows.length}`);
        }
        await this.processReceipt(this.currentReceipt.quickbooksId, this.currentReceipt.rows);
        this.currentReceipt = null;
      }
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.ctx.stats.warnings.push(`Failed to process final receipt: ${errorMessage}`);
    } finally {
      // Final garbage collection
      global.gc?.();
    }
  }

  private createLineItem(row: SalesReceiptRow): OrderItemData | null {
    // Skip tax and shipping line items
    if (!row['Product/Service'] || 
        ['NJ Sales Tax', 'Shipping', 'Discount'].includes(row['Product/Service'])) {
      return null;
    }

    return {
      productCode: row['Product/Service'],
      description: row['Product/Service Description'],
      quantity: parseFloat(row['Product/Service Quantity'] || '0'),
      unitPrice: parseFloat(row['Product/Service Rate'] || '0'),
      amount: parseFloat(row['Product/Service Amount'] || '0'),
      serviceDate: parseDate(row['Product/Service Service Date'])
    };
  }

  private determineClass(orderNumber: string, customerName: string): string | null {
    // Check for eStore pattern: 3D- followed by 4-5 digits
    if (/^3D-\d{4,5}$/.test(orderNumber)) {
      return 'eStore';
    }
    
    // Check for Amazon pattern: XXX-XXXXXXX (where X is any character, not just uppercase letters)
    if (/^[A-Z0-9]{3}-\d{7}$/.test(orderNumber)) {
      if (customerName.includes('Amazon FBA')) {
        return 'Amazon FBA';
      }
      if (customerName.includes('Amazon')) {
        return 'Amazon Direct';
      }
    }
    
    return null;
  }

  private async processReceipt(quickbooksId: string, rows: SalesReceiptRow[]): Promise<void> {
    const stats = this.ctx.stats;
    
    // Use first row as primary since CSV is sorted
    const primaryRow = rows[0];
    
    // Validate total amount
    if (parseDecimal(primaryRow['Total Amount']) <= 0) {
      stats.warnings.push(`Receipt ${quickbooksId}: Invalid total amount`);
      return;
    }
    const customerName = primaryRow['Customer'];

    if (!customerName) {
      stats.warnings.push(`Receipt ${quickbooksId}: Missing Customer Name`);
      return;
    }

    // Create line items from all valid rows
    if (this.ctx.debug && quickbooksId === '3A6FF-1715898545') {
      console.log('\n=== START RECEIPT PROCESSING ===');
      console.log(`Processing receipt ${quickbooksId}:`);
      console.log(`Total rows to process: ${rows.length}`);
      rows.forEach(row => {
        console.log('\nRow details:');
        console.log(`  - Product: ${row['Product/Service']}`);
        console.log(`  - Description: ${row['Product/Service Description']}`);
        console.log(`  - Quantity: ${row['Product/Service Quantity']}`);
        console.log(`  - Rate: ${row['Product/Service Rate']}`);
        console.log(`  - Amount: ${row['Product/Service Amount']}`);
      });
    }

    const lineItems: OrderItemData[] = [];
    for (const row of rows) {
      if (this.ctx.debug && quickbooksId === '3A6FF-1715898545') {
        console.log('\nProcessing row for line item:');
        console.log(`  - Product/Service: ${row['Product/Service']}`);
        console.log(`  - Is filtered product: ${['NJ Sales Tax', 'Shipping', 'Discount'].includes(row['Product/Service'])}`);
      }

      const item = this.createLineItem(row);
      if (item) {
        if (this.ctx.debug && quickbooksId === '3A6FF-1715898545') {
          console.log('  - Created line item:');
          console.log(`    * Product Code: ${item.productCode}`);
          console.log(`    * Description: ${item.description}`);
          console.log(`    * Quantity: ${item.quantity}`);
          console.log(`    * Unit Price: ${item.unitPrice}`);
          console.log(`    * Amount: ${item.amount}`);
        }
        lineItems.push(item);
      } else if (this.ctx.debug && quickbooksId === '3A6FF-1715898545') {
        console.log('  - Row filtered out');
      }
    }

    if (this.ctx.debug && quickbooksId === '3A6FF-1715898545') {
      console.log(`\nFinal line items count: ${lineItems.length}`);
      console.log('=== END RECEIPT PROCESSING ===\n');
    }

    // Process addresses from primary row
    const billingAddress = await processAddress(this.ctx, {
      line1: primaryRow['Billing Address Line 1'],
      line2: primaryRow['Billing Address Line 2'],
      line3: primaryRow['Billing Address Line 3'],
      city: primaryRow['Billing Address City'],
      state: primaryRow['Billing Address State'],
      postalCode: primaryRow['Billing Address Postal Code'],
      country: primaryRow['Billing Address Country'],
    });

    const shippingAddress = await processAddress(this.ctx, {
      line1: primaryRow['Shipping Address Line 1'],
      line2: primaryRow['Shipping Address Line 2'],
      line3: primaryRow['Shipping Address Line 3'],
      city: primaryRow['Shipping Address City'],
      state: primaryRow['Shipping Address State'],
      postalCode: primaryRow['Shipping Address Postal Code'],
      country: primaryRow['Shipping Address Country'],
    });

    // Parse dates from primary row
    const orderDate = parseDate(primaryRow['Sales Receipt Date']) || new Date();
    const dueDate = parseDate(primaryRow['Due Date']);
    const createdDate = parseDate(primaryRow['Created Date']) || new Date();
    const modifiedDate = parseDate(primaryRow['Modified Date']) || new Date();
    const shipDate = parseDate(primaryRow['Ship Date']);

    // Parse amounts from primary row
    const taxAmount = parseDecimal(primaryRow['Total Tax']);
    const totalAmount = parseDecimal(primaryRow['Total Amount']);

    // Create or update order
    const existingOrder = await this.ctx.prisma.order.findFirst({
      where: { quickbooksId }
    });

    // Find or create customer
    const customerId = await this.findOrCreateCustomer(customerName);

    const orderData = {
      orderNumber: primaryRow['Sales Receipt No'],
      orderDate,
      customerId,
      billingAddressId: billingAddress?.id || null,
      shippingAddressId: shippingAddress?.id || null,
      status: OrderStatus.CLOSED, // Sales receipts are always closed/paid
      paymentStatus: PaymentStatus.PAID,
      paymentMethod: primaryRow['Payment Method'] || null,
      paymentDate: orderDate, // Sales receipts are paid at time of sale
      dueDate,
      terms: null,
      subtotal: totalAmount - taxAmount,
      taxAmount,
      taxPercent: taxAmount > 0 ? (taxAmount / (totalAmount - taxAmount)) * 100 : 0,
      totalAmount,
      shipDate,
      shippingMethod: primaryRow['Shipping Method'] || null,
      modifiedAt: modifiedDate,
      quickbooksId,
      class: this.determineClass(primaryRow['Sales Receipt No'], customerName),
      sourceData: primaryRow,
    };

    try {
      const order = await this.createOrUpdateOrder(orderData, existingOrder);
      
      // Process all order items at once
      await this.processOrderItems(order.id, lineItems);

      stats.processed++;
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      throw new Error(`Failed to process order ${orderData.orderNumber}: ${errorMessage}`);
    } finally {
      // Clear references to large objects
      lineItems.length = 0;
    }
  }
}
