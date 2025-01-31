import { OrderStatus, PaymentStatus } from '@prisma/client';
import { ImportContext } from '../shared/types';
import { BaseOrderProcessor } from '../shared/order-processor';
import { OrderItemData } from '../shared/order-types';
import { parseDate, parseDecimal, processAddress } from '../shared/utils';

interface InvoiceRow {
  'QuickBooks Internal Id': string;
  'Invoice No': string;
  'Customer': string;
  'Invoice Date': string;
  'Product/Service': string;
  'Product/Service Description': string;
  'Product/Service Quantity': string;
  'Product/Service Rate': string;
  'Product/Service  Amount': string;
  'Product/Service Class': string;
  'Product/Service Service Date': string;
  'Ship Date': string;
  'Shipping Method': string;
  'Due Date': string;
  'Terms': string;
  'Status': string;
  'Total Tax': string;
  'Total Amount': string;
  'Created Date': string;
  'Modified Date': string;
  'Billing Address Line1': string;
  'Billing Address Line2': string;
  'Billing Address Line3': string;
  'Billing Address Line4': string;
  'Billing Address Line5': string;
  'Billing Address City': string;
  'Billing Address State': string;
  'Billing Address Postal Code': string;
  'Billing Address Country': string;
  'Shipping Address Line1': string;
  'Shipping Address Line2': string;
  'Shipping Address Line3': string;
  'Shipping Address Line4': string;
  'Shipping Address Line5': string;
  'Shipping Address City': string;
  'Shipping Address State': string;
  'Shipping Address Postal Code': string;
  'Shipping Address Country': string;
  [key: string]: string;
}

export class InvoiceProcessor extends BaseOrderProcessor {
  private currentInvoice: { invoiceNo: string; rows: InvoiceRow[] } | null = null;

  constructor(ctx: ImportContext) {
    super(ctx);
  }

  async processRow(row: InvoiceRow): Promise<void> {
    try {
      const stats = this.ctx.stats;
      const invoiceNo = row['Invoice No'];
      
      if (!invoiceNo) {
        stats.warnings.push(`Skipping row: Missing Invoice Number`);
        return;
      }

      // If this is a new invoice, process the previous one first
      if (this.currentInvoice && this.currentInvoice.invoiceNo !== invoiceNo) {
        await this.processInvoice(this.currentInvoice.invoiceNo, this.currentInvoice.rows);
        this.currentInvoice = null;
      }

      // Initialize or add to current invoice
      if (!this.currentInvoice) {
        this.currentInvoice = {
          invoiceNo,
          rows: []
        };
      }
      this.currentInvoice.rows.push(row);

    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.ctx.stats.warnings.push(`Unexpected error processing row: ${errorMessage}`);
      // Clear current invoice on error
      this.currentInvoice = null;
    }
  }

  private createLineItem(row: InvoiceRow): OrderItemData | null {
    // Skip empty lines or special items
    if (!row['Product/Service'] || 
        ['NJ Sales Tax', 'Shipping', 'Handling Fee', 'Discount'].includes(row['Product/Service'])) {
      if (this.ctx.debug) {
        console.log(`Skipping line: ${row['Product/Service'] || 'Empty product'}`);
      }
      return null;
    }

    return {
      productCode: row['Product/Service'],
      description: row['Product/Service Description'],
      quantity: parseFloat(row['Product/Service Quantity'] || '0'),
      unitPrice: parseFloat(row['Product/Service Rate'] || '0'),
      amount: parseFloat(row['Product/Service  Amount'] || '0'),
      serviceDate: parseDate(row['Product/Service Service Date'])
    };
  }

  async finalize(): Promise<void> {
    try {
      // Process the last invoice if there is one
      if (this.currentInvoice) {
        await this.processInvoice(this.currentInvoice.invoiceNo, this.currentInvoice.rows);
        this.currentInvoice = null;
      }
    } catch (error: any) {
      const errorMessage = error?.message || 'Unknown error';
      this.ctx.stats.warnings.push(`Failed to process final invoice: ${errorMessage}`);
    }
  }

  private async processInvoice(invoiceNo: string, rows: InvoiceRow[]): Promise<void> {
    const stats = this.ctx.stats;
    
    // Use first row as primary since CSV is sorted
    const primaryRow = rows[0];
    
    // Validate total amount
    if (parseDecimal(primaryRow['Total Amount']) <= 0) {
      stats.warnings.push(`Invoice ${invoiceNo}: Invalid total amount`);
      return;
    }
    const customerName = primaryRow['Customer'];

    if (!customerName) {
      stats.warnings.push(`Invoice ${invoiceNo}: Missing Customer Name`);
      return;
    }

    // Create line items from all valid rows
    const lineItems: OrderItemData[] = rows
      .map(row => this.createLineItem(row))
      .filter((item): item is OrderItemData => item !== null);

    // Process addresses from primary row
    const billingAddress = await processAddress(this.ctx, {
      line1: primaryRow['Billing Address Line1'],
      line2: [primaryRow['Billing Address Line2'], primaryRow['Billing Address Line3']]
        .filter(Boolean)
        .join(', '),
      line3: [primaryRow['Billing Address Line4'], primaryRow['Billing Address Line5']]
        .filter(Boolean)
        .join(', '),
      city: primaryRow['Billing Address City'],
      state: primaryRow['Billing Address State'],
      postalCode: primaryRow['Billing Address Postal Code'],
      country: primaryRow['Billing Address Country'],
    });

    const shippingAddress = await processAddress(this.ctx, {
      line1: primaryRow['Shipping Address Line1'],
      line2: [primaryRow['Shipping Address Line2'], primaryRow['Shipping Address Line3']]
        .filter(Boolean)
        .join(', '),
      line3: [primaryRow['Shipping Address Line4'], primaryRow['Shipping Address Line5']]
        .filter(Boolean)
        .join(', '),
      city: primaryRow['Shipping Address City'],
      state: primaryRow['Shipping Address State'],
      postalCode: primaryRow['Shipping Address Postal Code'],
      country: primaryRow['Shipping Address Country'],
    });

    // Parse dates from primary row
    const orderDate = parseDate(primaryRow['Invoice Date']) || new Date();
    const dueDate = parseDate(primaryRow['Due Date']);
    const createdDate = parseDate(primaryRow['Created Date']) || new Date();
    const modifiedDate = parseDate(primaryRow['Modified Date']) || new Date();
    const shipDate = parseDate(primaryRow['Ship Date']);

    // Parse amounts from primary row (the one with total)
    const taxAmount = parseDecimal(primaryRow['Total Tax']);
    const totalAmount = parseDecimal(primaryRow['Total Amount']);

    // Find existing order by QuickBooks ID or order number
    const existingOrder = await this.ctx.prisma.order.findFirst({
      where: {
        OR: [
          { quickbooksId: primaryRow['QuickBooks Internal Id'] },
          { orderNumber: primaryRow['Invoice No'] }
        ]
      }
    });

    // Find or create customer
    const customerId = await this.findOrCreateCustomer(customerName);

    const orderData = {
      orderNumber: primaryRow['Invoice No'],
      orderDate,
      customerId,
      billingAddressId: billingAddress?.id || null,
      shippingAddressId: shippingAddress?.id || null,
      status: primaryRow['Status'] === 'Paid' ? OrderStatus.CLOSED : OrderStatus.OPEN,
      paymentStatus: primaryRow['Status'] === 'Paid' ? PaymentStatus.PAID : PaymentStatus.UNPAID,
      paymentMethod: 'Invoice',
      paymentDate: null,
      dueDate,
      terms: primaryRow['Terms'] || null,
      subtotal: totalAmount - taxAmount,
      taxAmount,
      taxPercent: taxAmount > 0 ? (taxAmount / (totalAmount - taxAmount)) * 100 : 0,
      totalAmount,
      shipDate,
      shippingMethod: primaryRow['Shipping Method'] || null,
      modifiedAt: modifiedDate,
      quickbooksId: primaryRow['QuickBooks Internal Id'],
      class: primaryRow['Product/Service Class'] || null,
      sourceData: primaryRow,
    };

    const order = await this.createOrUpdateOrder(orderData, existingOrder);
    await this.processOrderItems(order.id, lineItems);

    stats.processed++;
  }
}
