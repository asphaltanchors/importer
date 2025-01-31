import { PrismaClient } from '@prisma/client';
import { ImportContext } from './types';
import { OrderImportStats, OrderItemData, BaseOrderData } from './order-types';

export abstract class BaseOrderProcessor {
  protected ctx: ImportContext;

  constructor(ctx: ImportContext) {
    this.ctx = ctx;
  }

  protected async processOrderItems(orderId: string, items: OrderItemData[]) {
    const stats = this.ctx.stats as OrderImportStats;

    // Delete any existing items for this order
    await this.ctx.prisma.orderItem.deleteMany({
      where: { orderId }
    });

    // Create new items using a transaction to ensure data consistency
    await this.ctx.prisma.$transaction(async (prisma) => {
      for (const item of items) {
        // Create or update product
        const product = await prisma.product.upsert({
          where: { productCode: item.productCode },
          update: {
            name: item.description || item.productCode,
            description: item.description,
            modifiedAt: new Date()
          },
          create: {
            productCode: item.productCode,
            name: item.description || item.productCode,
            description: item.description,
            createdAt: new Date(),
            modifiedAt: new Date()
          }
        });

        if (this.ctx.debug) {
          console.log(`Upserted product: ${product.productCode} (${product.name})`);
        }

        // Create order item with guaranteed product reference
        await prisma.orderItem.create({
          data: {
            orderId,
            productCode: item.productCode,
            description: item.description,
            quantity: item.quantity,
            unitPrice: item.unitPrice,
            amount: item.amount,
            serviceDate: item.serviceDate
          }
        });

        if (this.ctx.debug) {
          console.log(`Created order item: ${item.productCode} (${item.quantity} @ ${item.unitPrice})`);
        }
      }
    });

    // Update stats after successful transaction
    for (const item of items) {
      const existingProduct = await this.ctx.prisma.product.findUnique({
        where: { productCode: item.productCode }
      });
      if (existingProduct) {
        stats.productsUpdated++;
      } else {
        stats.productsCreated++;
      }
    }
  }

  protected async findOrCreateCustomer(customerName: string): Promise<string> {
    // Look for existing customer
    const existingCustomer = await this.ctx.prisma.customer.findFirst({
      where: { customerName }
    });

    if (existingCustomer) {
      if (this.ctx.debug) console.log(`Found existing customer: ${customerName}`);
      return existingCustomer.id;
    }

    // Create new customer
    const newCustomer = await this.ctx.prisma.customer.create({
      data: {
        quickbooksId: `IMPORT-${Date.now()}`, // Generate a unique ID
        customerName,
        status: 'ACTIVE',
        createdAt: new Date(),
        modifiedAt: new Date(),
        sourceData: { importedName: customerName }
      }
    });

    if (this.ctx.debug) console.log(`Created new customer: ${customerName}`);
    return newCustomer.id;
  }

  protected async createOrUpdateOrder(orderData: BaseOrderData, existingOrder: { id: string } | null) {
    const stats = this.ctx.stats as OrderImportStats;

    if (existingOrder) {
      const order = await this.ctx.prisma.order.update({
        where: { id: existingOrder.id },
        data: orderData,
      });
      if (this.ctx.debug) console.log(`Updated order: ${order.orderNumber}`);
      stats.ordersUpdated++;
      return order;
    } else {
      const order = await this.ctx.prisma.order.create({
        data: {
          ...orderData,
          createdAt: orderData.modifiedAt,
        },
      });
      if (this.ctx.debug) console.log(`Created order: ${order.orderNumber}`);
      stats.ordersCreated++;
      return order;
    }
  }

  // Abstract methods that must be implemented by specific processors
  abstract processRow(row: any): Promise<void>;
}
