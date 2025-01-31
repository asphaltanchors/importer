import { OrderStatus, PaymentStatus } from '@prisma/client';
import { BaseImportStats } from './types';

export interface OrderItemData {
  productCode: string;
  description: string;
  quantity: number;
  unitPrice: number;
  amount: number;
  serviceDate?: Date | null;
  class?: string;
}

export interface OrderImportStats extends BaseImportStats {
  ordersCreated: number;
  ordersUpdated: number;
  productsCreated: number;
  productsUpdated: number;
  addressesCreated: number;
}

export interface BaseOrderData {
  orderNumber: string;
  orderDate: Date;
  customerId: string;
  billingAddressId: string | null;
  shippingAddressId: string | null;
  status: OrderStatus;
  paymentStatus: PaymentStatus;
  paymentMethod: string | null;
  paymentDate: Date | null;
  dueDate: Date | null;
  terms: string | null;
  subtotal: number;
  taxAmount: number;
  taxPercent: number;
  totalAmount: number;
  shipDate: Date | null;
  shippingMethod: string | null;
  modifiedAt: Date;
  quickbooksId: string;
  sourceData: any;
}
