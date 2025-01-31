import { PrismaClient } from '@prisma/client';
import { ImportContext } from './shared/types';
import { OrderImportStats } from './shared/order-types';
import { createCsvParser, processImport, setupImportCommand } from './shared/utils';
import { SalesReceiptProcessor } from './processors/sales-receipt-processor';

// Initialize Prisma with recommended settings for large imports
const prisma = new PrismaClient({
  log: ['warn', 'error'],
  datasources: {
    db: {
      url: process.env.DATABASE_URL
    }
  }
});

async function importSalesReceipts(filePath: string, debug: boolean, options: { skipLines?: number }) {
  // Enable garbage collection for better memory management
  if (!global.gc) {
    console.log('Garbage collection is not exposed. Run with --expose-gc flag for better memory management.');
  }

  const stats: OrderImportStats = {
    processed: 0,
    ordersCreated: 0,
    ordersUpdated: 0,
    productsCreated: 0,
    productsUpdated: 0,
    addressesCreated: 0,
    warnings: [],
  };

  const ctx: ImportContext = {
    prisma,
    debug,
    stats,
  };

  const processor = new SalesReceiptProcessor(ctx);
  const parser = await createCsvParser(filePath, options.skipLines);
  await processImport(ctx, parser, async (row) => {
    await processor.processRow(row);
  });
  
  // Process any remaining receipts
  await processor.finalize();

  // Log additional statistics
  console.log(`- Orders created: ${stats.ordersCreated}`);
  console.log(`- Orders updated: ${stats.ordersUpdated}`);
  console.log(`- Products created: ${stats.productsCreated}`);
  console.log(`- Products updated: ${stats.productsUpdated}`);
  console.log(`- Addresses created: ${stats.addressesCreated}`);
}

// Export the main function for programmatic use
export { importSalesReceipts };

// Setup CLI command when run directly
if (require.main === module) {
  setupImportCommand(
    'import-salesreceipt',
    'Import sales receipt data from QuickBooks CSV export',
    importSalesReceipts
  );
}
