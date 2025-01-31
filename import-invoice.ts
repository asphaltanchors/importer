import { PrismaClient } from '@prisma/client';
import { ImportContext } from './shared/types';
import { OrderImportStats } from './shared/order-types';
import { createCsvParser, processImport, setupImportCommand } from './shared/utils';
import { InvoiceProcessor } from './processors/invoice-processor';

const prisma = new PrismaClient();

async function importInvoices(filePath: string, debug: boolean, options: { skipLines?: number }) {
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

  const processor = new InvoiceProcessor(ctx);
  const parser = await createCsvParser(filePath, options.skipLines);
  await processImport(ctx, parser, async (row) => {
    await processor.processRow(row);
  });
  
  // Process collected invoices
  await processor.finalize();

  // Log additional statistics
  console.log(`- Orders created: ${stats.ordersCreated}`);
  console.log(`- Orders updated: ${stats.ordersUpdated}`);
  console.log(`- Products created: ${stats.productsCreated}`);
  console.log(`- Products updated: ${stats.productsUpdated}`);
  console.log(`- Addresses created: ${stats.addressesCreated}`);
}

// Export the main function for programmatic use
export { importInvoices };

// Setup CLI command when run directly
if (require.main === module) {
  setupImportCommand(
    'import-invoice',
    'Import invoice data from CSV export',
    importInvoices
  );
}
