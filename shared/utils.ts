import { parse } from 'csv-parse';
import { createReadStream } from 'fs';
import { Command } from 'commander';
import { PrismaClient } from '@prisma/client';
import { AddressData, ImportContext } from './types';

export function normalizeAddressField(value: string | null | undefined): string | null {
  if (!value) return null;
  return value
    .trim()
    .toLowerCase()
    .replace(/\bst\b\.?/g, 'street')
    .replace(/\brd\b\.?/g, 'road')
    .replace(/\bave?\b\.?/g, 'avenue')
    .replace(/\bsuite\b\.?\s+/i, 'ste ')
    .replace(/\bapartment\b\.?\s+/i, 'apt ')
    .replace(/\bunit\b\.?\s+/i, 'unit ')
    .replace(/[^a-z0-9\s#-]/g, '')
    .replace(/\s+/g, ' ');
}

export function parseDate(dateStr: string): Date | null {
  if (!dateStr) return null;
  // Assuming date format MM-DD-YYYY
  const [month, day, year] = dateStr.split('-');
  return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
}

export function parseDecimal(value: string): number {
  if (!value) return 0;
  // Remove currency symbols and commas, then parse
  return parseFloat(value.replace(/[$,]/g, ''));
}

export async function processAddress(
  ctx: ImportContext,
  addressData: AddressData
): Promise<{ id: string } | null> {
  if (!addressData.line1 || !addressData.city) return null;

  // Normalize address fields for comparison
  const normalizedAddress = {
    line1: normalizeAddressField(addressData.line1),
    line2: normalizeAddressField(addressData.line2),
    line3: normalizeAddressField(addressData.line3),
    city: normalizeAddressField(addressData.city),
    state: normalizeAddressField(addressData.state),
    postalCode: addressData.postalCode ? addressData.postalCode.replace(/[^0-9-]/g, '') : null,
    country: normalizeAddressField(addressData.country),
  };

  if (!normalizedAddress.line1 || !normalizedAddress.city) return null;

  // Look for existing address with normalized field matching
  // Focus on core address fields: city, state, postalCode
  const existingAddresses = await ctx.prisma.address.findMany({
    where: {
      AND: [
        // Match on city (required)
        {
          city: {
            equals: normalizedAddress.city,
            mode: 'insensitive' as const
          }
        },
        // Match on state if present
        ...(normalizedAddress.state ? [{
          state: {
            equals: normalizedAddress.state,
            mode: 'insensitive' as const
          }
        }] : []),
        // Match on postal code if present
        ...(normalizedAddress.postalCode ? [{
          postalCode: {
            equals: normalizedAddress.postalCode
          }
        }] : [])
      ]
    }
  });

  // If we found matches based on city/state/postal, do a basic line1 check
  // to avoid obviously wrong matches
  if (existingAddresses.length > 0) {
    for (const addr of existingAddresses) {
      const addrLine1Normalized = normalizeAddressField(addr.line1);
      if (!addrLine1Normalized) continue;

      const line1Words = new Set(normalizedAddress.line1.split(' '));
      const addrLine1Words = new Set(addrLine1Normalized.split(' '));
      
      // Check if at least 50% of words match between the two line1s
      const commonWords = [...line1Words].filter(word => addrLine1Words.has(word));
      const matchRatio = commonWords.length / Math.min(line1Words.size, addrLine1Words.size);
      if (matchRatio >= 0.5) {
        if (ctx.debug) {
          console.log(`Found address match: ${addr.line1}, ${addr.city}`);
          console.log(`  - Matched on ${commonWords.length} common words in line1`);
        }
        return addr;
      }
    }
  }

  // Create new address with original (non-normalized) values, but clean them
  const newAddress = await ctx.prisma.address.create({
    data: {
      line1: addressData.line1.trim(),
      line2: addressData.line2?.trim() || null,
      line3: addressData.line3?.trim() || null,
      city: addressData.city.trim(),
      state: addressData.state?.trim() || null,
      postalCode: addressData.postalCode ? addressData.postalCode.replace(/[^0-9-]/g, '') : null,
      country: addressData.country?.trim() || null,
    },
  });

  if (ctx.debug) console.log(`Created new address: ${newAddress.line1}, ${newAddress.city}`);
  return newAddress;
}

export async function createCsvParser(filePath: string, skipLines?: number) {
  if (skipLines) {
    console.log(`Reading header row then skipping ${skipLines} lines`);
  }
  
  const fileStream = createReadStream(filePath);
  const parser = parse({
    columns: true,
    skip_empty_lines: true,
    on_record: (record, { lines }) => {
      if (skipLines && lines > 1 && lines <= skipLines + 1) {
        return null;
      }
      return record;
    }
  });

  // Handle cleanup on errors
  parser.on('error', () => {
    fileStream.destroy();
  });

  fileStream.on('error', () => {
    parser.destroy();
  });

  return fileStream.pipe(parser);
}

export function setupImportCommand(
  name: string,
  description: string,
  importFn: (file: string, debug: boolean, options: { skipLines?: number }) => Promise<void>
) {
  const program = new Command();

  program
    .name(name)
    .description(description)
    .argument('<file>', 'CSV file to import')
    .option('-d, --debug', 'Enable debug logging')
    .option('-s, --skip-lines <number>', 'Skip first N lines of CSV file')
    .action(async (file: string, options: { debug: boolean; skipLines?: string }) => {
      const skipLines = options.skipLines ? parseInt(options.skipLines, 10) : undefined;
      try {
        await importFn(file, options.debug, { skipLines });
        process.exit(0);
      } catch (error) {
        console.error('Import failed:', error);
        process.exit(1);
      }
    });

  program.parse();
}

export async function processImport<T>(
  ctx: ImportContext,
  parser: AsyncIterable<T>,
  processFn: (row: T) => Promise<void>
) {
  console.log('Starting import...');
  const startTime = Date.now();
  let lastProgressUpdate = startTime;

  try {
    for await (const row of parser) {
      let attempts = 0;
      const maxAttempts = 3;
      let lastError: Error | null = null;
      
      while (attempts < maxAttempts) {
        try {
          await ctx.prisma.$transaction(
            async (tx) => {
              await processFn(row);
            },
            {
              timeout: 120000, // 120 second timeout per record
              maxWait: 30000, // 30 seconds max wait for transaction to start
            }
          );
          break; // Success, exit retry loop
        } catch (error) {
          attempts++;
          lastError = error as Error;
          
          if (attempts < maxAttempts) {
            // Exponential backoff between retries
            const delay = Math.pow(2, attempts) * 1000;
            console.log(`Attempt ${attempts} failed, retrying in ${delay/1000}s...`);
            await new Promise(resolve => setTimeout(resolve, delay));
          }
        }
      }

      if (lastError && attempts === maxAttempts) {
        console.error(`Error details:`, lastError);
        throw new Error(`Failed after ${maxAttempts} attempts: ${lastError.message}`);
      }

      ctx.stats.processed++;

      // Show progress every 100 records or if 5 seconds have passed
      const now = Date.now();
      if (!ctx.debug && (ctx.stats.processed % 100 === 0 || now - lastProgressUpdate >= 5000)) {
        console.log(`Processed ${ctx.stats.processed} records...`);
        lastProgressUpdate = now;
      }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\nImport completed successfully!');
    console.log(`- Records processed: ${ctx.stats.processed} in ${duration}s`);
    
    if (ctx.stats.warnings.length > 0) {
      console.log('\nWarnings:');
      ctx.stats.warnings.forEach((warning) => console.log(`- ${warning}`));
    }
  } catch (error) {
    console.error('Import failed:', error);
    throw error;
  } finally {
    // Ensure Prisma connection is properly closed
    await ctx.prisma.$disconnect();
  }
}
