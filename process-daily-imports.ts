#!/usr/bin/env node
import { Command } from 'commander';
import { PrismaClient } from '@prisma/client';
import path from 'path';
import fs from 'fs/promises';
import { existsSync } from 'fs';
import { z } from 'zod';

// Configuration type
interface ImportConfig {
  importDir: string;
  archiveDir: string;
  failedDir: string;
  logDir: string;
  filePatterns: {
    customers: string;
    invoices: string;
    salesReceipts: string;
  };
  retentionDays: number;
  batchSize: number;
  maxRetries: number;
  debug: boolean;
}

// Import result types
interface ImportResult {
  filename: string;
  importType: 'customers' | 'invoices' | 'salesReceipts';
  success: boolean;
  startTime: Date;
  endTime: Date;
  recordsProcessed: number;
  errors: string[];
  warnings: string[];
}

interface DailyImportLog {
  date: string;
  imports: ImportResult[];
  archiveDir: string;
}

function getConfig(baseDir: string = '/CSV'): ImportConfig {
  return {
    importDir: baseDir,
    archiveDir: path.join(baseDir, 'archive'),
    failedDir: path.join(baseDir, 'failed'),
    logDir: path.join(baseDir, 'logs'),
    filePatterns: {
      customers: 'customers_*.csv',
      invoices: 'invoices_*.csv',
      salesReceipts: 'sales_receipts_*.csv'
    },
    retentionDays: 30,
    batchSize: 100,
    maxRetries: 3,
    debug: false
  };
}

const prisma = new PrismaClient();

async function ensureDirectoryExists(dir: string) {
  try {
    await fs.access(dir);
  } catch {
    await fs.mkdir(dir, { recursive: true });
  }
}

async function moveFile(source: string, destDir: string): Promise<string> {
  const filename = path.basename(source);
  const destination = path.join(destDir, filename);
  await fs.rename(source, destination);
  return destination;
}

async function cleanupOldFiles(directory: string, retentionDays: number) {
  const files = await fs.readdir(directory);
  const now = new Date();
  
  for (const file of files) {
    const filePath = path.join(directory, file);
    const stats = await fs.stat(filePath);
    const ageInDays = (now.getTime() - stats.mtime.getTime()) / (1000 * 60 * 60 * 24);
    
    if (ageInDays > retentionDays) {
      await fs.unlink(filePath);
    }
  }
}

async function writeImportLog(config: ImportConfig, log: DailyImportLog) {
  const logPath = path.join(config.logDir, `import_${log.date}.json`);
  await fs.writeFile(logPath, JSON.stringify(log, null, 2));
}

async function importFile(
  filePath: string,
  importType: 'customers' | 'invoices' | 'salesReceipts',
  config: ImportConfig
): Promise<ImportResult> {
  const startTime = new Date();
  const result: ImportResult = {
    filename: path.basename(filePath),
    importType,
    success: false,
    startTime,
    endTime: startTime,
    recordsProcessed: 0,
    errors: [],
    warnings: []
  };

  try {
    // Import the appropriate processor based on type
    let importFn;
    switch (importType) {
      case 'customers':
        const { importCustomers } = await import('./import-customer');
        importFn = importCustomers;
        break;
      case 'invoices':
        const { importInvoices } = await import('./import-invoice');
        importFn = importInvoices;
        break;
      case 'salesReceipts':
        const { importSalesReceipts } = await import('./import-salesreceipt');
        importFn = importSalesReceipts;
        break;
    }

    // Run the import with options
    await importFn(filePath, config.debug, { skipLines: 0 });
    
    result.success = true;
  } catch (error) {
    result.errors.push(error instanceof Error ? error.message : String(error));
  }

  result.endTime = new Date();
  return result;
}

async function validateDirectoryStructure(config: ImportConfig) {
  // Check if import directory exists
  try {
    await fs.access(config.importDir);
  } catch {
    throw new Error(`Import directory ${config.importDir} does not exist. Please create it and add your CSV files.`);
  }

  // Create other directories if they don't exist
  await Promise.all([
    ensureDirectoryExists(config.archiveDir),
    ensureDirectoryExists(config.failedDir),
    ensureDirectoryExists(config.logDir)
  ]);

  // Check if any matching files exist
  const files = await fs.readdir(config.importDir);
  const importTypes = ['customers', 'invoices', 'salesReceipts'] as const;
  let hasMatchingFiles = false;

  for (const importType of importTypes) {
    const pattern = config.filePatterns[importType];
    const matchingFiles = files.filter(f => f.match(pattern.replace('*', '\\d{8}')));
    if (matchingFiles.length > 0) {
      hasMatchingFiles = true;
      break;
    }
  }

  if (!hasMatchingFiles) {
    throw new Error(
      `No matching CSV files found in ${config.importDir}. Expected files matching patterns:\n` +
      Object.entries(config.filePatterns)
        .map(([type, pattern]) => `  - ${pattern} (${type})`)
        .join('\n')
    );
  }
}

async function processImports(baseDir?: string) {
  const config = getConfig(baseDir);

  // Validate directory structure and check for files
  await validateDirectoryStructure(config);

  // Initialize daily log
  const today = new Date().toISOString().split('T')[0];
  const dailyLog: DailyImportLog = {
    date: today,
    imports: [],
    archiveDir: config.archiveDir
  };

  // Process each import type
  const importTypes = ['customers', 'invoices', 'salesReceipts'] as const;
  
  for (const importType of importTypes) {
    const pattern = config.filePatterns[importType];
    const files = await fs.readdir(config.importDir);
    
    // Find matching files from last 24 hours
    const recentFiles = await Promise.all(
      files
        .filter(f => f.match(pattern.replace('*', '\\d{8}')))
        .map(async f => {
          const filePath = path.join(config.importDir, f);
          const stats = await fs.stat(filePath);
          return { path: filePath, stats };
        })
    );

    const last24Hours = Date.now() - 24 * 60 * 60 * 1000;
    const filesToProcess = recentFiles
      .filter(f => f.stats.mtime.getTime() > last24Hours)
      .map(f => f.path);

    // Process each file
    for (const filePath of filesToProcess) {
      const result = await importFile(filePath, importType, config);
      dailyLog.imports.push(result);

      // Move file based on result
      const targetDir = result.success ? config.archiveDir : config.failedDir;
      await moveFile(filePath, targetDir);
    }
  }

  // Write daily log
  await writeImportLog(config, dailyLog);

  // Cleanup old files
  await cleanupOldFiles(config.archiveDir, config.retentionDays);
  await cleanupOldFiles(config.logDir, config.retentionDays);

  // Output summary
  console.log(`Import Summary for ${today}:`);
  console.log(`Total files processed: ${dailyLog.imports.length}`);
  console.log(`Successful imports: ${dailyLog.imports.filter(i => i.success).length}`);
  console.log(`Failed imports: ${dailyLog.imports.filter(i => !i.success).length}`);

  if (dailyLog.imports.some(i => !i.success)) {
    process.exit(1);
  }
}

// Setup CLI
const program = new Command();

program
  .name('process-daily-imports')
  .description('Process daily CSV imports for customers, invoices, and sales receipts')
  .argument('[directory]', 'Base directory for CSV files (defaults to /CSV)')
  .action(async (directory?: string) => {
    try {
      await processImports(directory);
    } catch (error) {
      console.error('Import failed:', error);
      process.exit(1);
    }
  });

program.parse();
