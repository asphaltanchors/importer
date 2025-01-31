import { PrismaClient } from '@prisma/client';

export interface BaseImportStats {
  processed: number;
  warnings: string[];
}

export interface AddressData {
  line1: string;
  line2?: string;
  line3?: string;
  city: string;
  state?: string;
  postalCode?: string;
  country?: string;
}

export interface ImportContext {
  prisma: PrismaClient;
  debug: boolean;
  stats: BaseImportStats;
}
