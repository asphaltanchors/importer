import { PrismaClient, Prisma } from '@prisma/client';
import { AddressData } from './types';
import { DEFAULT_BATCH_SIZE } from './batch-utils';

export class BatchAddressProcessor {
  private batch: Array<AddressData & { hash: string }> = [];
  private processedAddresses = new Map<string, string>();
  
  constructor(
    private prisma: PrismaClient,
    private batchSize: number = DEFAULT_BATCH_SIZE,
    private stats?: { addressesCreated?: number }
  ) {}

  private hashAddress(address: AddressData): string {
    return JSON.stringify({
      line1: address.line1,
      line2: address.line2,
      line3: address.line3,
      city: address.city,
      state: address.state,
      postalCode: address.postalCode,
      country: address.country
    });
  }

  async add(address: AddressData): Promise<string | null> {
    if (!address.line1) return null;
    
    const hash = this.hashAddress(address);
    
    // Return existing address ID if we've processed this address before
    if (this.processedAddresses.has(hash)) {
      return this.processedAddresses.get(hash)!;
    }
    
    this.batch.push({ ...address, hash });
    
    if (this.batch.length >= this.batchSize) {
      await this.flush();
    }
    
    return null; // Will be processed during flush
  }

  async flush(tx?: Prisma.TransactionClient): Promise<void> {
    if (this.batch.length === 0) return;

    const client = tx || this.prisma;
    
    // First find any existing addresses
    const existingAddresses = await client.address.findMany({
      where: {
        OR: this.batch.map(({ hash, ...address }) => ({
          AND: {
            line1: address.line1,
            line2: address.line2 ?? null,
            line3: address.line3 ?? null,
            city: address.city,
            state: address.state,
            postalCode: address.postalCode,
            country: address.country
          }
        }))
      },
      select: {
        id: true,
        line1: true,
        line2: true,
        line3: true,
        city: true,
        state: true,
        postalCode: true,
        country: true
      }
    });

    // Create a map of existing addresses by their hash
    const existingAddressMap = new Map<string, string>();
    for (const address of existingAddresses) {
      const addressData: AddressData = {
        line1: address.line1,
        line2: address.line2 ?? undefined,
        line3: address.line3 ?? undefined,
        city: address.city,
        state: address.state ?? undefined,
        postalCode: address.postalCode ?? undefined,
        country: address.country ?? undefined
      };
      const hash = this.hashAddress(addressData);
      existingAddressMap.set(hash, address.id);
      this.processedAddresses.set(hash, address.id);
    }

    // Filter out addresses that already exist
    const newAddresses = this.batch.filter(({ hash }) => !existingAddressMap.has(hash));

    if (newAddresses.length > 0) {
      // Create new addresses
      const result = await client.address.createMany({
        data: newAddresses.map(({ hash, ...addressData }) => addressData),
        skipDuplicates: true
      });

      if (result.count > 0) {
        // Fetch the newly created addresses to get their IDs
        const createdAddresses = await client.address.findMany({
          where: {
            OR: newAddresses.map(({ hash, ...address }) => ({
              AND: {
                line1: address.line1,
                line2: address.line2 ?? null,
                line3: address.line3 ?? null,
                city: address.city,
                state: address.state,
                postalCode: address.postalCode,
                country: address.country
              }
            }))
          },
          select: {
            id: true,
            line1: true,
            line2: true,
            line3: true,
            city: true,
            state: true,
            postalCode: true,
            country: true
          }
        });

        // Update our processed addresses map with new addresses
        for (const address of createdAddresses) {
          const addressData: AddressData = {
            line1: address.line1,
            line2: address.line2 ?? undefined,
            line3: address.line3 ?? undefined,
            city: address.city,
            state: address.state ?? undefined,
            postalCode: address.postalCode ?? undefined,
            country: address.country ?? undefined
          };
          const hash = this.hashAddress(addressData);
          this.processedAddresses.set(hash, address.id);
        }

        if (this.stats?.addressesCreated !== undefined) {
          this.stats.addressesCreated += result.count;
        }
      }
    }

    this.batch = [];
  }

  getProcessedAddressId(address: AddressData): string | null {
    if (!address.line1) return null;
    const hash = this.hashAddress(address);
    return this.processedAddresses.get(hash) ?? null;
  }
}
