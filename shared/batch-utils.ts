import { PrismaClient, Prisma, EmailType, PhoneType } from '@prisma/client';

export const DEFAULT_BATCH_SIZE = 500;

export interface BatchProcessor<T> {
  add(record: T): Promise<void>;
  flush(tx?: Prisma.TransactionClient): Promise<void>;
}

export class BatchEmailProcessor {
  private batch: Array<{
    email: string;
    type: 'MAIN' | 'CC';
    isPrimary: boolean;
    customerId: string;
  }> = [];
  
  constructor(
    private prisma: PrismaClient,
    private batchSize: number = DEFAULT_BATCH_SIZE
  ) {}

  async add(record: {
    email: string;
    type: 'MAIN' | 'CC';
    isPrimary: boolean;
    customerId: string;
  }): Promise<void> {
    this.batch.push(record);
    
    if (this.batch.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush(tx?: Prisma.TransactionClient): Promise<void> {
    if (this.batch.length === 0) return;

    const client = tx || this.prisma;

    // First check which emails already exist to avoid unique constraint violations
    const existingEmails = await client.customerEmail.findMany({
      where: {
        OR: this.batch.map(record => ({
          AND: {
            email: record.email,
            customerId: record.customerId
          }
        }))
      },
      select: {
        email: true,
        customerId: true
      }
    });

    // Create a Set for quick lookups
    const existingSet = new Set(
      existingEmails.map(record => `${record.email}-${record.customerId}`)
    );

    // Filter out existing records
    const newRecords = this.batch.filter(
      record => !existingSet.has(`${record.email}-${record.customerId}`)
    );

    if (newRecords.length > 0) {
      await client.customerEmail.createMany({
        data: newRecords,
        skipDuplicates: true
      });
    }

    this.batch = [];
  }
}

export class BatchPhoneProcessor {
  private batch: Array<{
    phone: string;
    type: PhoneType;
    isPrimary: boolean;
    customerId: string;
  }> = [];
  
  constructor(
    private prisma: PrismaClient,
    private batchSize: number = DEFAULT_BATCH_SIZE
  ) {}

  async add(record: {
    phone: string;
    type: PhoneType;
    isPrimary: boolean;
    customerId: string;
  }): Promise<void> {
    this.batch.push(record);
    
    if (this.batch.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush(tx?: Prisma.TransactionClient): Promise<void> {
    if (this.batch.length === 0) return;

    const client = tx || this.prisma;

    // First check which phones already exist to avoid unique constraint violations
    const existingPhones = await client.$queryRaw<Array<{ phone: string, customerId: string }>>`
      SELECT phone, "customerId"
      FROM "CustomerPhone"
      WHERE (phone, "customerId") IN (
        ${Prisma.join(
          this.batch.map(r => 
            Prisma.sql`(${r.phone}, ${r.customerId})`
          )
        )}
      )
    `;

    // Create a Set for quick lookups
    const existingSet = new Set(
      existingPhones.map(record => `${record.phone}-${record.customerId}`)
    );

    // Filter out existing records
    const newRecords = this.batch.filter(
      record => !existingSet.has(`${record.phone}-${record.customerId}`)
    );

    if (newRecords.length > 0) {
      await client.customerPhone.createMany({
        data: newRecords as Prisma.CustomerPhoneCreateManyInput[],
        skipDuplicates: true
      });
    }

    this.batch = [];
  }
}
