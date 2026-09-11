import {
  type KafkaClient,
  type Logger,
  type Producer,
  TransientError,
  createIdempotentProducer,
  describeError,
  scanTopic,
} from '@order-pipeline/shared';
import { z } from 'zod';

import type { ProductEntry } from './aggregator.js';

/**
 * The aggregation changelog: a hand-rolled equivalent of a Kafka Streams
 * state store (D3, ADR 006).
 *
 * `orders.aggregate.state` is log-compacted and keyed by product. After every
 * processed order, the product's updated Welford state is written here
 * **before** the order's offset is committed. Compaction keeps only the latest
 * value per key, so the topic is, at any moment, a complete snapshot of every
 * product's aggregate — one that survives restarts, crashes and rebalances
 * without a database.
 *
 * Restoring is reading that topic to its end and keeping the last value seen
 * per key. It is done for a *set of partitions* — the ones just assigned —
 * because ownership follows partitions (see `aggregator.ts`).
 */

const CHANGELOG_VERSION = 1;

/**
 * Wire schema for a changelog value. Versioned and validated on read: a
 * corrupt or foreign record must be skipped with a warning, not crash the
 * restore and take the consumer down with it.
 */
const changelogValueSchema = z.object({
  v: z.literal(CHANGELOG_VERSION),
  product: z.string().min(1),
  partition: z.number().int().nonnegative(),
  lastOffset: z.string(),
  state: z.object({
    count: z.number().int().positive(),
    mean: z.number().finite(),
    m2: z.number().finite().nonnegative(),
    min: z.number().finite(),
    max: z.number().finite(),
    lastUpdated: z.number().finite().nonnegative(),
  }),
});

export function encodeEntry(entry: ProductEntry): Buffer {
  const value: z.infer<typeof changelogValueSchema> = {
    v: CHANGELOG_VERSION,
    product: entry.product,
    partition: entry.partition,
    lastOffset: entry.lastOffset,
    state: entry.state,
  };
  return Buffer.from(JSON.stringify(value), 'utf8');
}

export function decodeEntry(value: Buffer): ProductEntry {
  const parsed: unknown = JSON.parse(value.toString('utf8'));
  const result = changelogValueSchema.safeParse(parsed);

  if (!result.success) {
    throw new Error(`changelog value failed validation: ${result.error.message}`);
  }

  const { product, partition, lastOffset, state } = result.data;
  return { product, partition, lastOffset, state };
}

export interface StateStoreOptions {
  readonly kafka: KafkaClient;
  readonly topic: string;
  /** The consumer's group id; restore consumers derive an ephemeral id from it. */
  readonly groupId: string;
  readonly logger: Logger;
  /** Upper bound on a restore. Exceeding it is treated as a failure, not a partial success. */
  readonly restoreTimeoutMs?: number;
}

export interface StateStore {
  /** Appends the entry to the changelog, resolving once the broker has acknowledged it. */
  write: (entry: ProductEntry) => Promise<void>;
  /**
   * Reads the changelog to its current end and returns the latest entry for
   * every product whose `partition` is in the given set.
   */
  restore: (partitions: readonly number[]) => Promise<ProductEntry[]>;
  close: () => Promise<void>;
}

export async function createStateStore({
  kafka,
  topic,
  groupId,
  logger,
  restoreTimeoutMs = 30_000,
}: StateStoreOptions): Promise<StateStore> {
  const producer: Producer = await createIdempotentProducer({
    kafka,
    logger,
    purpose: 'aggregation-changelog',
  });

  return {
    async write(entry) {
      try {
        await producer.send({
          topic,
          messages: [{ key: entry.product, value: encodeEntry(entry) }],
        });
      } catch (error) {
        // A changelog write that fails is a broker problem, not a record
        // problem. Surfacing it as transient means the pipeline does not commit
        // and the order is redelivered — the aggregate stays consistent with
        // what the changelog actually holds.
        throw new TransientError(`changelog write failed: ${describeError(error)}`, {
          cause: error,
        });
      }
    },

    async restore(partitions) {
      const wanted = new Set(partitions);
      const latest = new Map<string, ProductEntry>();
      let skipped = 0;

      const result = await scanTopic({
        kafka,
        topic,
        groupIdPrefix: `${groupId}-restore`,
        logger,
        timeoutMs: restoreTimeoutMs,
        onRecord: (record) => {
          if (record.value === null) {
            return;
          }
          try {
            const entry = decodeEntry(record.value);
            // Later offsets win: compaction may not have run yet, so a product
            // can appear many times. Filter by *orders* partition, which is
            // recorded in the entry — not by changelog partition.
            if (wanted.has(entry.partition)) {
              latest.set(entry.product, entry);
            }
          } catch (error) {
            skipped += 1;
            logger.warn(
              { topic, partition: record.partition, offset: record.offset, err: error },
              'skipping undecodable changelog record',
            );
          }
        },
      });

      const entries = [...latest.values()];
      logger.info(
        {
          topic,
          partitions,
          scanned: result.scanned,
          skipped,
          restored: entries.length,
          products: entries.map((e) => e.product).sort(),
          elapsedMs: result.elapsedMs,
        },
        result.scanned === 0
          ? 'changelog is empty; nothing to restore'
          : 'aggregation state restored from changelog',
      );
      return entries;
    },

    async close() {
      await producer.flush({ timeout: 5_000 });
      await producer.disconnect();
    },
  };
}
