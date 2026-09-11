import { randomUUID } from 'node:crypto';

import {
  type KafkaClient,
  type Logger,
  type Producer,
  TransientError,
  createIdempotentProducer,
  createKafkaLogger,
  describeError,
  toKafkaLogLevel,
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
      const started = Date.now();

      // 1. Where does the topic end right now? Anything written after this
      //    point is a live update we will receive through the normal path.
      const admin = kafka.admin();
      await admin.connect();
      let goals: Map<number, bigint>;
      try {
        const watermarks = await admin.fetchTopicOffsets(topic);
        goals = new Map(
          watermarks
            .filter((w) => BigInt(w.high) > BigInt(w.low))
            .map((w) => [w.partition, BigInt(w.high) - 1n]),
        );
      } finally {
        await admin.disconnect();
      }

      if (goals.size === 0) {
        logger.info({ topic, partitions }, 'changelog is empty; nothing to restore');
        return [];
      }

      // 2. Read every non-empty changelog partition up to that end. A unique
      //    group id gives this reader all partitions and keeps its offsets
      //    separate from the real consumer group's; it is deleted afterwards.
      const restoreGroupId = `${groupId}-restore-${randomUUID()}`;
      const reader = kafka.consumer({
        kafkaJS: {
          groupId: restoreGroupId,
          fromBeginning: true,
          autoCommit: false,
          allowAutoTopicCreation: false,
          logger: createKafkaLogger(logger),
          logLevel: toKafkaLogLevel(logger.level),
        },
      });

      const latest = new Map<string, ProductEntry>();
      const reached = new Map<number, bigint>();
      let scanned = 0;
      let skipped = 0;

      const complete = (): boolean =>
        [...goals].every(([partition, goal]) => (reached.get(partition) ?? -1n) >= goal);

      let resolveDone: () => void = () => undefined;
      const done = new Promise<void>((resolve) => {
        resolveDone = resolve;
      });

      await reader.connect();
      try {
        await reader.subscribe({ topics: [topic] });
        await reader.run({
          eachMessage: async ({ partition, message }) => {
            await Promise.resolve();
            scanned += 1;
            reached.set(partition, BigInt(message.offset));

            if (message.value !== null) {
              try {
                const entry = decodeEntry(message.value);
                // Later offsets win: compaction may not have run yet, so a
                // product can appear many times. Filter by *orders* partition,
                // which is recorded in the entry — not by changelog partition.
                if (wanted.has(entry.partition)) {
                  latest.set(entry.product, entry);
                }
              } catch (error) {
                skipped += 1;
                logger.warn(
                  { topic, partition, offset: message.offset, err: error },
                  'skipping undecodable changelog record',
                );
              }
            }

            if (complete()) {
              resolveDone();
            }
          },
        });

        const timeout = new Promise<never>((_, reject) => {
          setTimeout(() => {
            reject(
              new Error(
                `changelog restore timed out after ${String(restoreTimeoutMs)}ms; ` +
                  `reached ${JSON.stringify([...reached].map(([p, o]) => [p, o.toString()]))} ` +
                  `of ${JSON.stringify([...goals].map(([p, o]) => [p, o.toString()]))}`,
              ),
            );
          }, restoreTimeoutMs).unref();
        });

        await Promise.race([done, timeout]);
      } finally {
        await reader.disconnect();
        // Best effort: an orphaned restore group is harmless (no commits, expires
        // on its own) but clutters the group list in Kafbat UI.
        const admin2 = kafka.admin();
        try {
          await admin2.connect();
          await admin2.deleteGroups([restoreGroupId]);
        } catch (error) {
          logger.debug({ restoreGroupId, err: error }, 'could not delete restore group');
        } finally {
          await admin2.disconnect();
        }
      }

      const entries = [...latest.values()];
      logger.info(
        {
          topic,
          partitions,
          scanned,
          skipped,
          restored: entries.length,
          products: entries.map((e) => e.product).sort(),
          elapsedMs: Date.now() - started,
        },
        'aggregation state restored from changelog',
      );
      return entries;
    },

    async close() {
      await producer.flush({ timeout: 5_000 });
      await producer.disconnect();
    },
  };
}
