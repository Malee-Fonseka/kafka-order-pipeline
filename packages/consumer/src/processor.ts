import {
  CORRELATION_ID_HEADER,
  type ClassifiedError,
  type Logger,
  type Order,
  type OrderDeserializer,
  type PermanentError,
  type RetryTier,
  isPermanent,
  readHeader,
  readRetryMetadata,
} from '@order-pipeline/shared';

import type { OrderHandler } from './handler.js';
import type { IncomingRecord } from './pipeline.js';
import { type BackoffOptions, InPlaceRetryExhausted, retryInPlace } from './retry/backoff.js';
import type { RetryPublisher } from './retry/publisher.js';

/**
 * What happens to one record from the main topic.
 *
 *   deserialize ─┐
 *                ├─ stage 1: in-place, jittered, ≤3 attempts, ≤2 s ─┬─ ok ──▶ processed
 *   handle ──────┘                                                  │
 *                                                       still transient
 *                                                                   │
 *                        stage 2: republish to tier by attempt count ┼─ tier ──▶ retried
 *                                                                   └─ none ──▶ exhausted
 *   permanent at any point ────────────────────────────────────────────────▶ skipped
 *
 * Every variant returned here is **terminal** — the pipeline commits on any
 * resolved outcome (D7) — so each must mean the record's fate is recorded
 * somewhere durable: in the aggregate, on a retry topic, or (Phase 7) in the
 * DLQ. `skipped` and `exhausted` are the two Phase 7 replaces with DLQ writes;
 * until then they are counted and logged at `warn`.
 *
 * Only a failure of the *republish itself* escapes as a throw: that is a
 * broker problem, nothing durable has happened, and the pipeline must not
 * commit — the client redelivers.
 */

export type Outcome =
  | {
      readonly kind: 'processed';
      readonly order: Order;
      readonly correlationId: string | undefined;
      readonly delivery: number;
      readonly attempts: number;
    }
  | {
      readonly kind: 'skipped';
      readonly error: PermanentError;
      readonly correlationId: string | undefined;
    }
  | {
      readonly kind: 'retried';
      readonly tier: RetryTier;
      readonly attempt: number;
      readonly error: ClassifiedError;
      readonly correlationId: string | undefined;
    }
  | {
      readonly kind: 'exhausted';
      readonly attempt: number;
      readonly error: ClassifiedError;
      readonly correlationId: string | undefined;
    }
  | {
      readonly kind: 'forwarded';
      readonly fromTopic: string;
      readonly attempt: number;
      readonly correlationId: string | undefined;
    };

export interface ProcessorOptions {
  readonly deserializer: OrderDeserializer;
  readonly handler: OrderHandler;
  readonly publisher: RetryPublisher;
  readonly backoff: BackoffOptions;
  readonly logger: Logger;
}

export type RecordProcessor = (record: IncomingRecord) => Promise<Outcome>;

export function createRecordProcessor({
  deserializer,
  handler,
  publisher,
  backoff,
  logger,
}: ProcessorOptions): RecordProcessor {
  return async (record) => {
    const correlationId = readHeader(record.headers, CORRELATION_ID_HEADER);
    const meta = readRetryMetadata(record.headers);
    const delivery = meta.attempt + 1;
    const location = {
      topic: record.topic,
      partition: record.partition,
      offset: record.offset,
      key: record.key?.toString('utf8'),
      correlationId,
      delivery,
    };

    // Aggregation ownership is by the partition the record was *originally*
    // consumed from. A retried record re-enters `orders` on the same partition
    // (same key, same partitioner), so the two agree; the header is the
    // explicit source of truth regardless.
    const source = {
      partition: meta.originalPartition ?? record.partition,
      offset: record.offset,
      timestamp: Number(record.timestamp),
    };

    let result: { value: Order; attempts: number };
    try {
      result = await retryInPlace(async (attempt) => {
        const order = await deserializer.deserialize(record.value);
        await handler(order, { delivery, attempt, source });
        return order;
      }, backoff);
    } catch (error) {
      if (isPermanent(error)) {
        logger.warn(
          { ...location, reason: error.reason, err: error },
          'record cannot be processed; skipping until the DLQ writer lands in phase 7',
        );
        return { kind: 'skipped', error, correlationId };
      }

      if (error instanceof InPlaceRetryExhausted) {
        const { failure } = error;
        logger.warn(
          {
            ...location,
            attempts: failure.attempts,
            elapsedMs: failure.elapsedMs,
            err: failure.error,
          },
          'in-place retries exhausted; escalating to retry tiers',
        );

        const outcome = await publisher.escalate(record, failure.error);
        if (outcome.kind === 'retried') {
          return {
            kind: 'retried',
            tier: outcome.tier,
            attempt: outcome.attempt,
            error: failure.error,
            correlationId,
          };
        }
        logger.warn(
          { ...location, attempt: outcome.attempt, err: outcome.error },
          'retry tiers exhausted; skipping until the DLQ writer lands in phase 7',
        );
        return { kind: 'exhausted', attempt: outcome.attempt, error: outcome.error, correlationId };
      }

      // A transient error from the republish path itself — the broker refused
      // the retry-topic write. Nothing durable has happened, so this is not
      // terminal: propagate, the pipeline will not commit, the client redelivers.
      throw error;
    }

    logger.info(
      {
        ...location,
        attempts: result.attempts,
        orderId: result.value.orderId,
        product: result.value.product,
        price: result.value.price,
      },
      delivery === 1 ? 'order received' : 'order received after retry',
    );

    return {
      kind: 'processed',
      order: result.value,
      correlationId,
      delivery,
      attempts: result.attempts,
    };
  };
}

/**
 * What happens to one record from a retry tier that is due: it goes back to
 * the main topic, unchanged, for the owning instance to process.
 */
export function createForwardProcessor({
  publisher,
}: Pick<ProcessorOptions, 'publisher'>): RecordProcessor {
  return async (record) => {
    await publisher.forward(record);
    return {
      kind: 'forwarded',
      fromTopic: record.topic,
      attempt: readRetryMetadata(record.headers).attempt,
      correlationId: readHeader(record.headers, CORRELATION_ID_HEADER),
    };
  };
}
