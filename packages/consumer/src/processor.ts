import {
  CORRELATION_ID_HEADER,
  type ClassifiedError,
  type DlqErrorType,
  type Logger,
  type Order,
  type OrderDeserializer,
  type RetryTier,
  isPermanent,
  readHeader,
  readRetryMetadata,
} from '@order-pipeline/shared';

import type { DlqWriter } from './dlq/writer.js';
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
 *                                                                   └─ none ──▶ dead-lettered (transient-exhausted)
 *   permanent at any point ──────────────────────────────────────────────▶ dead-lettered (permanent)
 *
 * Every variant returned here is **terminal** — the pipeline commits on any
 * resolved outcome (D7) — so each means the record's fate is recorded
 * somewhere durable: in the aggregate, on a retry topic, or in the DLQ with
 * its raw bytes and forensic headers (D6).
 *
 * Only a failure of a *write to another topic* — the retry republish or the
 * DLQ write itself — escapes as a throw: that is a broker problem, nothing
 * durable has happened, and the pipeline must not commit. The client
 * redelivers.
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
      readonly kind: 'retried';
      readonly tier: RetryTier;
      readonly attempt: number;
      readonly error: ClassifiedError;
      readonly correlationId: string | undefined;
    }
  | {
      readonly kind: 'dead-lettered';
      readonly errorType: DlqErrorType;
      readonly error: ClassifiedError;
      readonly attempt: number;
      readonly dlqOffset: string | undefined;
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
  readonly dlq: DlqWriter;
  readonly backoff: BackoffOptions;
  readonly logger: Logger;
}

export type RecordProcessor = (record: IncomingRecord) => Promise<Outcome>;

export function createRecordProcessor({
  deserializer,
  handler,
  publisher,
  dlq,
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
        // Straight to the DLQ: no in-place attempt was retried and no tier
        // is involved. The attempt count is the deliveries so far.
        const written = await dlq.write(record, error, delivery);
        return {
          kind: 'dead-lettered',
          errorType: written.errorType,
          error,
          attempt: delivery,
          dlqOffset: written.offset,
          correlationId,
        };
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
        // Out of tiers: the transient failure is now, for our purposes,
        // permanent. The DLQ records it as transient-exhausted so the
        // inspector can tell "never decodable" from "downstream was down".
        const written = await dlq.write(record, outcome.error, outcome.attempt);
        return {
          kind: 'dead-lettered',
          errorType: written.errorType,
          error: outcome.error,
          attempt: outcome.attempt,
          dlqOffset: written.offset,
          correlationId,
        };
      }

      // A transient error from a write to another topic — the broker refused
      // the retry-topic or DLQ write. Nothing durable has happened, so this is
      // not terminal: propagate, the pipeline will not commit, the client
      // redelivers.
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
