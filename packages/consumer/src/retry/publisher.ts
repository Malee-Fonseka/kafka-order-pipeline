import {
  type ClassifiedError,
  type Escalation,
  type Logger,
  type Producer,
  type RetryMetadata,
  type RetryTier,
  type TopicRegistry,
  TransientError,
  describeError,
  encodeHeaders,
  escalate,
  readRetryMetadata,
  retryHeaderValues,
} from '@order-pipeline/shared';

import type { IncomingRecord } from '../pipeline.js';

/**
 * Stage 2 of the retry strategy (D5): the tiered retry topics.
 *
 * Retry topics are **delay queues, not processing queues**. A record that has
 * exhausted its in-place attempts is republished — raw bytes, same key, same
 * headers plus retry metadata — to the tier its attempt count earns. A
 * consumer of that tier waits for `x-retry-not-before` and then forwards the
 * record, unchanged, back to `orders`, where the owning instance processes it
 * like any other record.
 *
 * Forwarding rather than processing in place is what keeps aggregation
 * ownership (ADR 002, ADR 006) intact. Processing on the retry topic would be
 * correct only if the instance holding `orders.retry.5s` partition 1 were
 * always the instance holding `orders` partition 1 — co-partitioning *and*
 * co-assignment, neither of which is a guarantee this system should rest on.
 * One extra hop per retry is a small price for not having two instances write
 * the same product's changelog entry.
 */

export type RetryOutcome =
  | {
      readonly kind: 'retried';
      readonly tier: RetryTier;
      readonly attempt: number;
      readonly notBefore: Date;
    }
  | { readonly kind: 'exhausted'; readonly attempt: number; readonly error: ClassifiedError };

export interface RetryPublisherOptions {
  readonly producer: Producer;
  readonly topics: TopicRegistry;
  readonly logger: Logger;
  readonly now?: () => Date;
}

export interface RetryPublisher {
  /** Republishes a failed record to the next tier, or reports exhaustion. */
  escalate: (record: IncomingRecord, error: ClassifiedError) => Promise<RetryOutcome>;
  /** Sends a due retry record back to the main topic, headers intact. */
  forward: (record: IncomingRecord) => Promise<void>;
  /** The record's retry metadata; exposed so the handler can vary behaviour by attempt. */
  metadataOf: (record: IncomingRecord) => RetryMetadata;
}

/** Copies incoming headers into the shape the producer accepts. */
function copyHeaders(
  headers: Readonly<Record<string, unknown>> | undefined,
): Record<string, Buffer | string> {
  const copied: Record<string, Buffer | string> = {};
  if (headers === undefined) {
    return copied;
  }
  for (const [name, value] of Object.entries(headers)) {
    if (Buffer.isBuffer(value) || typeof value === 'string') {
      copied[name] = value;
    }
  }
  return copied;
}

export function createRetryPublisher({
  producer,
  topics,
  logger,
  now = () => new Date(),
}: RetryPublisherOptions): RetryPublisher {
  const send = async (
    topic: string,
    record: IncomingRecord,
    headers: Record<string, Buffer | string>,
  ): Promise<void> => {
    try {
      await producer.send({
        topic,
        // Raw bytes, never re-serialized: the record must arrive on the next
        // topic exactly as it was, or a later decode failure would be ours.
        messages: [{ key: record.key, value: record.value, headers }],
      });
    } catch (error) {
      // The republish itself failed — a broker problem. Surfacing it as
      // transient means the pipeline does not commit and the record is
      // redelivered, so it is never lost between topics.
      throw new TransientError(`republish to ${topic} failed: ${describeError(error)}`, {
        cause: error,
      });
    }
  };

  return {
    metadataOf: (record) => readRetryMetadata(record.headers),

    async escalate(record, error) {
      const existing = readRetryMetadata(record.headers);
      const next: Escalation = escalate(existing.attempt, topics.retryTiers);

      if (next.kind === 'exhausted') {
        logger.warn(
          {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            attempt: next.attempt,
            err: error,
          },
          'retry tiers exhausted',
        );
        return { kind: 'exhausted', attempt: next.attempt, error };
      }

      const at = now();
      const notBefore = new Date(at.getTime() + next.tier.delayMs);
      const headers = {
        ...copyHeaders(record.headers),
        ...encodeHeaders(
          retryHeaderValues({
            attempt: next.attempt,
            notBefore: notBefore.getTime(),
            now: at,
            existing,
            source: {
              topic: record.topic,
              partition: record.partition,
              offset: record.offset,
              timestamp: Number(record.timestamp),
            },
          }),
        ),
      };

      await send(next.tier.topic, record, headers);

      logger.info(
        {
          topic: record.topic,
          partition: record.partition,
          offset: record.offset,
          attempt: next.attempt,
          tier: next.tier.label,
          retryTopic: next.tier.topic,
          notBefore: notBefore.toISOString(),
          reason: error.message,
        },
        'record republished to retry tier',
      );

      return { kind: 'retried', tier: next.tier, attempt: next.attempt, notBefore };
    },

    async forward(record) {
      await send(topics.orders, record, copyHeaders(record.headers));
      const meta = readRetryMetadata(record.headers);
      logger.info(
        {
          fromTopic: record.topic,
          partition: record.partition,
          offset: record.offset,
          attempt: meta.attempt,
          toTopic: topics.orders,
        },
        'retry delay elapsed; record forwarded back to main topic',
      );
    },
  };
}
