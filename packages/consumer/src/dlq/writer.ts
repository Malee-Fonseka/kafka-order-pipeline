import {
  type ClassifiedError,
  type DlqErrorType,
  type Logger,
  type Producer,
  TransientError,
  describeError,
  dlqErrorTypeFor,
  dlqHeaderValues,
  encodeHeaders,
} from '@order-pipeline/shared';

import type { IncomingRecord } from '../pipeline.js';

/**
 * The dead letter writer (design decision D6).
 *
 * Writes the record **exactly as received** — the original key, the original
 * value bytes — to the DLQ topic, with every diagnostic in headers. Nothing is
 * re-serialized, because the record in front of us is very often one that
 * *could not* be deserialized, and a writer that needs a decoded object has
 * nothing to write for exactly the records it exists for.
 *
 * Dead-lettering is a terminal outcome (ADR 005): once the broker has
 * acknowledged the DLQ write, the record's fate is durably recorded elsewhere
 * and the source offset may commit. If the write fails, nothing durable has
 * happened, so the failure surfaces as transient: no commit, redelivery, and
 * the record is never lost in the gap between two topics.
 */

export interface DlqWriteResult {
  readonly errorType: DlqErrorType;
  readonly partition: number | undefined;
  readonly offset: string | undefined;
}

export interface DlqWriterOptions {
  readonly producer: Producer;
  readonly topic: string;
  readonly consumerGroup: string;
  readonly appVersion: string;
  readonly logger: Logger;
  readonly now?: () => Date;
}

export interface DlqWriter {
  write: (
    record: IncomingRecord,
    error: ClassifiedError,
    attempt: number,
  ) => Promise<DlqWriteResult>;
}

export function createDlqWriter({
  producer,
  topic,
  consumerGroup,
  appVersion,
  logger,
  now = () => new Date(),
}: DlqWriterOptions): DlqWriter {
  return {
    async write(record, error, attempt) {
      const errorType = dlqErrorTypeFor(error);
      const headers = encodeHeaders(
        dlqHeaderValues(
          {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            timestamp: Number(record.timestamp),
            key: record.key,
            headers: record.headers,
          },
          { error, attempt, failedAt: now() },
          { consumerGroup, appVersion },
        ),
      );

      let metadata: { partition: number; offset?: string } | undefined;
      try {
        // Raw bytes, original key. Never `serializer.serialize(...)` here.
        [metadata] = await producer.send({
          topic,
          messages: [{ key: record.key, value: record.value, headers }],
        });
      } catch (cause) {
        throw new TransientError(`dead letter write failed: ${describeError(cause)}`, { cause });
      }

      logger.warn(
        {
          topic: record.topic,
          partition: record.partition,
          offset: record.offset,
          key: record.key?.toString('utf8'),
          errorType,
          errorClass: error.name,
          attempt,
          dlqPartition: metadata?.partition,
          dlqOffset: metadata?.offset,
          err: error,
        },
        'record dead-lettered',
      );

      return { errorType, partition: metadata?.partition, offset: metadata?.offset };
    },
  };
}
