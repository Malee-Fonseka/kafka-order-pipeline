import { type ClassifiedError, isPermanent } from './errors.js';
import {
  APP_VERSION_HEADER,
  ATTEMPT_COUNT_HEADER,
  CONSUMER_GROUP_HEADER,
  CORRELATION_ID_HEADER,
  ERROR_CLASS_HEADER,
  ERROR_MESSAGE_HEADER,
  ERROR_STACK_HEADER,
  ERROR_TYPE_HEADER,
  FIRST_FAILED_AT_HEADER,
  LAST_FAILED_AT_HEADER,
  ORIGINAL_KEY_HEADER,
  ORIGINAL_OFFSET_HEADER,
  ORIGINAL_PARTITION_HEADER,
  ORIGINAL_TIMESTAMP_HEADER,
  ORIGINAL_TOPIC_HEADER,
  readHeader,
  readIntHeader,
} from './headers.js';
import { type RetryMetadata, readRetryMetadata } from './retry.js';

/**
 * The dead letter record's forensic headers (design decision D6).
 *
 * The DLQ record's **value is the original bytes, untouched**. It cannot be
 * anything else: the single most common permanent failure is that the bytes
 * would not deserialize, and a DLQ that stores deserialized records has
 * nothing to store for exactly the records it exists for. So the value is
 * opaque by design, and every diagnostic lives in a header instead.
 *
 * This module is the one place the header set is defined — the writer builds
 * it, the inspector reads it, the replay tool strips it — so the three can
 * never disagree about a name.
 */

/**
 * Why the record was dead-lettered. The only two ways in (ADR 003, ADR 010):
 * a permanent classification, or a transient one that ran out of tiers.
 */
export type DlqErrorType = 'permanent' | 'transient-exhausted';

/** Stack traces are capped so a pathological error cannot bloat a record. */
export const MAX_STACK_HEADER_BYTES = 2_048;

/** Some error-reason headers carry free text; cap them for the same reason. */
const MAX_MESSAGE_HEADER_BYTES = 1_024;

/** Extra headers the replay tool adds so a replayed record is recognisable. */
export const REPLAYED_FROM_OFFSET_HEADER = 'x-replayed-from-dlq-offset';
export const REPLAYED_AT_HEADER = 'x-replayed-at';
export const REPLAY_COUNT_HEADER = 'x-replay-count';

export interface DlqSource {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  /** Broker timestamp of the record as consumed, epoch ms. */
  readonly timestamp: number;
  readonly key: Buffer | null;
  readonly headers: Readonly<Record<string, unknown>> | undefined;
}

export interface DlqFailure {
  readonly error: ClassifiedError;
  /** Total failed deliveries, including this one. */
  readonly attempt: number;
  readonly failedAt: Date;
}

export interface DlqWriterContext {
  readonly consumerGroup: string;
  readonly appVersion: string;
}

/** Truncates to a UTF-8 byte budget without splitting a multi-byte sequence. */
function truncateUtf8(text: string, maxBytes: number): string {
  const buffer = Buffer.from(text, 'utf8');
  if (buffer.length <= maxBytes) {
    return text;
  }
  const marker = '…[truncated]';
  const keep = maxBytes - Buffer.byteLength(marker, 'utf8');
  return `${buffer.subarray(0, keep).toString('utf8').replace(/�+$/u, '')}${marker}`;
}

export function dlqErrorTypeFor(error: ClassifiedError): DlqErrorType {
  return isPermanent(error) ? 'permanent' : 'transient-exhausted';
}

/**
 * The complete D6 header set for one dead letter.
 *
 * Origin fields prefer what the retry metadata already recorded on the first
 * hop, so a record that rode every tier still points at where it was *first*
 * consumed from — not the retry topic it happened to be on last.
 */
export function dlqHeaderValues(
  source: DlqSource,
  failure: DlqFailure,
  context: DlqWriterContext,
): Record<string, string | number> {
  const retry: RetryMetadata = readRetryMetadata(source.headers);
  const failedAt = failure.failedAt.toISOString();
  const { error } = failure;

  return {
    [ORIGINAL_TOPIC_HEADER]: retry.originalTopic ?? source.topic,
    [ORIGINAL_PARTITION_HEADER]: retry.originalPartition ?? source.partition,
    [ORIGINAL_OFFSET_HEADER]: retry.originalOffset ?? source.offset,
    [ORIGINAL_TIMESTAMP_HEADER]: retry.originalTimestamp ?? source.timestamp,
    [ORIGINAL_KEY_HEADER]: source.key === null ? '' : source.key.toString('utf8'),

    [ERROR_TYPE_HEADER]: dlqErrorTypeFor(error),
    [ERROR_CLASS_HEADER]: error.name,
    [ERROR_MESSAGE_HEADER]: truncateUtf8(
      isPermanent(error) ? `${error.reason}: ${error.message}` : error.message,
      MAX_MESSAGE_HEADER_BYTES,
    ),
    [ERROR_STACK_HEADER]: truncateUtf8(error.stack ?? '', MAX_STACK_HEADER_BYTES),

    [ATTEMPT_COUNT_HEADER]: failure.attempt,
    [FIRST_FAILED_AT_HEADER]: retry.firstFailedAt ?? failedAt,
    [LAST_FAILED_AT_HEADER]: failedAt,

    [CONSUMER_GROUP_HEADER]: context.consumerGroup,
    [CORRELATION_ID_HEADER]: readHeader(source.headers, CORRELATION_ID_HEADER) ?? '',
    [APP_VERSION_HEADER]: context.appVersion,
  };
}

/** A dead letter's headers, decoded. Every field is optional: the DLQ may hold records written by another version. */
export interface DlqMetadata {
  readonly originalTopic: string | undefined;
  readonly originalPartition: number | undefined;
  readonly originalOffset: string | undefined;
  readonly originalTimestamp: number | undefined;
  readonly originalKey: string | undefined;
  readonly errorType: DlqErrorType | undefined;
  readonly errorClass: string | undefined;
  readonly errorMessage: string | undefined;
  readonly errorStack: string | undefined;
  readonly attempt: number | undefined;
  readonly firstFailedAt: string | undefined;
  readonly lastFailedAt: string | undefined;
  readonly consumerGroup: string | undefined;
  readonly correlationId: string | undefined;
  readonly appVersion: string | undefined;
  readonly replayedFromOffset: string | undefined;
  readonly replayCount: number | undefined;
}

function asErrorType(value: string | undefined): DlqErrorType | undefined {
  return value === 'permanent' || value === 'transient-exhausted' ? value : undefined;
}

export function readDlqMetadata(
  headers: Readonly<Record<string, unknown>> | undefined,
): DlqMetadata {
  return {
    originalTopic: readHeader(headers, ORIGINAL_TOPIC_HEADER),
    originalPartition: readIntHeader(headers, ORIGINAL_PARTITION_HEADER),
    originalOffset: readHeader(headers, ORIGINAL_OFFSET_HEADER),
    originalTimestamp: readIntHeader(headers, ORIGINAL_TIMESTAMP_HEADER),
    originalKey: readHeader(headers, ORIGINAL_KEY_HEADER),
    errorType: asErrorType(readHeader(headers, ERROR_TYPE_HEADER)),
    errorClass: readHeader(headers, ERROR_CLASS_HEADER),
    errorMessage: readHeader(headers, ERROR_MESSAGE_HEADER),
    errorStack: readHeader(headers, ERROR_STACK_HEADER),
    attempt: readIntHeader(headers, ATTEMPT_COUNT_HEADER),
    firstFailedAt: readHeader(headers, FIRST_FAILED_AT_HEADER),
    lastFailedAt: readHeader(headers, LAST_FAILED_AT_HEADER),
    consumerGroup: readHeader(headers, CONSUMER_GROUP_HEADER),
    correlationId: readHeader(headers, CORRELATION_ID_HEADER),
    appVersion: readHeader(headers, APP_VERSION_HEADER),
    replayedFromOffset: readHeader(headers, REPLAYED_FROM_OFFSET_HEADER),
    replayCount: readIntHeader(headers, REPLAY_COUNT_HEADER),
  };
}

/**
 * Headers that describe a *past* failure and must not travel on a replay.
 *
 * A replayed record must earn a fresh set of retry tiers and a fresh DLQ
 * entry if it fails again; carrying `x-attempt-count: 4` back onto `orders`
 * would send it straight to exhaustion. The correlation id is deliberately
 * not in this list — it is the thread that connects the replay to its history.
 */
export const HEADERS_STRIPPED_ON_REPLAY: readonly string[] = [
  ORIGINAL_TOPIC_HEADER,
  ORIGINAL_PARTITION_HEADER,
  ORIGINAL_OFFSET_HEADER,
  ORIGINAL_TIMESTAMP_HEADER,
  ORIGINAL_KEY_HEADER,
  ERROR_TYPE_HEADER,
  ERROR_CLASS_HEADER,
  ERROR_MESSAGE_HEADER,
  ERROR_STACK_HEADER,
  ATTEMPT_COUNT_HEADER,
  FIRST_FAILED_AT_HEADER,
  LAST_FAILED_AT_HEADER,
  CONSUMER_GROUP_HEADER,
  'x-retry-not-before',
];

export interface ReplaySource {
  readonly dlqOffset: string;
  readonly headers: Readonly<Record<string, unknown>> | undefined;
  readonly replayedAt: Date;
}

/**
 * The headers a replayed record carries back to the main topic: everything
 * that is not a description of the past failure, plus a marker of the replay.
 */
export function replayHeaders(source: ReplaySource): Record<string, Buffer | string> {
  const stripped = new Set(HEADERS_STRIPPED_ON_REPLAY);
  const kept: Record<string, Buffer | string> = {};

  for (const [name, value] of Object.entries(source.headers ?? {})) {
    if (stripped.has(name)) {
      continue;
    }
    if (Buffer.isBuffer(value) || typeof value === 'string') {
      kept[name] = value;
    }
  }

  const previousReplays = readIntHeader(source.headers, REPLAY_COUNT_HEADER) ?? 0;
  kept[REPLAYED_FROM_OFFSET_HEADER] = source.dlqOffset;
  kept[REPLAYED_AT_HEADER] = source.replayedAt.toISOString();
  kept[REPLAY_COUNT_HEADER] = String(previousReplays + 1);

  return kept;
}
