/**
 * Canonical Kafka record header names and encoding helpers.
 *
 * Kafka headers are raw bytes, not strings — every value crosses the wire as a
 * `Buffer` and comes back as one. Centralising the names and the
 * encode/decode pair here means the producer, the retry tiers, the DLQ writer
 * and the inspector CLI cannot disagree about either the spelling of a header
 * or how its value was framed, which is the kind of drift that turns DLQ
 * forensics into guesswork.
 *
 * The full set is declared up front because it is specified in D6; the
 * retry and DLQ headers are written by Phases 6 and 7.
 */

/** Headers attached by the producer to every record it emits. */
export const CORRELATION_ID_HEADER = 'x-correlation-id';
export const APP_VERSION_HEADER = 'x-app-version';

/** Headers added when a record is republished to a retry tier (Phase 6, D5). */
export const ATTEMPT_COUNT_HEADER = 'x-attempt-count';
export const RETRY_NOT_BEFORE_HEADER = 'x-retry-not-before';
export const FIRST_FAILED_AT_HEADER = 'x-first-failed-at';
export const LAST_FAILED_AT_HEADER = 'x-last-failed-at';

/** Forensic headers written alongside the raw bytes in the DLQ (Phase 7, D6). */
export const ORIGINAL_TOPIC_HEADER = 'x-original-topic';
export const ORIGINAL_PARTITION_HEADER = 'x-original-partition';
export const ORIGINAL_OFFSET_HEADER = 'x-original-offset';
export const ORIGINAL_TIMESTAMP_HEADER = 'x-original-timestamp';
export const ORIGINAL_KEY_HEADER = 'x-original-key';
export const ERROR_TYPE_HEADER = 'x-error-type';
export const ERROR_CLASS_HEADER = 'x-error-class';
export const ERROR_MESSAGE_HEADER = 'x-error-message';
export const ERROR_STACK_HEADER = 'x-error-stack';
export const CONSUMER_GROUP_HEADER = 'x-consumer-group';

/**
 * Header values as this codebase produces them, before encoding.
 *
 * `undefined` is permitted so callers can build a header map with conditional
 * entries; {@link encodeHeaders} drops those keys rather than emitting an empty
 * header, because a present-but-empty header reads as "we looked and found
 * nothing" when it actually means "we never set this".
 */
export type HeaderValue = string | number | undefined;

export type HeaderMap = Readonly<Record<string, HeaderValue>>;

/** Encoded form: what the Kafka client accepts and returns. */
export type EncodedHeaders = Record<string, Buffer>;

/** Encodes header values as UTF-8 bytes, omitting entries with no value. */
export function encodeHeaders(headers: HeaderMap): EncodedHeaders {
  const encoded: EncodedHeaders = {};

  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined) {
      continue;
    }
    encoded[name] = Buffer.from(typeof value === 'number' ? String(value) : value, 'utf8');
  }

  return encoded;
}

/**
 * Reads one header back as a string.
 *
 * Returns `undefined` rather than throwing for a missing header: headers are
 * written by other services and older versions of this one, so absence is an
 * expected state and not an error.
 */
export function readHeader(
  headers: Readonly<Record<string, unknown>> | undefined,
  name: string,
): string | undefined {
  const value = headers?.[name];

  if (value === undefined || value === null) {
    return undefined;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString('utf8');
  }
  if (typeof value === 'string') {
    return value;
  }

  return undefined;
}

/** Reads a header that carries an integer, e.g. an attempt count or offset. */
export function readIntHeader(
  headers: Readonly<Record<string, unknown>> | undefined,
  name: string,
): number | undefined {
  const raw = readHeader(headers, name);

  if (raw === undefined) {
    return undefined;
  }

  const parsed = Number.parseInt(raw, 10);
  return Number.isNaN(parsed) ? undefined : parsed;
}
