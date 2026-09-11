import {
  ATTEMPT_COUNT_HEADER,
  FIRST_FAILED_AT_HEADER,
  LAST_FAILED_AT_HEADER,
  ORIGINAL_OFFSET_HEADER,
  ORIGINAL_PARTITION_HEADER,
  ORIGINAL_TOPIC_HEADER,
  RETRY_NOT_BEFORE_HEADER,
  readHeader,
  readIntHeader,
} from './headers.js';
import type { RetryTier } from './topics.js';

/**
 * Retry metadata as it travels in record headers across tier hops (D5).
 *
 * The attempt count is the whole reason these live in headers rather than in
 * consumer memory: a record's journey is orders → retry.5s → orders →
 * retry.30s → orders → …, and each hop may be handled by a different instance.
 * The ceiling can only be enforced globally if the count rides with the
 * record.
 */

export interface RetryMetadata {
  /**
   * Deliveries that have failed so far. A delivery is one pass through the
   * in-place stage (up to three handler calls); this counts deliveries, not
   * handler calls, because it is deliveries that map to tiers.
   */
  readonly attempt: number;
  /** Epoch ms before which a retry-tier consumer must not forward the record. */
  readonly notBefore: number | undefined;
  readonly firstFailedAt: string | undefined;
  readonly lastFailedAt: string | undefined;
  /** Where the record was first consumed from — preserved across every hop. */
  readonly originalTopic: string | undefined;
  readonly originalPartition: number | undefined;
  readonly originalOffset: string | undefined;
}

export function readRetryMetadata(
  headers: Readonly<Record<string, unknown>> | undefined,
): RetryMetadata {
  return {
    attempt: readIntHeader(headers, ATTEMPT_COUNT_HEADER) ?? 0,
    notBefore: readIntHeader(headers, RETRY_NOT_BEFORE_HEADER),
    firstFailedAt: readHeader(headers, FIRST_FAILED_AT_HEADER),
    lastFailedAt: readHeader(headers, LAST_FAILED_AT_HEADER),
    originalTopic: readHeader(headers, ORIGINAL_TOPIC_HEADER),
    originalPartition: readIntHeader(headers, ORIGINAL_PARTITION_HEADER),
    originalOffset: readHeader(headers, ORIGINAL_OFFSET_HEADER),
  };
}

export type Escalation =
  | { readonly kind: 'retry'; readonly tier: RetryTier; readonly attempt: number }
  | { readonly kind: 'exhausted'; readonly attempt: number };

/**
 * Where a record goes after its `attempt`-th failed delivery.
 *
 * Tiers are consumed in order: the first failure earns the first tier, the
 * second the second, and a failure with no tier left is exhausted. With the
 * default three tiers that is 5s → 30s → 5m → DLQ, and the total time a
 * transient failure is given before it is declared permanent is the sum of
 * the delays, a little under six minutes.
 */
export function escalate(previousAttempt: number, tiers: readonly RetryTier[]): Escalation {
  const attempt = previousAttempt + 1;
  const tier = tiers[attempt - 1];
  return tier === undefined ? { kind: 'exhausted', attempt } : { kind: 'retry', tier, attempt };
}

export interface RetryHeaderUpdate {
  readonly attempt: number;
  readonly notBefore: number;
  readonly now: Date;
  readonly existing: RetryMetadata;
  /** The record's current location, recorded as the origin on the first hop only. */
  readonly source: { readonly topic: string; readonly partition: number; readonly offset: string };
}

/** The header values to set when republishing to a retry tier. */
export function retryHeaderValues({
  attempt,
  notBefore,
  now,
  existing,
  source,
}: RetryHeaderUpdate): Record<string, string | number> {
  const iso = now.toISOString();
  return {
    [ATTEMPT_COUNT_HEADER]: attempt,
    [RETRY_NOT_BEFORE_HEADER]: notBefore,
    [FIRST_FAILED_AT_HEADER]: existing.firstFailedAt ?? iso,
    [LAST_FAILED_AT_HEADER]: iso,
    [ORIGINAL_TOPIC_HEADER]: existing.originalTopic ?? source.topic,
    [ORIGINAL_PARTITION_HEADER]: existing.originalPartition ?? source.partition,
    [ORIGINAL_OFFSET_HEADER]: existing.originalOffset ?? source.offset,
  };
}
