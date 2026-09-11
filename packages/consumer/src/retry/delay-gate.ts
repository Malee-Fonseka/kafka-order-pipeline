import { type Logger, readRetryMetadata } from '@order-pipeline/shared';

import type { IncomingRecord } from '../pipeline.js';

/**
 * The delay mechanism for retry tiers (D5) — the part §2.1 is about.
 *
 * A record on `orders.retry.30s` must not be forwarded until thirty seconds
 * after it failed. The obvious implementation — `await sleep(remaining)`
 * inside the handler — is the one this project exists to avoid: the consumer
 * stops polling for the duration, and once that exceeds `max.poll.interval.ms`
 * the broker declares it dead, revokes its partitions, and the group enters a
 * rebalance. For the five-minute tier that is a certainty, not a risk.
 *
 * Instead, when a record arrives early:
 *
 * 1. **pause** the partition — the client stops fetching from it, but keeps
 *    polling everything else and keeps heartbeating, so the broker sees a
 *    healthy member throughout;
 * 2. **seek** the partition back to this record's offset — the record is not
 *    lost; it will be the next thing delivered when fetching resumes;
 * 3. **schedule a resume** for when the record is due, on a timer that runs
 *    outside any handler;
 * 4. **return without committing** — nothing terminal has happened.
 *
 * When the timer fires, the partition resumes, the same record is redelivered,
 * and this time it is due, so it is forwarded. Wall-clock time passes; the
 * handler never blocks.
 *
 * Pausing is per partition, which is Kafka's granularity. Any other record on
 * the same partition waits too — but retry topics are appended in failure
 * order, so the head record is always the earliest due, and nothing behind it
 * could have been forwarded sooner anyway.
 */

export interface PartitionControls {
  /** Pauses this record's partition; returns the function that resumes it. */
  readonly pause: () => () => void;
  /** Seeks this record's partition to the given offset. */
  readonly seek: (offset: string) => void;
}

export type GateDecision =
  | { readonly kind: 'due' }
  | { readonly kind: 'deferred'; readonly remainingMs: number; readonly resumeAt: Date };

export interface DelayGateOptions {
  readonly logger: Logger;
  readonly now?: () => number;
  readonly setTimer?: (fn: () => void, ms: number) => NodeJS.Timeout;
  readonly clearTimer?: (timer: NodeJS.Timeout) => void;
}

export interface DelayGate {
  /**
   * Decides whether the record may be forwarded now. On `deferred`, the
   * partition has been paused and seeked, and a resume is scheduled; the
   * caller must return without committing.
   */
  check: (record: IncomingRecord, controls: PartitionControls) => GateDecision;
  /** Partitions currently held back, for the dashboard and tests. */
  readonly paused: readonly string[];
  /** Cancels pending resumes. Paused partitions are released by disconnect. */
  close: () => void;
}

const partitionKey = (record: IncomingRecord): string =>
  `${record.topic}[${String(record.partition)}]`;

export function createDelayGate({
  logger,
  now = Date.now,
  setTimer = (fn, ms) => setTimeout(fn, ms),
  clearTimer = (timer) => {
    clearTimeout(timer);
  },
}: DelayGateOptions): DelayGate {
  const pending = new Map<string, NodeJS.Timeout>();

  return {
    check(record, controls) {
      const { notBefore } = readRetryMetadata(record.headers);
      const current = now();

      // No timestamp means the record did not come through the republisher —
      // a manual replay, perhaps. Nothing to wait for.
      if (notBefore === undefined || notBefore <= current) {
        return { kind: 'due' };
      }

      const key = partitionKey(record);
      const remainingMs = notBefore - current;

      // The client redelivers the head record after every resume, so a
      // partition can be checked again while a timer is already pending. Do
      // not stack timers; the existing one is correct.
      if (pending.has(key)) {
        return { kind: 'deferred', remainingMs, resumeAt: new Date(notBefore) };
      }

      const resume = controls.pause();
      controls.seek(record.offset);

      const timer = setTimer(() => {
        pending.delete(key);
        try {
          resume();
          logger.debug({ partition: key }, 'retry partition resumed');
        } catch (error) {
          // Disconnected in the meantime; the record is uncommitted and will
          // be redelivered on the next start.
          logger.debug({ partition: key, err: error }, 'resume after delay failed');
        }
      }, remainingMs);
      timer.unref();
      pending.set(key, timer);

      logger.info(
        {
          partition: key,
          offset: record.offset,
          remainingMs,
          resumeAt: new Date(notBefore).toISOString(),
        },
        'retry record not yet due; partition paused and seeked back',
      );

      return { kind: 'deferred', remainingMs, resumeAt: new Date(notBefore) };
    },

    get paused() {
      return [...pending.keys()];
    },

    close() {
      for (const timer of pending.values()) {
        clearTimer(timer);
      }
      pending.clear();
    },
  };
}
