/**
 * The commit discipline (design decision D7), isolated so it can be tested
 * without a broker.
 *
 * `enable.auto.commit` is off. Auto-commit acknowledges offsets on a timer,
 * regardless of whether the handler has finished — so a crash between the
 * commit and the end of processing loses the message silently. The rule here
 * is the opposite and it is simple: **an offset is committed only after the
 * record it belongs to has reached a terminal state.** Terminal means one of:
 *
 * - processed successfully;
 * - handed off — republished to a retry tier (Phase 6) or written to the DLQ
 *   (Phase 7). The record's fate is now recorded on another topic, so it is
 *   safe to move past it here.
 *
 * Anything else — the handler threw an error it did not classify — is not
 * terminal. Nothing is committed and the error propagates; the client seeks
 * back to the same offset and redelivers. That is the correct behaviour for
 * an at-least-once system facing an unexpected failure: reprocess, never
 * skip.
 *
 * The cost of this rule is the duplicate-on-crash window, stated in ADR 005:
 * a crash *after* processing but *before* the commit reprocesses that record
 * on restart. That is what "at-least-once" means, and it is documented rather
 * than hidden.
 */

/** A record as delivered by the consumer, with nothing decoded yet. */
export interface IncomingRecord {
  readonly topic: string;
  readonly partition: number;
  /** Kafka offsets are int64; the client surfaces them as strings. */
  readonly offset: string;
  readonly timestamp: string;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers: Readonly<Record<string, unknown>> | undefined;
}

/** The position to commit: the offset of the *next* record to read. */
export interface CommitPosition {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
}

/**
 * Terminal outcomes are what commits are gated on. Every variant a processor
 * can return must be one the system is genuinely finished with.
 */
export interface TerminalOutcome {
  readonly kind: string;
}

export interface PipelineOptions<TOutcome extends TerminalOutcome> {
  /** Handles one record. Resolving means terminal; throwing means not. */
  readonly process: (record: IncomingRecord) => Promise<TOutcome>;
  /** Commits a position. Called only after `process` resolves. */
  readonly commit: (position: CommitPosition) => Promise<void>;
}

export interface PipelineStats {
  readonly handled: number;
  readonly committed: number;
  readonly failed: number;
}

export interface Pipeline<TOutcome extends TerminalOutcome> {
  /** Process then commit. Rejects, without committing, if `process` throws. */
  handle: (record: IncomingRecord) => Promise<TOutcome>;
  /**
   * Resolves once every record currently being handled has reached the end
   * of `handle` — committed or failed. Shutdown must await this before
   * disconnecting, or an in-flight record's commit is lost.
   */
  drain: () => Promise<void>;
  readonly inFlight: number;
  readonly stats: PipelineStats;
}

/** The offset to commit for a record: one past the record itself. */
export function nextPosition(record: IncomingRecord): CommitPosition {
  // BigInt, not Number: offsets are int64 and a partition can legitimately
  // exceed 2^53 over a long enough life. Cheap insurance against an
  // off-by-one that only appears after years in production.
  return {
    topic: record.topic,
    partition: record.partition,
    offset: (BigInt(record.offset) + 1n).toString(),
  };
}

export function createPipeline<TOutcome extends TerminalOutcome>({
  process,
  commit,
}: PipelineOptions<TOutcome>): Pipeline<TOutcome> {
  const active = new Set<Promise<unknown>>();
  let handled = 0;
  let committed = 0;
  let failed = 0;

  const handleOne = async (record: IncomingRecord): Promise<TOutcome> => {
    let outcome: TOutcome;
    try {
      outcome = await process(record);
    } catch (error) {
      failed += 1;
      // Deliberately no commit on this path. Rethrow so the client seeks back
      // and redelivers rather than silently advancing past an unhandled record.
      throw error;
    }

    handled += 1;
    await commit(nextPosition(record));
    committed += 1;
    return outcome;
  };

  return {
    handle: async (record) => {
      const task = handleOne(record);
      active.add(task);
      try {
        return await task;
      } finally {
        active.delete(task);
      }
    },

    drain: async () => {
      // Snapshot: a record arriving mid-drain is the client's problem to hold
      // back, not ours to wait for indefinitely.
      await Promise.allSettled([...active]);
    },

    get inFlight() {
      return active.size;
    },

    get stats() {
      return { handled, committed, failed };
    },
  };
}
