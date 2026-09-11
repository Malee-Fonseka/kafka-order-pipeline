import { type ClassifiedError, classifyError, isTransient } from '@order-pipeline/shared';

/**
 * Stage 1 of the retry strategy (D5): bounded, in-place, jittered.
 *
 * Most transient failures are over in milliseconds — a dropped connection
 * that reconnects, a registry that was restarting. Republishing to a retry
 * topic for those is expensive and slow: an extra produce, an extra consume,
 * a five-second wait. So the first few attempts happen right here, inside the
 * handler, with two hard limits that together keep the consumer well inside
 * `max.poll.interval.ms` (300 s):
 *
 * - at most `maxAttempts` calls (default 3);
 * - at most `budgetMs` of elapsed time including the sleeps (default 2 s).
 *
 * Whichever is hit first ends the stage. The budget is the one that matters
 * for group membership: three attempts with unbounded backoff could exceed the
 * poll interval; 2 s cannot.
 *
 * **Jitter is mandatory.** Without it, N records that failed together retry
 * together, and a downstream that was just recovering is hit by a wave of
 * synchronised retries and falls over again. Full jitter — a uniform draw from
 * `[0, exponential]` — spreads the wave out.
 */

export interface BackoffOptions {
  readonly maxAttempts: number;
  readonly budgetMs: number;
  /** First delay ceiling; each attempt doubles it. */
  readonly baseMs: number;
  /** Upper bound on any single delay. */
  readonly capMs: number;
  /** Injected for tests. */
  readonly random?: () => number;
  readonly now?: () => number;
  readonly sleep?: (ms: number) => Promise<void>;
}

export const DEFAULT_BACKOFF: Omit<BackoffOptions, 'random' | 'now' | 'sleep'> = {
  maxAttempts: 3,
  budgetMs: 2_000,
  baseMs: 100,
  capMs: 800,
};

/** Full-jitter exponential delay for the gap *after* the given 1-based attempt. */
export function jitteredDelay(
  attempt: number,
  { baseMs, capMs }: Pick<BackoffOptions, 'baseMs' | 'capMs'>,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(capMs, baseMs * 2 ** (attempt - 1));
  return Math.floor(random() * ceiling);
}

export interface InPlaceResult<T> {
  readonly value: T;
  /** Handler calls it took, including the successful one. */
  readonly attempts: number;
}

export interface InPlaceFailure {
  readonly error: ClassifiedError;
  readonly attempts: number;
  readonly elapsedMs: number;
}

export class InPlaceRetryExhausted extends Error {
  public readonly failure: InPlaceFailure;

  public constructor(failure: InPlaceFailure) {
    super(
      `still failing after ${String(failure.attempts)} in-place attempt(s) over ${String(failure.elapsedMs)}ms: ${failure.error.message}`,
      { cause: failure.error },
    );
    this.name = 'InPlaceRetryExhausted';
    this.failure = failure;
  }
}

const defaultSleep = async (ms: number): Promise<void> => {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
};

/**
 * Runs `operation`, retrying **transient** failures in place.
 *
 * - A permanent error is thrown immediately, classified; retrying it would be
 *   the §2.3 livelock.
 * - A transient error is retried until an attempt succeeds or a limit is hit,
 *   at which point {@link InPlaceRetryExhausted} is thrown carrying the last
 *   error, so stage 2 can escalate with the right diagnostics.
 * - Unclassified errors are classified first, so the decision to retry is
 *   always the taxonomy's, never a `catch` block's guess.
 */
export async function retryInPlace<T>(
  operation: (attempt: number) => Promise<T>,
  options: BackoffOptions,
): Promise<InPlaceResult<T>> {
  const {
    maxAttempts,
    budgetMs,
    random = Math.random,
    now = Date.now,
    sleep = defaultSleep,
  } = options;
  const started = now();

  for (let attempt = 1; ; attempt += 1) {
    try {
      const value = await operation(attempt);
      return { value, attempts: attempt };
    } catch (raw) {
      const error = classifyError(raw);
      if (!isTransient(error)) {
        throw error;
      }

      const elapsedMs = now() - started;
      const delay = jitteredDelay(attempt, options, random);
      const outOfAttempts = attempt >= maxAttempts;
      const outOfBudget = elapsedMs + delay > budgetMs;

      if (outOfAttempts || outOfBudget) {
        throw new InPlaceRetryExhausted({ error, attempts: attempt, elapsedMs });
      }

      await sleep(delay);
    }
  }
}
