/**
 * Paced, interruptible emission loop.
 *
 * Two properties matter here and neither is free:
 *
 * 1. **The delay is cancellable.** A plain `await sleep(200)` inside the loop
 *    would make shutdown wait out the current interval before it could even
 *    begin flushing. At one message every few seconds that is a visibly slow
 *    Ctrl-C; the same mistake in the *consumer* is the rebalance bug §2.1
 *    warns about, so the pattern is worth establishing correctly here.
 * 2. **A send in progress is awaited, not abandoned.** Shutdown stops the loop
 *    from starting new work and then lets the current send settle, so the
 *    producer's buffer is flushed rather than dropped (§12).
 */

export interface EmitterOptions {
  /** Messages per second. Fractional rates are allowed. */
  readonly ratePerSecond: number;
  /** Stop after this many emissions. Unset means run until stopped. */
  readonly maxMessages?: number | undefined;
  /** One emission. Rejections propagate and stop the loop. */
  readonly emit: () => Promise<void>;
}

export interface Emitter {
  /** Resolves when the loop ends: stopped, or the message budget is spent. */
  run: () => Promise<void>;
  /** Stops the loop, cancels any pending delay, and waits for the in-flight emit. */
  stop: () => Promise<void>;
  readonly emitted: number;
}

export function createEmitter({ ratePerSecond, maxMessages, emit }: EmitterOptions): Emitter {
  const intervalMs = 1000 / ratePerSecond;

  let running = true;
  let emitted = 0;
  let inFlight: Promise<void> = Promise.resolve();
  let cancelDelay: (() => void) | undefined;

  /** Resolves after `ms`, or immediately when the loop is stopped. */
  const delay = async (ms: number): Promise<void> => {
    await new Promise<void>((resolvePromise) => {
      const timer = setTimeout(() => {
        cancelDelay = undefined;
        resolvePromise();
      }, ms);

      cancelDelay = (): void => {
        clearTimeout(timer);
        cancelDelay = undefined;
        resolvePromise();
      };
    });
  };

  /**
   * Read through a function, not the variable directly. `stop` mutates
   * `running` from another closure while the loop is suspended at an `await`,
   * which the compiler's control-flow analysis cannot see — it would narrow the
   * flag to its loop-entry value and treat the re-check below as dead code.
   */
  const shouldContinue = (): boolean =>
    running && (maxMessages === undefined || emitted < maxMessages);

  return {
    async run(): Promise<void> {
      while (shouldContinue()) {
        inFlight = emit();
        await inFlight;
        emitted += 1;

        // Re-checked after the await: the budget may now be spent, or shutdown
        // may have run. Either way, do not start a delay nobody is waiting for.
        if (!shouldContinue()) {
          break;
        }

        await delay(intervalMs);
      }
    },

    async stop(): Promise<void> {
      running = false;
      cancelDelay?.();

      // Swallow here only: a failing send has already been reported by `run`,
      // and shutdown must continue to the flush regardless.
      await inFlight.catch(() => undefined);
    },

    get emitted(): number {
      return emitted;
    },
  };
}
