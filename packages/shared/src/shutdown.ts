import type { Logger } from 'pino';

export type ShutdownHook = () => Promise<void> | void;

export interface ShutdownManager {
  /** Hooks run in reverse registration order, so register dependencies first. */
  register: (name: string, hook: ShutdownHook) => void;
  /** Blocks until a termination signal is received and all hooks have run. */
  wait: () => Promise<void>;
  /**
   * Starts the same shutdown sequence a signal would, from application code.
   *
   * Needed whenever a service finishes its work on its own terms rather than
   * being interrupted — a producer that has emitted its configured message
   * budget, or a CLI that has completed its command. Without this, such a
   * process either exits with buffers unflushed or blocks forever in
   * {@link ShutdownManager.wait} waiting for a signal that is never coming.
   *
   * Note that the returned promise does not resolve on the success path: the
   * hook sequence ends in `process.exit`, exactly as it does for a signal.
   * Treat a call to this as the last statement of the program.
   */
  trigger: (reason: string, exitCode?: number) => Promise<void>;
}

export interface ShutdownOptions {
  readonly logger: Logger;
  /** Hard limit before the process is killed regardless of hook progress. */
  readonly timeoutMs?: number;
}

/**
 * Centralised graceful shutdown.
 *
 * Correctness here is not cosmetic: an ungraceful exit leaves the producer's
 * in-flight buffer unflushed and the consumer's processed-but-uncommitted
 * offsets on the floor, which is exactly the data loss the retry/DLQ design
 * exists to prevent.
 */
export function createShutdownManager({
  logger,
  timeoutMs = 10_000,
}: ShutdownOptions): ShutdownManager {
  const hooks: { name: string; hook: ShutdownHook }[] = [];
  let shuttingDown = false;
  let resolveWait: (() => void) | undefined;

  const waitPromise = new Promise<void>((resolve) => {
    resolveWait = resolve;
  });

  const runHooks = async (reason: string, exitCode: number): Promise<void> => {
    if (shuttingDown) {
      logger.warn({ reason }, 'shutdown already in progress; ignoring');
      return;
    }
    shuttingDown = true;
    logger.info({ reason, hooks: hooks.length }, 'graceful shutdown started');

    const forceExit = setTimeout(() => {
      logger.fatal({ timeoutMs }, 'shutdown timed out; forcing exit');
      process.exit(1);
    }, timeoutMs);
    forceExit.unref();

    for (const { name, hook } of [...hooks].reverse()) {
      try {
        await hook();
        logger.debug({ hook: name }, 'shutdown hook completed');
      } catch (error) {
        logger.error({ hook: name, err: error }, 'shutdown hook failed');
      }
    }

    clearTimeout(forceExit);
    logger.info('graceful shutdown complete');
    resolveWait?.();
    process.exit(exitCode);
  };

  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, () => {
      void runHooks(signal, 0);
    });
  }

  process.on('uncaughtException', (error) => {
    logger.fatal({ err: error }, 'uncaught exception');
    void runHooks('uncaughtException', 1);
  });

  process.on('unhandledRejection', (reason) => {
    logger.fatal({ err: reason }, 'unhandled promise rejection');
    void runHooks('unhandledRejection', 1);
  });

  return {
    register: (name, hook) => {
      hooks.push({ name, hook });
    },
    wait: () => waitPromise,
    trigger: (reason, exitCode = 0) => runHooks(reason, exitCode),
  };
}
