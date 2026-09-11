import { describe, expect, it, vi } from 'vitest';

import { type IncomingRecord, createPipeline, nextPosition } from './pipeline.js';

const record = (offset: string, partition = 0): IncomingRecord => ({
  topic: 'orders',
  partition,
  offset,
  timestamp: '1700000000000',
  key: Buffer.from('Item1'),
  value: Buffer.from([0x00, 0, 0, 0, 1]),
  headers: undefined,
});

interface Outcome {
  kind: 'processed';
}

const processed: Outcome = { kind: 'processed' };

/** A promise whose settlement is under the test's control. */
function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (e: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('nextPosition', () => {
  it('commits one past the record, the offset of the next record to read', () => {
    expect(nextPosition(record('41', 2))).toEqual({ topic: 'orders', partition: 2, offset: '42' });
  });

  it('survives offsets beyond Number precision', () => {
    // int64 on the broker; a Number would round this and commit the wrong
    // position on a very long-lived partition.
    expect(nextPosition(record('9007199254740993')).offset).toBe('9007199254740994');
  });
});

describe('pipeline commit discipline (D7)', () => {
  it('commits only after process resolves, and with the next position', async () => {
    const order: string[] = [];
    const pipeline = createPipeline<Outcome>({
      process: () => {
        order.push('process');
        return Promise.resolve(processed);
      },
      commit: (position) => {
        order.push(`commit:${position.offset}`);
        return Promise.resolve();
      },
    });

    await pipeline.handle(record('7'));

    expect(order).toEqual(['process', 'commit:8']);
  });

  it('does not commit when process throws, and rethrows', async () => {
    // The load-bearing assertion. Committing here would acknowledge a record
    // that was never handled — the silent data loss auto-commit causes.
    const commit = vi.fn(async () => Promise.resolve());
    const pipeline = createPipeline<Outcome>({
      process: async () => {
        await Promise.resolve();
        throw new Error('unexpected handler failure');
      },
      commit,
    });

    await expect(pipeline.handle(record('7'))).rejects.toThrow('unexpected handler failure');
    expect(commit).not.toHaveBeenCalled();
    expect(pipeline.stats).toEqual({ handled: 0, committed: 0, failed: 1 });
  });

  it('does not begin the commit until process has actually finished', async () => {
    const processing = deferred<Outcome>();
    const commit = vi.fn(async () => Promise.resolve());
    const pipeline = createPipeline<Outcome>({
      process: () => processing.promise,
      commit,
    });

    const handling = pipeline.handle(record('7'));
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(commit).not.toHaveBeenCalled();
    processing.resolve(processed);
    await handling;
    expect(commit).toHaveBeenCalledTimes(1);
  });

  it('surfaces a commit failure without counting the record as committed', async () => {
    const pipeline = createPipeline<Outcome>({
      process: () => Promise.resolve(processed),
      commit: async () => {
        await Promise.resolve();
        throw new Error('coordinator unavailable');
      },
    });

    await expect(pipeline.handle(record('7'))).rejects.toThrow('coordinator unavailable');
    expect(pipeline.stats).toEqual({ handled: 1, committed: 0, failed: 0 });
  });

  it('returns the processor outcome to the caller', async () => {
    const pipeline = createPipeline<Outcome>({
      process: () => Promise.resolve(processed),
      commit: async () => Promise.resolve(),
    });

    await expect(pipeline.handle(record('7'))).resolves.toBe(processed);
  });
});

describe('pipeline drain', () => {
  it('tracks in-flight records', async () => {
    const processing = deferred<Outcome>();
    const pipeline = createPipeline<Outcome>({
      process: () => processing.promise,
      commit: async () => Promise.resolve(),
    });

    expect(pipeline.inFlight).toBe(0);
    const handling = pipeline.handle(record('7'));
    expect(pipeline.inFlight).toBe(1);

    processing.resolve(processed);
    await handling;
    expect(pipeline.inFlight).toBe(0);
  });

  it('waits for the in-flight record to commit before resolving', async () => {
    // Shutdown calls drain() then disconnect(). If drain returned early the
    // commit would race the disconnect and the record would be reprocessed on
    // restart — exactly the offset loss the gate forbids.
    const processing = deferred<Outcome>();
    const events: string[] = [];
    const pipeline = createPipeline<Outcome>({
      process: () => processing.promise,
      commit: () => {
        events.push('commit');
        return Promise.resolve();
      },
    });

    void pipeline.handle(record('7'));
    const draining = pipeline.drain().then(() => {
      events.push('drained');
    });

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(events).toEqual([]);

    processing.resolve(processed);
    await draining;
    expect(events).toEqual(['commit', 'drained']);
  });

  it('resolves even when an in-flight record fails', async () => {
    // A failing record must not wedge shutdown; the failure is already
    // reported by handle's rejection.
    const processing = deferred<Outcome>();
    const pipeline = createPipeline<Outcome>({
      process: () => processing.promise,
      commit: async () => Promise.resolve(),
    });

    const handling = pipeline.handle(record('7')).catch(() => undefined);
    const draining = pipeline.drain();

    processing.reject(new Error('boom'));
    await handling;
    await expect(draining).resolves.toBeUndefined();
    expect(pipeline.inFlight).toBe(0);
  });

  it('resolves immediately with nothing in flight', async () => {
    const pipeline = createPipeline<Outcome>({
      process: () => Promise.resolve(processed),
      commit: async () => Promise.resolve(),
    });

    await expect(pipeline.drain()).resolves.toBeUndefined();
  });
});
