import { PermanentError, TransientError } from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import {
  type BackoffOptions,
  InPlaceRetryExhausted,
  jitteredDelay,
  retryInPlace,
} from './backoff.js';

/** A fake clock the sleep advances, so elapsed time is exact and instant. */
function fakeClock(): { now: () => number; sleep: (ms: number) => Promise<void>; slept: number[] } {
  let t = 0;
  const slept: number[] = [];
  return {
    now: () => t,
    sleep: async (ms) => {
      slept.push(ms);
      t += ms;
      await Promise.resolve();
    },
    slept,
  };
}

function options(
  clock: ReturnType<typeof fakeClock>,
  overrides: Partial<BackoffOptions> = {},
): BackoffOptions {
  return {
    maxAttempts: 3,
    budgetMs: 2_000,
    baseMs: 100,
    capMs: 800,
    random: () => 0.5,
    now: clock.now,
    sleep: clock.sleep,
    ...overrides,
  };
}

describe('jitteredDelay', () => {
  it('draws uniformly below an exponentially growing ceiling', () => {
    const spec = { baseMs: 100, capMs: 800 };

    // random = 1 (exclusive upper bound) shows the ceilings: 100, 200, 400, 800, 800.
    expect(jitteredDelay(1, spec, () => 0.999)).toBe(99);
    expect(jitteredDelay(2, spec, () => 0.999)).toBe(199);
    expect(jitteredDelay(3, spec, () => 0.999)).toBe(399);
    expect(jitteredDelay(4, spec, () => 0.999)).toBe(799);
    expect(jitteredDelay(5, spec, () => 0.999)).toBe(799);
  });

  it('can be zero — full jitter includes the floor', () => {
    expect(jitteredDelay(3, { baseMs: 100, capMs: 800 }, () => 0)).toBe(0);
  });

  it('spreads a wave: many draws are not the same value', () => {
    // The whole point of jitter (D5): N failures must not retry in lockstep.
    const delays = new Set<number>();
    for (let i = 0; i < 200; i += 1) {
      delays.add(jitteredDelay(3, { baseMs: 100, capMs: 800 }));
    }
    expect(delays.size).toBeGreaterThan(50);
  });
});

describe('retryInPlace', () => {
  it('returns on the first success without sleeping', async () => {
    const clock = fakeClock();
    const operation = vi.fn(async () => Promise.resolve('ok'));

    const result = await retryInPlace(operation, options(clock));

    expect(result).toEqual({ value: 'ok', attempts: 1 });
    expect(clock.slept).toEqual([]);
  });

  it('retries a transient failure with a jittered sleep and passes the attempt number', async () => {
    const clock = fakeClock();
    const attempts: number[] = [];
    const operation = async (attempt: number): Promise<string> => {
      attempts.push(attempt);
      await Promise.resolve();
      if (attempt < 3) {
        throw new TransientError('blip');
      }
      return 'recovered';
    };

    const result = await retryInPlace(operation, options(clock));

    expect(result).toEqual({ value: 'recovered', attempts: 3 });
    expect(attempts).toEqual([1, 2, 3]);
    // random = 0.5 → half of the 100 and 200 ms ceilings.
    expect(clock.slept).toEqual([50, 100]);
  });

  it('gives up after maxAttempts with the last transient error', async () => {
    const clock = fakeClock();
    const operation = async (): Promise<never> => {
      await Promise.resolve();
      throw new TransientError('still down');
    };

    await expect(retryInPlace(operation, options(clock))).rejects.toBeInstanceOf(
      InPlaceRetryExhausted,
    );
    await expect(retryInPlace(operation, options(clock))).rejects.toMatchObject({
      failure: { attempts: 3, error: { kind: 'transient', message: 'still down' } },
    });
  });

  it('gives up when the next sleep would exceed the elapsed budget', async () => {
    // Attempts are plentiful; time is not. This is the limit that keeps the
    // handler inside max.poll.interval.ms regardless of backoff settings.
    const clock = fakeClock();
    const operation = async (): Promise<never> => {
      await Promise.resolve();
      throw new TransientError('slow');
    };

    await expect(
      retryInPlace(
        operation,
        options(clock, { maxAttempts: 100, budgetMs: 120, random: () => 0.999 }),
      ),
    ).rejects.toMatchObject({ failure: { attempts: 2 } });
    // First sleep 99 ms fits (99 ≤ 120); second would be 99 + 199 > 120 → stop.
    expect(clock.slept).toEqual([99]);
  });

  it('throws a permanent error immediately, classified, without retrying', async () => {
    const clock = fakeClock();
    const operation = vi.fn(async (): Promise<never> => {
      await Promise.resolve();
      throw new PermanentError('validation', 'negative price');
    });

    await expect(retryInPlace(operation, options(clock))).rejects.toBeInstanceOf(PermanentError);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(clock.slept).toEqual([]);
  });

  it('classifies an unknown error before deciding — a raw ECONNRESET is retried', async () => {
    const clock = fakeClock();
    let calls = 0;
    const operation = async (): Promise<string> => {
      calls += 1;
      await Promise.resolve();
      if (calls === 1) {
        throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
      }
      return 'ok';
    };

    await expect(retryInPlace(operation, options(clock))).resolves.toEqual({
      value: 'ok',
      attempts: 2,
    });
  });

  it('classifies an unknown error before deciding — a plain bug is permanent', async () => {
    const clock = fakeClock();
    const operation = vi.fn(async (): Promise<never> => {
      await Promise.resolve();
      throw new TypeError('cannot read properties of undefined');
    });

    await expect(retryInPlace(operation, options(clock))).rejects.toMatchObject({
      kind: 'permanent',
      reason: 'unclassified',
    });
    expect(operation).toHaveBeenCalledTimes(1);
  });
});
