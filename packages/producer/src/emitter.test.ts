import { describe, expect, it, vi } from 'vitest';

import { createEmitter } from './emitter.js';

describe('createEmitter', () => {
  it('stops at the message budget', async () => {
    const emit = vi.fn(async () => Promise.resolve());
    const emitter = createEmitter({ ratePerSecond: 1000, maxMessages: 5, emit });

    await emitter.run();

    expect(emit).toHaveBeenCalledTimes(5);
    expect(emitter.emitted).toBe(5);
  });

  it('does not wait out the final interval before returning', async () => {
    // A bounded run must finish as soon as the budget is spent. Sleeping after
    // the last message would add a full interval to every scripted demo.
    const emitter = createEmitter({
      ratePerSecond: 2, // 500ms between messages
      maxMessages: 2,
      emit: async () => Promise.resolve(),
    });

    const started = Date.now();
    await emitter.run();
    const elapsed = Date.now() - started;

    // One interval between the two messages, none after the second.
    expect(elapsed).toBeLessThan(900);
  });

  it('stops promptly rather than waiting out the pending delay', async () => {
    // The property that matters for Ctrl-C: at one message every two seconds, a
    // non-cancellable delay would make shutdown take up to two seconds before
    // it could even start flushing.
    const emitter = createEmitter({
      ratePerSecond: 0.5, // 2s between messages
      emit: async () => Promise.resolve(),
    });

    const running = emitter.run();
    // Let the first emission happen and the loop enter its delay.
    await new Promise((resolve) => setTimeout(resolve, 50));

    const started = Date.now();
    await emitter.stop();
    await running;
    const elapsed = Date.now() - started;

    expect(elapsed).toBeLessThan(500);
    expect(emitter.emitted).toBe(1);
  });

  it('lets an in-flight emission settle before stop resolves', async () => {
    // Shutdown must not abandon a send that is already on the wire, or the
    // producer buffer is dropped rather than flushed.
    let settled = false;
    const emitter = createEmitter({
      ratePerSecond: 1000,
      emit: async () => {
        await new Promise((resolve) => setTimeout(resolve, 120));
        settled = true;
      },
    });

    const running = emitter.run();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await emitter.stop();

    expect(settled).toBe(true);
    await running;
  });

  it('propagates a failing emission out of run', async () => {
    const emitter = createEmitter({
      ratePerSecond: 1000,
      emit: async () => {
        await Promise.resolve();
        throw new Error('broker unavailable');
      },
    });

    await expect(emitter.run()).rejects.toThrow('broker unavailable');
  });

  it('does not re-throw the failed emission from stop', async () => {
    // `run` already reported it; a second rejection during shutdown would abort
    // the remaining hooks and skip the flush.
    const emitter = createEmitter({
      ratePerSecond: 1000,
      emit: async () => {
        await Promise.resolve();
        throw new Error('broker unavailable');
      },
    });

    await expect(emitter.run()).rejects.toThrow();
    await expect(emitter.stop()).resolves.toBeUndefined();
  });

  it('runs unbounded until stopped', async () => {
    const emitter = createEmitter({
      ratePerSecond: 500,
      emit: async () => Promise.resolve(),
    });

    const running = emitter.run();
    await new Promise((resolve) => setTimeout(resolve, 60));
    await emitter.stop();
    await running;

    expect(emitter.emitted).toBeGreaterThan(1);
  });
});
