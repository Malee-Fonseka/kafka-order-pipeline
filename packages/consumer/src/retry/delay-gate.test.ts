import { RETRY_NOT_BEFORE_HEADER, encodeHeaders } from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import type { IncomingRecord } from '../pipeline.js';
import { type PartitionControls, createDelayGate } from './delay-gate.js';

import type { Logger } from 'pino';

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

const NOW = 1_700_000_000_000;

function record(notBefore: number | undefined, offset = '17', partition = 1): IncomingRecord {
  return {
    topic: 'orders.retry.30s',
    partition,
    offset,
    timestamp: String(NOW),
    key: Buffer.from('Item1'),
    value: Buffer.from([1, 2, 3]),
    headers:
      notBefore === undefined ? undefined : encodeHeaders({ [RETRY_NOT_BEFORE_HEADER]: notBefore }),
  };
}

/** A fake partition with the three operations the gate needs. */
function fakeControls(): PartitionControls & {
  pauses: number;
  resumes: number;
  seeks: string[];
} {
  const state = {
    pauses: 0,
    resumes: 0,
    seeks: [] as string[],
    pause: (): (() => void) => {
      state.pauses += 1;
      return () => {
        state.resumes += 1;
      };
    },
    seek: (offset: string): void => {
      state.seeks.push(offset);
    },
  };
  return state;
}

/** Fake timers under the test's control; nothing is ever actually slept. */
function fakeTimers(): {
  setTimer: (fn: () => void, ms: number) => NodeJS.Timeout;
  clearTimer: (t: NodeJS.Timeout) => void;
  scheduled: { fn: () => void; ms: number; cleared: boolean }[];
  fire: (index: number) => void;
} {
  const scheduled: { fn: () => void; ms: number; cleared: boolean }[] = [];
  return {
    scheduled,
    setTimer: (fn, ms) => {
      const entry = { fn, ms, cleared: false };
      scheduled.push(entry);
      // A minimal object satisfying the Timeout interface the gate uses.
      return {
        unref: () => undefined,
        [Symbol.toPrimitive]: () => scheduled.length - 1,
      } as unknown as NodeJS.Timeout;
    },
    clearTimer: (t) => {
      const index = Number(t);
      const entry = scheduled[index];
      if (entry !== undefined) {
        entry.cleared = true;
      }
    },
    fire: (index) => {
      scheduled[index]?.fn();
    },
  };
}

describe('delay gate (D5 pause + seek)', () => {
  it('lets a record through when its delay has elapsed', () => {
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW });
    const controls = fakeControls();

    const decision = gate.check(record(NOW - 1), controls);

    expect(decision).toEqual({ kind: 'due' });
    expect(controls.pauses).toBe(0);
    expect(controls.seeks).toEqual([]);
  });

  it('lets a record with no not-before header through — nothing to wait for', () => {
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW });

    expect(gate.check(record(undefined), fakeControls())).toEqual({ kind: 'due' });
  });

  it('pauses the partition, seeks back to the record, and schedules a resume for exactly when it is due', () => {
    // The mechanism §2.1 is about, step by step. No sleep anywhere.
    const timers = fakeTimers();
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW, ...timers });
    const controls = fakeControls();

    const decision = gate.check(record(NOW + 25_000, '17'), controls);

    expect(decision).toMatchObject({ kind: 'deferred', remainingMs: 25_000 });
    expect(controls.pauses).toBe(1);
    expect(controls.seeks).toEqual(['17']); // this record's own offset, so it is redelivered
    expect(controls.resumes).toBe(0); // not yet
    expect(timers.scheduled).toHaveLength(1);
    expect(timers.scheduled[0]?.ms).toBe(25_000);
    expect(gate.paused).toEqual(['orders.retry.30s[1]']);
  });

  it('resumes the partition when the timer fires, and forgets it', () => {
    const timers = fakeTimers();
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW, ...timers });
    const controls = fakeControls();
    gate.check(record(NOW + 5_000), controls);

    timers.fire(0);

    expect(controls.resumes).toBe(1);
    expect(gate.paused).toEqual([]);
  });

  it('does not stack timers when the same partition is checked again while paused', () => {
    // After a resume the client redelivers the head record; if it is still
    // early (clock skew, a second early record) the gate must not pause and
    // schedule again on top of a pending resume.
    const timers = fakeTimers();
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW, ...timers });
    const controls = fakeControls();

    gate.check(record(NOW + 5_000, '17'), controls);
    const second = gate.check(record(NOW + 5_000, '17'), controls);

    expect(second.kind).toBe('deferred');
    expect(controls.pauses).toBe(1);
    expect(timers.scheduled).toHaveLength(1);
  });

  it('tracks partitions independently', () => {
    const timers = fakeTimers();
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW, ...timers });
    const p1 = fakeControls();
    const p2 = fakeControls();

    gate.check(record(NOW + 1_000, '3', 1), p1);
    gate.check(record(NOW + 9_000, '8', 2), p2);

    expect(gate.paused).toEqual(['orders.retry.30s[1]', 'orders.retry.30s[2]']);
    timers.fire(0);
    expect(p1.resumes).toBe(1);
    expect(p2.resumes).toBe(0);
    expect(gate.paused).toEqual(['orders.retry.30s[2]']);
  });

  it('cancels pending resumes on close, leaving partitions to disconnect', () => {
    const timers = fakeTimers();
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW, ...timers });
    gate.check(record(NOW + 300_000), fakeControls());

    gate.close();

    expect(timers.scheduled[0]?.cleared).toBe(true);
    expect(gate.paused).toEqual([]);
  });

  it('survives a resume that throws because the consumer already disconnected', () => {
    const timers = fakeTimers();
    const logger = { ...silentLogger, debug: vi.fn() } as unknown as Logger;
    const gate = createDelayGate({ logger, now: () => NOW, ...timers });
    const controls: PartitionControls = {
      pause: () => () => {
        throw new Error('Resume can only be called while connected.');
      },
      seek: () => undefined,
    };
    gate.check(record(NOW + 1_000), controls);

    expect(() => {
      timers.fire(0);
    }).not.toThrow();
    expect(gate.paused).toEqual([]);
  });

  it('never blocks: check returns synchronously regardless of the delay', () => {
    // A 5-minute tier must cost the handler nothing. If this took 300 s the
    // test would time out — that is the assertion.
    const gate = createDelayGate({ logger: silentLogger, now: () => NOW, ...fakeTimers() });
    const started = Date.now();

    gate.check(record(NOW + 300_000), fakeControls());

    expect(Date.now() - started).toBeLessThan(50);
  });
});
