import { describe, expect, it } from 'vitest';

import {
  EMPTY_STATE,
  type WelfordState,
  merge,
  stddev,
  toView,
  update,
  variance,
} from './welford.js';

function fold(values: readonly number[], startAt = 1_000): WelfordState {
  return values.reduce((state, value, i) => update(state, value, startAt + i), EMPTY_STATE);
}

describe('Welford control set (phase 5 gate)', () => {
  // Worked by hand:
  //   values      10, 20, 30, 40, 50           n = 5
  //   sum         150                          mean = 150 / 5 = 30
  //   deviations  -20, -10, 0, 10, 20
  //   squares     400, 100, 0, 100, 400        m2 = 1000
  //   variance    1000 / (5 - 1) = 250         stddev = √250 = 15.8113883…
  it('matches the hand-computed figures for 10..50', () => {
    const state = fold([10, 20, 30, 40, 50]);

    expect(state.count).toBe(5);
    expect(state.mean).toBe(30);
    expect(state.m2).toBeCloseTo(1000, 10);
    expect(variance(state)).toBeCloseTo(250, 10);
    expect(stddev(state)).toBeCloseTo(15.811388300841896, 10);
    expect(state.min).toBe(10);
    expect(state.max).toBe(50);
  });

  // Worked by hand, non-integer:
  //   values      2.5, 4.0, 4.0, 5.5, 9.0      n = 5
  //   sum         25.0                         mean = 5.0
  //   deviations  -2.5, -1.0, -1.0, 0.5, 4.0
  //   squares     6.25, 1.0, 1.0, 0.25, 16.0   m2 = 24.5
  //   variance    24.5 / 4 = 6.125             stddev = √6.125 = 2.4748737…
  it('matches the hand-computed figures for a fractional set', () => {
    const state = fold([2.5, 4.0, 4.0, 5.5, 9.0]);

    expect(state.mean).toBeCloseTo(5.0, 12);
    expect(state.m2).toBeCloseTo(24.5, 12);
    expect(variance(state)).toBeCloseTo(6.125, 12);
    expect(stddev(state)).toBeCloseTo(2.474873734152916, 12);
  });

  it('is order-independent for the mean and m2', () => {
    const forward = fold([10, 20, 30, 40, 50]);
    const shuffled = fold([50, 10, 40, 20, 30]);

    expect(shuffled.mean).toBeCloseTo(forward.mean, 12);
    expect(shuffled.m2).toBeCloseTo(forward.m2, 9);
  });
});

describe('numerical stability — why not a running sum', () => {
  it('holds the mean exact where a float32 running sum drifts', () => {
    // 100 000 copies of float32(0.1). The true mean is exactly that value. A
    // float32 accumulator loses low bits on every addition once the sum is
    // large relative to the addend; the drift compounds into the mean.
    const x = Math.fround(0.1);
    const n = 100_000;

    let naiveSum32 = 0;
    for (let i = 0; i < n; i += 1) {
      naiveSum32 = Math.fround(naiveSum32 + x);
    }
    const naiveMean = naiveSum32 / n;

    let state = EMPTY_STATE;
    for (let i = 0; i < n; i += 1) {
      state = update(state, x, i);
    }

    expect(Math.abs(state.mean - x)).toBeLessThan(1e-12);
    expect(Math.abs(naiveMean - x)).toBeGreaterThan(1e-6);
  });

  it('holds the variance where E[x²] − E[x]² collapses', () => {
    // Four values near 1e9 with a true sample variance of 30:
    //   deviations from the mean (1e9 + 10): -6, -3, 3, 6
    //   squares 36, 9, 9, 36 → 90 / 3 = 30
    // The textbook formula subtracts two ~1e18 quantities and, in double
    // precision, has nothing left after cancellation.
    const values = [1e9 + 4, 1e9 + 7, 1e9 + 13, 1e9 + 16];

    const sum = values.reduce((a, b) => a + b, 0);
    const sumSq = values.reduce((a, b) => a + b * b, 0);
    const textbook = (sumSq / values.length - (sum / values.length) ** 2) * (4 / 3);

    expect(variance(fold(values))).toBeCloseTo(30, 6);
    expect(Math.abs(textbook - 30)).toBeGreaterThan(1);
  });
});

describe('merge', () => {
  it('equals folding all values sequentially', () => {
    // Hand check of the merge itself:
    //   a = fold(10, 20, 30):  n=3, mean=20, m2=200
    //   b = fold(40, 50):      n=2, mean=45, m2=50
    //   delta = 25
    //   mean = 20 + 25·2/5 = 30
    //   m2   = 200 + 50 + 25²·3·2/5 = 250 + 750 = 1000
    const merged = merge(fold([10, 20, 30]), fold([40, 50]));
    const sequential = fold([10, 20, 30, 40, 50]);

    expect(merged.count).toBe(5);
    expect(merged.mean).toBeCloseTo(30, 12);
    expect(merged.m2).toBeCloseTo(1000, 9);
    expect(merged.min).toBe(10);
    expect(merged.max).toBe(50);
    expect(merged.mean).toBeCloseTo(sequential.mean, 12);
    expect(merged.m2).toBeCloseTo(sequential.m2, 9);
  });

  it('is commutative', () => {
    const a = fold([1, 2, 3]);
    const b = fold([100, 200]);

    const ab = merge(a, b);
    const ba = merge(b, a);

    expect(ab.mean).toBeCloseTo(ba.mean, 12);
    expect(ab.m2).toBeCloseTo(ba.m2, 9);
  });

  it('treats an empty side as the identity', () => {
    const a = fold([7, 9]);

    expect(merge(a, EMPTY_STATE)).toEqual(a);
    expect(merge(EMPTY_STATE, a)).toEqual(a);
    expect(merge(EMPTY_STATE, EMPTY_STATE)).toEqual(EMPTY_STATE);
  });

  it('keeps the newest lastUpdated', () => {
    const older = update(EMPTY_STATE, 1, 1_000);
    const newer = update(EMPTY_STATE, 2, 5_000);

    expect(merge(older, newer).lastUpdated).toBe(5_000);
  });
});

describe('edge cases', () => {
  it('has no variance with fewer than two values', () => {
    expect(variance(EMPTY_STATE)).toBeUndefined();
    expect(variance(update(EMPTY_STATE, 42, 1))).toBeUndefined();
    expect(stddev(update(EMPTY_STATE, 42, 1))).toBeUndefined();
  });

  it('reports a single value as its own mean, min and max', () => {
    const state = update(EMPTY_STATE, 42, 1);

    expect(state).toMatchObject({ count: 1, mean: 42, m2: 0, min: 42, max: 42 });
  });

  it('does not mutate the previous state', () => {
    const before = fold([1, 2]);
    const snapshot = { ...before };

    update(before, 3, 99);

    expect(before).toEqual(snapshot);
  });

  it('never lets lastUpdated go backwards on out-of-order timestamps', () => {
    const state = update(update(EMPTY_STATE, 1, 5_000), 2, 4_000);

    expect(state.lastUpdated).toBe(5_000);
  });
});

describe('toView', () => {
  it('replaces infinity sentinels with null when empty', () => {
    expect(toView(EMPTY_STATE)).toEqual({
      count: 0,
      mean: 0,
      min: null,
      max: null,
      variance: null,
      stddev: null,
      lastUpdated: null,
    });
  });

  it('renders derived figures and an ISO timestamp', () => {
    const view = toView(fold([10, 20, 30, 40, 50], Date.UTC(2026, 0, 1)));

    expect(view.count).toBe(5);
    expect(view.mean).toBe(30);
    expect(view.variance).toBeCloseTo(250, 10);
    expect(view.stddev).toBeCloseTo(15.811388300841896, 10);
    expect(view.lastUpdated).toBe('2026-01-01T00:00:00.004Z');
  });
});
