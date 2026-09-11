/**
 * Welford's online algorithm (design decision D3).
 *
 * The assignment asks for a running average of `price`, and `price` is an Avro
 * `float` — 32 bits. The obvious implementation keeps a running `sum` and
 * divides by `count`. Two things go wrong with it:
 *
 * 1. **The sum accumulates rounding error.** Each addition rounds; after tens
 *    of thousands of additions the low bits of the sum are noise, and the
 *    mean inherits that noise.
 * 2. **Variance from `sum` and `sumOfSquares` is catastrophically unstable.**
 *    `E[x²] − E[x]²` subtracts two large, nearly-equal numbers and can even go
 *    negative.
 *
 * Welford's method instead updates the mean *incrementally* — each new value
 * moves the mean by `(x − mean) / n` — and accumulates `m2`, the sum of squared
 * deviations from the *running* mean. Both are numerically stable, both run in
 * O(1) per value with O(1) state, and variance falls out for free.
 *
 * All arithmetic is in JavaScript `number`, i.e. IEEE 754 double. The inputs
 * are float32 (as decoded from Avro) but the accumulators are not; widening
 * at the aggregation boundary is the whole point.
 *
 * Everything here is pure and the state is a plain immutable value, so it
 * serialises to the changelog topic without ceremony and restores by simply
 * being read back.
 */

export interface WelfordState {
  /** Values folded in so far. */
  readonly count: number;
  /** Running mean, double precision. */
  readonly mean: number;
  /** Sum of squared deviations from the running mean. Variance = m2 / (n − 1). */
  readonly m2: number;
  readonly min: number;
  readonly max: number;
  /** Epoch milliseconds of the most recent update; 0 when empty. */
  readonly lastUpdated: number;
}

export const EMPTY_STATE: WelfordState = Object.freeze({
  count: 0,
  mean: 0,
  m2: 0,
  min: Number.POSITIVE_INFINITY,
  max: Number.NEGATIVE_INFINITY,
  lastUpdated: 0,
});

/** Folds one value in. */
export function update(state: WelfordState, value: number, at: number): WelfordState {
  const count = state.count + 1;
  const delta = value - state.mean;
  const mean = state.mean + delta / count;
  // delta2 uses the *new* mean; the product of the two deltas is the standard
  // Welford increment and is what keeps the accumulation stable.
  const delta2 = value - mean;

  return {
    count,
    mean,
    m2: state.m2 + delta * delta2,
    min: Math.min(state.min, value),
    max: Math.max(state.max, value),
    lastUpdated: Math.max(state.lastUpdated, at),
  };
}

/**
 * Combines two independent aggregates (Chan, Golub & LeVeque, 1979).
 *
 * The result equals folding every underlying value into one state, without
 * revisiting a single value. Two uses in this system: the global aggregate is
 * the merge of the per-product ones an instance owns, and it is the mechanism
 * that would make the bucketed-key upgrade in ADR 002 work.
 */
export function merge(a: WelfordState, b: WelfordState): WelfordState {
  if (a.count === 0) {
    return b;
  }
  if (b.count === 0) {
    return a;
  }

  const count = a.count + b.count;
  const delta = b.mean - a.mean;

  return {
    count,
    // Weighted by the smaller share so the mean never leaves [a.mean, b.mean].
    mean: a.mean + (delta * b.count) / count,
    m2: a.m2 + b.m2 + (delta * delta * a.count * b.count) / count,
    min: Math.min(a.min, b.min),
    max: Math.max(a.max, b.max),
    lastUpdated: Math.max(a.lastUpdated, b.lastUpdated),
  };
}

/** Sample variance (Bessel-corrected). Undefined for fewer than two values. */
export function variance(state: WelfordState): number | undefined {
  return state.count < 2 ? undefined : state.m2 / (state.count - 1);
}

export function stddev(state: WelfordState): number | undefined {
  const v = variance(state);
  return v === undefined ? undefined : Math.sqrt(v);
}

/** The shape exposed over the API: derived figures included, sentinels removed. */
export interface AggregateView {
  readonly count: number;
  readonly mean: number;
  readonly min: number | null;
  readonly max: number | null;
  readonly variance: number | null;
  readonly stddev: number | null;
  /** ISO timestamp, or null when nothing has been aggregated. */
  readonly lastUpdated: string | null;
}

export function toView(state: WelfordState): AggregateView {
  const empty = state.count === 0;
  return {
    count: state.count,
    mean: state.mean,
    min: empty ? null : state.min,
    max: empty ? null : state.max,
    variance: variance(state) ?? null,
    stddev: stddev(state) ?? null,
    lastUpdated: empty ? null : new Date(state.lastUpdated).toISOString(),
  };
}
