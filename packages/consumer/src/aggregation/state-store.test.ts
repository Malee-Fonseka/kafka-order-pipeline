import { describe, expect, it } from 'vitest';

import type { ProductEntry } from './aggregator.js';
import { decodeEntry, encodeEntry } from './state-store.js';
import { EMPTY_STATE, update } from './welford.js';

const entry: ProductEntry = {
  product: 'Item1',
  partition: 2,
  lastOffset: '9007199254740993',
  state: update(update(EMPTY_STATE, 19.99, 1_700_000_000_000), 5.25, 1_700_000_001_000),
};

/** The encoded entry as a plain object, for building corrupt variants. */
function encodedObject(): Record<string, unknown> {
  return JSON.parse(encodeEntry(entry).toString('utf8')) as Record<string, unknown>;
}

describe('changelog codec', () => {
  it('round-trips an entry exactly', () => {
    expect(decodeEntry(encodeEntry(entry))).toEqual(entry);
  });

  it('preserves double precision in the accumulators', () => {
    // m2 for these two values is not representable in float32; the changelog
    // must carry the double so restore does not reintroduce the rounding
    // Welford exists to avoid.
    const decoded = decodeEntry(encodeEntry(entry));

    expect(decoded.state.m2).toBe(entry.state.m2);
    expect(decoded.state.mean).toBe(entry.state.mean);
  });

  it('keeps the offset as a string so int64 survives', () => {
    expect(decodeEntry(encodeEntry(entry)).lastOffset).toBe('9007199254740993');
  });

  it('writes human-readable JSON for Kafbat UI', () => {
    const text = encodeEntry(entry).toString('utf8');
    const parsed: unknown = JSON.parse(text);

    expect(parsed).toMatchObject({ v: 1, product: 'Item1', partition: 2 });
  });

  it.each([
    { label: 'a different version', value: { ...encodedObject(), v: 2 } },
    { label: 'a missing state', value: { v: 1, product: 'Item1', partition: 0, lastOffset: '1' } },
    { label: 'a zero count', value: { ...encodedObject(), state: { ...entry.state, count: 0 } } },
    { label: 'a negative m2', value: { ...encodedObject(), state: { ...entry.state, m2: -1 } } },
    { label: 'not an object', value: 'garbage' },
  ])('rejects $label rather than restoring it', ({ value }) => {
    // A corrupt changelog record must be skipped with a warning by the
    // restore loop; decodeEntry's job is to refuse it loudly.
    expect(() => decodeEntry(Buffer.from(JSON.stringify(value)))).toThrow();
  });

  it('rejects bytes that are not JSON', () => {
    expect(() => decodeEntry(Buffer.from([0x00, 0x01, 0x02]))).toThrow();
  });
});
