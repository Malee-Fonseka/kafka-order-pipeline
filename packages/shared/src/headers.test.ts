import { describe, expect, it } from 'vitest';

import {
  ATTEMPT_COUNT_HEADER,
  CORRELATION_ID_HEADER,
  encodeHeaders,
  readHeader,
  readIntHeader,
} from './headers.js';

describe('encodeHeaders', () => {
  it('encodes strings and numbers as utf-8 bytes', () => {
    const encoded = encodeHeaders({
      [CORRELATION_ID_HEADER]: 'abc-123',
      [ATTEMPT_COUNT_HEADER]: 3,
    });

    expect(Buffer.isBuffer(encoded[CORRELATION_ID_HEADER])).toBe(true);
    expect(encoded[CORRELATION_ID_HEADER]?.toString('utf8')).toBe('abc-123');
    expect(encoded[ATTEMPT_COUNT_HEADER]?.toString('utf8')).toBe('3');
  });

  it('omits undefined values rather than writing an empty header', () => {
    // A present-but-empty header reads as "we looked and found nothing" when it
    // actually means "we never set this" — a meaningful difference in the DLQ.
    const encoded = encodeHeaders({ present: 'yes', absent: undefined });

    expect(Object.keys(encoded)).toEqual(['present']);
    expect(ATTEMPT_COUNT_HEADER in encoded).toBe(false);
  });

  it('preserves non-ascii values through a round trip', () => {
    const encoded = encodeHeaders({ note: 'café ✓' });

    expect(readHeader(encoded, 'note')).toBe('café ✓');
  });

  it('encodes zero, which must not be mistaken for absent', () => {
    const encoded = encodeHeaders({ [ATTEMPT_COUNT_HEADER]: 0 });

    expect(readIntHeader(encoded, ATTEMPT_COUNT_HEADER)).toBe(0);
  });
});

describe('readHeader', () => {
  it('decodes a Buffer value', () => {
    expect(readHeader({ a: Buffer.from('value', 'utf8') }, 'a')).toBe('value');
  });

  it('passes a string value through', () => {
    expect(readHeader({ a: 'value' }, 'a')).toBe('value');
  });

  it.each([
    { label: 'a missing header', headers: {}, name: 'absent' },
    { label: 'undefined headers', headers: undefined, name: 'absent' },
    { label: 'a null value', headers: { a: null }, name: 'a' },
    { label: 'an unexpected type', headers: { a: { nested: true } }, name: 'a' },
  ])('returns undefined for $label', ({ headers, name }) => {
    expect(readHeader(headers, name)).toBeUndefined();
  });
});

describe('readIntHeader', () => {
  it('parses an integer', () => {
    expect(readIntHeader({ n: Buffer.from('42') }, 'n')).toBe(42);
  });

  it('returns undefined for a non-numeric value instead of NaN', () => {
    // NaN would silently poison an attempt-count comparison and let a message
    // retry forever.
    expect(readIntHeader({ n: Buffer.from('not-a-number') }, 'n')).toBeUndefined();
  });

  it('returns undefined when the header is absent', () => {
    expect(readIntHeader({}, 'n')).toBeUndefined();
  });
});
