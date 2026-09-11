import { describe, expect, it } from 'vitest';

import {
  type ClassifiedError,
  PermanentError,
  PipelineError,
  TransientError,
  describeError,
  isClassifiedError,
  isPermanent,
  isTransient,
} from './errors.js';

describe('error taxonomy', () => {
  it('discriminates the two variants on `kind`', () => {
    const errors: ClassifiedError[] = [
      new TransientError('registry unreachable'),
      new PermanentError('deserialization', 'bad bytes'),
    ];

    // The exhaustive switch is the point of the discriminant: adding a third
    // variant without handling it here becomes a compile error, not a runtime
    // message silently taking the wrong branch.
    const routed = errors.map((error): string => {
      switch (error.kind) {
        case 'transient':
          return 'retry';
        case 'permanent':
          return `dlq:${error.reason}`;
      }
    });

    expect(routed).toEqual(['retry', 'dlq:deserialization']);
  });

  it('reports a name matching the concrete class, for DLQ x-error-class headers', () => {
    expect(new TransientError('x').name).toBe('TransientError');
    expect(new PermanentError('validation', 'x').name).toBe('PermanentError');
  });

  it('preserves the underlying cause', () => {
    const cause = new Error('ECONNREFUSED');
    expect(new TransientError('wrapped', { cause }).cause).toBe(cause);
  });

  it('omits `cause` entirely when none is supplied', () => {
    // Not merely cosmetic: an own-property of `undefined` would serialise into
    // the DLQ headers as an empty cause and read as "we looked and found none".
    expect('cause' in new TransientError('no cause')).toBe(false);
  });

  it('narrows via the type guards', () => {
    const transient: unknown = new TransientError('x');
    const permanent: unknown = new PermanentError('validation', 'x');
    const plain: unknown = new Error('unclassified');

    expect(isTransient(transient)).toBe(true);
    expect(isPermanent(transient)).toBe(false);
    expect(isPermanent(permanent)).toBe(true);
    expect(isClassifiedError(plain)).toBe(false);
    expect(isClassifiedError(permanent)).toBe(true);
  });

  it('shares a base class so unclassified escapes are detectable', () => {
    expect(new TransientError('x')).toBeInstanceOf(PipelineError);
    expect(new PermanentError('validation', 'x')).toBeInstanceOf(PipelineError);
    expect(new TransientError('x')).toBeInstanceOf(Error);
  });
});

describe('describeError', () => {
  it.each([
    { label: 'an Error', value: new Error('boom'), expected: 'boom' },
    { label: 'a string', value: 'boom', expected: 'boom' },
    { label: 'a plain object', value: { code: 42 }, expected: '{"code":42}' },
    { label: 'undefined', value: undefined, expected: undefined },
  ])('describes $label', ({ value, expected }) => {
    const described = describeError(value);
    expect(typeof described).toBe('string');
    if (expected !== undefined) {
      expect(described).toBe(expected);
    }
  });

  it('survives a value that cannot be stringified', () => {
    const circular: Record<string, unknown> = {};
    circular['self'] = circular;
    expect(() => describeError(circular)).not.toThrow();
  });
});
