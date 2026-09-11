import { describe, expect, it } from 'vitest';

import {
  type ClassifiedError,
  PermanentError,
  PipelineError,
  TransientError,
  classifyError,
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

describe('classifyError (D4)', () => {
  it('returns an already-classified error unchanged', () => {
    const permanent = new PermanentError('validation', 'negative price');
    const transient = new TransientError('registry unreachable');

    expect(classifyError(permanent)).toBe(permanent);
    expect(classifyError(transient)).toBe(transient);
  });

  it.each([
    'ECONNRESET',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'EPIPE',
    'EAI_AGAIN',
    'ENOTFOUND',
    'EHOSTUNREACH',
  ])('classifies a %s system error as transient', (code) => {
    const classified = classifyError(Object.assign(new Error('socket trouble'), { code }));

    expect(classified.kind).toBe('transient');
    expect(classified.message).toContain(code);
    expect(classified.cause).toBeInstanceOf(Error);
  });

  it.each([408, 429, 500, 502, 503, 504])('classifies HTTP %i as transient', (status) => {
    expect(classifyError(Object.assign(new Error('downstream'), { status })).kind).toBe(
      'transient',
    );
    expect(classifyError(Object.assign(new Error('downstream'), { statusCode: status })).kind).toBe(
      'transient',
    );
  });

  it.each([400, 401, 403, 404, 422])(
    'classifies HTTP %i as permanent — a fact about the request',
    (status) => {
      const classified = classifyError(Object.assign(new Error('rejected'), { status }));

      expect(classified).toBeInstanceOf(PermanentError);
      expect((classified as PermanentError).reason).toBe('unclassified');
    },
  );

  it('classifies a client error that declares itself retriable as transient', () => {
    expect(
      classifyError(Object.assign(new Error('leader not available'), { retriable: true })).kind,
    ).toBe('transient');
  });

  it('classifies a timeout by message as transient', () => {
    expect(classifyError(new Error('request timed out after 5000ms')).kind).toBe('transient');
    expect(classifyError(new Error('Timeout waiting for coordinator')).kind).toBe('transient');
  });

  it('classifies an unrecognised error as permanent, unclassified', () => {
    // A bug is deterministic; retrying it through six minutes of tiers helps
    // nobody. The DLQ has replay, so nothing is lost by parking it.
    const classified = classifyError(
      new TypeError("Cannot read properties of undefined (reading 'x')"),
    );

    expect(classified).toBeInstanceOf(PermanentError);
    expect((classified as PermanentError).reason).toBe('unclassified');
    expect(classified.message).toContain('Cannot read properties');
  });

  it('classifies a thrown non-Error as permanent without crashing', () => {
    expect(classifyError('a string was thrown').kind).toBe('permanent');
    expect(classifyError({ weird: true }).kind).toBe('permanent');
    expect(classifyError(undefined).kind).toBe('permanent');
  });

  it('is exhaustive over its own output — every result is one of the two kinds', () => {
    const inputs: unknown[] = [
      new Error('x'),
      Object.assign(new Error('y'), { code: 'ECONNRESET' }),
      Object.assign(new Error('z'), { status: 503 }),
      null,
      42,
    ];
    for (const input of inputs) {
      const kind: 'transient' | 'permanent' = classifyError(input).kind;
      expect(['transient', 'permanent']).toContain(kind);
    }
  });
});
