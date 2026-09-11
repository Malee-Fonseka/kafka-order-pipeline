import { describe, expect, it } from 'vitest';

import {
  HEADERS_STRIPPED_ON_REPLAY,
  MAX_STACK_HEADER_BYTES,
  REPLAYED_AT_HEADER,
  REPLAYED_FROM_OFFSET_HEADER,
  REPLAY_COUNT_HEADER,
  dlqHeaderValues,
  readDlqMetadata,
  replayHeaders,
} from './dlq.js';
import { PermanentError, TransientError } from './errors.js';
import {
  ATTEMPT_COUNT_HEADER,
  CORRELATION_ID_HEADER,
  ERROR_STACK_HEADER,
  ERROR_TYPE_HEADER,
  encodeHeaders,
  readHeader,
} from './headers.js';

const NOW = new Date('2026-01-01T12:00:00.000Z');

const source = {
  topic: 'orders',
  partition: 1,
  offset: '10',
  timestamp: NOW.getTime() - 1_000,
  key: Buffer.from('Item2'),
  headers: encodeHeaders({ [CORRELATION_ID_HEADER]: 'corr-9' }),
};
const context = { consumerGroup: 'order-consumers', appVersion: '1.2.3' };

describe('dlqHeaderValues', () => {
  it('produces every header in the D6 table', () => {
    const values = dlqHeaderValues(
      source,
      {
        error: new PermanentError('validation', 'price must not be negative'),
        attempt: 1,
        failedAt: NOW,
      },
      context,
    );

    expect(Object.keys(values).sort()).toEqual(
      [
        'x-app-version',
        'x-attempt-count',
        'x-consumer-group',
        'x-correlation-id',
        'x-error-class',
        'x-error-message',
        'x-error-stack',
        'x-error-type',
        'x-first-failed-at',
        'x-last-failed-at',
        'x-original-key',
        'x-original-offset',
        'x-original-partition',
        'x-original-timestamp',
        'x-original-topic',
      ].sort(),
    );
  });

  it('prefixes a permanent error message with its reason code', () => {
    const values = dlqHeaderValues(
      source,
      {
        error: new PermanentError('unknown-schema-id', 'id 999999 not found'),
        attempt: 1,
        failedAt: NOW,
      },
      context,
    );

    expect(values['x-error-message']).toBe('unknown-schema-id: id 999999 not found');
    expect(values['x-error-type']).toBe('permanent');
  });

  it('caps the stack trace at 2 KB without splitting a multi-byte character', () => {
    const error = new TransientError('x');
    error.stack = `TransientError: x\n${'    at déjà vu (file.js:1:1)\n'.repeat(400)}`;

    const values = dlqHeaderValues(source, { error, attempt: 4, failedAt: NOW }, context);
    const stack = String(values[ERROR_STACK_HEADER]);

    expect(Buffer.byteLength(stack, 'utf8')).toBeLessThanOrEqual(MAX_STACK_HEADER_BYTES);
    expect(stack.endsWith('…[truncated]')).toBe(true);
    expect(stack).not.toContain('�');
  });

  it('survives an error with no stack', () => {
    const error = new TransientError('x');
    delete (error as { stack?: string }).stack;

    expect(
      dlqHeaderValues(source, { error, attempt: 1, failedAt: NOW }, context)[ERROR_STACK_HEADER],
    ).toBe('');
  });

  it('round-trips through the header codec and back into metadata', () => {
    const values = dlqHeaderValues(
      source,
      { error: new TransientError('downstream down'), attempt: 4, failedAt: NOW },
      context,
    );

    const meta = readDlqMetadata(encodeHeaders(values));

    expect(meta).toMatchObject({
      originalTopic: 'orders',
      originalPartition: 1,
      originalOffset: '10',
      originalTimestamp: NOW.getTime() - 1_000,
      originalKey: 'Item2',
      errorType: 'transient-exhausted',
      errorClass: 'TransientError',
      errorMessage: 'downstream down',
      attempt: 4,
      firstFailedAt: NOW.toISOString(),
      lastFailedAt: NOW.toISOString(),
      consumerGroup: 'order-consumers',
      correlationId: 'corr-9',
      appVersion: '1.2.3',
      replayedFromOffset: undefined,
      replayCount: undefined,
    });
  });

  it('reads a record written without our headers as all-undefined rather than throwing', () => {
    const meta = readDlqMetadata({ unrelated: Buffer.from('x') });

    expect(meta.errorType).toBeUndefined();
    expect(meta.attempt).toBeUndefined();
  });

  it('ignores an unrecognised error type value', () => {
    expect(
      readDlqMetadata(encodeHeaders({ [ERROR_TYPE_HEADER]: 'weird' })).errorType,
    ).toBeUndefined();
  });
});

describe('replayHeaders', () => {
  const deadLetter = encodeHeaders({
    ...dlqHeaderValues(
      source,
      { error: new TransientError('downstream down'), attempt: 4, failedAt: NOW },
      context,
    ),
    'x-retry-not-before': NOW.getTime(),
    'x-custom-passthrough': 'keep me',
  });

  it('strips every header that describes the past failure', () => {
    const headers = replayHeaders({ dlqOffset: '5', headers: deadLetter, replayedAt: NOW });

    for (const stripped of HEADERS_STRIPPED_ON_REPLAY) {
      expect(headers).not.toHaveProperty(stripped);
    }
    // In particular the attempt count: a replay must earn a fresh set of tiers.
    expect(headers).not.toHaveProperty(ATTEMPT_COUNT_HEADER);
  });

  it('keeps the correlation id and unrelated headers', () => {
    const headers = replayHeaders({ dlqOffset: '5', headers: deadLetter, replayedAt: NOW });

    expect(readHeader(headers, CORRELATION_ID_HEADER)).toBe('corr-9');
    expect(readHeader(headers, 'x-custom-passthrough')).toBe('keep me');
    expect(readHeader(headers, 'x-app-version')).toBe('1.2.3');
  });

  it('marks the record as replayed and counts replays', () => {
    const first = replayHeaders({ dlqOffset: '5', headers: deadLetter, replayedAt: NOW });

    expect(readHeader(first, REPLAYED_FROM_OFFSET_HEADER)).toBe('5');
    expect(readHeader(first, REPLAYED_AT_HEADER)).toBe(NOW.toISOString());
    expect(readHeader(first, REPLAY_COUNT_HEADER)).toBe('1');

    // Dead-lettered again and replayed again: the count climbs, so an
    // operator can spot a record that keeps coming back.
    const second = replayHeaders({ dlqOffset: '12', headers: first, replayedAt: NOW });
    expect(readHeader(second, REPLAY_COUNT_HEADER)).toBe('2');
    expect(readHeader(second, REPLAYED_FROM_OFFSET_HEADER)).toBe('12');
  });

  it('copes with a record that has no headers at all', () => {
    const headers = replayHeaders({ dlqOffset: '0', headers: undefined, replayedAt: NOW });

    expect(Object.keys(headers).sort()).toEqual(
      [REPLAYED_AT_HEADER, REPLAYED_FROM_OFFSET_HEADER, REPLAY_COUNT_HEADER].sort(),
    );
  });
});
