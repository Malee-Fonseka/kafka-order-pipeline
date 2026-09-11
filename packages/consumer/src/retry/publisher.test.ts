import {
  ATTEMPT_COUNT_HEADER,
  CORRELATION_ID_HEADER,
  FIRST_FAILED_AT_HEADER,
  LAST_FAILED_AT_HEADER,
  ORIGINAL_OFFSET_HEADER,
  ORIGINAL_PARTITION_HEADER,
  ORIGINAL_TOPIC_HEADER,
  type Producer,
  RETRY_NOT_BEFORE_HEADER,
  TransientError,
  buildTopicRegistry,
  encodeHeaders,
  readHeader,
  readIntHeader,
} from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import type { IncomingRecord } from '../pipeline.js';
import { createRetryPublisher } from './publisher.js';

import type { Logger } from 'pino';

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

const topics = buildTopicRegistry('orders');
const NOW = new Date('2026-01-01T00:00:00.000Z');

interface Sent {
  topic: string;
  key: Buffer | string | null | undefined;
  value: Buffer | string | null;
  headers: Record<string, unknown>;
}

function fakeProducer(): Producer & { sent: Sent[] } {
  const sent: Sent[] = [];
  return {
    sent,
    send: vi.fn(
      async (record: {
        topic: string;
        messages: {
          key?: Buffer | string | null;
          value: Buffer | string | null;
          headers?: Record<string, unknown>;
        }[];
      }) => {
        for (const m of record.messages) {
          sent.push({ topic: record.topic, key: m.key, value: m.value, headers: m.headers ?? {} });
        }
        return Promise.resolve([]);
      },
    ),
  } as unknown as Producer & { sent: Sent[] };
}

function record(headers: Record<string, string | number> = {}, topic = 'orders'): IncomingRecord {
  return {
    topic,
    partition: 2,
    offset: '41',
    timestamp: String(NOW.getTime()),
    key: Buffer.from('Item1'),
    value: Buffer.from([0x00, 0, 0, 0, 1, 0xaa]),
    headers: encodeHeaders({ [CORRELATION_ID_HEADER]: 'corr-1', ...headers }),
  };
}

describe('retry publisher — escalate', () => {
  it('republishes the raw bytes, same key, to the first tier on the first failure', async () => {
    const producer = fakeProducer();
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => NOW,
    });
    const source = record();

    const outcome = await publisher.escalate(source, new TransientError('down'));

    expect(outcome).toMatchObject({ kind: 'retried', tier: { label: '5s' }, attempt: 1 });
    expect(producer.sent).toHaveLength(1);
    const sent = producer.sent[0];
    expect(sent?.topic).toBe('orders.retry.5s');
    expect(sent?.key).toEqual(source.key);
    // Never re-serialized: byte-identical.
    expect(sent?.value).toEqual(source.value);
  });

  it('writes the retry metadata headers and preserves the correlation id', async () => {
    const producer = fakeProducer();
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => NOW,
    });

    await publisher.escalate(record(), new TransientError('down'));

    const headers = producer.sent[0]?.headers;
    expect(readHeader(headers, CORRELATION_ID_HEADER)).toBe('corr-1');
    expect(readIntHeader(headers, ATTEMPT_COUNT_HEADER)).toBe(1);
    expect(readIntHeader(headers, RETRY_NOT_BEFORE_HEADER)).toBe(NOW.getTime() + 5_000);
    expect(readHeader(headers, FIRST_FAILED_AT_HEADER)).toBe(NOW.toISOString());
    expect(readHeader(headers, LAST_FAILED_AT_HEADER)).toBe(NOW.toISOString());
    expect(readHeader(headers, ORIGINAL_TOPIC_HEADER)).toBe('orders');
    expect(readIntHeader(headers, ORIGINAL_PARTITION_HEADER)).toBe(2);
    expect(readHeader(headers, ORIGINAL_OFFSET_HEADER)).toBe('41');
  });

  it('escalates by attempt count across hops: 1 → 5s, 2 → 30s, 3 → 5m', async () => {
    const producer = fakeProducer();
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => NOW,
    });

    const second = await publisher.escalate(
      record({ [ATTEMPT_COUNT_HEADER]: 1 }),
      new TransientError('x'),
    );
    const third = await publisher.escalate(
      record({ [ATTEMPT_COUNT_HEADER]: 2 }),
      new TransientError('x'),
    );

    expect(second).toMatchObject({ kind: 'retried', tier: { label: '30s' }, attempt: 2 });
    expect(third).toMatchObject({ kind: 'retried', tier: { label: '5m' }, attempt: 3 });
    expect(readIntHeader(producer.sent[0]?.headers, RETRY_NOT_BEFORE_HEADER)).toBe(
      NOW.getTime() + 30_000,
    );
    expect(readIntHeader(producer.sent[1]?.headers, RETRY_NOT_BEFORE_HEADER)).toBe(
      NOW.getTime() + 300_000,
    );
  });

  it('keeps the first-failed timestamp and origin from the first hop', async () => {
    // The ceiling is enforced globally because this metadata rides the record.
    const producer = fakeProducer();
    const later = new Date(NOW.getTime() + 60_000);
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => later,
    });

    await publisher.escalate(
      record({
        [ATTEMPT_COUNT_HEADER]: 1,
        [FIRST_FAILED_AT_HEADER]: NOW.toISOString(),
        [ORIGINAL_TOPIC_HEADER]: 'orders',
        [ORIGINAL_PARTITION_HEADER]: 0,
        [ORIGINAL_OFFSET_HEADER]: '7',
      }),
      new TransientError('x'),
    );

    const headers = producer.sent[0]?.headers;
    expect(readHeader(headers, FIRST_FAILED_AT_HEADER)).toBe(NOW.toISOString());
    expect(readHeader(headers, LAST_FAILED_AT_HEADER)).toBe(later.toISOString());
    expect(readIntHeader(headers, ORIGINAL_PARTITION_HEADER)).toBe(0);
    expect(readHeader(headers, ORIGINAL_OFFSET_HEADER)).toBe('7');
  });

  it('reports exhaustion after the last tier without publishing anything', async () => {
    const producer = fakeProducer();
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => NOW,
    });
    const error = new TransientError('still down');

    const outcome = await publisher.escalate(record({ [ATTEMPT_COUNT_HEADER]: 3 }), error);

    expect(outcome).toEqual({ kind: 'exhausted', attempt: 4, error });
    expect(producer.sent).toHaveLength(0);
  });

  it('surfaces a failed republish as transient so the record is redelivered, not lost', async () => {
    const producer = fakeProducer();
    (producer.send as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error('Local: Broker transport failure'),
    );
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => NOW,
    });

    await expect(publisher.escalate(record(), new TransientError('down'))).rejects.toBeInstanceOf(
      TransientError,
    );
  });
});

describe('retry publisher — forward', () => {
  it('sends the record back to the main topic with headers intact', async () => {
    const producer = fakeProducer();
    const publisher = createRetryPublisher({
      producer,
      topics,
      logger: silentLogger,
      now: () => NOW,
    });
    const source = record(
      { [ATTEMPT_COUNT_HEADER]: 2, [RETRY_NOT_BEFORE_HEADER]: 1 },
      'orders.retry.30s',
    );

    await publisher.forward(source);

    const sent = producer.sent[0];
    expect(sent?.topic).toBe('orders');
    expect(sent?.key).toEqual(source.key);
    expect(sent?.value).toEqual(source.value);
    expect(readIntHeader(sent?.headers, ATTEMPT_COUNT_HEADER)).toBe(2);
    expect(readHeader(sent?.headers, CORRELATION_ID_HEADER)).toBe('corr-1');
  });
});
