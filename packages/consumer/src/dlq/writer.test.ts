import {
  APP_VERSION_HEADER,
  ATTEMPT_COUNT_HEADER,
  CONSUMER_GROUP_HEADER,
  CORRELATION_ID_HEADER,
  ERROR_CLASS_HEADER,
  ERROR_MESSAGE_HEADER,
  ERROR_STACK_HEADER,
  ERROR_TYPE_HEADER,
  FIRST_FAILED_AT_HEADER,
  LAST_FAILED_AT_HEADER,
  ORIGINAL_KEY_HEADER,
  ORIGINAL_OFFSET_HEADER,
  ORIGINAL_PARTITION_HEADER,
  ORIGINAL_TIMESTAMP_HEADER,
  ORIGINAL_TOPIC_HEADER,
  PermanentError,
  type Producer,
  TransientError,
  encodeHeaders,
  readHeader,
  readIntHeader,
} from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import type { IncomingRecord } from '../pipeline.js';
import { createDlqWriter } from './writer.js';

import type { Logger } from 'pino';

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

const NOW = new Date('2026-01-01T12:00:00.000Z');

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
        return Promise.resolve([
          { topicName: record.topic, partition: 0, errorCode: 0, offset: '77' },
        ]);
      },
    ),
  } as unknown as Producer & { sent: Sent[] };
}

/** Deliberately not valid Avro: byte 0 is '{'. */
const POISON = Buffer.from('{"orderId":"9999","note":"not avro"}', 'utf8');

function record(overrides: Partial<IncomingRecord> = {}): IncomingRecord {
  return {
    topic: 'orders',
    partition: 2,
    offset: '41',
    timestamp: String(NOW.getTime() - 60_000),
    key: Buffer.from('Item1'),
    value: POISON,
    headers: encodeHeaders({ [CORRELATION_ID_HEADER]: 'corr-1', 'x-app-version': '0.9.0' }),
    ...overrides,
  };
}

function writer(producer: Producer): ReturnType<typeof createDlqWriter> {
  return createDlqWriter({
    producer,
    topic: 'orders.dlq',
    consumerGroup: 'order-consumers',
    appVersion: '1.0.0',
    logger: silentLogger,
    now: () => NOW,
  });
}

describe('dlq writer', () => {
  it('writes the original bytes and key, never re-serialized', async () => {
    // The point of D6. This value cannot be deserialized; the DLQ must hold
    // it anyway, byte for byte.
    const producer = fakeProducer();
    const source = record();

    const result = await writer(producer).write(
      source,
      new PermanentError('deserialization', 'bad magic byte'),
      1,
    );

    expect(producer.sent).toHaveLength(1);
    const sent = producer.sent[0];
    expect(sent?.topic).toBe('orders.dlq');
    expect(sent?.value).toBe(POISON);
    expect(sent?.key).toBe(source.key);
    expect(result).toEqual({ errorType: 'permanent', partition: 0, offset: '77' });
  });

  it('writes the complete D6 header set for a permanent failure', async () => {
    const producer = fakeProducer();
    const error = new PermanentError('deserialization', 'expected magic byte 0x00, found 0x7b');

    await writer(producer).write(record(), error, 1);

    const h = producer.sent[0]?.headers;
    expect(readHeader(h, ORIGINAL_TOPIC_HEADER)).toBe('orders');
    expect(readIntHeader(h, ORIGINAL_PARTITION_HEADER)).toBe(2);
    expect(readHeader(h, ORIGINAL_OFFSET_HEADER)).toBe('41');
    expect(readIntHeader(h, ORIGINAL_TIMESTAMP_HEADER)).toBe(NOW.getTime() - 60_000);
    expect(readHeader(h, ORIGINAL_KEY_HEADER)).toBe('Item1');
    expect(readHeader(h, ERROR_TYPE_HEADER)).toBe('permanent');
    expect(readHeader(h, ERROR_CLASS_HEADER)).toBe('PermanentError');
    expect(readHeader(h, ERROR_MESSAGE_HEADER)).toBe(
      'deserialization: expected magic byte 0x00, found 0x7b',
    );
    expect(readHeader(h, ERROR_STACK_HEADER)).toContain('PermanentError');
    expect(readIntHeader(h, ATTEMPT_COUNT_HEADER)).toBe(1);
    expect(readHeader(h, FIRST_FAILED_AT_HEADER)).toBe(NOW.toISOString());
    expect(readHeader(h, LAST_FAILED_AT_HEADER)).toBe(NOW.toISOString());
    expect(readHeader(h, CONSUMER_GROUP_HEADER)).toBe('order-consumers');
    expect(readHeader(h, CORRELATION_ID_HEADER)).toBe('corr-1');
    // The *consumer's* version, not the producer's that was on the record.
    expect(readHeader(h, APP_VERSION_HEADER)).toBe('1.0.0');
  });

  it('marks a transient failure that ran out of tiers as transient-exhausted', async () => {
    const producer = fakeProducer();

    const result = await writer(producer).write(
      record(),
      new TransientError('downstream never came back'),
      4,
    );

    expect(result.errorType).toBe('transient-exhausted');
    expect(readHeader(producer.sent[0]?.headers, ERROR_TYPE_HEADER)).toBe('transient-exhausted');
    expect(readHeader(producer.sent[0]?.headers, ERROR_CLASS_HEADER)).toBe('TransientError');
    expect(readIntHeader(producer.sent[0]?.headers, ATTEMPT_COUNT_HEADER)).toBe(4);
  });

  it('points origin headers at where the record was first consumed, not the last retry hop', async () => {
    // A record that rode every tier arrives back on orders with the retry
    // metadata describing its very first delivery. The DLQ must keep that.
    const producer = fakeProducer();
    const firstFailed = new Date(NOW.getTime() - 400_000).toISOString();
    const source = record({
      partition: 2,
      offset: '900',
      headers: encodeHeaders({
        [CORRELATION_ID_HEADER]: 'corr-1',
        [ATTEMPT_COUNT_HEADER]: 3,
        [FIRST_FAILED_AT_HEADER]: firstFailed,
        [ORIGINAL_TOPIC_HEADER]: 'orders',
        [ORIGINAL_PARTITION_HEADER]: 2,
        [ORIGINAL_OFFSET_HEADER]: '41',
        [ORIGINAL_TIMESTAMP_HEADER]: NOW.getTime() - 500_000,
      }),
    });

    await writer(producer).write(source, new TransientError('still down'), 4);

    const h = producer.sent[0]?.headers;
    expect(readHeader(h, ORIGINAL_OFFSET_HEADER)).toBe('41');
    expect(readIntHeader(h, ORIGINAL_TIMESTAMP_HEADER)).toBe(NOW.getTime() - 500_000);
    expect(readHeader(h, FIRST_FAILED_AT_HEADER)).toBe(firstFailed);
    expect(readHeader(h, LAST_FAILED_AT_HEADER)).toBe(NOW.toISOString());
  });

  it('writes a null value faithfully and an empty original-key header for a null key', async () => {
    const producer = fakeProducer();

    await writer(producer).write(
      record({ key: null, value: null }),
      new PermanentError('deserialization', 'record value is null'),
      1,
    );

    expect(producer.sent[0]?.value).toBeNull();
    expect(producer.sent[0]?.key).toBeNull();
    expect(readHeader(producer.sent[0]?.headers, ORIGINAL_KEY_HEADER)).toBe('');
  });

  it('surfaces a failed write as transient so the record is redelivered, not lost', async () => {
    const producer = fakeProducer();
    (producer.send as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error('Local: Broker transport failure'),
    );

    await expect(
      writer(producer).write(record(), new PermanentError('deserialization', 'x'), 1),
    ).rejects.toBeInstanceOf(TransientError);
  });
});
