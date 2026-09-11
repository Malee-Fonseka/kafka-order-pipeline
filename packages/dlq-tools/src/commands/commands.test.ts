import { MockClient } from '@confluentinc/schemaregistry';
import {
  CORRELATION_ID_HEADER,
  PermanentError,
  type Producer,
  REPLAY_COUNT_HEADER,
  TransientError,
  createOrderDeserializer,
  createOrderSerializer,
  dlqHeaderValues,
  encodeHeaders,
  ensureOrderSchemaRegistered,
  readHeader,
} from '@order-pipeline/shared';
import { describe, expect, it, vi } from 'vitest';

import { type DeadLetter, selectLetters, toDeadLetter } from '../dlq-reader.js';
import { type Output, renderTable } from '../output.js';
import { buildDecodeReport } from './decode.js';
import { runList, toListRow } from './list.js';
import { runReplay } from './replay.js';

import type { Logger } from 'pino';

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

const NOW = new Date('2026-01-01T12:00:00.000Z');

function capture(): Output & { lines: string[]; docs: unknown[] } {
  const lines: string[] = [];
  const docs: unknown[] = [];
  return {
    lines,
    docs,
    line: (text = '') => {
      lines.push(text);
    },
    json: (value) => {
      docs.push(value);
    },
  };
}

/** A dead letter as the consumer's writer would have produced it. */
function deadLetter(
  offset: string,
  value: Buffer | null,
  error: PermanentError | TransientError,
  attempt = 1,
): DeadLetter {
  const headers = encodeHeaders(
    dlqHeaderValues(
      {
        topic: 'orders',
        partition: 2,
        offset: String(100 + Number(offset)),
        timestamp: NOW.getTime() - 5_000,
        key: Buffer.from('Item1'),
        headers: encodeHeaders({ [CORRELATION_ID_HEADER]: `corr-${offset}` }),
      },
      { error, attempt, failedAt: NOW },
      { consumerGroup: 'order-consumers', appVersion: '1.0.0' },
    ),
  );
  return toDeadLetter({
    topic: 'orders.dlq',
    partition: 0,
    offset,
    timestamp: String(NOW.getTime()),
    key: Buffer.from('Item1'),
    value,
    headers,
  });
}

const POISON = Buffer.from('{"not":"avro"}');

describe('list', () => {
  const poisonLetter = deadLetter(
    '0',
    POISON,
    new PermanentError('deserialization', 'expected magic byte 0x00, found 0x7b'),
  );
  const exhaustedLetter = deadLetter(
    '1',
    Buffer.from([0, 0, 0, 0, 1]),
    new TransientError('downstream down'),
    4,
  );
  const letters = [poisonLetter, exhaustedLetter];

  it('summarises each dead letter in one row', () => {
    const row = toListRow(poisonLetter);

    expect(row).toMatchObject({
      offset: '0',
      errorType: 'permanent',
      errorClass: 'PermanentError',
      reason: 'deserialization: expected magic byte 0x00, found 0x7b',
      origin: 'orders[2]@100',
      attempt: '1',
      key: 'Item1',
      correlationId: 'corr-0',
    });
  });

  it('renders a table with a per-type summary', () => {
    const out = capture();

    runList(letters, { json: false }, out);

    expect(out.lines[0]).toMatch(/^OFFSET\s+WRITTEN\s+TYPE/);
    expect(out.lines.some((l) => l.includes('transient-exhausted'))).toBe(true);
    expect(out.lines.at(-1)).toBe(
      '2 of 2 dead letter(s) shown — 1 permanent, 1 transient-exhausted',
    );
  });

  it('emits structured rows under --json', () => {
    const out = capture();

    runList(letters, { json: true }, out);

    expect(out.lines).toEqual([]);
    expect(out.docs[0]).toMatchObject({
      total: 2,
      shown: 2,
      letters: [{ offset: '0' }, { offset: '1' }],
    });
  });

  it('shows only the newest N with --limit', () => {
    const out = capture();

    runList(letters, { json: true, limit: 1 }, out);

    expect(out.docs[0]).toMatchObject({ total: 2, shown: 1, letters: [{ offset: '1' }] });
  });

  it('says so when the queue is empty', () => {
    const out = capture();

    runList([], { json: false }, out);

    expect(out.lines).toEqual(['dead letter queue is empty']);
  });
});

describe('renderTable', () => {
  it('aligns columns and clips long values', () => {
    const lines = renderTable(
      [{ a: 'x', b: 'a very long value indeed' }],
      [
        { header: 'A', value: (r) => r.a, align: 'right' },
        { header: 'B', value: (r) => r.b, maxWidth: 10 },
      ],
    );

    expect(lines[0]).toBe('A  B         ');
    expect(lines[2]).toBe('x  a very lo…');
  });
});

describe('selectLetters', () => {
  const letters = ['0', '1', '2', '3'].map((o) =>
    deadLetter(o, POISON, new PermanentError('validation', 'x')),
  );

  it('selects by explicit offsets', () => {
    expect(selectLetters(letters, { offsets: ['1', '3'] }).map((l) => l.offset)).toEqual([
      '1',
      '3',
    ]);
  });

  it('selects everything with --all', () => {
    expect(selectLetters(letters, { all: true })).toHaveLength(4);
  });

  it('selects an inclusive range', () => {
    expect(selectLetters(letters, { from: '1', to: '2' }).map((l) => l.offset)).toEqual(['1', '2']);
    expect(selectLetters(letters, { from: '2' }).map((l) => l.offset)).toEqual(['2', '3']);
  });

  it('selects nothing by default — replay is never accidental', () => {
    expect(selectLetters(letters, {})).toEqual([]);
    expect(selectLetters(letters, { offsets: [] })).toEqual([]);
  });
});

describe('decode', () => {
  it('reports every layer for a poison pill, and still decodes what it can', async () => {
    const client = new MockClient({ baseURLs: ['mock://'] });
    await ensureOrderSchemaRegistered({ client, topic: 'orders', logger: silentLogger });
    const deserializer = createOrderDeserializer({ client, topic: 'orders', validate: false });

    const report = await buildDecodeReport(
      deadLetter('0', POISON, new PermanentError('deserialization', 'bad magic')),
      deserializer,
    );

    expect(report.headers['x-error-type']).toBe('permanent');
    expect(report.frame).toEqual({ error: 'not Confluent-framed (first byte 0x7b, length 14)' });
    expect(report.bytes).toMatchObject({ length: 14, text: '{"not":"avro"}' });
    expect(report.decoded).toMatchObject({ kind: 'permanent' });
  });

  it('decodes a validation failure with validation off — the operator wants to see it', async () => {
    const client = new MockClient({ baseURLs: ['mock://'] });
    await ensureOrderSchemaRegistered({ client, topic: 'orders', logger: silentLogger });
    const bad = await createOrderSerializer({ client, topic: 'orders' }).serialize({
      orderId: '1',
      product: 'Item1',
      price: -5,
    });
    const deserializer = createOrderDeserializer({ client, topic: 'orders', validate: false });

    const report = await buildDecodeReport(
      deadLetter('0', bad, new PermanentError('validation', 'price must not be negative')),
      deserializer,
    );

    expect(report.frame).toMatchObject({ magicByte: '0x00' });
    expect(report.decoded).toMatchObject({ orderId: '1', product: 'Item1', price: -5 });
  });

  it('handles a null value without throwing', async () => {
    const client = new MockClient({ baseURLs: ['mock://'] });
    const deserializer = createOrderDeserializer({ client, topic: 'orders', validate: false });

    const report = await buildDecodeReport(
      deadLetter('0', null, new PermanentError('deserialization', 'record value is null')),
      deserializer,
    );

    expect(report.frame).toEqual({ error: 'record value is null' });
    expect(report.bytes).toBeNull();
    expect(report.decoded).toMatchObject({ kind: 'permanent' });
  });
});

describe('replay', () => {
  function fakeProducer(): Pick<Producer, 'send'> & {
    sent: { topic: string; key: unknown; value: unknown; headers: Record<string, unknown> }[];
  } {
    const sent: {
      topic: string;
      key: unknown;
      value: unknown;
      headers: Record<string, unknown>;
    }[] = [];
    return {
      sent,
      send: vi.fn(
        async (record: {
          topic: string;
          messages: { key?: unknown; value: unknown; headers?: Record<string, unknown> }[];
        }) => {
          for (const m of record.messages) {
            sent.push({
              topic: record.topic,
              key: m.key,
              value: m.value,
              headers: m.headers ?? {},
            });
          }
          return Promise.resolve([
            { topicName: record.topic, partition: 2, errorCode: 0, offset: '500' },
          ]);
        },
      ),
    };
  }

  it('sends the original key and bytes back to the main topic with failure headers stripped', async () => {
    const producer = fakeProducer();
    const letter = deadLetter('7', POISON, new TransientError('downstream down'), 4);
    const out = capture();

    const results = await runReplay(
      [letter],
      producer,
      { targetTopic: 'orders', dryRun: false, json: false, now: () => NOW },
      silentLogger,
      out,
    );

    expect(producer.sent).toHaveLength(1);
    const sent = producer.sent[0];
    expect(sent?.topic).toBe('orders');
    expect(sent?.key).toBe(letter.key);
    expect(sent?.value).toBe(POISON);
    expect(sent?.headers).not.toHaveProperty('x-attempt-count');
    expect(sent?.headers).not.toHaveProperty('x-error-type');
    expect(readHeader(sent?.headers, CORRELATION_ID_HEADER)).toBe('corr-7');
    expect(readHeader(sent?.headers, REPLAY_COUNT_HEADER)).toBe('1');
    expect(readHeader(sent?.headers, 'x-replayed-from-dlq-offset')).toBe('7');
    expect(results[0]).toMatchObject({
      dlqOffset: '7',
      replayCount: 1,
      targetPartition: 2,
      targetOffset: '500',
    });
    expect(out.lines.at(-1)).toBe('1 record(s) replayed to orders');
  });

  it('sends nothing on a dry run and needs no producer', async () => {
    const out = capture();

    const results = await runReplay(
      [deadLetter('7', POISON, new PermanentError('validation', 'x'))],
      undefined,
      { targetTopic: 'orders', dryRun: true, json: true },
      silentLogger,
      out,
    );

    expect(results).toHaveLength(1);
    expect(out.docs[0]).toMatchObject({
      dryRun: true,
      replayed: [{ dlqOffset: '7', targetOffset: undefined }],
    });
  });

  it('refuses a real replay without a producer', async () => {
    await expect(
      runReplay(
        [],
        undefined,
        { targetTopic: 'orders', dryRun: false, json: false },
        silentLogger,
        capture(),
      ),
    ).rejects.toThrow(/producer is required/);
  });
});
