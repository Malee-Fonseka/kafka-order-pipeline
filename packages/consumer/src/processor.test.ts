import { MockClient, type Client } from '@confluentinc/schemaregistry';
import {
  CORRELATION_ID_HEADER,
  type Order,
  TransientError,
  createOrderDeserializer,
  createOrderSerializer,
  encodeHeaders,
  encodeWireFormatHeader,
  ensureOrderSchemaRegistered,
} from '@order-pipeline/shared';
import { beforeAll, describe, expect, it, vi } from 'vitest';

import type { IncomingRecord } from './pipeline.js';
import { createRecordProcessor } from './processor.js';

import type { Logger } from 'pino';

const TOPIC = 'orders';
const ORDER: Order = { orderId: '1001', product: 'Item1', price: 12.5 };

function fakeLogger(): Logger & { warn: ReturnType<typeof vi.fn>; info: ReturnType<typeof vi.fn> } {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  } as unknown as Logger & { warn: ReturnType<typeof vi.fn>; info: ReturnType<typeof vi.fn> };
}

function recordWith(value: Buffer | null, headers?: Record<string, Buffer>): IncomingRecord {
  return {
    topic: TOPIC,
    partition: 1,
    offset: '42',
    timestamp: '1700000000000',
    key: Buffer.from(ORDER.product),
    value,
    headers,
  };
}

describe('record processor', () => {
  let client: Client;
  let validPayload: Buffer;

  beforeAll(async () => {
    client = new MockClient({ baseURLs: ['mock://registry'] });
    await ensureOrderSchemaRegistered({
      client,
      topic: TOPIC,
      logger: fakeLogger(),
    });
    validPayload = await createOrderSerializer({ client, topic: TOPIC }).serialize(ORDER);
  });

  it('decodes a valid record and reports it processed', async () => {
    const logger = fakeLogger();
    const process = createRecordProcessor({
      deserializer: createOrderDeserializer({ client, topic: TOPIC }),
      logger,
    });

    const outcome = await process(recordWith(validPayload));

    expect(outcome.kind).toBe('processed');
    if (outcome.kind === 'processed') {
      expect(outcome.order).toEqual(ORDER);
    }
    expect(logger.info).toHaveBeenCalledWith(
      expect.objectContaining({ orderId: '1001', partition: 1, offset: '42' }),
      'order received',
    );
  });

  it('carries the correlation id from the producer through to the outcome', async () => {
    const process = createRecordProcessor({
      deserializer: createOrderDeserializer({ client, topic: TOPIC }),
      logger: fakeLogger(),
    });
    const headers = encodeHeaders({ [CORRELATION_ID_HEADER]: 'corr-123' });

    const outcome = await process(recordWith(validPayload, headers));

    expect(outcome.correlationId).toBe('corr-123');
  });

  it.each([
    {
      label: 'a json poison pill',
      value: Buffer.from('{"not":"avro"}'),
      reason: 'deserialization',
    },
    { label: 'an empty payload', value: Buffer.alloc(0), reason: 'deserialization' },
    { label: 'a null value', value: null, reason: 'deserialization' },
    {
      label: 'an unknown schema id',
      value: encodeWireFormatHeader(999_999, Buffer.from([0x02, 0x41])),
      reason: 'unknown-schema-id',
    },
  ])('skips $label as a terminal outcome rather than throwing', async ({ value, reason }) => {
    // Throwing would make the client seek back and redeliver the same bytes
    // forever. A permanent failure must resolve so the pipeline commits past
    // it — into the DLQ from Phase 7, skipped and counted until then.
    const logger = fakeLogger();
    const process = createRecordProcessor({
      deserializer: createOrderDeserializer({ client, topic: TOPIC }),
      logger,
    });

    const outcome = await process(recordWith(value));

    expect(outcome.kind).toBe('skipped');
    if (outcome.kind === 'skipped') {
      expect(outcome.error.reason).toBe(reason);
    }
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  it('skips a record that decodes but fails validation', async () => {
    const bad = await createOrderSerializer({ client, topic: TOPIC }).serialize({
      ...ORDER,
      price: -1,
    });
    const process = createRecordProcessor({
      deserializer: createOrderDeserializer({ client, topic: TOPIC }),
      logger: fakeLogger(),
    });

    const outcome = await process(recordWith(bad));

    expect(outcome.kind).toBe('skipped');
    if (outcome.kind === 'skipped') {
      expect(outcome.error.reason).toBe('validation');
    }
  });

  it('lets a transient failure propagate so the record is redelivered', async () => {
    // A registry outage mid-decode is not a fact about the record. Resolving
    // "skipped" here would commit past a perfectly good order because the
    // registry blinked — data loss dressed up as poison handling.
    const process = createRecordProcessor({
      deserializer: {
        deserialize: async () => {
          await Promise.resolve();
          throw new TransientError('registry unavailable');
        },
      },
      logger: fakeLogger(),
    });

    await expect(process(recordWith(validPayload))).rejects.toBeInstanceOf(TransientError);
  });

  it('lets an unclassified failure propagate rather than guessing', async () => {
    const process = createRecordProcessor({
      deserializer: {
        deserialize: async () => {
          await Promise.resolve();
          throw new Error('something nobody anticipated');
        },
      },
      logger: fakeLogger(),
    });

    await expect(process(recordWith(validPayload))).rejects.toThrow('something nobody anticipated');
  });

  it('processes the transient-failure marker as an ordinary order in this phase', async () => {
    // The marker is structurally valid; the handler that fails on it lands
    // in Phase 6. Until then it must flow through, not be mistaken for poison.
    const marker = await createOrderSerializer({ client, topic: TOPIC }).serialize({
      ...ORDER,
      product: '__TRANSIENT_FAIL__',
    });
    const process = createRecordProcessor({
      deserializer: createOrderDeserializer({ client, topic: TOPIC }),
      logger: fakeLogger(),
    });

    await expect(process(recordWith(marker))).resolves.toMatchObject({ kind: 'processed' });
  });
});
