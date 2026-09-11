import { MockClient, type Client } from '@confluentinc/schemaregistry';
import {
  ATTEMPT_COUNT_HEADER,
  CORRELATION_ID_HEADER,
  type ClassifiedError,
  type Order,
  type OrderDeserializer,
  type PermanentError,
  TransientError,
  buildTopicRegistry,
  createOrderDeserializer,
  createOrderSerializer,
  encodeHeaders,
  encodeWireFormatHeader,
  dlqErrorTypeFor,
  ensureOrderSchemaRegistered,
} from '@order-pipeline/shared';
import { beforeAll, describe, expect, it, vi } from 'vitest';

import type { DlqWriter } from './dlq/writer.js';
import type { OrderHandler } from './handler.js';
import type { IncomingRecord } from './pipeline.js';
import { createForwardProcessor, createRecordProcessor } from './processor.js';
import type { BackoffOptions } from './retry/backoff.js';
import type { RetryPublisher } from './retry/publisher.js';

import type { Logger } from 'pino';

const TOPIC = 'orders';
const ORDER: Order = { orderId: '1001', product: 'Item1', price: 12.5 };
const topics = buildTopicRegistry('orders');

/** Instant backoff: no sleeping, deterministic bounds. */
const backoff: BackoffOptions = {
  maxAttempts: 3,
  budgetMs: 2_000,
  baseMs: 0,
  capMs: 0,
  sleep: async () => Promise.resolve(),
};

function fakeLogger(): Logger & { warn: ReturnType<typeof vi.fn>; info: ReturnType<typeof vi.fn> } {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  } as unknown as Logger & { warn: ReturnType<typeof vi.fn>; info: ReturnType<typeof vi.fn> };
}

function fakePublisher(): RetryPublisher & {
  escalate: ReturnType<typeof vi.fn>;
  forward: ReturnType<typeof vi.fn>;
} {
  const tier = topics.retryTiers[0];
  if (tier === undefined) {
    throw new Error('no tiers');
  }
  return {
    escalate: vi.fn(async () =>
      Promise.resolve({ kind: 'retried' as const, tier, attempt: 1, notBefore: new Date() }),
    ),
    forward: vi.fn(async () => Promise.resolve()),
    metadataOf: () => ({
      attempt: 0,
      notBefore: undefined,
      firstFailedAt: undefined,
      lastFailedAt: undefined,
      originalTopic: undefined,
      originalPartition: undefined,
      originalOffset: undefined,
      originalTimestamp: undefined,
    }),
  };
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

const okHandler: OrderHandler = async () => Promise.resolve();

function fakeDlq(): DlqWriter & { write: ReturnType<typeof vi.fn> } {
  return {
    write: vi.fn(async (_record: IncomingRecord, error: ClassifiedError, _attempt: number) =>
      Promise.resolve({ errorType: dlqErrorTypeFor(error), partition: 0, offset: '9' }),
    ),
  };
}

/** `expect.objectContaining` is typed `any`; this keeps the call sites lint-clean. */
function containing(shape: Record<string, unknown>): unknown {
  return expect.objectContaining(shape) as unknown;
}

describe('record processor', () => {
  let client: Client;
  let validPayload: Buffer;
  let deserializer: OrderDeserializer;

  beforeAll(async () => {
    client = new MockClient({ baseURLs: ['mock://registry'] });
    await ensureOrderSchemaRegistered({ client, topic: TOPIC, logger: fakeLogger() });
    validPayload = await createOrderSerializer({ client, topic: TOPIC }).serialize(ORDER);
    deserializer = createOrderDeserializer({ client, topic: TOPIC });
  });

  it('decodes, handles and reports a valid record processed on the first attempt', async () => {
    const handler = vi.fn(okHandler);
    const logger = fakeLogger();
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher: fakePublisher(),
      dlq: fakeDlq(),
      backoff,
      logger,
    });

    const outcome = await process(recordWith(validPayload));

    expect(outcome).toMatchObject({ kind: 'processed', order: ORDER, delivery: 1, attempts: 1 });
    expect(handler).toHaveBeenCalledWith(
      ORDER,
      containing({ delivery: 1, attempt: 1, source: containing({ partition: 1 }) }),
    );
    expect(logger.info).toHaveBeenCalledWith(
      expect.objectContaining({ orderId: '1001' }),
      'order received',
    );
  });

  it('carries the correlation id and the delivery number from headers', async () => {
    const process = createRecordProcessor({
      deserializer,
      handler: okHandler,
      publisher: fakePublisher(),
      dlq: fakeDlq(),
      backoff,
      logger: fakeLogger(),
    });
    const headers = encodeHeaders({
      [CORRELATION_ID_HEADER]: 'corr-123',
      [ATTEMPT_COUNT_HEADER]: 2,
    });

    const outcome = await process(recordWith(validPayload, headers));

    expect(outcome).toMatchObject({ kind: 'processed', correlationId: 'corr-123', delivery: 3 });
  });

  it('retries a transient handler failure in place and succeeds', async () => {
    let calls = 0;
    const handler: OrderHandler = async () => {
      calls += 1;
      await Promise.resolve();
      if (calls < 3) {
        throw new TransientError('blip');
      }
    };
    const publisher = fakePublisher();
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher,
      dlq: fakeDlq(),
      backoff,
      logger: fakeLogger(),
    });

    const outcome = await process(recordWith(validPayload));

    expect(outcome).toMatchObject({ kind: 'processed', attempts: 3 });
    expect(publisher.escalate).not.toHaveBeenCalled();
  });

  it('escalates to a retry tier once in-place attempts are exhausted', async () => {
    const handler: OrderHandler = async () => {
      await Promise.resolve();
      throw new TransientError('downstream down');
    };
    const publisher = fakePublisher();
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher,
      dlq: fakeDlq(),
      backoff,
      logger: fakeLogger(),
    });

    const outcome = await process(recordWith(validPayload));

    expect(outcome).toMatchObject({ kind: 'retried', tier: { label: '5s' }, attempt: 1 });
    expect(publisher.escalate).toHaveBeenCalledTimes(1);
    expect(publisher.escalate).toHaveBeenCalledWith(
      expect.objectContaining({ offset: '42' }),
      expect.any(TransientError),
    );
  });

  it('dead-letters as transient-exhausted when no tier is left', async () => {
    const handler: OrderHandler = async () => {
      await Promise.resolve();
      throw new TransientError('still down');
    };
    const publisher = fakePublisher();
    const stillDown = new TransientError('still down');
    publisher.escalate.mockResolvedValue({ kind: 'exhausted', attempt: 4, error: stillDown });
    const dlq = fakeDlq();
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher,
      dlq,
      backoff,
      logger: fakeLogger(),
    });

    const outcome = await process(recordWith(validPayload));

    expect(outcome).toMatchObject({
      kind: 'dead-lettered',
      errorType: 'transient-exhausted',
      attempt: 4,
      dlqOffset: '9',
    });
    expect(dlq.write).toHaveBeenCalledWith(expect.objectContaining({ offset: '42' }), stillDown, 4);
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
  ])(
    'dead-letters $label as permanent without retrying, in place or via tiers',
    async ({ value, reason }) => {
      // Retrying a poison pill is the §2.3 livelock: no in-place attempts, no
      // republish. Straight to the DLQ, on the first delivery.
      const handler = vi.fn(okHandler);
      const publisher = fakePublisher();
      const dlq = fakeDlq();
      const process = createRecordProcessor({
        deserializer,
        handler,
        publisher,
        dlq,
        backoff,
        logger: fakeLogger(),
      });

      const outcome = await process(recordWith(value));

      expect(outcome).toMatchObject({ kind: 'dead-lettered', errorType: 'permanent', attempt: 1 });
      if (outcome.kind === 'dead-lettered') {
        expect((outcome.error as PermanentError).reason).toBe(reason);
      }
      expect(handler).not.toHaveBeenCalled();
      expect(publisher.escalate).not.toHaveBeenCalled();
      expect(dlq.write).toHaveBeenCalledTimes(1);
    },
  );

  it('treats a permanent handler failure as terminal immediately', async () => {
    const handler: OrderHandler = async () => {
      await Promise.resolve();
      throw new Error('a bug nobody anticipated');
    };
    const publisher = fakePublisher();
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher,
      dlq: fakeDlq(),
      backoff,
      logger: fakeLogger(),
    });

    const outcome = await process(recordWith(validPayload));

    // Unclassified → permanent 'unclassified' → DLQ, one attempt, no tiers.
    expect(outcome).toMatchObject({
      kind: 'dead-lettered',
      errorType: 'permanent',
      error: { reason: 'unclassified' },
    });
    expect(publisher.escalate).not.toHaveBeenCalled();
  });

  it('propagates a failed republish so the pipeline does not commit', async () => {
    const handler: OrderHandler = async () => {
      await Promise.resolve();
      throw new TransientError('down');
    };
    const publisher = fakePublisher();
    publisher.escalate.mockRejectedValue(
      new TransientError('republish failed: broker unavailable'),
    );
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher,
      dlq: fakeDlq(),
      backoff,
      logger: fakeLogger(),
    });

    await expect(process(recordWith(validPayload))).rejects.toBeInstanceOf(TransientError);
  });

  it('propagates a failed DLQ write so the pipeline does not commit', async () => {
    // D7 clause (c): the offset commits only after the DLQ write succeeds.
    // A broker that refuses the write leaves nothing durable, so redeliver.
    const dlq = fakeDlq();
    dlq.write.mockRejectedValue(new TransientError('dead letter write failed: broker away'));
    const process = createRecordProcessor({
      deserializer,
      handler: okHandler,
      publisher: fakePublisher(),
      dlq,
      backoff,
      logger: fakeLogger(),
    });

    await expect(process(recordWith(Buffer.from('{"not":"avro"}')))).rejects.toBeInstanceOf(
      TransientError,
    );
  });

  it('uses the original partition from headers as the aggregation source', async () => {
    // A record forwarded back from a retry tier lands on the same partition,
    // but the header is the explicit source of truth for ownership.
    const handler = vi.fn(okHandler);
    const process = createRecordProcessor({
      deserializer,
      handler,
      publisher: fakePublisher(),
      dlq: fakeDlq(),
      backoff,
      logger: fakeLogger(),
    });
    const headers = encodeHeaders({ [ATTEMPT_COUNT_HEADER]: 1, 'x-original-partition': 2 });

    await process({ ...recordWith(validPayload, headers), partition: 1 });

    expect(handler).toHaveBeenCalledWith(
      ORDER,
      containing({ delivery: 2, source: containing({ partition: 2 }) }),
    );
  });
});

describe('forward processor', () => {
  it('forwards a due retry record and reports it terminal', async () => {
    const publisher = fakePublisher();
    const process = createForwardProcessor({ publisher });
    const record: IncomingRecord = {
      ...recordWith(Buffer.from([1, 2, 3]), encodeHeaders({ [ATTEMPT_COUNT_HEADER]: 2 })),
      topic: 'orders.retry.30s',
    };

    const outcome = await process(record);

    expect(outcome).toMatchObject({ kind: 'forwarded', fromTopic: 'orders.retry.30s', attempt: 2 });
    expect(publisher.forward).toHaveBeenCalledWith(record);
  });
});
