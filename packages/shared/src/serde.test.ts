import { Compatibility, MockClient, type Client } from '@confluentinc/schemaregistry';
import { beforeEach, describe, expect, it } from 'vitest';

import { PermanentError, type PermanentReason, TransientError } from './errors.js';
import type { Order } from './order.js';
import { ensureOrderSchemaRegistered, orderSchemaInfo } from './registry.js';
import { createOrderDeserializer, createOrderSerializer } from './serde.js';
import { encodeWireFormatHeader, readWireFormatHeader } from './wire-format.js';

import type { Logger } from 'pino';

const TOPIC = 'orders';
const SUBJECT = 'orders-value';

/**
 * `MockClient` is the vendor's own in-memory implementation of the `Client`
 * interface, so these tests drive the **real** `AvroSerializer` and
 * `AvroDeserializer` and produce genuinely Confluent-framed bytes — only the
 * HTTP transport is substituted. End-to-end behaviour against a live registry
 * is covered by the testcontainers suite in Phase 8.
 */
function createTestClient(): Client {
  return new MockClient({ baseURLs: ['mock://registry'] });
}

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

async function registeredClient(): Promise<Client> {
  const client = createTestClient();
  await ensureOrderSchemaRegistered({ client, topic: TOPIC, logger: silentLogger });
  return client;
}

describe('schema registration', () => {
  it('registers the order schema under <topic>-value with BACKWARD compatibility', async () => {
    const client = createTestClient();

    const registration = await ensureOrderSchemaRegistered({
      client,
      topic: TOPIC,
      logger: silentLogger,
    });

    expect(registration.subject).toBe(SUBJECT);
    expect(registration.schemaId).toBeGreaterThan(0);
    expect(registration.version).toBe(1);
    expect(registration.compatibility).toBe(Compatibility.BACKWARD);
    await expect(client.getAllSubjects()).resolves.toContain(SUBJECT);
  });

  it('is idempotent — repeated boots reuse the schema id and version', async () => {
    // Every service calls this at startup. If it allocated a new version each
    // time, a three-service stack would create three versions per restart.
    const client = createTestClient();
    const options = { client, topic: TOPIC, logger: silentLogger };

    const first = await ensureOrderSchemaRegistered(options);
    const second = await ensureOrderSchemaRegistered(options);
    const third = await ensureOrderSchemaRegistered(options);

    expect(second).toEqual(first);
    expect(third).toEqual(first);
    await expect(client.getAllVersions(SUBJECT)).resolves.toHaveLength(1);
  });
});

describe('order round trip', () => {
  let client: Client;

  beforeEach(async () => {
    client = await registeredClient();
  });

  it('survives serialize then deserialize', async () => {
    const serializer = createOrderSerializer({ client, topic: TOPIC });
    const deserializer = createOrderDeserializer({ client, topic: TOPIC });
    const order: Order = { orderId: '1001', product: 'Item1', price: 19.5 };

    const bytes = await serializer.serialize(order);
    const decoded = await deserializer.deserialize(bytes);

    expect(decoded).toEqual(order);
  });

  it('produces Confluent-framed bytes carrying the registered schema id', async () => {
    const serializer = createOrderSerializer({ client, topic: TOPIC });

    const bytes = await serializer.serialize({
      orderId: '1001',
      product: 'Item1',
      price: 19.5,
    });
    const header = readWireFormatHeader(bytes);
    const expectedId = await client.getId(SUBJECT, orderSchemaInfo(), true);

    expect(bytes.readUInt8(0)).toBe(0x00);
    expect(header.schemaId).toBe(expectedId);
    // Framing plus a compact binary body — nowhere near large enough to be
    // carrying the schema itself, which is the whole point of the registry.
    expect(bytes.length).toBeLessThan(40);
  });

  it('round-trips every product key without collision', async () => {
    const serializer = createOrderSerializer({ client, topic: TOPIC });
    const deserializer = createOrderDeserializer({ client, topic: TOPIC });
    const orders: Order[] = [
      { orderId: '1001', product: 'Item1', price: 0 },
      { orderId: '1002', product: 'Item2', price: 12.25 },
      { orderId: '1003', product: 'Item with spaces', price: 999.75 },
      { orderId: '1004', product: 'Unicode check', price: 1.5 },
    ];

    const decoded = await Promise.all(
      orders.map(async (order) => deserializer.deserialize(await serializer.serialize(order))),
    );

    expect(decoded).toEqual(orders);
  });

  it('narrows float32 precision on the wire, which is why aggregation uses doubles', async () => {
    // `price` is Avro `float` (32-bit) per the assignment schema, but JavaScript
    // numbers are doubles, so a value like 19.99 does not survive intact. This
    // is exactly the rounding error a naive running sum would compound over
    // tens of thousands of messages, and why D3 specifies Welford in double
    // precision.
    const serializer = createOrderSerializer({ client, topic: TOPIC });
    const deserializer = createOrderDeserializer({ client, topic: TOPIC });

    const decoded = await deserializer.deserialize(
      await serializer.serialize({ orderId: '1', product: 'Item1', price: 19.99 }),
    );

    expect(decoded.price).not.toBe(19.99);
    expect(decoded.price).toBe(Math.fround(19.99));
    expect(decoded.price).toBeCloseTo(19.99, 5);
  });
});

describe('corrupt payloads', () => {
  let client: Client;

  beforeEach(async () => {
    client = await registeredClient();
  });

  async function expectPermanent(
    payload: Buffer | null | undefined,
    reason: PermanentReason,
  ): Promise<void> {
    const deserializer = createOrderDeserializer({ client, topic: TOPIC });

    await expect(deserializer.deserialize(payload)).rejects.toBeInstanceOf(PermanentError);
    await expect(deserializer.deserialize(payload)).rejects.toMatchObject({
      kind: 'permanent',
      reason,
    });
    // The load-bearing assertion for §2.3: never transient, so a poison pill
    // can never enter the retry tiers and livelock.
    await expect(deserializer.deserialize(payload)).rejects.not.toBeInstanceOf(TransientError);
  }

  it('rejects a foreign magic byte', async () => {
    await expectPermanent(Buffer.from(JSON.stringify({ orderId: '1' })), 'deserialization');
  });

  it('rejects random bytes', async () => {
    await expectPermanent(Buffer.from([0x13, 0x37, 0x42, 0x99, 0x01, 0x02]), 'deserialization');
  });

  it('rejects an empty payload instead of decoding it to null', async () => {
    // Verified against the raw library: `deserialize` returns `null` for a
    // zero-byte payload without throwing. Unwrapped, that null would reach the
    // aggregator and corrupt the running average rather than reaching the DLQ.
    await expectPermanent(Buffer.alloc(0), 'deserialization');
  });

  it('rejects a null record value', async () => {
    await expectPermanent(null, 'deserialization');
  });

  it('rejects a well-framed but truncated payload', async () => {
    const serializer = createOrderSerializer({ client, topic: TOPIC });
    const bytes = await serializer.serialize({ orderId: '1001', product: 'Item1', price: 5 });

    await expectPermanent(bytes.subarray(0, 8), 'deserialization');
  });

  it('classifies an unknown schema id as permanent, not as a registry outage', async () => {
    // The subtle one. The library resolves the schema id over HTTP, so an
    // unresolvable id surfaces as a 404 RestError. Classified by transport
    // alone it looks like a registry problem and would be retried forever;
    // it is in fact a fact about the message, and permanent.
    const framed = encodeWireFormatHeader(9_999, Buffer.from([0x02, 0x41]));

    await expectPermanent(framed, 'unknown-schema-id');
  });

  it('rejects a decoded record that violates business rules', async () => {
    // Avro guarantees shape, not sense: a negative float is valid Avro. The
    // record decodes cleanly and is then rejected as a validation failure.
    const serializer = createOrderSerializer({ client, topic: TOPIC });
    const bytes = await serializer.serialize({
      orderId: '1001',
      product: 'Item1',
      price: -42.5,
    });

    await expectPermanent(bytes, 'validation');
  });

  it('can decode a rule-violating record when validation is disabled', async () => {
    // The DLQ inspector's use case: it exists to show operators the record that
    // failed, so it must decode without rejecting a second time.
    const serializer = createOrderSerializer({ client, topic: TOPIC });
    const bytes = await serializer.serialize({
      orderId: '1001',
      product: 'Item1',
      price: -42.5,
    });
    const inspector = createOrderDeserializer({ client, topic: TOPIC, validate: false });

    await expect(inspector.deserialize(bytes)).resolves.toMatchObject({ price: -42.5 });
  });
});

describe('serializer registration policy', () => {
  it('refuses to auto-register an unregistered schema', async () => {
    // `autoRegisterSchemas: false` is load-bearing: a producer that can invent
    // schema versions at runtime defeats the compatibility gate entirely. With
    // nothing registered, serialization must fail rather than silently create
    // a version.
    const serializer = createOrderSerializer({ client: createTestClient(), topic: TOPIC });
    const attempt = async (): Promise<Buffer> =>
      serializer.serialize({ orderId: '1001', product: 'Item1', price: 1 });

    await expect(attempt()).rejects.toThrow();
    // Whatever the cause, it leaves the serde layer classified — nothing
    // downstream should ever have to inspect a raw vendor error.
    await expect(attempt()).rejects.toSatisfy(
      (error: unknown) => error instanceof PermanentError || error instanceof TransientError,
    );
  });
});
