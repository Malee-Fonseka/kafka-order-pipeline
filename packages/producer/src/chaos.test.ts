import { MockClient, type Client } from '@confluentinc/schemaregistry';
import {
  PermanentError,
  TransientError,
  type Order,
  createOrderDeserializer,
  createOrderSerializer,
  ensureOrderSchemaRegistered,
  isOrder,
  readWireFormatHeader,
} from '@order-pipeline/shared';
import { beforeAll, describe, expect, it } from 'vitest';

import {
  POISON_FLAVOURS,
  TRANSIENT_FAIL_PRODUCT,
  type PoisonFlavour,
  createChaosInjector,
  createPoisonPayload,
} from './chaos.js';

import type { Logger } from 'pino';

const TOPIC = 'orders';
const ORDER: Order = { orderId: '1001', product: 'Item1', price: 12.5 };

const silentLogger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

/** Returns a fixed roll, so rate boundaries can be probed exactly. */
function rollOf(value: number): () => number {
  return () => value;
}

describe('chaos injector', () => {
  it('emits only valid orders when disabled', () => {
    // The rates are deliberately 1.0: disabled must mean disabled, not scaled.
    const injector = createChaosInjector({
      enabled: false,
      transientRate: 1,
      poisonRate: 1,
      random: rollOf(0),
    });

    const emission = injector.plan(ORDER);
    expect(emission.kind).toBe('valid');
  });

  it('routes a roll below the poison rate to a poison pill', () => {
    const injector = createChaosInjector({
      enabled: true,
      transientRate: 0.2,
      poisonRate: 0.1,
      random: rollOf(0.05),
    });

    expect(injector.plan(ORDER).kind).toBe('poison');
  });

  it('routes a roll inside the transient band to the marker product', () => {
    const injector = createChaosInjector({
      enabled: true,
      transientRate: 0.2,
      poisonRate: 0.1,
      random: rollOf(0.25),
    });
    const emission = injector.plan(ORDER);

    expect(emission.kind).toBe('transient');
    if (emission.kind === 'transient') {
      expect(emission.order.product).toBe(TRANSIENT_FAIL_PRODUCT);
      // Still a structurally valid order — it must fail in the *handler*, not
      // in deserialization, or it would exercise the DLQ instead of the retries.
      expect(isOrder(emission.order)).toBe(true);
      expect(emission.order.orderId).toBe(ORDER.orderId);
      expect(emission.order.price).toBe(ORDER.price);
    }
  });

  it('routes a roll above both bands to a valid order', () => {
    const injector = createChaosInjector({
      enabled: true,
      transientRate: 0.2,
      poisonRate: 0.1,
      random: rollOf(0.95),
    });

    expect(injector.plan(ORDER).kind).toBe('valid');
  });

  it('keeps the real product as the key for poison pills', () => {
    // D1 still applies to a corrupt record: it must land on a live partition
    // that a consumer actually owns, or it will never be seen to fail.
    const injector = createChaosInjector({
      enabled: true,
      transientRate: 0,
      poisonRate: 1,
      random: rollOf(0),
    });
    const emission = injector.plan({ ...ORDER, product: 'Item3' });

    expect(emission.kind).toBe('poison');
    if (emission.kind === 'poison') {
      expect(emission.product).toBe('Item3');
    }
  });

  it('cycles poison flavours so a run exercises all three', () => {
    const injector = createChaosInjector({
      enabled: true,
      transientRate: 0,
      poisonRate: 1,
      random: rollOf(0),
    });

    const flavours: PoisonFlavour[] = [];
    for (let i = 0; i < POISON_FLAVOURS.length * 2; i += 1) {
      const emission = injector.plan(ORDER);
      if (emission.kind === 'poison') {
        flavours.push(emission.flavour);
      }
    }

    expect(flavours).toEqual([...POISON_FLAVOURS, ...POISON_FLAVOURS]);
  });

  it('holds roughly to the configured rates over many draws', () => {
    const injector = createChaosInjector({
      enabled: true,
      transientRate: 0.2,
      poisonRate: 0.1,
      random: Math.random,
    });
    const tally = { valid: 0, transient: 0, poison: 0 };

    for (let i = 0; i < 10_000; i += 1) {
      tally[injector.plan(ORDER).kind] += 1;
    }

    expect(tally.poison / 10_000).toBeCloseTo(0.1, 1);
    expect(tally.transient / 10_000).toBeCloseTo(0.2, 1);
    expect(tally.valid / 10_000).toBeCloseTo(0.7, 1);
  });
});

describe('poison payloads', () => {
  let client: Client;
  let validPayload: Buffer;

  beforeAll(async () => {
    client = new MockClient({ baseURLs: ['mock://registry'] });
    await ensureOrderSchemaRegistered({ client, topic: TOPIC, logger: silentLogger });
    validPayload = await createOrderSerializer({ client, topic: TOPIC }).serialize(ORDER);
  });

  it('produces bytes the real deserializer rejects as permanent', async () => {
    // The point of the whole exercise: a poison pill must genuinely fail to
    // decode. If any flavour were merely unusual-but-decodable it would be
    // aggregated instead of dead-lettered, and the demo would show nothing.
    const deserializer = createOrderDeserializer({ client, topic: TOPIC });

    for (const flavour of POISON_FLAVOURS) {
      const bytes = createPoisonPayload(flavour, validPayload);

      await expect(
        deserializer.deserialize(bytes),
        `flavour ${flavour} should be undecodable`,
      ).rejects.toBeInstanceOf(PermanentError);

      // Never transient — a poison pill in a retry tier is the §2.3 livelock.
      await expect(deserializer.deserialize(bytes)).rejects.not.toBeInstanceOf(TransientError);
    }
  });

  it('gives each flavour a distinct corruption', () => {
    const payloads = POISON_FLAVOURS.map((flavour) =>
      createPoisonPayload(flavour, validPayload).toString('hex'),
    );

    expect(new Set(payloads).size).toBe(POISON_FLAVOURS.length);
  });

  it('writes json-not-avro with a non-zero magic byte', () => {
    const bytes = createPoisonPayload('json-not-avro');

    expect(bytes.readUInt8(0)).not.toBe(0x00);
    expect(bytes.toString('utf8')).toContain('not avro');
  });

  it('frames unknown-schema-id correctly but with an unregistered id', () => {
    // Correct framing is the point: this exercises the registry lookup failure
    // path rather than the magic-byte check.
    const header = readWireFormatHeader(createPoisonPayload('unknown-schema-id'));

    expect(header.schemaId).toBe(999_999);
  });

  it('truncates a real payload when given one', () => {
    const bytes = createPoisonPayload('truncated-payload', validPayload);

    expect(bytes.length).toBeLessThan(validPayload.length);
    expect(bytes.subarray(0, 5)).toEqual(validPayload.subarray(0, 5));
  });

  it('falls back to a framed payload when no reference is available', () => {
    // Before the first valid record is serialized there is nothing to truncate.
    // The fallback must still be undecodable rather than accidentally valid.
    const bytes = createPoisonPayload('truncated-payload');

    expect(readWireFormatHeader(bytes).schemaId).toBe(999_999);
  });
});
