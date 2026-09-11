import {
  AvroDeserializer,
  AvroSerializer,
  RestError,
  SerdeType,
  type Client,
} from '@confluentinc/schemaregistry';

import { PermanentError, describeError } from './errors.js';
import { type Order, parseOrder } from './order.js';
import { classifyRegistryError } from './registry.js';
import { readWireFormatHeader } from './wire-format.js';

/**
 * Typed wrappers around the Confluent Avro serializer and deserializer.
 *
 * The vendor API is `serialize(topic, msg: any)` / `deserialize(): Promise<any>`.
 * §9 forbids `any` in this codebase, so this module is the narrow typed boundary
 * the rule anticipates: `any` stops here, and every caller downstream sees
 * `Order`.
 *
 * The wrapper is not only about types. It closes two failure modes that the raw
 * deserializer leaves open, both verified against the real library:
 *
 * 1. **An empty payload decodes to `null`, silently.** No exception. Unwrapped,
 *    a zero-byte record flows into the aggregator as `null` and corrupts the
 *    running average rather than landing in the DLQ.
 * 2. **A corrupt frame surfaces as an HTTP 404 from the registry.** The
 *    deserializer reads whatever four bytes follow the magic byte as a schema
 *    ID and looks it up. Classified naively by HTTP status, a poison pill would
 *    look like a registry outage and be retried forever — the exact livelock
 *    §2.3 warns about.
 */

export interface OrderSerializer {
  /** Encodes an order into Confluent-framed Avro bytes. */
  serialize: (order: Order) => Promise<Buffer>;
}

export interface OrderDeserializer {
  /** Decodes Confluent-framed Avro bytes, throwing a classified error on failure. */
  deserialize: (payload: Buffer | null | undefined) => Promise<Order>;
}

export interface SerdeOptions {
  readonly client: Client;
  /** Topic the records belong to; determines the `<topic>-value` subject. */
  readonly topic: string;
}

export interface DeserializerOptions extends SerdeOptions {
  /**
   * Apply business validation after decoding. Default `true`.
   *
   * The DLQ inspector sets this to `false`: it exists to show operators records
   * that failed validation, so it must be able to decode one without rejecting
   * it a second time.
   */
  readonly validate?: boolean;
}

/**
 * Serializer for the `orders` value.
 *
 * `autoRegisterSchemas: false` is deliberate and load-bearing. Auto-registration
 * lets any producer silently create a new schema version at runtime, which
 * defeats the entire point of a compatibility-gated registry. Registration is
 * an explicit bootstrap step (`ensureOrderSchemaRegistered`); a producer whose
 * schema is not already registered must fail at startup, loudly.
 *
 * `useLatestVersion: true` then pins encoding to the registered schema.
 */
export function createOrderSerializer({ client, topic }: SerdeOptions): OrderSerializer {
  const serializer = new AvroSerializer(client, SerdeType.VALUE, {
    autoRegisterSchemas: false,
    useLatestVersion: true,
  });

  return {
    serialize: async (order: Order): Promise<Buffer> => {
      try {
        return await serializer.serialize(topic, order);
      } catch (error) {
        // Encoding failures are about *our* record or *our* registry state, and
        // a registry blip here is retryable — so defer to the registry
        // classifier rather than assuming permanence.
        throw classifyRegistryError(error, `serialize order ${order.orderId}`);
      }
    },
  };
}

/**
 * Deserializer for the `orders` value.
 *
 * Default classification here is **permanent**, the inverse of the registry
 * default: bytes that cannot be decoded now cannot be decoded later either.
 * Only an explicit registry-side transient fault escapes as retryable.
 */
export function createOrderDeserializer({
  client,
  topic,
  validate = true,
}: DeserializerOptions): OrderDeserializer {
  const deserializer = new AvroDeserializer(client, SerdeType.VALUE, {});

  return {
    deserialize: async (payload: Buffer | null | undefined): Promise<Order> => {
      if (payload === null || payload === undefined) {
        throw new PermanentError('deserialization', 'record value is null');
      }

      // Validate the frame before handing bytes to the library: this converts
      // "HTTP 404 from the registry" into an accurate "byte 0 is not 0x00", and
      // rejects the empty payload that would otherwise decode to null.
      const header = readWireFormatHeader(payload);

      let decoded: unknown;
      try {
        decoded = (await deserializer.deserialize(topic, payload)) as unknown;
      } catch (error) {
        if (error instanceof RestError) {
          // The frame was well-formed, so a registry error is about the schema
          // ID, not the bytes. A missing ID is permanent; an outage is not.
          throw classifyRegistryError(
            error,
            `deserialize record written with schema id ${String(header.schemaId)}`,
          );
        }

        throw new PermanentError(
          'deserialization',
          `avro decode failed for schema id ${String(header.schemaId)}: ${describeError(error)}`,
          { cause: error },
        );
      }

      // Belt and braces: the library returns null for input it cannot make
      // sense of rather than throwing.
      if (decoded === null || decoded === undefined) {
        throw new PermanentError(
          'deserialization',
          `avro decode produced no value for schema id ${String(header.schemaId)}`,
        );
      }

      if (!validate) {
        return decoded as Order;
      }

      // Also normalises: the library returns an `avsc`-generated class instance,
      // and zod hands back a plain object that is safe to spread, clone and log.
      return parseOrder(decoded);
    },
  };
}
