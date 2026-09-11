import { encodeWireFormatHeader } from './wire-format.js';

/**
 * Fault injection shared by the producer that emits it, the consumer that
 * reacts to it, and the integration tests that exercise both (design
 * decision D8, ADR 009).
 *
 * One exported home rather than a string literal in two packages: the
 * coupling is real and is better visible than duplicated.
 */

/**
 * Marker product that makes the consumer's handler fail transiently.
 *
 * Deliberately not a plausible product name: it must never collide with real
 * data, and a reader scanning a topic in Kafbat UI should be able to tell at a
 * glance that the record is synthetic.
 */
export const TRANSIENT_FAIL_PRODUCT = '__TRANSIENT_FAIL__';

/**
 * The ways a payload can be undecodable.
 *
 * Cycled rather than picked at random so a demo run exercises all three, and so
 * the DLQ ends up showing genuinely different failure reasons instead of the
 * same one three times.
 */
export type PoisonFlavour =
  /** Someone published JSON to an Avro topic: the magic byte is wrong. */
  | 'json-not-avro'
  /** Correctly framed, but the schema id resolves to nothing in the registry. */
  | 'unknown-schema-id'
  /** Correctly framed and a real schema id, but the payload is cut short. */
  | 'truncated-payload';

export const POISON_FLAVOURS: readonly PoisonFlavour[] = [
  'json-not-avro',
  'unknown-schema-id',
  'truncated-payload',
];

/** A schema id no registry in this project will ever allocate. */
const UNREGISTERED_SCHEMA_ID = 999_999;

/**
 * Builds bytes that the consumer genuinely cannot decode.
 *
 * `referencePayload` is a real serialized order, used only by the truncation
 * flavour — cutting a valid record short is the one corruption that cannot be
 * fabricated without a valid record to start from. Without it, that flavour
 * falls back to an unknown schema id rather than emitting something that would
 * accidentally decode.
 */
export function createPoisonPayload(flavour: PoisonFlavour, referencePayload?: Buffer): Buffer {
  switch (flavour) {
    case 'json-not-avro':
      // Byte 0 is '{' (0x7b), not the 0x00 magic byte.
      return Buffer.from(
        JSON.stringify({ orderId: '9999', product: 'Item1', price: 42, note: 'not avro' }),
        'utf8',
      );

    case 'unknown-schema-id':
      return encodeWireFormatHeader(UNREGISTERED_SCHEMA_ID, Buffer.from([0x02, 0x41, 0x00]));

    case 'truncated-payload':
      if (referencePayload !== undefined && referencePayload.length > 8) {
        return Buffer.from(referencePayload.subarray(0, 8));
      }
      return encodeWireFormatHeader(UNREGISTERED_SCHEMA_ID, Buffer.from([0x02]));
  }
}
