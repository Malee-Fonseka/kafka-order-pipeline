import { PermanentError } from './errors.js';

/**
 * The Confluent Schema Registry wire format.
 *
 * A "Avro message" on a Confluent-style topic is not bare Avro binary. Every
 * value is framed:
 *
 * ```
 *  byte 0      bytes 1..4              bytes 5..n
 * ┌────────┬───────────────────────┬─────────────────────┐
 * │  0x00  │ schema ID, int32 BE   │ Avro binary payload │
 * └────────┴───────────────────────┴─────────────────────┘
 *  magic     registry lookup key     no embedded schema
 * ```
 *
 * The payload carries **no** schema of its own — that is the entire point. The
 * writer schema is fetched from the registry by ID and cached, so a 3-byte
 * order costs 5 bytes of framing instead of a kilobyte of repeated JSON schema
 * on every single message.
 *
 * These helpers exist because two later phases need to reason about the frame
 * without decoding the payload: the DLQ writer records the schema ID of bytes
 * it could not decode (D6), and the inspector CLI decides whether a record is
 * even worth attempting to decode.
 */

/** Confluent framing marker. Any other leading byte is not a registry payload. */
export const MAGIC_BYTE = 0x00;

/** Magic byte plus the 4-byte big-endian schema ID. */
export const WIRE_HEADER_LENGTH = 5;

export interface WireFormatHeader {
  /** Registry schema ID the payload was written with. */
  readonly schemaId: number;
  /** Byte offset at which the Avro payload begins. Always {@link WIRE_HEADER_LENGTH}. */
  readonly payloadOffset: number;
}

/**
 * Parses the frame without touching the payload.
 *
 * Throws a **permanent** error: a malformed frame is a poison pill by
 * definition, and no amount of retrying will make byte 0 become `0x00`.
 */
export function readWireFormatHeader(buffer: Buffer): WireFormatHeader {
  if (buffer.length < WIRE_HEADER_LENGTH) {
    throw new PermanentError(
      'deserialization',
      `payload is ${String(buffer.length)} bytes; Confluent framing requires at least ${String(WIRE_HEADER_LENGTH)}`,
    );
  }

  const magic = buffer.readUInt8(0);
  if (magic !== MAGIC_BYTE) {
    throw new PermanentError(
      'deserialization',
      `expected magic byte 0x00, found 0x${magic.toString(16).padStart(2, '0')}`,
    );
  }

  return { schemaId: buffer.readInt32BE(1), payloadOffset: WIRE_HEADER_LENGTH };
}

/**
 * Non-throwing variant for diagnostic paths.
 *
 * The DLQ writer and the inspector CLI run *because* something already failed;
 * they must never throw a second error while describing the first.
 */
export function tryReadWireFormatHeader(buffer: Buffer): WireFormatHeader | undefined {
  try {
    return readWireFormatHeader(buffer);
  } catch {
    return undefined;
  }
}

/**
 * Builds a frame around an already-encoded Avro payload.
 *
 * Production code never calls this — the serializer does the framing — but
 * tests need to construct payloads that are deliberately valid or deliberately
 * corrupt, and hand-assembling magic bytes inline in three test files is how
 * the framing constant silently drifts.
 */
export function encodeWireFormatHeader(schemaId: number, payload: Buffer): Buffer {
  const header = Buffer.alloc(WIRE_HEADER_LENGTH);
  header.writeUInt8(MAGIC_BYTE, 0);
  header.writeInt32BE(schemaId, 1);
  return Buffer.concat([header, payload]);
}
