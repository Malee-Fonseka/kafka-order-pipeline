import { describe, expect, it } from 'vitest';

import { PermanentError } from './errors.js';
import {
  MAGIC_BYTE,
  WIRE_HEADER_LENGTH,
  encodeWireFormatHeader,
  readWireFormatHeader,
  tryReadWireFormatHeader,
} from './wire-format.js';

describe('Confluent wire format', () => {
  it('frames a payload as magic byte + big-endian schema id + payload', () => {
    const framed = encodeWireFormatHeader(1, Buffer.from([0xaa, 0xbb]));

    // magic byte, then schema id 1 as int32 big-endian, then the payload
    expect(framed.toString('hex')).toBe('00' + '00000001' + 'aabb');
    expect(framed.readUInt8(0)).toBe(MAGIC_BYTE);
    expect(framed.length).toBe(WIRE_HEADER_LENGTH + 2);
  });

  it('reads back a schema id larger than one byte', () => {
    // Guards the endianness: 0x0001e240 read little-endian would be 1_074_659_328.
    const framed = encodeWireFormatHeader(123_456, Buffer.alloc(0));

    expect(framed.subarray(1, 5).toString('hex')).toBe('0001e240');
    expect(readWireFormatHeader(framed)).toEqual({
      schemaId: 123_456,
      payloadOffset: WIRE_HEADER_LENGTH,
    });
  });

  it('rejects a foreign magic byte as permanent', () => {
    // The canonical "someone published JSON to an Avro topic" case: `{` is 0x7b.
    const json = Buffer.from(JSON.stringify({ orderId: '1' }), 'utf8');

    expect(() => readWireFormatHeader(json)).toThrowError(PermanentError);
    expect(() => readWireFormatHeader(json)).toThrowError(/magic byte 0x00, found 0x7b/);
  });

  it('rejects a payload too short to carry a frame', () => {
    for (const length of [0, 1, 4]) {
      const short = Buffer.alloc(length);
      expect(() => readWireFormatHeader(short)).toThrowError(PermanentError);
    }
  });

  it('accepts a frame with an empty payload — framing and decoding are separate concerns', () => {
    expect(readWireFormatHeader(Buffer.from([0x00, 0x00, 0x00, 0x00, 0x07])).schemaId).toBe(7);
  });

  it('classifies every framing failure as permanent, never transient', () => {
    try {
      readWireFormatHeader(Buffer.from([0x01]));
      expect.unreachable('should have thrown');
    } catch (error) {
      expect(error).toBeInstanceOf(PermanentError);
      expect((error as PermanentError).reason).toBe('deserialization');
    }
  });

  it('never throws from the diagnostic variant', () => {
    // The DLQ writer runs because something already failed; a second throw
    // while describing the first would lose the record entirely.
    expect(tryReadWireFormatHeader(Buffer.alloc(0))).toBeUndefined();
    expect(tryReadWireFormatHeader(Buffer.from([0xff, 0xff]))).toBeUndefined();
    expect(tryReadWireFormatHeader(encodeWireFormatHeader(9, Buffer.alloc(0)))?.schemaId).toBe(9);
  });
});
