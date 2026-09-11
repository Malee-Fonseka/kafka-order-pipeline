import {
  type Order,
  type OrderDeserializer,
  isClassifiedError,
  readHeader,
  tryReadWireFormatHeader,
} from '@order-pipeline/shared';

import type { DeadLetter } from '../dlq-reader.js';
import type { Output } from '../output.js';

/**
 * `dlq-inspector decode <offset>` — everything knowable about one dead letter.
 *
 * "Best effort" is the whole design. The record is in the DLQ because
 * something about it failed, quite possibly decoding itself, so this command
 * never assumes the bytes are Avro. It reports in layers, each independent of
 * the last:
 *
 * 1. the headers, which are always readable;
 * 2. the wire-format frame — is there a magic byte, what schema id;
 * 3. the raw bytes as hex and as text, so a human can recognise JSON or junk;
 * 4. an Avro decode with validation **off**, because a record that failed
 *    validation is exactly one the operator wants to see decoded.
 *
 * A failure at any layer is reported in that layer's slot and the next layer
 * still runs.
 */

export interface DecodeReport {
  readonly offset: string;
  readonly writtenAt: string;
  readonly key: string | null;
  readonly headers: Record<string, string>;
  readonly frame:
    { readonly magicByte: string; readonly schemaId: number } | { readonly error: string };
  readonly bytes: { readonly length: number; readonly hex: string; readonly text: string } | null;
  readonly decoded: Order | { readonly error: string; readonly kind: string };
}

const HEX_PREVIEW_BYTES = 64;

function headersAsStrings(
  headers: Readonly<Record<string, unknown>> | undefined,
): Record<string, string> {
  const result: Record<string, string> = {};
  for (const name of Object.keys(headers ?? {}).sort()) {
    result[name] = readHeader(headers, name) ?? '<binary>';
  }
  return result;
}

export async function buildDecodeReport(
  letter: DeadLetter,
  deserializer: OrderDeserializer,
): Promise<DecodeReport> {
  const { value } = letter;

  const frame = (() => {
    if (value === null) {
      return { error: 'record value is null' };
    }
    const header = tryReadWireFormatHeader(value);
    if (header === undefined) {
      const first =
        value.length === 0 ? 'none' : `0x${value.readUInt8(0).toString(16).padStart(2, '0')}`;
      return {
        error: `not Confluent-framed (first byte ${first}, length ${String(value.length)})`,
      };
    }
    return { magicByte: '0x00', schemaId: header.schemaId };
  })();

  const bytes =
    value === null
      ? null
      : {
          length: value.length,
          hex:
            value.subarray(0, HEX_PREVIEW_BYTES).toString('hex') +
            (value.length > HEX_PREVIEW_BYTES ? '…' : ''),
          text: value
            .subarray(0, HEX_PREVIEW_BYTES)
            .toString('utf8')
            .replace(/[^\x20-\x7e]/g, '.'),
        };

  let decoded: DecodeReport['decoded'];
  try {
    decoded = await deserializer.deserialize(value);
  } catch (error) {
    decoded = {
      error: error instanceof Error ? error.message : String(error),
      kind: isClassifiedError(error) ? error.kind : 'unclassified',
    };
  }

  return {
    offset: letter.offset,
    writtenAt: letter.writtenAt,
    key: letter.key === null ? null : letter.key.toString('utf8'),
    headers: headersAsStrings(letter.headers),
    frame,
    bytes,
    decoded,
  };
}

export function renderDecodeReport(report: DecodeReport, out: Output): void {
  out.line(
    `dead letter @${report.offset}  written ${report.writtenAt}  key=${report.key ?? '<null>'}`,
  );
  out.line();
  out.line('headers');
  for (const [name, value] of Object.entries(report.headers)) {
    // The stack is multi-line; show its first line and mark the rest.
    const firstLine = value.split('\n')[0] ?? '';
    const shown =
      name === 'x-error-stack' ? `${firstLine}${value.includes('\n') ? '  …' : ''}` : value;
    out.line(`  ${name.padEnd(24)} ${shown}`);
  }
  out.line();
  out.line('wire format');
  if ('error' in report.frame) {
    out.line(`  ${report.frame.error}`);
  } else {
    out.line(`  magic byte ${report.frame.magicByte}, schema id ${String(report.frame.schemaId)}`);
  }
  out.line();
  out.line('bytes');
  if (report.bytes === null) {
    out.line('  <null>');
  } else {
    out.line(`  ${String(report.bytes.length)} byte(s)`);
    out.line(`  hex   ${report.bytes.hex}`);
    out.line(`  text  ${report.bytes.text}`);
  }
  out.line();
  out.line('avro decode (validation off)');
  if ('error' in report.decoded) {
    out.line(`  failed (${report.decoded.kind}): ${report.decoded.error}`);
  } else {
    out.line(`  ${JSON.stringify(report.decoded)}`);
  }
}
