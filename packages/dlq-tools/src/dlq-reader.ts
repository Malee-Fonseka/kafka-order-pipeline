import {
  type DlqMetadata,
  type KafkaClient,
  type Logger,
  type ScannedRecord,
  readDlqMetadata,
  scanTopic,
} from '@order-pipeline/shared';

/**
 * Reads the dead letter queue into memory.
 *
 * The DLQ is small by construction — every record in it is an operator's
 * problem to look at — and it never expires (`retention.ms=-1`), so "read the
 * whole thing" is the right primitive for a tool whose job is inspection. A
 * DLQ large enough to make that uncomfortable is a DLQ that needed attention
 * long before the tool was run.
 */

export interface DeadLetter {
  readonly offset: string;
  readonly partition: number;
  /** When the dead letter was written (broker timestamp), ISO. */
  readonly writtenAt: string;
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  readonly headers: Readonly<Record<string, unknown>> | undefined;
  readonly meta: DlqMetadata;
}

export interface ReadDlqOptions {
  readonly kafka: KafkaClient;
  readonly topic: string;
  readonly logger: Logger;
  readonly timeoutMs?: number;
}

export function toDeadLetter(record: ScannedRecord): DeadLetter {
  return {
    offset: record.offset,
    partition: record.partition,
    writtenAt: new Date(Number(record.timestamp)).toISOString(),
    key: record.key,
    value: record.value,
    headers: record.headers,
    meta: readDlqMetadata(record.headers),
  };
}

/** Every dead letter currently on the topic, oldest first. */
export async function readDlq({
  kafka,
  topic,
  logger,
  timeoutMs,
}: ReadDlqOptions): Promise<DeadLetter[]> {
  const letters: DeadLetter[] = [];

  await scanTopic({
    kafka,
    topic,
    groupIdPrefix: 'dlq-inspector',
    logger,
    ...(timeoutMs === undefined ? {} : { timeoutMs }),
    onRecord: (record) => {
      letters.push(toDeadLetter(record));
    },
  });

  // The DLQ has one partition, so offset order is arrival order; sort anyway
  // in case that ever changes.
  return letters.sort((a, b) => a.partition - b.partition || Number(a.offset) - Number(b.offset));
}

/** Selects dead letters by offset, `--all`, or an inclusive range. */
export interface Selection {
  readonly offsets?: readonly string[];
  readonly all?: boolean;
  readonly from?: string;
  readonly to?: string;
}

export function selectLetters(letters: readonly DeadLetter[], selection: Selection): DeadLetter[] {
  if (selection.all === true) {
    return [...letters];
  }
  if (selection.offsets !== undefined && selection.offsets.length > 0) {
    const wanted = new Set(selection.offsets);
    return letters.filter((l) => wanted.has(l.offset));
  }
  if (selection.from !== undefined || selection.to !== undefined) {
    const from = selection.from === undefined ? -Infinity : Number(selection.from);
    const to = selection.to === undefined ? Infinity : Number(selection.to);
    return letters.filter((l) => Number(l.offset) >= from && Number(l.offset) <= to);
  }
  return [];
}
