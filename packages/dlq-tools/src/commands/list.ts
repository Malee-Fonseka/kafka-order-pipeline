import type { DeadLetter } from '../dlq-reader.js';
import { type Column, type Output, renderTable } from '../output.js';

/**
 * `dlq-inspector list` — one row per dead letter, most useful facts first.
 *
 * The table answers the operator's first three questions without a second
 * command: how many, why, and where from. `--json` emits the same rows as
 * structured data for scripting.
 */

export interface ListRow {
  readonly offset: string;
  readonly writtenAt: string;
  readonly errorType: string;
  readonly errorClass: string;
  readonly reason: string;
  readonly origin: string;
  readonly attempt: string;
  readonly key: string;
  readonly correlationId: string;
  readonly replays: string;
}

export function toListRow(letter: DeadLetter): ListRow {
  const m = letter.meta;
  const origin =
    m.originalTopic === undefined
      ? '?'
      : `${m.originalTopic}[${String(m.originalPartition ?? '?')}]@${m.originalOffset ?? '?'}`;

  return {
    offset: letter.offset,
    writtenAt: letter.writtenAt,
    errorType: m.errorType ?? '?',
    errorClass: m.errorClass ?? '?',
    reason: m.errorMessage ?? '',
    origin,
    attempt: m.attempt === undefined ? '?' : String(m.attempt),
    key: m.originalKey ?? letter.key?.toString('utf8') ?? '',
    correlationId: m.correlationId ?? '',
    replays: m.replayCount === undefined ? '' : String(m.replayCount),
  };
}

const columns: readonly Column<ListRow>[] = [
  { header: 'OFFSET', value: (r) => r.offset, align: 'right' },
  { header: 'WRITTEN', value: (r) => r.writtenAt.replace('T', ' ').slice(0, 19) },
  { header: 'TYPE', value: (r) => r.errorType },
  { header: 'CLASS', value: (r) => r.errorClass },
  { header: 'ATT', value: (r) => r.attempt, align: 'right' },
  { header: 'KEY', value: (r) => r.key, maxWidth: 20 },
  { header: 'ORIGIN', value: (r) => r.origin },
  { header: 'CORRELATION', value: (r) => r.correlationId.slice(0, 8) },
  { header: 'REASON', value: (r) => r.reason, maxWidth: 60 },
];

export interface ListOptions {
  readonly json: boolean;
  /** Show only the newest N. */
  readonly limit?: number;
}

export function runList(letters: readonly DeadLetter[], options: ListOptions, out: Output): void {
  const shown = options.limit === undefined ? letters : letters.slice(-options.limit);
  const rows = shown.map(toListRow);

  if (options.json) {
    out.json({ total: letters.length, shown: rows.length, letters: rows });
    return;
  }

  if (rows.length === 0) {
    out.line('dead letter queue is empty');
    return;
  }

  for (const line of renderTable(rows, columns)) {
    out.line(line);
  }
  out.line();
  const byType = new Map<string, number>();
  for (const row of rows) {
    byType.set(row.errorType, (byType.get(row.errorType) ?? 0) + 1);
  }
  const summary = [...byType].map(([type, n]) => `${String(n)} ${type}`).join(', ');
  out.line(`${String(rows.length)} of ${String(letters.length)} dead letter(s) shown — ${summary}`);
}
