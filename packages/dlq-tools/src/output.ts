/* eslint-disable no-console -- see the module comment */

/**
 * The CLI's output channel.
 *
 * `no-console` is an error everywhere else in this repository (§9): every
 * diagnostic goes through the structured logger so that demo logs are
 * greppable. This module is the one deliberate exception, and it is not a
 * diagnostic channel — it is the CLI's *product*. A table, or a JSON document
 * meant for `jq`, is what the user asked for, and it goes to **stdout**.
 * Diagnostics still go through pino, which the CLI points at **stderr**, so
 * `dlq-inspector list --json | jq` sees data and only data.
 */

export interface Output {
  /** A line of human-readable output. */
  line: (text?: string) => void;
  /** A machine-readable document, pretty-printed for a terminal, compact for a pipe. */
  json: (value: unknown) => void;
}

export function createOutput(pretty: boolean = process.stdout.isTTY): Output {
  return {
    line(text = '') {
      console.log(text);
    },
    json(value) {
      console.log(pretty ? JSON.stringify(value, null, 2) : JSON.stringify(value));
    },
  };
}

/** Fixed-width table rendering for `list`. */
export interface Column<T> {
  readonly header: string;
  readonly value: (row: T) => string;
  readonly align?: 'left' | 'right';
  readonly maxWidth?: number;
}

function clip(text: string, max: number | undefined): string {
  if (max === undefined || text.length <= max) {
    return text;
  }
  return `${text.slice(0, Math.max(0, max - 1))}…`;
}

export function renderTable<T>(rows: readonly T[], columns: readonly Column<T>[]): string[] {
  const cells = rows.map((row) => columns.map((c) => clip(c.value(row), c.maxWidth)));
  const widths = columns.map((c, i) =>
    Math.max(c.header.length, ...cells.map((r) => r[i]?.length ?? 0)),
  );
  const pad = (text: string, i: number): string =>
    columns[i]?.align === 'right' ? text.padStart(widths[i] ?? 0) : text.padEnd(widths[i] ?? 0);

  const lines = [
    columns.map((c, i) => pad(c.header, i)).join('  '),
    widths.map((w) => '─'.repeat(w)).join('  '),
  ];
  for (const row of cells) {
    lines.push(row.map((text, i) => pad(text, i)).join('  '));
  }
  return lines;
}
