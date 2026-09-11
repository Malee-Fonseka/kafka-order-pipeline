import { type Logger, type Producer, replayHeaders } from '@order-pipeline/shared';

import type { DeadLetter } from '../dlq-reader.js';
import type { Output } from '../output.js';

/**
 * `dlq-inspector replay` — send dead letters back to the main topic.
 *
 * This is what makes the DLQ a parking lot rather than a grave. A record that
 * failed because a downstream was down, or because a bug has since been
 * fixed, goes back onto `orders` **exactly as it was** — same key, same
 * bytes — and takes its normal path through the consumer, including a fresh
 * set of retry tiers and, if it fails again, a fresh dead letter.
 *
 * What travels with it: every header that is not a description of the past
 * failure (the correlation id above all, so the replay is traceable to its
 * history), plus `x-replayed-from-dlq-offset`, `x-replayed-at` and a replay
 * count that climbs each time the same record comes back.
 *
 * What does not: the DLQ record itself. Kafka topics are append-only, so the
 * dead letter stays where it is as the permanent record that this happened.
 * The replay count on any *future* dead letter for the same record is how an
 * operator tells "replayed once, fine now" from "keeps coming back".
 */

export interface ReplayOptions {
  readonly targetTopic: string;
  readonly dryRun: boolean;
  readonly json: boolean;
  readonly now?: () => Date;
}

export interface ReplayResult {
  readonly dlqOffset: string;
  readonly key: string | null;
  readonly correlationId: string | undefined;
  readonly replayCount: number;
  readonly targetPartition: number | undefined;
  readonly targetOffset: string | undefined;
}

/** Only `send` is needed, and not at all for a dry run. */
export type ReplaySender = Pick<Producer, 'send'>;

export async function runReplay(
  letters: readonly DeadLetter[],
  producer: ReplaySender | undefined,
  options: ReplayOptions,
  logger: Logger,
  out: Output,
): Promise<ReplayResult[]> {
  if (!options.dryRun && producer === undefined) {
    throw new Error('a producer is required unless --dry-run is set');
  }
  const now = options.now ?? (() => new Date());
  const results: ReplayResult[] = [];

  for (const letter of letters) {
    const headers = replayHeaders({
      dlqOffset: letter.offset,
      headers: letter.headers,
      replayedAt: now(),
    });
    const replayCount = Number(headers['x-replay-count'] ?? '1');
    const key = letter.key === null ? null : letter.key.toString('utf8');

    if (options.dryRun || producer === undefined) {
      results.push({
        dlqOffset: letter.offset,
        key,
        correlationId: letter.meta.correlationId,
        replayCount,
        targetPartition: undefined,
        targetOffset: undefined,
      });
      continue;
    }

    // Same key, same bytes. The value is passed through untouched — it may
    // well be undecodable, and that is not this tool's concern.
    const [metadata] = await producer.send({
      topic: options.targetTopic,
      messages: [{ key: letter.key, value: letter.value, headers }],
    });

    logger.info(
      {
        dlqOffset: letter.offset,
        key,
        correlationId: letter.meta.correlationId,
        replayCount,
        targetTopic: options.targetTopic,
        targetPartition: metadata?.partition,
        targetOffset: metadata?.offset,
      },
      'dead letter replayed',
    );

    results.push({
      dlqOffset: letter.offset,
      key,
      correlationId: letter.meta.correlationId,
      replayCount,
      targetPartition: metadata?.partition,
      targetOffset: metadata?.offset,
    });
  }

  if (options.json) {
    out.json({ dryRun: options.dryRun, targetTopic: options.targetTopic, replayed: results });
  } else if (results.length === 0) {
    out.line('nothing selected — use an offset, --from/--to, or --all');
  } else {
    for (const r of results) {
      const where =
        r.targetOffset === undefined
          ? options.dryRun
            ? 'would replay'
            : 'replayed'
          : `replayed → ${options.targetTopic}[${String(r.targetPartition ?? '?')}]@${r.targetOffset}`;
      out.line(
        `dlq@${r.dlqOffset.padStart(4)}  key=${(r.key ?? '<null>').padEnd(20)}  replay #${String(r.replayCount)}  ${where}`,
      );
    }
    out.line();
    out.line(
      `${String(results.length)} record(s) ${options.dryRun ? 'would be' : ''} replayed to ${options.targetTopic}`.replace(
        '  ',
        ' ',
      ),
    );
  }

  return results;
}
