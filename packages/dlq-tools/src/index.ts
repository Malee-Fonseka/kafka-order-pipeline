import { parseArgs } from 'node:util';

import {
  baseEnvSchema,
  buildTopicRegistry,
  createIdempotentProducer,
  createKafkaClient,
  createLogger,
  createOrderDeserializer,
  createRegistryClient,
  describeError,
  loadConfig,
} from '@order-pipeline/shared';

import { buildDecodeReport, renderDecodeReport } from './commands/decode.js';
import { runList } from './commands/list.js';
import { runReplay } from './commands/replay.js';
import { readDlq, selectLetters } from './dlq-reader.js';
import { createOutput } from './output.js';

/**
 * `dlq-inspector` — list, decode and replay dead letters.
 *
 *   dlq-inspector list   [--limit N] [--json]
 *   dlq-inspector decode <offset> [--json]
 *   dlq-inspector replay <offset>... | --all | --from A [--to B]   [--dry-run] [--json]
 *
 * Output goes to stdout; logs go to stderr. Configuration comes from the same
 * environment as the services (`--env-file=.env` from the repo root).
 */

const USAGE = `dlq-inspector — inspect and replay the dead letter queue

  list    [--limit N] [--json]                    every dead letter, oldest first
  decode  <offset> [--json]                       everything knowable about one record
  replay  <offset>... | --all | --from A [--to B]  send records back to the main topic
          [--dry-run] [--json]

Reads KAFKA_BROKERS, SCHEMA_REGISTRY_URL and TOPIC_PREFIX from the environment.`;

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    json: { type: 'boolean', default: false },
    limit: { type: 'string' },
    all: { type: 'boolean', default: false },
    from: { type: 'string' },
    to: { type: 'string' },
    'dry-run': { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false },
  },
});

const [command, ...args] = positionals;
const out = createOutput(!values.json);

if (values.help || command === undefined) {
  out.line(USAGE);
  process.exitCode = command === undefined && !values.help ? 2 : 0;
} else {
  const config = loadConfig(baseEnvSchema);
  const logger = createLogger({
    service: 'dlq-inspector',
    level: config.LOG_LEVEL,
    pretty: config.NODE_ENV === 'development' && !values.json,
    destination: 'stderr',
  });
  const topics = buildTopicRegistry(config.TOPIC_PREFIX);
  const kafka = createKafkaClient({
    brokers: config.KAFKA_BROKERS,
    clientId: `${config.KAFKA_CLIENT_ID}-dlq-inspector`,
    logger,
  });

  const run = async (): Promise<void> => {
    switch (command) {
      case 'list': {
        const letters = await readDlq({ kafka, topic: topics.dlq, logger });
        const limit = values.limit === undefined ? undefined : Number(values.limit);
        runList(letters, { json: values.json, ...(limit === undefined ? {} : { limit }) }, out);
        return;
      }

      case 'decode': {
        const [offset] = args;
        if (offset === undefined) {
          throw new Error('decode needs an offset: dlq-inspector decode <offset>');
        }
        const letters = await readDlq({ kafka, topic: topics.dlq, logger });
        const letter = letters.find((l) => l.offset === offset);
        if (letter === undefined) {
          throw new Error(
            `no dead letter at offset ${offset} (${String(letters.length)} on the topic)`,
          );
        }

        // Validation off: a record that failed validation is exactly one the
        // operator wants to see decoded.
        const registry = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });
        try {
          const deserializer = createOrderDeserializer({
            client: registry,
            topic: topics.orders,
            validate: false,
          });
          const report = await buildDecodeReport(letter, deserializer);
          if (values.json) {
            out.json(report);
          } else {
            renderDecodeReport(report, out);
          }
        } finally {
          registry.close();
        }
        return;
      }

      case 'replay': {
        const selection = {
          offsets: args,
          all: values.all,
          ...(values.from === undefined ? {} : { from: values.from }),
          ...(values.to === undefined ? {} : { to: values.to }),
        };
        const letters = await readDlq({ kafka, topic: topics.dlq, logger });
        const selected = selectLetters(letters, selection);

        if (selected.length === 0) {
          out.line('nothing selected — use an offset, --from/--to, or --all');
          process.exitCode = 1;
          return;
        }

        if (values['dry-run']) {
          // No producer needed; nothing is sent.
          await runReplay(
            selected,
            undefined,
            { targetTopic: topics.orders, dryRun: true, json: values.json },
            logger,
            out,
          );
          return;
        }

        const producer = await createIdempotentProducer({ kafka, logger, purpose: 'dlq-replay' });
        try {
          await runReplay(
            selected,
            producer,
            { targetTopic: topics.orders, dryRun: false, json: values.json },
            logger,
            out,
          );
        } finally {
          await producer.flush({ timeout: 5_000 });
          await producer.disconnect();
        }
        return;
      }

      default:
        throw new Error(`unknown command "${command}"\n\n${USAGE}`);
    }
  };

  run().catch((error: unknown) => {
    logger.error({ err: error }, describeError(error));
    process.exitCode = 1;
  });
}
