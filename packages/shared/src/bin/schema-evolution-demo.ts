import { Compatibility, type Client, type SchemaInfo } from '@confluentinc/schemaregistry';

import { baseEnvSchema, loadConfig } from '../config.js';
import { describeError, isPermanent } from '../errors.js';
import { createLogger, type Logger } from '../logger.js';
import {
  classifyRegistryError,
  createRegistryClient,
  ensureOrderSchemaRegistered,
} from '../registry.js';
import { readOrderSchemaJson } from '../schema-file.js';
import { buildTopicRegistry, valueSubjectFor } from '../topics.js';

/**
 * Demonstrates what `BACKWARD` compatibility actually enforces.
 *
 * Run against a live registry (`npm run schema:evolution-demo`). It proposes
 * three changes to the registered Order schema and reports the registry's
 * verdict on each, committing none of them:
 *
 * 1. **Adding a field with a default** — accepted. A reader on the new schema
 *    fills the field from its default when reading records written by the old
 *    one.
 * 2. **Adding a field with no default** — rejected. Reading an old record would
 *    leave the field with no value and nothing to fall back on.
 * 3. **Changing `price` from `float` to `string`** — rejected. Old records
 *    cannot be read with the new schema at all.
 *
 * The point is that the rejections happen *here*, before deployment, instead of
 * surfacing as wrong numbers in the aggregate three days later.
 */

interface Proposal {
  readonly label: string;
  readonly expectation: 'accepted' | 'rejected';
  readonly why: string;
  readonly schema: Record<string, unknown>;
}

interface Verdict {
  readonly label: string;
  readonly expectation: 'accepted' | 'rejected';
  readonly actual: 'accepted' | 'rejected';
  readonly matched: boolean;
  readonly detail: string;
}

function buildProposals(): readonly Proposal[] {
  const base = readOrderSchemaJson();
  const { fields } = base;

  return [
    {
      label: 'add optional `currency` with a default',
      expectation: 'accepted',
      why: 'a reader on the new schema fills the field from its default when reading old records',
      schema: {
        ...base,
        fields: [
          ...fields,
          {
            name: 'currency',
            type: 'string',
            default: 'EUR',
            doc: 'ISO 4217 currency code. Defaulted so old records stay readable.',
          },
        ],
      },
    },
    {
      label: 'add required `customerId` with no default',
      expectation: 'rejected',
      why: 'reading an old record leaves the field with no value and no default to fall back on',
      schema: {
        ...base,
        fields: [
          ...fields,
          { name: 'customerId', type: 'string', doc: 'No default — deliberately incompatible.' },
        ],
      },
    },
    {
      label: 'change `price` from float to string',
      expectation: 'rejected',
      why: 'a float on the wire cannot be promoted to a string, so old records become unreadable',
      schema: {
        ...base,
        fields: fields.map((field) =>
          field.name === 'price' ? { ...field, type: 'string' } : field,
        ),
      },
    },
  ];
}

/**
 * Asks the registry whether a proposal would be accepted, without registering it.
 *
 * `testSubjectCompatibility` is a dry run — the whole point is that this can be
 * wired into CI as a pre-merge gate without mutating the registry.
 */
async function evaluate(client: Client, subject: string, proposal: Proposal): Promise<Verdict> {
  const candidate: SchemaInfo = {
    schema: JSON.stringify(proposal.schema),
    schemaType: 'AVRO',
  };

  let actual: 'accepted' | 'rejected';
  let detail: string;

  try {
    const compatible = await client.testSubjectCompatibility(subject, candidate);
    actual = compatible ? 'accepted' : 'rejected';
    detail = compatible
      ? 'registry reports the change is compatible'
      : 'registry refused the change';
  } catch (error) {
    // A malformed or incompatible schema comes back as a classified permanent
    // error; anything else (a registry outage) must not be reported as a
    // compatibility verdict.
    const classified = classifyRegistryError(error, 'test schema compatibility');
    if (!isPermanent(classified)) {
      throw classified;
    }
    actual = 'rejected';
    detail = classified.message;
  }

  return {
    label: proposal.label,
    expectation: proposal.expectation,
    actual,
    matched: actual === proposal.expectation,
    detail,
  };
}

async function run(logger: Logger): Promise<number> {
  const config = loadConfig(baseEnvSchema);
  const topics = buildTopicRegistry(config.TOPIC_PREFIX);
  const subject = valueSubjectFor(topics.orders);
  const client = createRegistryClient({ url: config.SCHEMA_REGISTRY_URL });

  try {
    const registration = await ensureOrderSchemaRegistered({
      client,
      topic: topics.orders,
      logger,
    });

    logger.info(
      { subject, baseline: registration.version, compatibility: Compatibility.BACKWARD },
      'evaluating proposed schema changes against the registered baseline',
    );

    const verdicts: Verdict[] = [];
    for (const proposal of buildProposals()) {
      const verdict = await evaluate(client, subject, proposal);
      verdicts.push(verdict);

      logger.info(
        {
          change: verdict.label,
          expected: verdict.expectation,
          actual: verdict.actual,
          why: proposal.why,
          registry: verdict.detail,
        },
        verdict.matched ? 'verdict as expected' : 'VERDICT DID NOT MATCH EXPECTATION',
      );
    }

    const surprises = verdicts.filter((verdict) => !verdict.matched);
    if (surprises.length > 0) {
      logger.error(
        { surprises: surprises.map((verdict) => verdict.label) },
        'registry did not behave as BACKWARD compatibility requires',
      );
      return 1;
    }

    // Nothing was registered: every proposal went through the dry-run endpoint.
    // Printing the version list proves it — still one version, the baseline.
    const versions = await client.getAllVersions(subject);
    logger.info(
      { subject, versions },
      'demo complete; registry unchanged — all checks were dry runs',
    );

    return 0;
  } finally {
    client.close();
  }
}

const logger = createLogger({
  service: 'schema-evolution-demo',
  level: 'info',
  pretty: process.env['NODE_ENV'] !== 'production',
});

run(logger)
  .then((code) => {
    process.exitCode = code;
  })
  .catch((error: unknown) => {
    logger.fatal({ err: error }, `schema evolution demo failed: ${describeError(error)}`);
    process.exitCode = 1;
  });
