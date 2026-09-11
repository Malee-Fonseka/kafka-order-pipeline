import {
  CORRELATION_ID_HEADER,
  type Logger,
  type Order,
  type OrderDeserializer,
  type PermanentError,
  isPermanent,
  readHeader,
} from '@order-pipeline/shared';

import type { IncomingRecord } from './pipeline.js';

/**
 * What happens to one record (Phase 4 skeleton).
 *
 * The consumer's job is deserialize → handle → commit. This module owns the
 * first two; the pipeline owns the third. Both outcomes below are terminal,
 * because the pipeline commits on any resolved outcome (D7).
 *
 * `skipped` is a **placeholder**. A permanent failure — bytes that cannot be
 * decoded, a record that fails validation — belongs in the dead letter queue
 * with its raw bytes and forensic headers (D6). Until Phase 7 provides that
 * writer, the honest alternatives are to skip the record loudly or to stop
 * the consumer dead on the first poison pill. Skipping keeps the skeleton
 * usable against a topic that already carries chaos-mode records, and it is
 * counted and logged at `warn` so the gap is impossible to miss.
 *
 * Transient handling (the `__TRANSIENT_FAIL__` marker) arrives in Phase 6.
 * Here such a record is a valid order and is simply processed.
 */

/**
 * Both variants satisfy the pipeline's `TerminalOutcome` constraint, which the
 * compiler checks at the `createPipeline<Outcome>` call site.
 */
export type Outcome =
  | {
      readonly kind: 'processed';
      readonly order: Order;
      readonly correlationId: string | undefined;
    }
  | {
      readonly kind: 'skipped';
      readonly error: PermanentError;
      readonly correlationId: string | undefined;
    };

export interface ProcessorOptions {
  readonly deserializer: OrderDeserializer;
  readonly logger: Logger;
}

export type RecordProcessor = (record: IncomingRecord) => Promise<Outcome>;

export function createRecordProcessor({ deserializer, logger }: ProcessorOptions): RecordProcessor {
  return async (record) => {
    const correlationId = readHeader(record.headers, CORRELATION_ID_HEADER);
    const location = {
      topic: record.topic,
      partition: record.partition,
      offset: record.offset,
      key: record.key?.toString('utf8'),
      correlationId,
    };

    let order: Order;
    try {
      order = await deserializer.deserialize(record.value);
    } catch (error) {
      if (isPermanent(error)) {
        logger.warn(
          { ...location, reason: error.reason, err: error },
          'record cannot be processed; skipping until the DLQ writer lands in phase 7',
        );
        return { kind: 'skipped', error, correlationId };
      }

      // Transient (a registry blip mid-decode) or unclassified: neither is a
      // fact about the record, so neither is terminal. Let it propagate — the
      // pipeline will not commit, and the client redelivers.
      throw error;
    }

    logger.info(
      { ...location, orderId: order.orderId, product: order.product, price: order.price },
      'order received',
    );

    return { kind: 'processed', order, correlationId };
  };
}
