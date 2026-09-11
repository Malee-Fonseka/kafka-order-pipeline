import { type Order, TRANSIENT_FAIL_PRODUCT, TransientError } from '@order-pipeline/shared';

import type { Aggregator, RecordSource } from './aggregation/aggregator.js';
import type { StateStore } from './aggregation/state-store.js';

/**
 * The business step: what happens to a decoded order.
 *
 * Kept apart from decoding and from retry orchestration so that "what this
 * system does with an order" is one short function — fold it into the
 * aggregate, write the changelog — and the chaos rule that makes it fail on
 * demand is visibly separate from the real work.
 */

export interface HandlerContext {
  /** 1-based delivery: 1 on the first pass, 2 after the first retry tier, and so on. */
  readonly delivery: number;
  /** 1-based in-place attempt within this delivery. */
  readonly attempt: number;
  readonly source: RecordSource;
}

export interface ChaosHandlerOptions {
  /**
   * The delivery on which the transient marker finally succeeds. `1` means it
   * never fails; `2` (the default) fails the first delivery and succeeds when
   * it comes back from the 5s tier; `4` rides every tier and succeeds after
   * the 5m one; `5` or more is exhausted.
   */
  readonly transientSucceedAfterDelivery: number;
}

export interface OrderHandlerOptions {
  readonly aggregator: Aggregator;
  readonly stateStore: StateStore;
  readonly chaos: ChaosHandlerOptions;
  readonly onProcessed?: (order: Order) => void;
}

export type OrderHandler = (order: Order, context: HandlerContext) => Promise<void>;

export function createOrderHandler({
  aggregator,
  stateStore,
  chaos,
  onProcessed,
}: OrderHandlerOptions): OrderHandler {
  return async (order, context) => {
    // D8: the marker product simulates a downstream that is down for a while.
    // It fails *every* in-place attempt of a delivery — a real outage does not
    // clear in 300 ms — and recovers on a later delivery, i.e. after a tier.
    if (
      order.product === TRANSIENT_FAIL_PRODUCT &&
      context.delivery < chaos.transientSucceedAfterDelivery
    ) {
      throw new TransientError(
        `chaos: simulated downstream outage (delivery ${String(context.delivery)} of ${String(chaos.transientSucceedAfterDelivery)}, attempt ${String(context.attempt)})`,
      );
    }

    // Write-ahead, in three steps that must stay in this order:
    //   next  — compute the updated state without touching memory;
    //   write — make it durable in the changelog;
    //   apply — only now advance memory and notify the dashboard.
    // The offset commits after all three. On restart the restored aggregate
    // is never behind the committed position, and a failed changelog write
    // leaves memory untouched so a retry counts the order once (ADR 006).
    const entry = aggregator.next(order, context.source);
    await stateStore.write(entry);
    aggregator.apply(entry);
    onProcessed?.(order);
  };
}
