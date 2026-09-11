/**
 * Fault-injection markers shared by the producer that emits them and the
 * consumer that reacts to them (design decision D8, ADR 009).
 *
 * One exported constant rather than a string literal in two packages: the
 * coupling is real and is better visible than duplicated.
 */

/**
 * Marker product that makes the consumer's handler fail transiently.
 *
 * Deliberately not a plausible product name: it must never collide with real
 * data, and a reader scanning a topic in Kafbat UI should be able to tell at a
 * glance that the record is synthetic.
 */
export const TRANSIENT_FAIL_PRODUCT = '__TRANSIENT_FAIL__';
