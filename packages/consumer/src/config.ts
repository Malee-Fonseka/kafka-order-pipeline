import { baseEnvSchema } from '@order-pipeline/shared';
import { z } from 'zod';

/**
 * Consumer environment contract.
 *
 * Extends the shared base schema rather than adding consumer concerns to it
 * (Appendix B), so a producer-only deployment is never asked for a group id.
 */
export const consumerEnvSchema = baseEnvSchema.extend({
  /**
   * The consumer group. Every instance sharing this id splits the partitions
   * between them (the rebalance demonstration, §11 step 4); an instance with
   * a different id gets its own full copy of the stream.
   */
  CONSUMER_GROUP_ID: z.string().min(1).default('order-consumers'),

  /**
   * Where a *new* group starts. Only consulted when the group has no committed
   * offset for a partition — once a commit exists, that wins, which is what
   * makes "restart and resume exactly where it stopped" (§11 step 8) work.
   *
   * `earliest` for the demo, so a fresh group replays the topic; `latest` is
   * the production-typical choice.
   */
  CONSUMER_AUTO_OFFSET_RESET: z.enum(['earliest', 'latest']).default('earliest'),
});

export type ConsumerEnv = z.infer<typeof consumerEnvSchema>;
