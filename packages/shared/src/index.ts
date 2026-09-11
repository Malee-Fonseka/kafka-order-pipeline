export { baseEnvSchema, loadConfig, ConfigurationError, type BaseEnv } from './config.js';
export { createLogger, type Logger, type LoggerParams } from './logger.js';
export {
  buildTopicRegistry,
  valueSubjectFor,
  type RetryTier,
  type TopicRegistry,
} from './topics.js';
export {
  createShutdownManager,
  type ShutdownHook,
  type ShutdownManager,
  type ShutdownOptions,
} from './shutdown.js';

// --- Phase 2: error taxonomy, Avro schema, serde ---

export {
  PipelineError,
  PermanentError,
  TransientError,
  describeError,
  isClassifiedError,
  isPermanent,
  isTransient,
  type ClassifiedError,
  type ErrorKind,
  type PermanentReason,
  type PipelineErrorOptions,
} from './errors.js';
export { formatOrder, isOrder, orderSchema, parseOrder, type Order } from './order.js';
export {
  ORDER_SCHEMA_PATH,
  ORDER_SCHEMA_TYPE,
  readOrderSchemaJson,
  readOrderSchemaString,
} from './schema-file.js';
export {
  Compatibility,
  ORDER_COMPATIBILITY,
  classifyRegistryError,
  createRegistryClient,
  ensureOrderSchemaRegistered,
  orderSchemaInfo,
  type EnsureSchemaOptions,
  type RegistryClientOptions,
  type SchemaRegistration,
} from './registry.js';
export {
  createOrderDeserializer,
  createOrderSerializer,
  type DeserializerOptions,
  type OrderDeserializer,
  type OrderSerializer,
  type SerdeOptions,
} from './serde.js';
export {
  MAGIC_BYTE,
  WIRE_HEADER_LENGTH,
  encodeWireFormatHeader,
  readWireFormatHeader,
  tryReadWireFormatHeader,
  type WireFormatHeader,
} from './wire-format.js';
