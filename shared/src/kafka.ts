import { Kafka, type Consumer, type Producer, type EachMessagePayload } from "kafkajs";
import type { ZodSchema } from "zod";
import type { Logger } from "pino";
import { TOPICS } from "./schemas.js";
import type { IdempotencyStore } from "./idempotency.js";

export const buildKafka = (clientId: string) =>
  new Kafka({ clientId, brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",") });

export type HandlerContext<T> = { event: T; raw: EachMessagePayload; attempt: number };
export type Handler<T> = (ctx: HandlerContext<T>) => Promise<void>;

type ConsumeOptions<T> = {
  topic: string;
  groupId: string;
  schema: ZodSchema<T>;
  handler: Handler<T>;
  idempotency: IdempotencyStore;
  logger: Logger;
  dlqProducer: Producer;
  maxRetries?: number;
};

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const backoff = (attempt: number) => Math.min(1000 * 2 ** attempt, 30_000);

// One canonical consumer with: schema validation, idempotency, retries w/ backoff, DLQ on terminal failure.
export const startConsumer = async <T extends { eventId: string }>(
  kafka: Kafka,
  opts: ConsumeOptions<T>,
): Promise<Consumer> => {
  const { topic, groupId, schema, handler, idempotency, logger, dlqProducer, maxRetries = 3 } = opts;
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async (raw) => {
      const value = raw.message.value?.toString();
      if (!value) return;

      let parsed: T;
      try {
        parsed = schema.parse(JSON.parse(value));
      } catch (err) {
        logger.error({ err, value }, "schema validation failed — sending to DLQ");
        await sendToDLQ(dlqProducer, topic, value, "schema_validation_failed");
        return;
      }

      const isFirstTime = await idempotency.tryConsume(groupId, parsed.eventId);
      if (!isFirstTime) {
        logger.debug({ eventId: parsed.eventId, groupId }, "duplicate event — skipping");
        return;
      }

      for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
          await handler({ event: parsed, raw, attempt });
          return;
        } catch (err) {
          logger.warn({ err, attempt, eventId: parsed.eventId }, "handler failed");
          if (attempt === maxRetries - 1) {
            await sendToDLQ(dlqProducer, topic, value, `handler_failed_after_${maxRetries}_attempts`);
            return;
          }
          await sleep(backoff(attempt));
        }
      }
    },
  });

  return consumer;
};

const sendToDLQ = async (producer: Producer, sourceTopic: string, payload: string, reason: string) => {
  await producer.send({
    topic: TOPICS.OrdersDLQ,
    messages: [{
      value: JSON.stringify({ sourceTopic, reason, failedAt: new Date().toISOString(), originalPayload: payload }),
    }],
  });
};

export const buildProducer = async (kafka: Kafka): Promise<Producer> => {
  const p = kafka.producer({ allowAutoTopicCreation: true, idempotent: true });
  await p.connect();
  return p;
};
