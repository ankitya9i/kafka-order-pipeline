import "dotenv/config";
import express from "express";
import { Redis } from "ioredis";
import { v4 as uuid } from "uuid";
import {
  buildKafka, buildProducer, startConsumer, IdempotencyStore, createLogger,
  TOPICS, PaymentProcessedSchema,
} from "@pipeline/shared";

const logger = createLogger("notification-service");
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");
const idempotency = new IdempotencyStore(redis);
const kafka = buildKafka("notification-service");

const app = express();
app.get("/health", (_req, res) => res.json({ ok: true, service: "notification" }));

const start = async () => {
  const producer = await buildProducer(kafka);

  // Notify the user once payment has gone through.
  await startConsumer(kafka, {
    topic: TOPICS.PaymentProcessed,
    groupId: "notification-service",
    schema: PaymentProcessedSchema,
    idempotency,
    logger,
    dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId } = event.payload;
      // Mock send: pretend we hit SES/SMS gateway here.
      logger.info({ orderId }, "📧 sent confirmation email (mock)");

      await producer.send({
        topic: TOPICS.NotificationSent,
        messages: [{
          key: orderId,
          value: JSON.stringify({
            eventId: uuid(),
            eventType: "NotificationSent",
            occurredAt: new Date().toISOString(),
            correlationId: event.correlationId,
            version: 1,
            payload: { orderId, channel: "email", recipient: "user@example.com" },
          }),
        }],
      });
    },
  });

  const port = Number(process.env.NOTIFICATION_PORT ?? 3004);
  app.listen(port, () => logger.info({ port }, "notification-service listening"));
};

start().catch((err) => { logger.error({ err }, "fatal"); process.exit(1); });
