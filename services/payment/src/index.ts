import "dotenv/config";
import express from "express";
import { Redis } from "ioredis";
import { v4 as uuid } from "uuid";
import {
  buildKafka, buildProducer, startConsumer, IdempotencyStore, createLogger,
  TOPICS, InventoryReservedSchema,
} from "@pipeline/shared";

const logger = createLogger("payment-service");
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");
const idempotency = new IdempotencyStore(redis);
const kafka = buildKafka("payment-service");

const app = express();
app.get("/health", (_req, res) => res.json({ ok: true, service: "payment" }));

// Mock payment gateway. ~90% success.
const charge = async (_orderId: string): Promise<{ ok: true; paymentId: string } | { ok: false; reason: string }> => {
  await new Promise(r => setTimeout(r, 200));
  if (Math.random() < 0.9) return { ok: true, paymentId: uuid() };
  return { ok: false, reason: "insufficient_funds" };
};

const start = async () => {
  const producer = await buildProducer(kafka);

  // Payment fires only AFTER inventory has been reserved (saga sequencing).
  await startConsumer(kafka, {
    topic: TOPICS.InventoryReserved,
    groupId: "payment-service",
    schema: InventoryReservedSchema,
    idempotency,
    logger,
    dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId } = event.payload;
      const result = await charge(orderId);

      if (result.ok) {
        await producer.send({
          topic: TOPICS.PaymentProcessed,
          messages: [{
            key: orderId,
            value: JSON.stringify({
              eventId: uuid(),
              eventType: "PaymentProcessed",
              occurredAt: new Date().toISOString(),
              correlationId: event.correlationId,
              version: 1,
              payload: { orderId, paymentId: result.paymentId, amount: 0 },
            }),
          }],
        });
        logger.info({ orderId, paymentId: result.paymentId }, "payment processed");
      } else {
        await producer.send({
          topic: TOPICS.PaymentFailed,
          messages: [{
            key: orderId,
            value: JSON.stringify({
              eventId: uuid(),
              eventType: "PaymentFailed",
              occurredAt: new Date().toISOString(),
              correlationId: event.correlationId,
              version: 1,
              payload: { orderId, reason: result.reason },
            }),
          }],
        });
        logger.warn({ orderId, reason: result.reason }, "payment failed");
      }
    },
  });

  const port = Number(process.env.PAYMENT_PORT ?? 3002);
  app.listen(port, () => logger.info({ port }, "payment-service listening"));
};

start().catch((err) => { logger.error({ err }, "fatal"); process.exit(1); });
