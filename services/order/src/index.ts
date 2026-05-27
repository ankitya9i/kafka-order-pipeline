import "dotenv/config";
import express from "express";
import { Redis } from "ioredis";
import { PrismaClient } from "@prisma/client";
import { v4 as uuid } from "uuid";
import {
  buildKafka, buildProducer, startConsumer, IdempotencyStore, createLogger,
  TOPICS, OrderCreatedSchema, InventoryReservedSchema, InventoryFailedSchema,
  PaymentProcessedSchema, PaymentFailedSchema, NotificationSentSchema,
} from "@pipeline/shared";

const logger = createLogger("order-service");
const prisma = new PrismaClient();
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");
const idempotency = new IdempotencyStore(redis);
const kafka = buildKafka("order-service");

const app = express();
app.use(express.json());

const start = async () => {
  const producer = await buildProducer(kafka);

  // ───────── REST: place an order ─────────
  app.post("/orders", async (req, res) => {
    const { userId, items, totalAmount, currency } = req.body;
    const orderId = uuid();

    await prisma.order.create({
      data: { id: orderId, userId, items, totalAmount, currency, status: "PENDING" },
    });

    const event = {
      eventId: uuid(),
      eventType: "OrderCreated" as const,
      occurredAt: new Date().toISOString(),
      correlationId: orderId,
      version: 1 as const,
      payload: { orderId, userId, items, totalAmount, currency },
    };

    // Partition by userId so per-user events stay ordered
    await producer.send({
      topic: TOPICS.OrdersCreated,
      messages: [{ key: userId, value: JSON.stringify(event) }],
    });

    logger.info({ orderId }, "order placed; saga started");
    res.status(202).json({ orderId, status: "PENDING" });
  });

  app.get("/orders/:id", async (req, res) => {
    const order = await prisma.order.findUnique({ where: { id: req.params.id } });
    if (!order) return res.status(404).json({ error: "not found" });
    res.json(order);
  });

  app.get("/health", (_req, res) => res.json({ ok: true, service: "order" }));

  // ───────── Saga: listen for reply events and advance order state ─────────
  await startConsumer(kafka, {
    topic: TOPICS.InventoryReserved, groupId: "order-saga-inventory-reserved",
    schema: InventoryReservedSchema, idempotency, logger, dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId, reservationId } = event.payload;
      await prisma.order.update({
        where: { id: orderId },
        data: { status: "INVENTORY_RESERVED", reservationId },
      });
      logger.info({ orderId }, "inventory reserved → awaiting payment");
    },
  });

  await startConsumer(kafka, {
    topic: TOPICS.InventoryFailed, groupId: "order-saga-inventory-failed",
    schema: InventoryFailedSchema, idempotency, logger, dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId, reason } = event.payload;
      await prisma.order.update({
        where: { id: orderId }, data: { status: "CANCELLED", failureReason: reason },
      });
      logger.warn({ orderId, reason }, "inventory failed → order cancelled");
    },
  });

  await startConsumer(kafka, {
    topic: TOPICS.PaymentProcessed, groupId: "order-saga-payment-processed",
    schema: PaymentProcessedSchema, idempotency, logger, dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId, paymentId } = event.payload;
      await prisma.order.update({ where: { id: orderId }, data: { status: "PAID", paymentId } });
      logger.info({ orderId }, "payment processed → awaiting notification");
    },
  });

  await startConsumer(kafka, {
    topic: TOPICS.PaymentFailed, groupId: "order-saga-payment-failed",
    schema: PaymentFailedSchema, idempotency, logger, dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId, reason } = event.payload;
      // TODO: compensating action — release reserved inventory
      await prisma.order.update({
        where: { id: orderId }, data: { status: "CANCELLED", failureReason: reason },
      });
      logger.warn({ orderId, reason }, "payment failed → order cancelled (compensation pending)");
    },
  });

  await startConsumer(kafka, {
    topic: TOPICS.NotificationSent, groupId: "order-saga-notification-sent",
    schema: NotificationSentSchema, idempotency, logger, dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId } = event.payload;
      await prisma.order.update({ where: { id: orderId }, data: { status: "COMPLETED" } });
      logger.info({ orderId }, "notification sent → order COMPLETED");
    },
  });

  const port = Number(process.env.ORDER_PORT ?? 3001);
  app.listen(port, () => logger.info({ port }, "order-service listening"));
};

start().catch((err) => {
  logger.error({ err }, "fatal");
  process.exit(1);
});
