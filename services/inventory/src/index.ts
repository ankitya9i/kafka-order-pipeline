import "dotenv/config";
import express from "express";
import { Redis } from "ioredis";
import { PrismaClient } from "../prisma/generated/index.js";
import { v4 as uuid } from "uuid";
import {
  buildKafka, buildProducer, startConsumer, IdempotencyStore, createLogger,
  TOPICS, OrderCreatedSchema,
} from "@pipeline/shared";

const logger = createLogger("inventory-service");
const prisma = new PrismaClient();
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");
const idempotency = new IdempotencyStore(redis);
const kafka = buildKafka("inventory-service");

const app = express();
app.get("/health", (_req, res) => res.json({ ok: true, service: "inventory" }));

const reserve = async (orderId: string, items: { sku: string; qty: number }[]) =>
  prisma.$transaction(async (tx) => {
    for (const it of items) {
      const stock = await tx.stock.findUnique({ where: { sku: it.sku } });
      if (!stock || stock.available < it.qty) {
        throw new Error(`insufficient stock for ${it.sku}`);
      }
    }
    for (const it of items) {
      await tx.stock.update({ where: { sku: it.sku }, data: { available: { decrement: it.qty } } });
    }
    return tx.reservation.create({ data: { orderId, items: items as object } });
  });

const start = async () => {
  const producer = await buildProducer(kafka);

  await startConsumer(kafka, {
    topic: TOPICS.OrdersCreated,
    groupId: "inventory-service",
    schema: OrderCreatedSchema,
    idempotency,
    logger,
    dlqProducer: producer,
    handler: async ({ event }) => {
      const { orderId, items } = event.payload;
      try {
        const reservation = await reserve(orderId, items);
        await producer.send({
          topic: TOPICS.InventoryReserved,
          messages: [{
            key: orderId,
            value: JSON.stringify({
              eventId: uuid(),
              eventType: "InventoryReserved",
              occurredAt: new Date().toISOString(),
              correlationId: event.correlationId,
              version: 1,
              payload: { orderId, reservationId: reservation.id },
            }),
          }],
        });
        logger.info({ orderId, reservationId: reservation.id }, "reserved");
      } catch (err) {
        const reason = (err as Error).message;
        await producer.send({
          topic: TOPICS.InventoryFailed,
          messages: [{
            key: orderId,
            value: JSON.stringify({
              eventId: uuid(),
              eventType: "InventoryFailed",
              occurredAt: new Date().toISOString(),
              correlationId: event.correlationId,
              version: 1,
              payload: { orderId, reason },
            }),
          }],
        });
        logger.warn({ orderId, reason }, "reservation failed");
      }
    },
  });

  const port = Number(process.env.INVENTORY_PORT ?? 3003);
  app.listen(port, () => logger.info({ port }, "inventory-service listening"));
};

start().catch((err) => { logger.error({ err }, "fatal"); process.exit(1); });
