# Kafka Order Pipeline

An event-driven order processing pipeline built on **Node.js + TypeScript + Kafka + Postgres + Redis**. Demonstrates production patterns that are bread-and-butter for fintech and consumer-tech backends: the **saga pattern**, **idempotent consumers**, **retries with exponential backoff**, **dead-letter queues**, and **partitioned per-user ordering**.

This repo is a learning project intended to be a defensible, end-to-end example of "how a real distributed system fits together" — small enough to read in one sitting, but realistic enough to talk through in a system-design interview.

## Architecture

```
                                             ┌─────────────────────┐
                                             │   Inventory Service │ → inventory.reserved / inventory.failed
                                             ├─────────────────────┤
 Order Service ──[orders.created]──────────▶ │   Payment Service   │ → payment.processed / payment.failed
       ▲                                     ├─────────────────────┤
       │ saga orchestration                  │ Notification Service│ → notification.sent
       │ (listens to all reply topics,       └─────────────────────┘
       │  updates Order state)                              │
       │                                                    │
       └────────────────────────────────────────────────────┘
                                                            │ on any handler failure after retries
                                                            ▼
                                                  ┌──────────────────┐
                                                  │   orders.dlq     │  separate consumer for replay
                                                  └──────────────────┘
```

Each downstream service:
- Consumes `orders.created` (partitioned by `userId`).
- Validates the message against a **Zod schema**; bad messages go to DLQ.
- Performs an **idempotency check** using Redis (`SET key NX EX`) keyed by `(consumerGroup, eventId)`.
- On handler error, retries with **exponential backoff** (3 attempts: ~1s, 2s, 4s). On terminal failure, the message goes to the DLQ.
- Emits a **reply event** on its result topic.

The Order Service is the **saga orchestrator**: it owns the order state machine and advances it as reply events arrive.

```
PENDING ──(inventory.reserved)──▶ INVENTORY_RESERVED ──(payment.processed)──▶ PAID ──(notification.sent)──▶ COMPLETED
   │                                       │                                      │
   └──(inventory.failed)──▶ CANCELLED      └──(payment.failed)──▶ CANCELLED       (compensation: release inventory — TODO)
```

## Key Patterns Demonstrated

| Pattern | Where to look |
|---|---|
| **Saga (orchestration)** | `services/order/src/index.ts` — all the reply-topic consumers |
| **Event envelope + schema validation** | `shared/src/schemas.ts` — Zod schemas with `eventId`, `correlationId`, `version` |
| **Idempotent consumers** | `shared/src/idempotency.ts` — Redis `SET NX EX` |
| **Retries with backoff** | `shared/src/kafka.ts` — `startConsumer` retry loop |
| **Dead-letter queue** | `shared/src/kafka.ts` — `sendToDLQ` + `orders.dlq` topic |
| **Partitioning by user** | `services/order/src/index.ts` — `messages: [{ key: userId, ... }]` |
| **Per-event consumer groups** | Each saga step is its own consumer group so failures are isolated |
| **Idempotent producer** | `shared/src/kafka.ts` — `kafka.producer({ idempotent: true })` |

## Tech Stack

- **Node.js** + **TypeScript** (ESM, npm workspaces)
- **Kafka** (Confluent images via Docker Compose) + **kafkajs**
- **Postgres** + **Prisma** (order state)
- **Redis** + **ioredis** (idempotency store)
- **Express** (per-service HTTP layer)
- **Zod** (event schema validation)
- **Pino** (structured logging)

## Getting Started

### 1. Boot the infra

```bash
docker compose up -d
# Kafka UI available at http://localhost:8080
```

### 2. Install deps + create topics

```bash
npm install
npm run topics:create
```

### 3. Run database migrations

```bash
npm run db:migrate
```

### 4. Start all services

```bash
npm run dev:all
```

Or start individually in separate terminals: `npm run dev:order`, `npm run dev:payment`, etc.

### 5. Place an order

```bash
curl -X POST http://localhost:3001/orders \
  -H "content-type: application/json" \
  -d '{
    "userId": "11111111-1111-1111-1111-111111111111",
    "items": [{"sku": "SKU-1", "qty": 2}],
    "totalAmount": 1499.00,
    "currency": "INR"
  }'
```

Then watch the order advance through saga states:

```bash
curl http://localhost:3001/orders/<orderId>
```

You'll see status progress: `PENDING → INVENTORY_RESERVED → PAID → COMPLETED`.

### 6. Watch events in Kafka UI

Open http://localhost:8080 and explore topics, partitions, consumer group lag, and DLQ contents.

## Project Layout

```
kafka-order-pipeline/
├── docker-compose.yml          # Kafka, ZK, Postgres, Redis, Kafka UI
├── scripts/
│   └── create-topics.mjs       # idempotent topic bootstrapper
├── shared/                     # @pipeline/shared — schemas, kafka helpers, idempotency
│   └── src/
│       ├── schemas.ts          # Zod event schemas + topic constants
│       ├── kafka.ts            # startConsumer with retries + DLQ
│       ├── idempotency.ts      # Redis-backed idempotency store
│       └── logger.ts
├── services/
│   ├── order/                  # saga orchestrator + REST API
│   ├── payment/                # consumes orders.created → emits payment.*
│   ├── inventory/              # consumes orders.created → emits inventory.*
│   └── notification/           # consumes orders.created → emits notification.sent
```

## Interview Talking Points

- **Why partition by `userId`?** Per-user event order is preserved without sacrificing parallelism across users.
- **Why one consumer group per saga step?** Failures in one step (e.g., payment) don't block another (e.g., inventory). Independent lag, independent retries.
- **Why Zod at the consumer boundary?** Defense against schema drift, malformed producers, and accidental contract breaks — schema-invalid messages go straight to DLQ instead of crash-looping.
- **Why `SET NX EX` for idempotency?** Atomic check-and-set. The 7-day TTL is a tradeoff between dedup window and Redis memory.
- **Why an idempotent producer?** Protects against duplicate publishes on retries (e.g., broker timeout where the message actually got through).
- **What's still missing for prod?** Outbox pattern on the producer side (so DB writes + event publishes are atomic), compensating actions on payment failure (release inventory), schema registry, OpenTelemetry tracing across the saga.

## Status

Personal learning project. Not intended for production use as-is — designed to be read, modified, and extended.

## License

MIT
