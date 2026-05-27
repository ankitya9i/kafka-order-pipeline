import { z } from "zod";

export const TOPICS = {
  OrdersCreated: "orders.created",
  InventoryReserved: "inventory.reserved",
  InventoryFailed: "inventory.failed",
  PaymentProcessed: "payment.processed",
  PaymentFailed: "payment.failed",
  NotificationSent: "notification.sent",
  OrdersDLQ: "orders.dlq",
} as const;

export const EnvelopeSchema = z.object({
  eventId: z.string().uuid(),
  eventType: z.string(),
  occurredAt: z.string().datetime(),
  correlationId: z.string().uuid(),
  version: z.literal(1),
});

export const OrderCreatedSchema = EnvelopeSchema.extend({
  eventType: z.literal("OrderCreated"),
  payload: z.object({
    orderId: z.string().uuid(),
    userId: z.string().uuid(),
    items: z.array(z.object({ sku: z.string(), qty: z.number().int().positive() })).min(1),
    totalAmount: z.number().positive(),
    currency: z.string().length(3),
  }),
});

export const InventoryReservedSchema = EnvelopeSchema.extend({
  eventType: z.literal("InventoryReserved"),
  payload: z.object({ orderId: z.string().uuid(), reservationId: z.string().uuid() }),
});

export const InventoryFailedSchema = EnvelopeSchema.extend({
  eventType: z.literal("InventoryFailed"),
  payload: z.object({ orderId: z.string().uuid(), reason: z.string() }),
});

export const PaymentProcessedSchema = EnvelopeSchema.extend({
  eventType: z.literal("PaymentProcessed"),
  payload: z.object({ orderId: z.string().uuid(), paymentId: z.string().uuid(), amount: z.number() }),
});

export const PaymentFailedSchema = EnvelopeSchema.extend({
  eventType: z.literal("PaymentFailed"),
  payload: z.object({ orderId: z.string().uuid(), reason: z.string() }),
});

export const NotificationSentSchema = EnvelopeSchema.extend({
  eventType: z.literal("NotificationSent"),
  payload: z.object({ orderId: z.string().uuid(), channel: z.enum(["email", "sms"]), recipient: z.string() }),
});

export type OrderCreated = z.infer<typeof OrderCreatedSchema>;
export type InventoryReserved = z.infer<typeof InventoryReservedSchema>;
export type InventoryFailed = z.infer<typeof InventoryFailedSchema>;
export type PaymentProcessed = z.infer<typeof PaymentProcessedSchema>;
export type PaymentFailed = z.infer<typeof PaymentFailedSchema>;
export type NotificationSent = z.infer<typeof NotificationSentSchema>;
