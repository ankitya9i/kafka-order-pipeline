import { Kafka } from "kafkajs";

const kafka = new Kafka({ clientId: "topic-bootstrap", brokers: [process.env.KAFKA_BROKERS ?? "localhost:9092"] });
const admin = kafka.admin();

const TOPICS = [
  { topic: "orders.created", numPartitions: 3 },
  { topic: "inventory.reserved", numPartitions: 3 },
  { topic: "inventory.failed", numPartitions: 3 },
  { topic: "payment.processed", numPartitions: 3 },
  { topic: "payment.failed", numPartitions: 3 },
  { topic: "notification.sent", numPartitions: 3 },
  { topic: "orders.dlq", numPartitions: 1 },
];

await admin.connect();
const existing = new Set(await admin.listTopics());
const toCreate = TOPICS.filter(t => !existing.has(t.topic));
if (toCreate.length === 0) {
  console.log("All topics already exist.");
} else {
  await admin.createTopics({ topics: toCreate });
  console.log("Created topics:", toCreate.map(t => t.topic).join(", "));
}
await admin.disconnect();
