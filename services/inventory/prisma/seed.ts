import { PrismaClient } from "./generated/index.js";
const prisma = new PrismaClient();

const SKUS = [
  { sku: "SKU-1", available: 100 },
  { sku: "SKU-2", available: 50 },
  { sku: "SKU-3", available: 0 }, // intentionally out of stock to exercise failure path
];

for (const s of SKUS) {
  await prisma.stock.upsert({ where: { sku: s.sku }, create: s, update: { available: s.available } });
}
console.log("seeded stock");
await prisma.$disconnect();
