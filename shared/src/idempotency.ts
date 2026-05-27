import { Redis } from "ioredis";

// SET key NX EX ttl — atomic "first time we've seen this eventId"
export class IdempotencyStore {
  constructor(private readonly redis: Redis, private readonly ttlSeconds = 60 * 60 * 24 * 7) {}

  async tryConsume(consumerName: string, eventId: string): Promise<boolean> {
    const key = `idem:${consumerName}:${eventId}`;
    const result = await this.redis.set(key, "1", "EX", this.ttlSeconds, "NX");
    return result === "OK";
  }
}
