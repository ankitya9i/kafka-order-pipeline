import pino from "pino";

export const createLogger = (service: string) =>
  pino({
    name: service,
    level: process.env.LOG_LEVEL ?? "info",
    transport: process.env.NODE_ENV === "production" ? undefined : { target: "pino-pretty" },
  });
