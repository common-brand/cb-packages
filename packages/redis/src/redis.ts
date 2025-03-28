import Debug from "debug";
import Redis, { RedisOptions } from "ioredis";
import * as Joi from "joi";

const debug = Debug("cbts:redis.ts");

const SENTINEL_HOST = process.env["REDIS_SENTINEL_HOST"] || "";
const SENTINEL_PORT = Joi.attempt(process.env["REDIS_SENTINEL_PORT"], Joi.number());
const NAME = Joi.attempt(process.env["REDIS_MASTER_GROUP"], Joi.string().default("mymaster"));
const REDIS_PASSWORD = Joi.attempt(process.env["REDIS_PASSWORD"], Joi.string().default(""));
const REDIS_DB = Joi.attempt(process.env["REDIS_DB"], Joi.number().default(0));
const SENTINEL_PASSWORD = process.env["REDIS_SENTINEL_PASSWORD"];

export const redisOptions: RedisOptions = SENTINEL_PASSWORD
  ? {
      sentinels: [
        {
          host: SENTINEL_HOST,
          port: SENTINEL_PORT,
        },
      ],
      name: NAME,
      db: REDIS_DB,
      sentinelPassword: SENTINEL_PASSWORD,
      password: REDIS_PASSWORD,
      maxRetriesPerRequest: null,
    }
  : {
      host: SENTINEL_HOST,
      port: SENTINEL_PORT,
      db: REDIS_DB,
      password: REDIS_PASSWORD,
      maxRetriesPerRequest: null,
    };
debug("redisOptions=%j", redisOptions);

export const connection = new Redis(redisOptions);

// connection.on("ready", () => debug("redis connection is ready %j", redisOptions));
connection.on("error", (err) => {
  debug("Redis connection error:%j", err.message);

  // For critical errors, you might want to terminate the process
  if (err.message.includes("ECONNREFUSED") || err.message.includes("Authentication failed")) {
    console.error("Critical Redis connection error, terminating process:", err.message);
    process.exit(1);
  }
});
