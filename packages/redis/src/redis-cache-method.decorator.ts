import { createHash } from "crypto";
import Debug from "debug";
import superjson from "superjson";

import { redis } from "./redis";

export interface RedisCacheMethodDecoratorOptions {
  enabled?: (self: object) => boolean;
  expire?: number; //cache expire in seconds, defaults to 60 seconds
  allowUndefined?: boolean; //allow to store undefined value, default is false
}

const debug = Debug("cbts:redis-cache-method.decorator.ts");

export function RedisCacheMethod(options: RedisCacheMethodDecoratorOptions = {}): MethodDecorator {
  return function (target: object, propertyName: string | symbol, descriptor: PropertyDescriptor): void {
    const { allowUndefined, enabled, expire = 60 } = options;
    if (typeof enabled === "function" && !enabled(target as never)) {
      return;
    }

    const original = descriptor.value;
    const className = target.constructor.name;

    descriptor.value = async function (this: never, ...args: never[]): Promise<unknown> {
      const hash = createHash("md5").update(JSON.stringify(args)).digest("hex");
      const cacheKey = `${className}::${propertyName.toString()}(${hash})`;
      const cachedData = await redis.get(cacheKey);
      if (cachedData) {
        // debug('return from cache %s', cacheKey);
        return superjson.parse(cachedData);
      }
      const result = await original.call(this, ...args);
      if ((result !== undefined && result !== null) || allowUndefined) {
        debug("write to cache %s", cacheKey);
        await redis.set(cacheKey, superjson.stringify(result), "EX", expire);
      }
      return result;
    };
  };
}
