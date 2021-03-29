"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const ioredis_1 = __importDefault(require("ioredis"));
;
class RedisHandler {
    constructor(redisConfig, logger) {
        this.connect = new Promise((resolve) => this.connectResolve = resolve);
        this.log = (content) => logger?.log(content, "redis");
        this.client = new ioredis_1.default(redisConfig);
        this.client.connect().then(() => {
            this.log("Connected.");
            if (this.connectResolve)
                this.connectResolve();
        });
    }
    /**
     * Get a REDIS hash field
     */
    getHashField(key, field) {
        // const startAt = Date.now();
        return this.client.hget(key, field).then((data) => {
            // this.log(`Hash field ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return data;
        });
    }
    /**
     * Get REDIS hash fields
     */
    getHashFields(key) {
        // const startAt = Date.now();
        return this.client.hgetall(key).then((data) => {
            // this.log(`Hash fields ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return data;
        });
    }
    /**
     * Set REDIS hash key(s)
     */
    setHash(key, data) {
        // this.log(`Caching hash ${key}`);
        const fields = Object.keys(data);
        if (fields.length > 1)
            return this.client.hmset(key, ...fields.map((field) => [field, data[field]]).flat());
        else
            return this.client.hset(key, fields[0], data[fields[0]]);
    }
    /**
     * Increment a REDIS hash
     */
    incrHashBy(key, field, num) {
        // this.log(`Incr ${key}#${field} by ${num}`);
        return this.client.hincrby(key, field, num);
    }
    getString(key, json) {
        // const startAt = Date.now();
        return this.client.get(key).then((data) => {
            // this.log(`String ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return json && data ? JSON.parse(data) : data;
        });
    }
    /**
     * Set a REDIS string key
     */
    setString(key, data) {
        // this.log(`Caching string ${key}`);
        return this.client.set(key, data);
    }
    /**
     * Get the REDIS keyspace statistics
     */
    getStats() {
        return new Promise((resolve) => {
            this.client.info("keyspace").then((data) => {
                const [, keys] = data.match(/db0:keys=([0-9]+)/) ?? [, '0'];
                resolve(keys);
            });
        });
    }
}
exports.default = RedisHandler;
