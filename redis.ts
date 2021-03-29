import Redis, { RedisOptions } from 'ioredis';
import Logger from './logger';

interface UnknownData {
    [key: string]: any;
};

export default class RedisHandler {

    public connect: Promise<void>;
    public connectResolve?: Function;

    public log: (message: string) => void;
    public client: Redis.Redis;

    constructor (redisConfig: RedisOptions, logger?: Logger) {

        this.connect = new Promise((resolve) => this.connectResolve = resolve);
        this.log = (content) => logger?.log(content, "redis");

        this.client = new Redis(redisConfig);
        this.client.connect().then(() => {
            this.log("Connected.");
            if (this.connectResolve) this.connectResolve();
        });

    }

    /**
     * Get a REDIS hash field
     */
    getHashField (key: string, field: string) {
        // const startAt = Date.now();
        return this.client.hget(key, field).then((data) => {
            // this.log(`Hash field ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return data;
        });
    }

    /**
     * Get REDIS hash fields
     */
    getHashFields (key: string) {
        // const startAt = Date.now();
        return this.client.hgetall(key).then((data) => {
            // this.log(`Hash fields ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return data;
        });
    }

    /**
     * Set REDIS hash key(s)
     */
    setHash (key: string, data: UnknownData) {
        // this.log(`Caching hash ${key}`);
        const fields = Object.keys(data);
        if (fields.length > 1) return this.client.hmset(key, ...fields.map((field) => [ field, data[field] ]).flat());
        else return this.client.hset(key, fields[0], data[fields[0]]);
    }

    /**
     * Increment a REDIS hash
     */
    incrHashBy (key: string, field: string, num: number) {
        // this.log(`Incr ${key}#${field} by ${num}`);
        return this.client.hincrby(key, field, num);
    }

    /**
     * Get a REDIS string key
     */
    getString (key: string, json?: false): Promise<string>
    getString (key: string, json?: true): Promise<Object>
    getString (key: string, json?: boolean): Promise<string> {
        // const startAt = Date.now();
        return this.client.get(key).then((data) => {
            // this.log(`String ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return json && data ? JSON.parse(data) : data;
        });
    }

    /**
     * Set a REDIS string key
     */
    setString (key: string, data: string) {
        // this.log(`Caching string ${key}`);
        return this.client.set(key, data);
    }

    /**
     * Get the REDIS keyspace statistics
     */
    getStats (): Promise<string> {
        return new Promise((resolve) => {
            this.client.info("keyspace").then((data) => {
                const [,keys] = data.match(/db0:keys=([0-9]+)/) ?? [, '0'];
                resolve(keys!);
            });
        });
    }

}
