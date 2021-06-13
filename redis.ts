import Redis, { RedisOptions, ValueType } from 'ioredis'
import { LogFunction } from './log'

export default class RedisHandler {

    public connect: Promise<void>
    public connectResolve?: () => void

    public log: (message: string) => void
    public client: Redis.Redis

    constructor (redisConfig: RedisOptions, log?: LogFunction) {

        this.connect = new Promise((resolve) => this.connectResolve = resolve)
        this.log = (content) => {
            if (log) log(content, 'redis')
        }

        this.client = new Redis(redisConfig)
        this.client.on('connect', () => {
            this.log("Connected.")
            if (this.connectResolve) this.connectResolve()
        })

    }

    /**
     * Get a REDIS set field
     */
    getSet (key: string): Promise<string[]> {
        return this.client.smembers(key)
    }

    /**
     * Add a value to a REDIS set
     */
    async addSet (key: string, value: string[]|string): Promise<void> {
        await this.client.sadd(key, value)
    }

    /**
     * Get a REDIS hash field
     */
    getHashField (key: string, field: string): Promise<string|null> {
        // const startAt = Date.now();
        return this.client.hget(key, field).then((data) => {
            // this.log(`Hash field ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return data
        })
    }

    /**
     * Get REDIS hash fields
     */
    getHashFields (key: string): Promise<Record<string, string>|null> {
        // const startAt = Date.now();
        return this.client.hgetall(key).then((data) => {
            // this.log(`Hash fields ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return data
        })
    }

    /**
     * Set REDIS hash key(s)
     */
    async setHash (key: string, data: Record<string, unknown>): Promise<void> {
        // this.log(`Caching hash ${key}`);
        const fields = Object.keys(data)
        if (fields.length > 1) await this.client.hmset(key, ...fields.map((field) => [ field, data[field] as ValueType ]).flat())
        else await this.client.hset(key, fields[0], data[fields[0]] as ValueType)
    }

    /**
     * Increment a REDIS hash
     */
    async incrHashBy (key: string, field: string, num: number): Promise<void> {
        // this.log(`Incr ${key}#${field} by ${num}`);
        await this.client.hincrby(key, field, num)
    }

    /**
     * Get a REDIS string key
     */
    getString (key: string, json?: false): Promise<string>
    getString (key: string, json?: true): Promise<unknown>
    getString (key: string, json?: boolean): Promise<string> {
        // const startAt = Date.now();
        return this.client.get(key).then((data) => {
            // this.log(`String ${key} retrieved in ${parseInt(Date.now() - startAt)}ms`);
            return json && data ? JSON.parse(data) : data
        })
    }

    /**
     * Set a REDIS string key
     */
    async setString (key: string, data: string): Promise<void> {
        // this.log(`Caching string ${key}`);
        await this.client.set(key, data)
    }

    /**
     * Get the REDIS keyspace statistics
     */
    getStats (): Promise<string> {
        return new Promise((resolve) => {
            this.client.info("keyspace").then((data) => {
                const [,keys] = data.match(/db0:keys=([0-9]+)/) ?? [null, '0']
                resolve(keys as string)
            })
        })
    }

    /**
     * Delete a key from REDIS
     */
    delete (key: string): Promise<number> {
        return this.client.del(key)
    }

}
