import { Pool, PoolConfig, QueryResult } from 'pg';
import Logger from './logger';

export default class PostgresHandler {

    public connect: Promise<void>;
    public connectResolve?: Function;

    public client: Pool;
    public connected: boolean;
    public log: (message: string) => void;

    constructor (postgresConfig: PoolConfig, logger?: Logger) {

        this.connect = new Promise((resolve) => this.connectResolve = resolve);
        this.connected = false;
        this.log = (content) => logger?.log(content, "postgres");

        this.client = new Pool(postgresConfig);
        this.client.on("connect", () => {
            if (!this.connected) {
                this.connected = true;
                this.log("Connected.");
                if (this.connectResolve) this.connectResolve();
            }
        });
        this.client.connect();

    }

    /**
     * Query the POSTGRES database
     */
    query (query: string, ...args: any[]): Promise<QueryResult<any>> {
        return new Promise((resolve, reject) => {
            // const startAt = Date.now();
            this.client.query(query, args, (error, results) => {
                // this.log(`Query run in ${parseInt(Date.now() - startAt)}ms`);
                if (error) reject(error);
                else resolve(results);
            });
        });
    }

};
