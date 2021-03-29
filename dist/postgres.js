"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const pg_1 = require("pg");
class PostgresHandler {
    constructor(postgresConfig, logger) {
        this.connect = new Promise((resolve) => this.connectResolve = resolve);
        this.connected = false;
        this.log = (content) => logger?.log(content, "postgres");
        this.client = new pg_1.Pool(postgresConfig);
        this.client.on("connect", () => {
            if (!this.connected) {
                this.connected = true;
                this.log("Connected.");
                if (this.connectResolve)
                    this.connectResolve();
            }
        });
        this.client.connect();
    }
    /**
     * Query the POSTGRES database
     */
    query(query, ...args) {
        return new Promise((resolve, reject) => {
            // const startAt = Date.now();
            this.client.query(query, args, (error, results) => {
                // this.log(`Query run in ${parseInt(Date.now() - startAt)}ms`);
                if (error)
                    reject(error);
                else
                    resolve(results);
            });
        });
    }
}
exports.default = PostgresHandler;
;
