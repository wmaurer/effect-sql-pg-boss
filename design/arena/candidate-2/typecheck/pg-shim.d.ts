// Stand-in for @types/pg (not installed in the sandbox). The real package depends on @types/pg.
declare module "pg" {
    export interface QueryResult<R = any> {
        rows: Array<R>;
        rowCount: number | null;
    }
    export interface QueryConfig {
        text: string;
        values?: Array<unknown>;
        rowMode?: "array";
    }
    export interface PoolClient {
        query<R = any>(text: string, values?: Array<unknown>): Promise<QueryResult<R>>;
        query<R = any>(config: QueryConfig): Promise<QueryResult<R>>;
        release(destroy?: boolean | Error): void;
    }
    export interface PoolConfig {
        connectionString?: string;
        max?: number;
        idleTimeoutMillis?: number;
        connectionTimeoutMillis?: number;
        application_name?: string;
        ssl?: any;
        types?: unknown;
    }
    export class Pool {
        constructor(config?: PoolConfig);
        readonly options: PoolConfig;
        connect(): Promise<PoolClient>;
        query<R = any>(text: string, values?: Array<unknown>): Promise<QueryResult<R>>;
        query<R = any>(config: QueryConfig): Promise<QueryResult<R>>;
        on(event: "error", listener: (err: Error) => void): this;
        end(): Promise<void>;
    }
    const pg: { Pool: typeof Pool };
    export default pg;
}
