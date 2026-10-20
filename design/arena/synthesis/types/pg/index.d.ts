// Typecheck-only shim for the slice of `pg` the housekeeping pool uses. The
// published package depends on `@types/pg`, which this sandbox lacks.
declare module "pg" {
    export interface QueryResult<R = any> {
        readonly rows: Array<R>;
        readonly rowCount: number | null;
        readonly command: string;
    }
    export interface PoolClient {
        on(event: "error", listener: (error: Error) => void): this;
        release(destroy?: boolean | Error): void;
    }
    export interface PoolConfig {
        connectionString?: string | undefined;
        host?: string | undefined;
        port?: number | undefined;
        database?: string | undefined;
        user?: string | undefined;
        password?: string | (() => string | Promise<string>) | undefined;
        ssl?: unknown;
        max?: number | undefined;
        application_name?: string | undefined;
        connectionTimeoutMillis?: number | undefined;
        options?: string | undefined;
    }
    export class Pool {
        constructor(config?: PoolConfig);
        readonly totalCount: number;
        readonly idleCount: number;
        readonly waitingCount: number;
        on(event: "error", listener: (error: Error, client: PoolClient) => void): this;
        on(event: "connect", listener: (client: PoolClient) => void): this;
        query(text: string, values?: ReadonlyArray<unknown>): Promise<QueryResult | Array<QueryResult>>;
        end(): Promise<void>;
    }
    const pg: { readonly Pool: typeof Pool };
    export default pg;
}
