// Typecheck-only shim for the slice of `pg` this sketch touches. The real
// package depends on `@types/pg`; it is not installed in the sketch sandbox.
declare module "pg" {
    export interface QueryResult<R = any> {
        readonly rows: Array<R>;
        readonly rowCount: number | null;
        readonly command: string;
        readonly fields: ReadonlyArray<{ readonly name: string; readonly dataTypeID: number }>;
    }
    export interface QueryConfig {
        readonly text: string;
        readonly values?: ReadonlyArray<unknown> | undefined;
        readonly rowMode?: "array" | undefined;
    }
    export interface ClientBase {
        query<R = any>(text: string, values?: ReadonlyArray<unknown>): Promise<QueryResult<R>>;
        query<R = any>(config: QueryConfig): Promise<QueryResult<R>>;
    }
    export interface PoolClient extends ClientBase {
        release(destroy?: boolean | Error): void;
    }
    export interface PoolConfig {
        connectionString?: string | undefined;
        host?: string | undefined;
        port?: number | undefined;
        database?: string | undefined;
        user?: string | undefined;
        password?: string | undefined;
        ssl?: unknown;
        max?: number | undefined;
        idleTimeoutMillis?: number | undefined;
        connectionTimeoutMillis?: number | undefined;
        application_name?: string | undefined;
    }
    export class Pool implements ClientBase {
        constructor(config?: PoolConfig);
        readonly totalCount: number;
        readonly idleCount: number;
        readonly waitingCount: number;
        readonly options: { readonly max: number };
        connect(): Promise<PoolClient>;
        query<R = any>(text: string, values?: ReadonlyArray<unknown>): Promise<QueryResult<R>>;
        query<R = any>(config: QueryConfig): Promise<QueryResult<R>>;
        end(): Promise<void>;
    }
    export class DatabaseError extends Error {
        readonly code: string | undefined;
        readonly constraint: string | undefined;
        readonly detail: string | undefined;
    }
}
