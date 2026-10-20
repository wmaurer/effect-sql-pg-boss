import type { SendOptions } from "./Queue.ts";
import type { Queue } from "./Queue.ts";
/**
 * Job graphs for `Boss.flow`. A node depends on other nodes by value, not by
 * name, so a flow cannot reference a missing parent, and it cannot contain a
 * cycle because a node can only be built from nodes that already exist.
 *
 * Invariants:
 * - `ref` is the key of the job's id in the result of `Boss.flow`. Two
 *   different nodes in one flow may not share a ref (checked when the flow is
 *   sent; it fails with `BossError` of kind `InvalidFlow` before any SQL runs).
 * - The same node reached through two paths is sent once.
 *
 * @since 0.1.0
 */
import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";

/** @since 0.1.0 */
export const TypeId: TypeId = "~@wmaurer/effect-sql-pg-boss/FlowNode";

/** @since 0.1.0 */
export type TypeId = "~@wmaurer/effect-sql-pg-boss/FlowNode";

/**
 * `Ref` is this node's name. `Refs` is every name in the graph below and
 * including it, which is what `Boss.flow` returns ids for.
 *
 * @since 0.1.0
 */
export interface FlowNode<
    out Ref extends string,
    out Refs extends string = Ref,
    out QueueNames extends string = string,
> {
    readonly [TypeId]: TypeId;
    readonly ref: Ref;
    /** Phantom: the names in this node's graph. Not present at runtime. */
    readonly _Refs: () => Refs;
    /** Phantom: the queue names used in this node's graph. Not present at runtime. */
    readonly _Queues: () => QueueNames;
    readonly queue: Queue<string, any>;
    readonly dependsOn: ReadonlyArray<FlowNode<string, string, string>>;
    readonly options: SendOptions | undefined;
    /** Encodes the payload when the flow is sent. Built by {@link node}, which is where payload and queue are tied together. */
    readonly encode: Effect.Effect<object, Schema.SchemaError>;
}

/** @since 0.1.0 */
export type RefsOf<N> = N extends FlowNode<string, infer R, string> ? R : never;

/** @since 0.1.0 */
export type QueueNamesOf<N> = N extends FlowNode<string, string, infer Q> ? Q : never;

/**
 * Creates a node. A node starts only after every node in `dependsOn` has
 * completed.
 *
 * @example
 * const charge = Flow.node("charge", ChargeCard, { orderId })
 * const receipt = Flow.node("receipt", SendReceipt, { orderId }, { dependsOn: [charge] })
 *
 * @since 0.1.0
 */
export const node = <
    const Ref extends string,
    Name extends string,
    A extends object,
    const Deps extends ReadonlyArray<FlowNode<string, string, string>> = readonly [],
>(
    ref: Ref,
    queue: Queue<Name, A>,
    data: A,
    options?: SendOptions & { readonly dependsOn?: Deps },
): FlowNode<Ref, Ref | RefsOf<Deps[number]>, Name | QueueNamesOf<Deps[number]>> => {
    // TODO: encode = Schema.encodeEffect(queue.codec)(data), strip dependsOn from options
    void ref;
    void queue;
    void data;
    void options;
    throw new Error("not implemented");
};

/**
 * All nodes reachable from `leaves`, parents before children, each once.
 * Pure. `Boss.flow` calls it and then maps nodes to pg-boss' flow rows.
 *
 * @since 0.1.0
 */
export const collect = (
    _leaves: ReadonlyArray<FlowNode<string, string, string>>,
): ReadonlyArray<FlowNode<string, string, string>> => {
    // TODO: depth-first, memoise by node identity, then check refs are unique
    throw new Error("not implemented");
};
