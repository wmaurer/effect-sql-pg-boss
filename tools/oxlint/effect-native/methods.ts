/** What a native array method is called in Effect's `Array` module, and what changes when you switch.
 *
 *  Split into two tiers in the spec by the COST of the fix, not by anything the rule does. Tier A is
 *  a rename with the receiver moved to the first argument. Tier B changes the return type, so it
 *  edits the caller too. The rule treats them identically; `note` is what carries the difference to
 *  the person reading the message.
 *
 *  `.slice` is deliberately absent. Effect has no `slice`, so there is no single name to suggest —
 *  the fix is one of `take`, `drop` or `takeRight` depending on the arguments — and `.slice` on a
 *  string is at least as common as on an array. */
export interface Replacement {
    /** The `Array` export to use instead. */
    readonly effect: string;
    /** A sentence appended to the message when switching is not a straight rename. */
    readonly note?: string;
}

export const REPLACEMENTS: ReadonlyMap<string, Replacement> = new Map([
    // Tier A — the receiver becomes the first argument and nothing else changes.
    ["map", { effect: "map" }],
    ["filter", { effect: "filter" }],
    ["flatMap", { effect: "flatMap" }],
    ["some", { effect: "some" }],
    ["every", { effect: "every" }],
    ["join", { effect: "join" }],
    ["forEach", { effect: "forEach" }],
    [
        "flat",
        {
            effect: "flatten",
            note: "`Array.flatten` only flattens one level, same as `.flat()` with no argument — `.flat(2)` or deeper has no single-call Effect replacement.",
        },
    ],
    // The spec filed this under Tier C, "in-place mutation". It is not: `.concat` returns a new
    // array and leaves its receiver alone, which makes it a Tier A rename like the rest of this
    // block. Kept here rather than in native-array-mutation so that rule's message can say
    // "mutates in place" and be true of every method it reports.
    ["concat", { effect: "appendAll" }],
    ["reduce", { effect: "reduce", note: "The argument order differs: `Array.reduce(xs, initial, f)`." }],
    [
        "reverse",
        {
            effect: "reverse",
            note: "`.reverse()` also mutates its receiver in place; `Array.reverse` returns a new array.",
        },
    ],

    // Tier B — the return type changes, so the caller changes too.
    ["find", { effect: "findFirst", note: "`Array.findFirst` returns an `Option<T>` rather than `T | undefined`." }],
    [
        "findIndex",
        { effect: "findFirstIndex", note: "`Array.findFirstIndex` returns an `Option<number>` rather than `-1`." },
    ],
    [
        "indexOf",
        {
            effect: "findFirstIndex",
            note: "`Array.findFirstIndex` takes a predicate and returns an `Option<number>` rather than `-1`.",
        },
    ],
    ["includes", { effect: "contains" }],
    [
        "sort",
        {
            effect: "sort",
            note: "`Array.sort` takes an `Order` (`Order.String`, `Order.Number`) rather than a `(a, b) => number` comparator, and does not mutate its receiver. Sorting by a derived key is `Array.sortWith(xs, key, order)`.",
        },
    ],
]);
