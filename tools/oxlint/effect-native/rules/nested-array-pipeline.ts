import { defineRule } from "@oxlint/plugins";

import { isEffectModule } from "../effect-import.ts";

import type { ESTree, SourceCode } from "@oxlint/plugins";

/** Only the array module counts. The plugin is named for arrays and the message names `Array`
 *  functions, so accepting `effect/Record` here would make both the name and the advice wrong. */
const ARRAY_MODULE = "Array";

/** How many nested calls it takes before nesting is worse than `pipe`.
 *
 *  Two is not a defect: `Array.sort(Array.filter(xs, f), Order.String)` reads fine, and upstream's
 *  `effecttsgo/missed-pipeable-opportunity` firing at two is exactly why that rule is "off" here —
 *  it reports every `Schema.optional(Schema.Array(…))`, where the nested constructor IS the idiom.
 *  Three is where the reader has to hold two pending calls in their head to find the data, which is
 *  the thing `pipe` fixes. Deliberately not configurable: a project that disagrees turns the rule
 *  off, which is one decision rather than a dial nobody knows how to set. */
const MIN_CHAIN = 3;

/** Bound on the recursion, for the same reason `receiver.ts` has one: stopping early returns a
 *  shorter chain, and a shorter chain is the silent answer. */
const MAX_CHAIN = 16;

/** How many `Array.*` calls this node chains through its DATA position.
 *
 *  Only argument 0 is followed, because that is what makes a nest a pipeline: in
 *  `Array.appendAll(xs, Array.map(ys, f))` the inner call is a second operand, not a step the data
 *  passes through, and rewriting it as `pipe` would not remove the nesting. Following argument 0
 *  alone is also what keeps a callback body — `Array.map(xs, (x) => Array.map(x, f))` — from
 *  counting: that inner call operates on a different value. */
function chainLength(node: ESTree.Node, sourceCode: SourceCode, depth = 0): number {
    if (depth >= MAX_CHAIN) return depth;
    if (node.type !== "CallExpression") return 0;
    const callee = node.callee;
    if (callee.type !== "MemberExpression" || callee.computed) return 0;
    if (callee.property.type !== "Identifier") return 0;
    if (!isEffectModule(callee.object, ARRAY_MODULE, sourceCode)) return 0;

    const first = node.arguments[0];
    // A spread argument is not a value this can follow into.
    if (first === undefined || first.type === "SpreadElement") return 1;
    return 1 + chainLength(first, sourceCode, depth + 1);
}

/** Suggest `pipe` for deeply nested `effect/Array` calls.
 *
 *  Sibling to the other two rules by subject rather than by form: they report native methods that
 *  should be `Array` calls, and this reports what those conversions tend to produce. Data-first
 *  `Array` calls nest inside-out, so a chain of them puts the data — the thing a reader is looking
 *  for — furthest from where the expression starts.
 *
 *  Upstream's `effecttsgo/missed-pipeable-opportunity` covers this for Pipeable receivers, by
 *  suggesting the `.pipe()` METHOD. A plain array is not Pipeable and has no `.pipe`, so that rule
 *  is structurally silent here and the only available form is standalone `pipe` from
 *  `effect/Function`. That gap is why this rule exists; `docs/features/effect-native.md` says so. */
export const nestedArrayPipelineRule = defineRule({
    meta: {
        type: "suggestion",
        docs: {
            description: "Suggest `pipe` where `effect/Array` calls are nested deeply enough to read inside-out.",
        },
        messages: {
            nestedArrayPipeline:
                "This nests {{length}} `Array` calls, so the data is innermost and the expression reads inside-out. " +
                "`pipe` from `effect/Function` reads in the order the steps run: `pipe(data, Array.f(…), Array.g(…))`.",
        },
    },
    createOnce(context) {
        // Inner links of a chain already reported at its outermost call. ESTree traversal reaches a
        // CallExpression before its arguments, so the outermost is always seen first, and one nest
        // produces one report rather than one per level.
        const covered = new WeakSet<ESTree.Node>();

        return {
            CallExpression(node: ESTree.CallExpression) {
                if (covered.has(node)) return;

                const length = chainLength(node, context.sourceCode);
                if (length < MIN_CHAIN) return;

                let inner: ESTree.Node | undefined = node.arguments[0];
                for (let step = 1; step < length && inner !== undefined; step += 1) {
                    covered.add(inner);
                    inner = inner.type === "CallExpression" ? inner.arguments[0] : undefined;
                }

                context.report({
                    node: node.callee.type === "MemberExpression" ? node.callee.property : node,
                    messageId: "nestedArrayPipeline",
                    data: { length: String(length) },
                });
            },
        };
    },
});
