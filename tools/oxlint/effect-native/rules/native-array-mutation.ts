import { defineRule } from "@oxlint/plugins";

import { MUTATORS } from "../mutators.ts";
import { isProvenArray } from "../receiver.ts";

import type { ESTree } from "@oxlint/plugins";

/** Disallow in-place mutation of an array, where the receiver can be proven to be one without types.
 *
 *  Separate from `native-array-method` because the argument is different in kind. That rule is a
 *  rename: the fix is local and mechanical. This one is not — Effect's array module is immutable, so
 *  every fix restructures the code around the call. That is a design decision a project should make
 *  deliberately, which is why it gets its own rule and can be turned off on its own.
 *
 *  Like every rule in this plugin it reports only a provable receiver; see `receiver.ts`. */
export const nativeArrayMutationRule = defineRule({
    meta: {
        type: "suggestion",
        docs: {
            description: "Disallow methods that mutate an array in place, where the receiver is provably an array.",
        },
        messages: {
            nativeArrayMutation:
                "`.{{native}}` mutates the array in place. Effect's `Array` module is immutable and has no equivalent. {{advice}}",
        },
    },
    createOnce(context) {
        return {
            CallExpression(node: ESTree.CallExpression) {
                const callee = node.callee;
                if (callee.type !== "MemberExpression" || callee.computed) return;
                if (callee.property.type !== "Identifier") return;

                const native = callee.property.name;
                const advice = MUTATORS.get(native);
                if (advice === undefined) return;
                if (!isProvenArray(callee.object, context.sourceCode)) return;

                context.report({
                    node: callee.property,
                    messageId: "nativeArrayMutation",
                    data: { native, advice },
                });
            },
        };
    },
});
