import { defineRule } from "@oxlint/plugins";

import { REPLACEMENTS } from "../methods.ts";
import { isProvenArray } from "../receiver.ts";

import type { ESTree } from "@oxlint/plugins";

/** Require Effect's `Array` module instead of the native array prototype, where the receiver can be
 *  proven to be an array without types.
 *
 *  oxlint offers no parser services to a custom JS plugin, so this rule cannot ask the checker what a
 *  receiver is — see `receiver.ts`. It therefore under-reports on purpose. The upstream home for a
 *  typed version of this rule is `@effect/tsgo`'s `effect-native` preset, which is why this plugin
 *  carries that name: if the rule lands there, the swap is `effect-native/native-array-method` to
 *  `effecttsgo/native-array-method` and this tree is deleted. */
export const nativeArrayMethodRule = defineRule({
    meta: {
        type: "suggestion",
        docs: {
            description:
                "Disallow native array prototype methods that have a direct counterpart in Effect's `Array` module, where the receiver is provably an array.",
        },
        messages: {
            nativeArrayMethod:
                "`.{{native}}` is the native array prototype. Use `Array.{{effect}}` from `effect` instead, with the receiver as its first argument.",
            nativeArrayMethodNote:
                "`.{{native}}` is the native array prototype. Use `Array.{{effect}}` from `effect` instead, with the receiver as its first argument. {{note}}",
        },
    },
    createOnce(context) {
        return {
            CallExpression(node: ESTree.CallExpression) {
                const callee = node.callee;
                if (callee.type !== "MemberExpression" || callee.computed) return;
                if (callee.property.type !== "Identifier") return;

                const native = callee.property.name;
                const replacement = REPLACEMENTS.get(native);
                if (replacement === undefined) return;
                if (!isProvenArray(callee.object, context.sourceCode)) return;

                if (replacement.note === undefined) {
                    context.report({
                        node: callee.property,
                        messageId: "nativeArrayMethod",
                        data: { native, effect: replacement.effect },
                    });
                    return;
                }
                context.report({
                    node: callee.property,
                    messageId: "nativeArrayMethodNote",
                    data: { native, effect: replacement.effect, note: replacement.note },
                });
            },
        };
    },
});
