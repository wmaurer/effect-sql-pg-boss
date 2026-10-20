import { defineRule } from "@oxlint/plugins";

import { isEffectModule } from "../effect-import.ts";
import { isGlobalNamespace } from "../receiver.ts";

import type { ESTree, SourceCode } from "@oxlint/plugins";

/** The curried decoders in Effect's `Schema` module: `Schema.<decoder>(schema)(input, options?)`.
 *  `Schema.decode` and `Schema.decodeTo` are not here, because they build a transformation and do
 *  not decode a value. */
const DECODERS: ReadonlySet<string> = new Set([
    "decodeEffect",
    "decodeExit",
    "decodeOption",
    "decodePromise",
    "decodeResult",
    "decodeSync",
    "decodeUnknownEffect",
    "decodeUnknownExit",
    "decodeUnknownOption",
    "decodeUnknownPromise",
    "decodeUnknownResult",
    "decodeUnknownSync",
]);

/** The decoder name when `callee` is `Schema.<decoder>(schema)`, with `Schema` imported from
 *  Effect; otherwise undefined. */
function decoderOf(callee: ESTree.Node, sourceCode: SourceCode): string | undefined {
    if (callee.type !== "CallExpression") return undefined;
    const member = callee.callee;
    if (member.type !== "MemberExpression" || member.computed) return undefined;
    if (member.property.type !== "Identifier" || !DECODERS.has(member.property.name)) return undefined;
    return isEffectModule(member.object, "Schema", sourceCode) ? member.property.name : undefined;
}

/** True when `node` is a call to the global `JSON.parse` with one argument and no reviver. */
// A reviver has no `Schema.fromJsonString` equivalent, so a call with one has no fix to suggest.
function isPlainJsonParse(node: ESTree.Node, sourceCode: SourceCode): boolean {
    if (node.type !== "CallExpression") return false;
    const callee = node.callee;
    if (callee.type !== "MemberExpression" || callee.computed) return false;
    if (callee.object.type !== "Identifier" || callee.object.name !== "JSON") return false;
    if (callee.property.type !== "Identifier" || callee.property.name !== "parse") return false;
    if (node.arguments.length !== 1 || node.arguments[0]?.type === "SpreadElement") return false;
    return isGlobalNamespace("JSON", callee.object, sourceCode);
}

/** Report `JSON.parse(text)` passed straight to a curried Schema decoder.
 *
 *  `Schema.decodeUnknownExit(S)(JSON.parse(text))` throws a `SyntaxError` when `text` is not JSON.
 *  `Schema.decodeUnknownExit(Schema.fromJsonString(S))(text)` returns that case as a decode failure.
 *  Only the direct argument is reported; a variable is not followed. */
// Upstream's `effecttsgo/prefer-schema-over-json` reports `JSON.parse` only inside an Effect
// context, so it is silent in a plain function or a test body. The narrow shape is what lets this
// rule stay on in tests: see `docs/features/effect-native.md`.
export const jsonParseIntoDecodeRule = defineRule({
    meta: {
        type: "suggestion",
        docs: {
            description:
                "Report `JSON.parse` passed straight to a Schema decoder, and suggest `Schema.fromJsonString`.",
        },
        messages: {
            jsonParseIntoDecode:
                "`JSON.parse` feeds `Schema.{{decoder}}`, so text that is not JSON throws a `SyntaxError` " +
                "instead of failing the decode. Pass the string to `Schema.{{decoder}}(Schema.fromJsonString(schema))`.",
        },
    },
    createOnce(context) {
        return {
            CallExpression(node: ESTree.CallExpression) {
                const decoder = decoderOf(node.callee, context.sourceCode);
                if (decoder === undefined) return;
                const input = node.arguments[0];
                if (input === undefined || !isPlainJsonParse(input, context.sourceCode)) return;
                context.report({ node: input, messageId: "jsonParseIntoDecode", data: { decoder } });
            },
        };
    },
});
