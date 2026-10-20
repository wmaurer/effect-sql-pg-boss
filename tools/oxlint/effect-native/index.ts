import { eslintCompatPlugin } from "@oxlint/plugins";

import { imperativeCollectionBuildRule } from "./rules/imperative-collection-build.ts";
import { jsonParseIntoDecodeRule } from "./rules/json-parse-into-decode.ts";
import { nativeArrayMethodRule } from "./rules/native-array-method.ts";
import { nativeArrayMutationRule } from "./rules/native-array-mutation.ts";
import { nestedArrayPipelineRule } from "./rules/nested-array-pipeline.ts";

/** First-party Oxlint rules that steer code to what Effect already provides. Four are about native
 *  collections. Two report a native array method you reached for, one reports what those
 *  conversions tend to produce, and one reports a `Map` or `Set` built by mutation instead of in one
 *  expression. The fifth reports `JSON.parse` passed straight to a Schema decoder, where
 *  `Schema.fromJsonString` does both steps. The shape of `@effect/tsgo`'s own `effect-native`
 *  preset, which is where these belong and where they would have real types. This plugin exists
 *  because that preset has no such rule and a local oxlint plugin gets no type information to
 *  write one properly.
 *
 *  Separate from `tools/oxlint/anti-slop`, which is vendored upstream source that PROVENANCE.md
 *  requires stay byte-identical, and from `tools/oxlint/prose`, which is documented as rules about
 *  the English in comments. The master copy of this tree is `tools/scripts/rules/effect-native/` in
 *  the scaffold repo, with its tests in `tools/scripts/test/effect-native/`. Edit it there; every
 *  other copy is generated. */
const effectNativePlugin = eslintCompatPlugin({
    meta: { name: "effect-native" },
    rules: {
        "imperative-collection-build": imperativeCollectionBuildRule,
        "json-parse-into-decode": jsonParseIntoDecodeRule,
        "native-array-method": nativeArrayMethodRule,
        "native-array-mutation": nativeArrayMutationRule,
        "nested-array-pipeline": nestedArrayPipelineRule,
    },
});

export default effectNativePlugin;
