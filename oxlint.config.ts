import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

import { defineConfig } from "oxlint";

import { correctnessRules } from "./correctness-rules.ts";
import { effectRules } from "./effect-rules.ts";

// The `effecttsgo` plugin only exists because scripts/patch-tsgo.js patches oxlint's
// tsgolint binding, and that patch is built against one specific oxlint build. A mismatch
// already fails loudly (`Unknown plugin: 'effecttsgo'`, exit 1) but does not name the cause.
const OXLINT_PINNED = "1.88.0";
const installedOxlint = createRequire(import.meta.url)("oxlint/package.json").version;
if (installedOxlint !== OXLINT_PINNED) {
    throw new Error(
        `oxlint ${installedOxlint} != pinned ${OXLINT_PINNED}. Bump only after checking the "Supported Package Versions" table in @effect/tsgo's README.`,
    );
}

export default defineConfig({
    // `plugins` replaces oxlint's defaults rather than extending them, so naming
    // `typescript` and `import` alone drops `unicorn` and `oxc` — 27 rules of the
    // correctness category, all bug-catchers rather than style. `unicorn/no-thenable`
    // is the one that earns its place in Effect code: an accidental `then` on a value
    // makes it awaitable and changes how it flows across async boundaries.
    //
    // `effecttsgo` is named explicitly. It used to arrive through the preset's
    // own `plugins` and merge across `extends`; without the preset it has to be
    // stated, and forgetting it is the worst failure available here — every
    // Effect rule silently does nothing and the run stays green.
    plugins: ["typescript", "import", "unicorn", "oxc", "effecttsgo"],

    // Vendored — see tools/oxlint/anti-slop/PROVENANCE.md. Node >= 22.18 strips
    // types natively, so the .ts entry points load without a build step.
    //
    // Absolute, not "./tools/…": oxlint 1.78.0 refuses a RELATIVE jsPlugin
    // specifier in any config reached through `extends`, and a package's own
    // oxlint config inherits this one that way.
    jsPlugins: [
        {
            name: "anti-slop",
            specifier: fileURLToPath(new URL("./tools/oxlint/anti-slop/index.ts", import.meta.url)),
        },
        {
            name: "anti-slop-effect",
            specifier: fileURLToPath(new URL("./tools/oxlint/anti-slop/effect/index.ts", import.meta.url)),
        },
        // The prose rules - first-party, not vendored. A separate plugin for that reason: the
        // anti-slop tree has to stay byte-identical to upstream so the two can be diffed. The
        // specifier is absolute for the same reason as the two above.
        {
            name: "prose",
            specifier: fileURLToPath(new URL("./tools/oxlint/prose/index.ts", import.meta.url)),
        },
        // The native-array rules. A separate plugin from `prose` above, which is about the English
        // in comments. Named `effect-native` after @effect/tsgo's own preset, which is where a typed
        // version of these belongs: if one lands there, the swap is a rename and this tree is
        // deleted. The specifier is absolute for the same reason as the three above.
        {
            name: "effect-native",
            specifier: fileURLToPath(new URL("./tools/oxlint/effect-native/index.ts", import.meta.url)),
        },
    ],

    env: {
        node: true,
        es2024: true,
    },

    // The `recommended` preset from `@effect/tsgo/oxlint-presets` used to set
    // this, back when this config extended it. effect-rules.ts carries the rule
    // list now, not the preset's options, so it is stated here for the same
    // reason `effecttsgo` is above: the failure mode is silent — the rules load,
    // none of the type-aware ones fire, and the run looks clean.
    // Equivalent to `--type-aware`; requires oxlint-tsgolint.
    options: {
        typeAware: true,
    },

    rules: {
        // The rules oxlint enables BY ITSELF from the `plugins` list above —
        // generated, see correctness-rules.ts. They already run; naming them is
        // what makes a change to the set a diff someone reviews rather than a
        // rule that quietly stops firing. Spread first, so a severity below wins.
        ...correctnessRules,

        // Every Effect rule, generated — see effect-rules.ts. All 118, including
        // the ones the `effecttsgo` plugin enables by itself: the generated file
        // states the whole rule set rather than only the part the plugin leaves
        // alone, so a severity is never left implicit. That is where the 13 the
        // `recommended` preset calls "error" come from — they are generated at
        // "error" and need no hand-written restatement here.
        //
        // A severity written after this spread wins, and that inline value is
        // what reaches the editor, because the mirror is built from what oxlint
        // resolves.
        ...effectRules,

        // Off, because it fires on the APIs this scaffold requires. Effect 4.0.5 marks
        // all of `effect/process` and `effect/cli` `@stability unstable`, and CLAUDE.md
        // sends every `node:child_process` call to `effect/process/ChildProcess` — so at
        // "warn" the rule fails `--max-warnings=0` on the first spawned process, with no
        // stable alternative to move to. `experimental-api-usage` stays on: nothing here
        // depends on an experimental API, and one arriving is worth a diff.
        "effecttsgo/unstable-api-usage": "off",

        "no-unused-vars": "off",
        "typescript/no-unused-vars": "warn",

        // anti-slop. A fresh scaffold has nothing to violate them, so they start
        // at "error" rather than an interim "warn" — the cheapest moment to
        // adopt a strict policy is before any code exists. Some of them do fire
        // on normal, correct TypeScript: no-unknown-parameters flags the canonical
        // type guard `(v: unknown): v is string`, no-runtime-typeof flags
        // `typeof v === "undefined"`. Suppress a deliberate one in place with
        // `// oxlint-disable-next-line anti-slop/no-runtime-typeof` rather than
        // downgrading the rule for the whole repo. Adopting this in a codebase
        // that already has violations is the one case for starting at "warn".
        "anti-slop/no-chained-type-assertions": "error",
        "anti-slop/no-conditional-empty-object-spread": "error",
        "anti-slop/no-known-value-widening": "error",
        "anti-slop/no-module-mocking": "error",
        "anti-slop/no-object-parameters": "error",
        "anti-slop/no-reflect-apply": "error",
        "anti-slop/no-reflect-get": "error",
        "anti-slop/no-runtime-typeof": "error",
        "anti-slop/no-shape-in-symbol-names": "error",
        "anti-slop/no-unknown-parameters": "error",
        "anti-slop/no-unknown-returns": "error",
        "anti-slop/no-unknown-type-aliases": "error",
        "anti-slop/no-unsafe-dictionary-type": "error",
        "anti-slop/no-widen-then-assert": "error",
        "anti-slop/require-safety-comment-for-type-assertion": "error",

        "anti-slop-effect/no-service-constructor-imports": "error",

        // prose: the English in comments. A fresh scaffold has no comments to violate these, so
        // "error" from the start costs nothing - the same argument anti-slop makes above. A
        // project ADOPTING them into existing code is the "warn" case; see
        // docs/features/prose.md.
        //
        // The messages point at AGENTS.md / CLAUDE.md headings. The scaffold ships neither file,
        // so paste the block from docs/features/prose.md into yours, or the pointer dangles.
        "prose/comment-sentence-structure": "error",
        "prose/no-history-in-public-docs": "error",
        "prose/plain-english-comments": "error",

        // Effect's `Array` module instead of the native prototype, where the receiver is provable
        // without types. "error" rather than "warn" for the same reason the anti-slop rules are:
        // a warning does not affect the exit code, so one left behind changes nothing.
        "effect-native/native-array-method": "error",

        // In-place array mutation. A separate rule from the one above because the fix is a
        // different kind of thing: that one is a rename, this one restructures the code around
        // the call. Both at "error"; see docs/features/effect-native.md for the argument.
        "effect-native/native-array-mutation": "error",

        // Deeply nested `Array` calls, which read inside-out. Not a native-vs-Effect rule like the
        // two above — it reports what those conversions tend to produce. `"error"` for the same
        // exit-code reason. Upstream's `effecttsgo/missed-pipeable-opportunity` does not cover this:
        // it suggests the `.pipe()` method, and a plain array is not Pipeable.
        "effect-native/nested-array-pipeline": "error",

        // A Map, Set or object literal declared and then filled by a mutating loop — the same
        // shape `native-array-mutation` reports for arrays, in the collections it cannot see. The
        // message names what to build instead; there is no fixer. `"error"` for the same
        // exit-code reason as the three above.
        "effect-native/imperative-collection-build": "error",

        // `JSON.parse(text)` passed straight to a Schema decoder, which throws on text that is not
        // JSON instead of failing the decode. Upstream's `effecttsgo/prefer-schema-over-json`
        // reports only inside an Effect context. Keep it on in tests: the fix,
        // `Schema.fromJsonString`, checks the same JSON boundary a test wants to check.
        "effect-native/json-parse-into-decode": "error",
    },

    // Grouped by the scaffold feature each entry belongs to, so a project that took
    // only some of them knows which lines to drop. Kept in step with .oxfmtrc.json's
    // ignorePatterns, which carries the same groups.
    ignorePatterns: [
        // workspace
        "**/dist",
        "scripts/typecheck.js",
        // tsgo
        "scripts/patch-tsgo.js",
        // lint-sync
        "scripts/lint-sync.js",
        // anti-slop
        "tools/oxlint/anti-slop",
        // prose. Generated from project-setup's tools/scripts/rules/prose, and linted there.
        "tools/oxlint/prose",
        // effect-native. The plugin tree is generated from project-setup's
        // tools/scripts/rules/effect-native, and linted there.
        "tools/oxlint/effect-native",
        "scripts/lint-types.js",
        // repos
        "scripts/repos.js",
        ".repos",
    ],
});
