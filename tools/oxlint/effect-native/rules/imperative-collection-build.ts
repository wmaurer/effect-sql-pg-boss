import { defineRule } from "@oxlint/plugins";

import { isGlobalNamespace } from "../receiver.ts";

import type { ESTree, Reference, SourceCode } from "@oxlint/plugins";

/** The collection kinds this rule reports. `"object"` is an object literal used as a dictionary. */
type Kind = "Map" | "Set" | "object";

/** Methods that change the collection in place, per constructor.
 *
 *  `WeakMap` and `WeakSet` are absent on purpose. They are identity-keyed, inherently mutable and
 *  have no declarative Effect counterpart, so every one of them would be a finding nobody can act
 *  on. That includes the `WeakSet` in this plugin's own `nested-array-pipeline.ts`, which would
 *  need an override suppressing the plugin inside its own directory. */
const MUTATORS: ReadonlyMap<Kind, ReadonlySet<string>> = new Map([
    ["Map", new Set(["set", "delete", "clear"])],
    ["Set", new Set(["add", "delete", "clear"])],
]);

/** What to build instead, per kind.
 *
 *  The advice is "build it in one expression", never "stop using `Map` and `Set`". A `Set` built
 *  from a finished array and used for membership is the normal way to write it, and the rule is
 *  already silent on it; the message must not read as disapproving of it either.
 *
 *  The `Effect.forEach` clause is not decoration. Two of the five sites this rule found in `src/`
 *  have an effectful early return inside the loop, so "build it in one expression" is unreachable
 *  advice for them until the validation is lifted out. */
const ADVICE: ReadonlyMap<Kind, string> = new Map([
    [
        "Map",
        "`Record.fromEntries(Array.map(…))` builds one from a list, and `Record.union(a, b, (first) => first)` merges two without a first-wins loop.",
    ],
    ["Set", "Pass the finished array to the constructor — `new Set(Array.map(…))`."],
    [
        "object",
        "`Record.fromEntries(Array.map(…))` builds one from a list, and `Record.union(a, b, (first) => first)` merges two without a first-wins loop.",
    ],
]);

/** The advice every kind shares, appended to the kind's own. */
const EFFECTFUL = "When the loop can fail, `Effect.forEach` first, then build from its result.";

/** Which collection this initialiser constructs, or undefined for anything else.
 *
 *  The initialiser need NOT be empty: two of this repo's five hits are `new Map(current.filter(…))`
 *  followed by a `.set` loop, so a rule keyed on `new Map()` alone would miss them.
 *
 *  `isGlobalNamespace` is what keeps a file that binds the name `Map` to something else from being
 *  reported against a module this rule knows nothing about. */
function kindOf(init: ESTree.Node | null | undefined, sourceCode: SourceCode): Kind | undefined {
    if (init == null) return undefined;
    if (init.type === "ObjectExpression") return "object";
    if (init.type !== "NewExpression") return undefined;
    const callee = init.callee;
    if (callee.type !== "Identifier") return undefined;
    if (callee.name !== "Map" && callee.name !== "Set") return undefined;
    return isGlobalNamespace(callee.name, callee, sourceCode) ? callee.name : undefined;
}

/** True when this reference writes into an object literal.
 *
 *  Four shapes, per the spec: `o.k = v`, `o[k] = v`, `delete o.k`, and `Object.assign(o, …)` where
 *  the declared object is the TARGET. `Object.assign({}, o)` copies out of it and is a read, which
 *  is why only argument 0 counts. */
function writesToObject(identifier: ESTree.Node, sourceCode: SourceCode): boolean {
    const parent = identifier.parent;
    if (parent?.type === "MemberExpression" && parent.object === identifier) {
        const outer = parent.parent;
        if (outer?.type === "AssignmentExpression") return outer.left === parent;
        return outer?.type === "UnaryExpression" && outer.operator === "delete";
    }
    if (parent?.type === "CallExpression" && parent.arguments[0] === identifier) {
        const callee = parent.callee;
        return (
            callee.type === "MemberExpression" &&
            !callee.computed &&
            callee.object.type === "Identifier" &&
            callee.object.name === "Object" &&
            callee.property.type === "Identifier" &&
            callee.property.name === "assign" &&
            isGlobalNamespace("Object", callee.object, sourceCode)
        );
    }
    return false;
}

/** True when this reference to the binding changes the collection rather than reading it.
 *
 *  `reference.identifier.parent` is the shape the use sits in, so this needs no traversal of its
 *  own and reaches a mutation however deeply it is nested — a loop body, a callback, or a function
 *  that outlives the declaration. The declaration's own write cannot match: its parent is the
 *  `VariableDeclarator`. */
function isMutation(reference: Reference, kind: Kind, sourceCode: SourceCode): boolean {
    const identifier = reference.identifier;
    if (kind === "object") return writesToObject(identifier, sourceCode);

    const parent = identifier.parent;
    if (parent?.type !== "MemberExpression" || parent.computed) return false;
    if (parent.object !== identifier) return false;
    if (parent.property.type !== "Identifier") return false;
    if (MUTATORS.get(kind)?.has(parent.property.name) !== true) return false;
    // `m.set` alone is a method reference, not a call that changes anything.
    const call = parent.parent;
    return call?.type === "CallExpression" && call.callee === parent;
}

/** Report a collection that is declared and then filled by mutation.
 *
 *  Native array mutation is `native-array-mutation`'s, not this rule's: covering `.push` here would
 *  double-report every accumulation loop. What is left is the shape nothing in the stack catches —
 *  `@effect/tsgo`'s 95 rules do not cover it, and this plugin's other three all key on the native
 *  array prototype, which a `Map` never touches.
 *
 *  Unlike its siblings this rule needs no proof about a receiver, so `receiver.ts`'s documented
 *  blind spot around type aliases does not apply: the trigger is a constructor call in the
 *  initialiser position, which is self-evident in the syntax.
 *
 *  The known false positive is a module-level cache, where mutation is the design and there is no
 *  declarative rewrite. That was chosen with the cost visible: a cache is rare, and making one
 *  carry an `oxlint-disable-next-line` comment makes it announce itself as deliberate. */
export const imperativeCollectionBuildRule = defineRule({
    meta: {
        type: "suggestion",
        docs: {
            description: "Disallow building a collection by declaring it and then filling it with a mutating loop.",
        },
        messages: {
            imperativeCollectionBuild:
                "This `{{kind}}` is declared and then filled by mutation. Build it in one expression instead. {{advice}}",
        },
    },
    createOnce(context) {
        return {
            VariableDeclarator(node: ESTree.VariableDeclarator) {
                if (node.id.type !== "Identifier") return;
                const kind = kindOf(node.init, context.sourceCode);
                if (kind === undefined) return;

                const declared = context.sourceCode.getDeclaredVariables(node);
                const mutated = declared.some((variable) =>
                    variable.references.some((reference) => isMutation(reference, kind, context.sourceCode)),
                );
                if (!mutated) return;

                // At the declarator, once per collection: the declaration is what gets
                // restructured, and a loop with three `.set` calls is one finding, not three.
                context.report({
                    node: node.id,
                    messageId: "imperativeCollectionBuild",
                    data: { kind, advice: `${ADVICE.get(kind) ?? ""} ${EFFECTFUL}` },
                });
            },
        };
    },
});
