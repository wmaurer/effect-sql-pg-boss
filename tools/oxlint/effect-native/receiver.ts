import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";

/** How far `isProvenArray` follows an identifier back to its initialiser.
 *
 *  Each hop is `const xs = ys` — a rename. Real code renames once or twice; a chain longer than this
 *  is either generated or a cycle the resolver would otherwise walk forever. Stopping returns
 *  "not proven", which is the silent answer, so the cap can only ever cost a report. */
const MAX_DEPTH = 8;

/** Calls that always produce a fresh array, whatever their arguments.
 *
 *  Keyed by the namespace object, because the property name alone proves nothing: `Object.entries`
 *  is an array, `Effect.entries` is not. The namespace itself is checked against the scope by
 *  `isGlobalNamespace` before this table is consulted.
 *
 *  Kept as an inferred literal object (via `satisfies`) rather than a widened `Record<string, …>` —
 *  the same convention `lint-sync/derive.ts`'s `LEVELS` uses. The lookup below narrows an arbitrary
 *  string to this object's keys through equality checks first, since indexing it with a bare
 *  `string` would be exactly the widening this avoids. */
const ARRAY_PRODUCERS = {
    Object: new Set(["entries", "keys", "values"]),
    Array: new Set(["from", "of"]),
} as const satisfies Readonly<Record<string, ReadonlySet<string>>>;

/** Methods that return an array when their receiver is one, so a proven receiver stays proven
 *  through them. `.slice` is here even though no rule reports it on its own: this table is about
 *  propagating the proof along a chain, not about what is banned. `.concat` IS reported, by
 *  `native-array-method`, but still belongs here too: a chain like `xs.concat(ys).forEach(…)` needs
 *  the proof to keep propagating past `.concat` so the chained `.forEach` is flagged as well. */
const ARRAY_RETURNING = new Set([
    "map",
    "filter",
    "slice",
    "concat",
    "flat",
    "flatMap",
    "sort",
    "reverse",
    "toSorted",
    "toReversed",
    "splice",
]);

/** `.split(…)` on anything is an array, and it is the one method name that needs no proven receiver:
 *  `String.prototype.split` is the only `split` in the language's standard library. A userland
 *  `split` that returns something else is rare enough to accept as a false positive risk that has
 *  not been observed. Kept separate from ARRAY_RETURNING for exactly that reason — it is the one
 *  entry there that does NOT require its own receiver to be proven first. */
const SPLIT = "split";

/** The variable `name` resolves to at `scope`, or undefined if nothing binds it. */
function lookup(scope: Scope | null, name: string): Variable | undefined {
    for (let current = scope; current !== null; current = current.upper) {
        const found = current.set.get(name);
        if (found !== undefined) return found;
    }
    return undefined;
}

/** True when `name` is the language's own global rather than something this file bound.
 *
 *  This is the `Array` shadowing trap from the spec, and `imperative-collection-build` asks the
 *  same question about `Map`, `Set` and `Object`. A file that does `import { Array } from "effect"`
 *  makes `Array.from(…)` Effect's own function, not the global, and treating it as array-producing
 *  would prove arrays off the very module the rule is steering people towards. Any binding at all
 *  disqualifies the name — an import, a local `const`, a parameter. Only an unbound name is the
 *  global.
 *
 *  Exported rather than copied: every rule that asks whether a name is the global asks the same
 *  question, so a second copy could drift into a second answer. `effect-import.ts` keeps its own
 *  `lookup`, because it asks which import a name is bound to, which this module does not ask. */
export function isGlobalNamespace(name: string, node: ESTree.Node, sourceCode: SourceCode): boolean {
    const found = lookup(sourceCode.getScope(node), name);
    return found === undefined || found.defs.length === 0;
}

/** True for a type annotation written as an array: `T[]`, `readonly T[]`, `Array<T>`, `ReadonlyArray<T>`.
 *
 *  A type ALIAS is deliberately not followed. `current: CorrectnessSeverities` is a `ReadonlyArray`
 *  in fact, but proving that needs the checker this rule does not have, and guessing from the name
 *  would be a false positive waiting to happen. */
function isArrayTypeAnnotation(annotation: ESTree.Node | null | undefined): boolean {
    if (!annotation) return false;
    // `x: T[]`
    if (annotation.type === "TSArrayType") return true;
    // `x: readonly T[]` — a type operator wrapping the array type.
    if (annotation.type === "TSTypeOperator") return isArrayTypeAnnotation(annotation.typeAnnotation);
    // `x: Array<T>` / `x: ReadonlyArray<T>`
    if (annotation.type === "TSTypeReference") {
        const name = annotation.typeName;
        return name?.type === "Identifier" && (name.name === "Array" || name.name === "ReadonlyArray");
    }
    return false;
}

/** The type annotation attached to a binding, unwrapping the `TSTypeAnnotation` node that carries it.
 *
 *  Narrowed to the binding-pattern node types that carry `typeAnnotation` in the plugin's ESTree
 *  types, rather than reading the property off `ESTree.Node` at large: most node types in that
 *  union — `ArrayExpression` among them — do not declare it at all. */
function annotationOf(node: ESTree.Node | null | undefined): ESTree.Node | undefined {
    const holder = node?.type === "VariableDeclarator" ? node.id : node;
    if (holder === null || holder === undefined) return undefined;
    if (
        holder.type !== "Identifier" &&
        holder.type !== "ObjectPattern" &&
        holder.type !== "ArrayPattern" &&
        holder.type !== "AssignmentPattern" &&
        holder.type !== "RestElement"
    ) {
        return undefined;
    }
    const wrapper = holder.typeAnnotation;
    if (wrapper === null || wrapper === undefined) return undefined;
    return wrapper.type === "TSTypeAnnotation" ? wrapper.typeAnnotation : wrapper;
}

/** True when the variable is only ever written once, by its own initialiser.
 *
 *  A reassigned `let` is not proven, per the spec: the initialiser this function would follow is not
 *  necessarily the value at the call site. `Reference.init` marks the declaration's own write, so
 *  any other write disqualifies the binding. */
function isEffectivelyConst(variable: Variable): boolean {
    return variable.references.every((reference) => !reference.isWrite() || reference.init);
}

/** True when `node` is syntactically provable as an array.
 *
 *  Everything this returns false for is silent, by design — see the spec. The four proofs are the
 *  four branches below, in the order the spec lists them. */
export function isProvenArray(node: ESTree.Node, sourceCode: SourceCode, depth = 0): boolean {
    if (depth > MAX_DEPTH) return false;

    // 1. A literal, including a spread: `[...xs].sort()` is the shape most of this repo uses.
    if (node.type === "ArrayExpression") return true;

    if (node.type === "CallExpression") {
        const callee = node.callee;
        if (callee.type !== "MemberExpression" || callee.computed) return false;
        const property = callee.property;
        if (property.type !== "Identifier") return false;

        // 2a. `xs.split(…)`.
        if (property.name === SPLIT) return true;

        // 2b. `Object.entries(…)`, `Array.from(…)` — but only the real globals.
        if (callee.object.type === "Identifier") {
            const namespace = callee.object.name;
            const producers = namespace === "Object" || namespace === "Array" ? ARRAY_PRODUCERS[namespace] : undefined;
            if (producers?.has(property.name) === true && isGlobalNamespace(namespace, callee.object, sourceCode)) {
                return true;
            }
        }

        // 3. A chain link: an array-returning method on an already-proven receiver.
        if (ARRAY_RETURNING.has(property.name)) {
            return isProvenArray(callee.object, sourceCode, depth + 1);
        }
        return false;
    }

    // 4. An identifier that resolves to a proven binding.
    if (node.type === "Identifier") {
        const variable = lookup(sourceCode.getScope(node), node.name);
        if (variable === undefined) return false;
        return variable.defs.some((definition) => {
            // A destructured declarator binds ELEMENTS, not the array: in `const [head] = s.split(",")`
            // `head` is a string. Only a declarator whose `id` IS the binding identifier may be followed,
            // for its annotation or for its initialiser.
            if (definition.node.type === "VariableDeclarator" && definition.node.id.type !== "Identifier") {
                return false;
            }
            // `definition.node` carries the annotation for a `Variable` binding — it is the
            // `VariableDeclarator`, and `annotationOf` unwraps that to its `.id`. It does NOT for a
            // `Parameter` binding: there `.node` is the *enclosing function*, which has no annotation of
            // its own. `definition.name` is always the binding's own `Identifier`, so it is what carries
            // the annotation for a parameter. Checking it for a `Variable` binding too costs nothing: the
            // guard above already excludes a destructured `.id`, so for the plain declarators that reach
            // this point `.name` is the very identifier `.node.id` points at.
            if (
                isArrayTypeAnnotation(annotationOf(definition.node)) ||
                isArrayTypeAnnotation(annotationOf(definition.name))
            ) {
                return true;
            }
            if (definition.type !== "Variable") return false;
            if (!isEffectivelyConst(variable)) return false;
            const init = definition.node.type === "VariableDeclarator" ? definition.node.init : undefined;
            return init != null && isProvenArray(init, sourceCode, depth + 1);
        });
    }

    return false;
}
