import type { ESTree, Scope, SourceCode, Variable } from "@oxlint/plugins";

const EFFECT_BARREL = "effect";

/** The variable `name` resolves to at `scope`, or undefined if nothing binds it. */
function lookup(scope: Scope | null, name: string): Variable | undefined {
    for (let current = scope; current !== null; current = current.upper) {
        const found = current.set.get(name);
        if (found !== undefined) return found;
    }
    return undefined;
}

/** True when `node` is an identifier bound to Effect's `module` namespace, such as `"Array"` or
 *  `"Schema"`. Two import forms count:
 *
 *  - `import { Schema as S } from "effect"`, the barrel form, under any alias;
 *  - `import * as S from "effect/Schema"`, the namespace form.
 *
 *  The check goes through the scope, not the name, so any alias works. A local binding that
 *  shadows the import does not count. A global of the same name, such as `Array`, never counts,
 *  because only an import binding does. */
// Both forms are needed. Every site in this repo takes the module off the `effect` barrel, so a check
// of the namespace form alone would match none of them.
export function isEffectModule(node: ESTree.Node, module: string, sourceCode: SourceCode): boolean {
    if (node.type !== "Identifier") return false;
    const variable = lookup(sourceCode.getScope(node), node.name);
    if (variable === undefined) return false;
    return variable.defs.some((definition) => {
        if (definition.type !== "ImportBinding") return false;
        if (definition.parent?.type !== "ImportDeclaration") return false;
        const source = definition.parent.source.value;

        if (definition.node.type === "ImportNamespaceSpecifier") return source === `${EFFECT_BARREL}/${module}`;

        // The alias is `local`, so the name to match is `imported`.
        if (definition.node.type === "ImportSpecifier") {
            const imported = definition.node.imported;
            return source === EFFECT_BARREL && imported.type === "Identifier" && imported.name === module;
        }
        return false;
    });
}
