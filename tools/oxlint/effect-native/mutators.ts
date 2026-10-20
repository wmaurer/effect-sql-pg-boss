/** Methods that change their receiver in place, and what to do instead.
 *  Unlike `methods.ts`'s table this one names no drop-in replacement, because there is none.
 *  `Array.append` and `Array.appendAll` return a NEW array, so switching is not a rewrite of the
 *  call — it is a rewrite of whatever loop or branch was doing the accumulating. The advice
 *  therefore describes the shape to move to rather than a function to substitute.
 *
 *  `.concat`, `.sort` and `.reverse` are not here. `.concat` does not mutate at all; `.sort` and
 *  `.reverse` do, but they also have direct Effect counterparts, so the replacement table reports
 *  them and says so in its own message.
 *
 *  Separate from the rule that reads it so both engines share one copy. The oxlint plugin proves
 *  its receiver from syntax; `pnpm lint:types` proves the same receiver from the checker. The
 *  advice a person reads must not depend on which of the two found the call. */
export const MUTATORS: ReadonlyMap<string, string> = new Map([
    [
        "push",
        "Build the array in one expression instead — `Array.map`, `Array.filter`, `Array.filterMap` or `Array.getSomes` over the source, or `Array.appendAll` to join two arrays.",
    ],
    [
        "splice",
        "Build the array in one expression instead — `Array.remove`, `Array.take`/`Array.drop`, or `Array.appendAll` of the pieces you want.",
    ],
    ["shift", "`Array.head` and `Array.tail` read the front without changing the array."],
    ["pop", "`Array.last` and `Array.init` read the back without changing the array."],
    ["unshift", "`Array.prepend` returns a new array with the element on the front."],
    ["fill", "`Array.makeBy` builds an array of a given length without mutating one."],
    ["copyWithin", "Build the array you want with `Array.appendAll` of slices taken by `Array.take` and `Array.drop`."],
]);
