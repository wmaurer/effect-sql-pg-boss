# AGENTS.md

Instructions for coding agents working in this repository.

`CLAUDE.md` is a symlink to this file, so both names resolve to the same rules.

## What This Repository Is

`effect-sql-pg-boss` brings pg-boss, the Postgres job queue for Node, to Effect. It is a pnpm workspace with one package:

- `packages/effect-sql-pg-boss`: the library, published as `@wmaurer/effect-sql-pg-boss`. It exports its TypeScript source from `src/index.ts` inside the workspace and builds to `dist/` for publishing. `effect` is a peer dependency.

## Effect Source and Docs (`node_modules/`)

Effect is the exception to the usual rule of not reading `node_modules/`. Every Effect package ships its TypeScript source in `src/` next to the build output, so the installed copy is the source at exactly the version in the lockfile. Do not clone Effect into `.repos/deps/`, and do not read it on GitHub, where `main` shows APIs this lockfile does not have.

Each Effect package also ships agent documentation, and it is the first place to look:

- `AGENTS.md` (with `CLAUDE.md` beside it) covers how to write Effect code: `Effect.gen` and `Effect.fn`, services with `Context.Service` and `Layer`, tagged errors, `Schema`, resources and scopes, streams, and testing with `@effect/vitest`. Follow its conventions.
- `ai-docs/src/` holds the runnable examples that `AGENTS.md` links to, one file per topic.
- `src/` is the source itself, for when the question is "what does this actually do".

The packages are not hoisted to the root `node_modules/`. Reach them through the workspace package that depends on them, for example `packages/effect-sql-pg-boss/node_modules/effect/`. They are symlinks into `node_modules/.pnpm/`, and the version in that path tells you which build you are reading.

## Other Dependency Source (`.repos/`)

`.repos/deps/` is for upstream dependencies that do not ship their source, cloned at the exact version used here. `.repos/refs/` is for repositories read as prior art, tracked at branch tip. Entries go in the `deps` and `refs` arrays of `repos.config.js`; `pnpm deps:fetch` and `pnpm refs:fetch` populate them, and `pnpm deps:check` confirms nothing has drifted. An existing checkout is skipped, so delete its folder to fetch it again. The directory is gitignored.

When you add an entry, list it here with one line on why to read it.

Current refs: none.

Current deps:

- `pg-boss`: the Postgres job queue this library wraps. Read it for the queue's schema, its SQL, and the API to mirror. Its npm package ships only `dist/`, so this clone is the source. It is pinned by `seed` until pg-boss is installed in the workspace; once it is, the probe answers instead and the seed can go.

`.repos/` is hidden and gitignored, so name it when you search: `rg --hidden --no-ignore 'something' .repos/refs/<name>`.

> **Location:** `.repos/` lives at the root of the **main repository checkout**, never inside a worktree. From a worktree such as `.claude/worktrees/agent-xxx/`, `git rev-parse --show-toplevel` returns the worktree and is the wrong answer. Use `--git-common-dir` instead. It returns the shared `.git` directory that every worktree points at, and the parent of that directory is the main checkout:
>
> ```sh
> "$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/.repos"
> ```
>
> Keep `--path-format=absolute`. Without it, git answers relative to the current directory. That answer is right where it is evaluated and wrong once the path is stored or passed anywhere else.

## Before You Push

The git `pre-push` hook runs `pnpm pre-push`, which is `fmt:check`, `lint`, `typecheck` and `test`. Run it yourself before you claim work is done. Do not edit `.git/hooks/pre-push`; edit the `pre-push` script in `package.json` instead.

Generated and vendored files carry a "do not edit" banner: everything under `tools/oxlint/`, the five bundles in `scripts/`, `effect-rules.ts`, `correctness-rules.ts` and `tsconfig.effect.json`. They come from `project-setup` and are replaced wholesale on upgrade. Change the source there, not the copy here.

Commit messages must not carry a `Co-Authored-By` line naming Claude, because the `commit-msg` hook rejects them.
