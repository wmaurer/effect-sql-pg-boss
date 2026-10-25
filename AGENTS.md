# AGENTS.md

Instructions for coding agents working in this repository.

`CLAUDE.md` is a symlink to this file, so both names resolve to the same rules.

## What This Repository Is

`effect-sql-pg-boss` brings pg-boss, the Postgres job queue for Node, to Effect. It is a pnpm workspace with the library and one example app:

- `packages/effect-sql-pg-boss`: the library, published as `@wmaurer/effect-sql-pg-boss`. It exports its TypeScript source from `src/index.ts` inside the workspace and builds to `dist/` for publishing. `effect`, `@effect/sql-pg` and `pg-boss` are peer dependencies.
- `examples/orders`: a private CLI app that runs the library against a Postgres in Docker. Its `README.md` is the tutorial. It is linted and typechecked with the library, and has no tests.

`design/README.md` explains how the library works and why: the investigation, the internals, what each test proves, and the API decisions. Read it before you change a decision it records, and update it when you make one. The original sketches and prototypes are at the git tag `design-record`.

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

- `pg-boss`: the Postgres job queue this library wraps. Read it for the queue's schema, its SQL, and the API to mirror. Its npm package ships only `dist/`, so this clone is the source. The npm probe reads the installed version, so the clone follows the lockfile.

`.repos/` is hidden and gitignored, so name it when you search: `rg --hidden --no-ignore 'something' .repos/refs/<name>`.

> **Location:** `.repos/` lives at the root of the **main repository checkout**, never inside a worktree. From a worktree such as `.claude/worktrees/agent-xxx/`, `git rev-parse --show-toplevel` returns the worktree and is the wrong answer. Use `--git-common-dir` instead. It returns the shared `.git` directory that every worktree points at, and the parent of that directory is the main checkout:
>
> ```sh
> "$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/.repos"
> ```
>
> Keep `--path-format=absolute`. Without it, git answers relative to the current directory. That answer is right where it is evaluated and wrong once the path is stored or passed anywhere else.

## Write Docs in B2 English

Write every `README.md` and `CHANGELOG.md` in B2 English: the level of a reader who learned English as a second language and reads technical text well, but not native idiom. Many readers of these files are such readers, and the rules help native readers too.

- Use common words. Write "use", not "leverage", and "start", not "bootstrap".
- Write one idea in each sentence, and keep most sentences under 25 words.
- Prefer the active voice, and name who does the action: "pg-boss completes the job", not "the job is completed".
- Do not use idioms, metaphors, or phrasal verbs that have a plain word: "start", not "spin up", and "check", not "keep an eye on".
- Use one name for each thing in the whole file, and write the real symbol in code font: `JobQueue.flow`, not "the flow helper".
- Do not use semicolons, em dashes, or slashes. End the sentence and start a new one.

A changelog entry says what changed for the user of the package: the new API, the changed behavior, and what an upgrade needs. It does not list commits.

## Before You Push

The git `pre-push` hook runs `pnpm pre-push`, which is `fmt:check`, `lint`, `typecheck` and `test`. Run it yourself before you claim work is done. Do not edit `.git/hooks/pre-push`; edit the `pre-push` script in `package.json` instead.

Generated and vendored files carry a "do not edit" banner: everything under `tools/oxlint/`, the five bundles in `scripts/`, `effect-rules.ts`, `correctness-rules.ts` and `tsconfig.effect.json`. They come from `project-setup` and are replaced wholesale on upgrade. Change the source there, not the copy here.

Commit messages must not carry a `Co-Authored-By` line naming Claude, because the `commit-msg` hook rejects them.

## Release a Version

Only the operator publishes. Never run `pnpm publish` without `--dry-run`, and never run `npm login`. npm never accepts a published version again, even after an unpublish, so a wrong publish uses up its version number.

To prepare a release, do these steps on `main`:

1. Set `version` in `packages/effect-sql-pg-boss/package.json`, and add the version's entry to `packages/effect-sql-pg-boss/CHANGELOG.md`.
2. Run `pnpm pre-push`. It must pass.
3. Run `pnpm publish --dry-run --no-git-checks` in `packages/effect-sql-pg-boss`. Check that it names the new version and that the file list has `LICENSE`, `README.md`, `CHANGELOG.md`, `dist/`, and `src/`.
4. Commit the changed files, and push `main` to `origin`.
5. Run `git status`, and check that it shows no changed and no untracked files.

The operator publishes, then tags the published commit and makes a GitHub release from the changelog entry. Replace `0.1.0` with the new version:

```sh
git fetch origin
(cd packages/effect-sql-pg-boss && pnpm publish)
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
gh release create v0.1.0 --title v0.1.0 --notes "$(awk '/^## 0\.1\.0$/ { found = 1; next } /^## / { found = 0 } found' packages/effect-sql-pg-boss/CHANGELOG.md)"
```

Tag only after `pnpm publish` succeeds, so that every `v*` tag is a version that is on npm.

Publish with `pnpm`, never with `npm publish`. `pnpm publish` replaces `exports` with `publishConfig.exports`, which point into `dist/`. npm ignores `publishConfig.exports`, so its tarball would point `exports` at the `.ts` files in `src/`.

`pnpm publish` stops with `ERR_PNPM_GIT_UNCLEAN` if the checkout has a changed or untracked file, and with `ERR_PNPM_GIT_NOT_LATEST` if `origin/main` has a commit that `main` does not have. It compares with the `origin/main` ref that the last `git fetch` wrote, so run `git fetch origin` first.

The `prepack` script deletes `dist/` and `tsconfig.tsbuildinfo`, then runs `tsc -b`. `pnpm pack` and `pnpm publish` both run it. `tsc -b` alone is incremental. It leaves the output of a deleted module in `dist/`, and the tarball would ship it.
