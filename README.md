# effect-sql-pg-boss

This repository holds the source of `@wmaurer/effect-sql-pg-boss`, which brings [pg-boss](https://github.com/timgit/pg-boss), the Postgres job queue for Node, to [Effect](https://effect.website). Jobs are sent inside the app's `sql.withTransaction` through `@effect/sql-pg`, so they commit or roll back with the app's own rows. A transactional worker's writes commit in the same transaction that completes the job.

To use the library, read [its README](packages/effect-sql-pg-boss/README.md). It covers installing the library, declaring queues, sending jobs, and working them. This file is for working on the repository.

## Repository layout

| Path                          | What it holds                                                                                         |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `packages/effect-sql-pg-boss` | The published library, with its source in `src/` and its tests in `test/`                             |
| `examples/orders`             | A private CLI app that runs the library against Postgres in Docker. Its README is the tutorial        |
| `design/README.md`            | How the library works and why: the investigation, the internals, what each test proves, the decisions |
| `tools/oxlint`                | The custom oxlint rules, generated from `project-setup`                                               |
| `scripts`                     | Bundled scripts from `project-setup` for typechecking, lint sync, and the `.repos/` clones            |

## Set up the repository

You need Node 22.18 or later, pnpm 10.33.0 as the `packageManager` field in `package.json` names, and Docker for the tests.

```sh
pnpm install
```

The install also runs `scripts/patch-tsgo.js`, which patches oxlint so that `pnpm lint` reports Effect's own rules, and installs the git hooks.

## Run the checks

| Command          | What it does                                                       |
| ---------------- | ------------------------------------------------------------------ |
| `pnpm fmt`       | Format the repository with oxfmt                                   |
| `pnpm lint`      | Run oxlint with Effect's rules and the repository's own rules      |
| `pnpm typecheck` | Typecheck the library, its tests, and the example with `tsc -b`    |
| `pnpm test`      | Run the library's tests against Postgres 18                        |
| `pnpm pre-push`  | Run `fmt:check`, `lint`, `typecheck`, and `test`, as the hook does |

The `pre-push` git hook runs `pnpm pre-push`, so a push with a failing check stops.

`pnpm test` starts a `postgres:18` container on a free port and removes it when the run ends. Each test file creates its own database in that container. To use a server you already run, set `DATABASE_URL` to a superuser URL. The tests then skip the assertions that read the server log, because only the container has statement logging turned on.

## Read the pg-boss source

The pg-boss npm package ships only `dist/`. `pnpm deps:fetch` clones the pg-boss repository into `.repos/deps/pg-boss`, at the version the lockfile installs. `pnpm deps:check` reports when the clone and the installed version differ.

## Change the design

`design/README.md` records each design decision with its reason. Read it before you change a decision, and update it in the same commit. The original sketches, prototypes, and experiments are at the git tag `design-record`.

[`AGENTS.md`](AGENTS.md) holds the rules for coding agents. It also lists the generated files that you must not edit by hand.

## License

MIT. See [`packages/effect-sql-pg-boss/LICENSE`](packages/effect-sql-pg-boss/LICENSE).
