// effect/process needs a ChildProcessSpawner from @effect/platform-node, which this package does not install.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { spawnSync } from "node:child_process";

import { Array as Arr, Effect } from "effect";

/**
 * Runs the `docker` CLI and returns what it printed. `docker logs` replays a
 * container's stderr on its own stderr, so both streams are kept.
 */
export const docker = (...args: ReadonlyArray<string>) =>
    Effect.suspend(() => {
        const result = spawnSync("docker", args, { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
        return result.status === 0
            ? Effect.succeed(`${result.stdout}${result.stderr}`.trim())
            : Effect.die(new Error(`docker ${Arr.join(args, " ")} failed: ${result.stderr}`));
    });
