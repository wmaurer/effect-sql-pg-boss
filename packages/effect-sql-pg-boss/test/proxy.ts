/**
 * A TCP proxy in front of the test server that makes LISTEN/NOTIFY sessions
 * fail the way a network does. A connection is a listener from the first
 * chunk its client sends that contains `LISTEN `; every other connection is
 * forwarded untouched, so a client's pool and pg-boss's maintenance
 * connections keep working through it.
 *
 * A frozen connection forwards nothing in either direction and keeps both of
 * its sockets open: no FIN, no RST, no error. That is a NAT entry expiring or
 * a route dropping, as the client sees it. The proxy destroys every socket
 * when its scope closes.
 */
import net from "node:net";

import { Array as Arr, Effect, MutableRef, Redacted } from "effect";

/** What the proxy does with a connection when its first LISTEN arrives. */
export type OnListen = "forward" | "freeze" | "destroy";

export interface TcpProxy {
    /** The target URL with the proxy's host and port. */
    readonly url: Redacted.Redacted;
    /** Freezes every listener so far, after its LISTEN went through. */
    readonly freezeListeners: () => void;
    /** How many connections have sent a LISTEN. */
    readonly listens: () => number;
}

/** forwarding -> listening (first LISTEN) -> frozen. A destroyed listener stays `listening`. */
type State = "forwarding" | "listening" | "frozen";

interface Connection {
    readonly client: net.Socket;
    readonly upstream: net.Socket;
    readonly state: MutableRef.MutableRef<State>;
}

/** Proxies to the server `target` names; the proxy and its sockets close with the scope. */
export const make = (target: Redacted.Redacted, onListen: OnListen = "forward") =>
    Effect.acquireRelease(
        Effect.callback<
            { readonly server: net.Server; readonly connections: () => ReadonlyArray<Connection> } & TcpProxy
        >((resume) => {
            const targetUrl = new URL(Redacted.value(target));
            const connections = MutableRef.make<ReadonlyArray<Connection>>([]);
            const listeners = () =>
                Arr.filter(MutableRef.get(connections), (c) => MutableRef.get(c.state) !== "forwarding");
            const server = net.createServer({ allowHalfOpen: true }, (client) => {
                const upstream = net.connect({
                    host: targetUrl.hostname,
                    port: Number(targetUrl.port),
                    allowHalfOpen: true,
                });
                const state = MutableRef.make<State>("forwarding");
                MutableRef.update(connections, Arr.append({ client, upstream, state }));
                const frozen = () => MutableRef.get(state) === "frozen";
                client.on("data", (chunk) => {
                    if (MutableRef.get(state) === "forwarding" && chunk.includes("LISTEN ")) {
                        MutableRef.set(state, onListen === "freeze" ? "frozen" : "listening");
                        if (onListen === "destroy") {
                            client.destroy();
                            upstream.destroy();
                            return;
                        }
                    }
                    if (!frozen()) upstream.write(chunk);
                });
                upstream.on("data", (chunk) => frozen() || client.write(chunk));
                client.on("end", () => frozen() || upstream.end());
                upstream.on("end", () => frozen() || client.end());
                client.on("close", () => frozen() || upstream.destroy());
                upstream.on("close", () => frozen() || client.destroy());
                client.on("error", () => {});
                upstream.on("error", () => {});
            });
            server.listen(0, "127.0.0.1", () => {
                // SAFETY: a server listening on a TCP port reports an AddressInfo.
                const { port } = server.address() as net.AddressInfo;
                const url = new URL(targetUrl);
                url.hostname = "127.0.0.1";
                url.port = String(port);
                resume(
                    Effect.succeed({
                        server,
                        connections: () => MutableRef.get(connections),
                        url: Redacted.make(url.toString()),
                        freezeListeners: () => {
                            for (const { state } of listeners()) MutableRef.set(state, "frozen");
                        },
                        listens: () => listeners().length,
                    }),
                );
            });
        }),
        ({ server, connections }) =>
            Effect.sync(() => {
                for (const { client, upstream } of connections()) {
                    client.destroy();
                    upstream.destroy();
                }
                server.close();
            }),
    );
