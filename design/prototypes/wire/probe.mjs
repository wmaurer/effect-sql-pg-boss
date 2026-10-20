import net from "node:net";
const sock = net.connect(Number(process.argv[2]), "127.0.0.1");
const msg = (type, body) => {
    const b = Buffer.alloc(5 + body.length);
    b[0] = type.charCodeAt(0);
    b.writeInt32BE(4 + body.length, 1);
    body.copy(b, 5);
    return b;
};
const cstr = (s) => Buffer.concat([Buffer.from(s, "utf8"), Buffer.from([0])]);
const i16 = (n) => {
    const b = Buffer.alloc(2);
    b.writeInt16BE(n);
    return b;
};
const i32 = (n) => {
    const b = Buffer.alloc(4);
    b.writeInt32BE(n);
    return b;
};
const startup = () => {
    const body = Buffer.concat([
        i32(196608),
        cstr("user"),
        cstr("postgres"),
        cstr("database"),
        cstr("postgres"),
        Buffer.from([0]),
    ]);
    return Buffer.concat([i32(body.length + 4), body]);
};
let buf = Buffer.alloc(0),
    waiters = [];
const log = [];
sock.on("data", (d) => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
        if (buf.length < 5) break;
        const len = buf.readInt32BE(1);
        if (buf.length < len + 1) break;
        const t = String.fromCharCode(buf[0]);
        const body = buf.subarray(5, len + 1);
        buf = buf.subarray(len + 1);
        let info = "";
        if (t === "E")
            info = body
                .toString("utf8")
                .split("\0")
                .filter((f) => f[0] === "C" || f[0] === "M")
                .join(" | ");
        if (t === "C") info = body.toString("utf8", 0, body.length - 1);
        if (t === "Z") info = String.fromCharCode(body[0]);
        if (t === "T") {
            const n = body.readInt16BE(0);
            let o = 2;
            const f = [];
            for (let i = 0; i < n; i++) {
                const e = body.indexOf(0, o);
                const name = body.toString("utf8", o, e);
                o = e + 1;
                const oid = body.readInt32BE(o + 6);
                const fmt = body.readInt16BE(o + 16);
                o += 18;
                f.push(`${name}:oid${oid}:fmt${fmt}`);
            }
            info = f.join(",");
        }
        if (t === "D") {
            const n = body.readInt16BE(0);
            let o = 2;
            const v = [];
            for (let i = 0; i < n; i++) {
                const l = body.readInt32BE(o);
                o += 4;
                v.push(l < 0 ? null : body.toString("utf8", o, o + l));
                o += Math.max(l, 0);
            }
            info = JSON.stringify(v);
        }
        if (t !== "S" && t !== "K" && t !== "R") log.push(`${t}${info ? " " + info : ""}`);
        if (t === "Z") {
            const w = waiters.shift();
            w && w();
        }
    }
});
const rfq = () => new Promise((r) => waiters.push(r));
const run = async (label, frames) => {
    log.length = 0;
    sock.write(Buffer.concat(frames));
    await rfq();
    console.log(`== ${label}\n  ` + log.join("\n  "));
};
sock.write(startup());
await rfq();
await run("extended unnamed Parse of 2 statements", [
    msg("P", Buffer.concat([cstr(""), cstr("select 1; select 2"), i16(0)])),
    msg("B", Buffer.concat([cstr(""), cstr(""), i16(0), i16(0), i16(1), i16(1)])),
    msg("D", Buffer.concat([Buffer.from("P"), cstr("")])),
    msg("E", Buffer.concat([cstr(""), i32(0)])),
    msg("S", Buffer.alloc(0)),
]);
await run("simple Query, 3 statements (insert, select, notify)", [
    msg(
        "Q",
        cstr(
            "create temp table t(id int8, j jsonb); insert into t values (1, '{\"a\":1}');\nselect * from t;\nselect pg_notify('x','y')",
        ),
    ),
]);
await run("BEGIN via simple", [msg("Q", cstr("begin"))]);
await run("simple Query in txn, error in 2nd statement", [
    msg("Q", cstr("insert into t values (2, null); select 1/0; select 3")),
]);
await run("simple Query empty", [msg("Q", cstr(""))]);
await run("simple after extended cycle in same write (pipelined)", [
    msg("P", Buffer.concat([cstr(""), cstr("select 7"), i16(0)])),
    msg("B", Buffer.concat([cstr(""), cstr(""), i16(0), i16(0), i16(1), i16(1)])),
    msg("D", Buffer.concat([Buffer.from("P"), cstr("")])),
    msg("E", Buffer.concat([cstr(""), i32(0)])),
    msg("S", Buffer.alloc(0)),
    msg("Q", cstr("rollback; select 8")),
]);
await rfq().catch(() => {}).then?.call;
console.log("  (second RFQ of pipelined write) " + log.join(" ; "));
sock.end(msg("X", Buffer.alloc(0)));
