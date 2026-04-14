import { expect, test } from "vite-plus/test";
import { existsSync, readFileSync } from "node:fs";
import { Runtime, logSkip, readKernelWasm, spawnWithOutput } from "../support/runtime-workload.js";
import { buildSnapshotFromSources, type SnapshotSource } from "../support/snapshot-sources.js";
import { buildNodeRuntimeSnapshot, readNodeBinary } from "./node/support.js";

const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";

const snapshotSources: readonly SnapshotSource[] = [
  {
    kind: "fileCopies",
    hostPath: busyboxBinary,
    guestPaths: ["/bin/busybox", "/bin/sh", "/bin/echo"],
    mode: 0o755,
  },
  {
    kind: "inlineFile",
    guestPath: "/work/wrapper.sh",
    data: 'exec /bin/echo wrapped-ok\n',
  },
];

test("generic shell-wrapper exec chain: /bin/sh wrapper -> /bin/echo", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!busyboxBinary || !existsSync(busyboxBinary)) {
    return logSkip(`missing busybox binary: ${busyboxBinary || "set TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }

  const snapshot = buildSnapshotFromSources(snapshotSources, {
    missingLabel: "shell wrapper artifact",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(busyboxBinary)),
      ["/bin/sh", "/work/wrapper.sh"],
      [],
      10_000,
    );
    expect(
      result.exitCode,
      [
        "shell-wrapper exec exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain("wrapped-ok");
  } finally {
    runtime.destroy();
  }
}, 20_000);

test("generic shell-wrapper exec chain: /bin/sh wrapper -> /bin/node --help", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!busyboxBinary || !existsSync(busyboxBinary)) {
    return logSkip(`missing busybox binary: ${busyboxBinary || "set TIDEMARK_TEST_BUSYBOX_BINARY"}`);
  }

  const nodeBytes = readNodeBinary();
  const nodeSnapshot = nodeBytes ? buildNodeRuntimeSnapshot(nodeBytes) : null;
  if (!nodeBytes || !nodeSnapshot) {
    return;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 256 * 1024 * 1024,
    maxKernelMemoryPages: 7168,
    maxProcessMemoryPages: 32768,
    maxForkMemoryPages: 40960,
  });
  try {
    await runtime.applyFsSnapshot([
      ...nodeSnapshot,
      { kind: "file", path: "/bin/busybox", data: new Uint8Array(readFileSync(busyboxBinary)), mode: 0o755 },
      { kind: "file", path: "/bin/sh", data: new Uint8Array(readFileSync(busyboxBinary)), mode: 0o755 },
      { kind: "file", path: "/root/.shrc", data: new TextEncoder().encode(""), mode: 0o644 },
      { kind: "file", path: "/work/node-help.sh", data: new TextEncoder().encode("exec /bin/node --help\n"), mode: 0o755 },
    ]);
    const result = await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(busyboxBinary)),
      ["/bin/sh", "/work/node-help.sh"],
      [
        "TERM=xterm",
        "HOME=/root",
        "ENV=/root/.shrc",
        "PATH=/bin:/usr/bin:/usr/local/go/bin",
        "PS1=$ ",
        "GOROOT=/usr/local/go",
        "GOPATH=/tmp/gopath",
        "GOCACHE=/tmp/gocache",
        "GOMAXPROCS=1",
        "GOTELEMETRY=off",
        "GOTELEMETRYDIR=/dev/null",
        "GOGC=off",
        "GOEXPERIMENT=nodwarf5",
        "GOFLAGS=-mod=mod",
        "GONOSUMDB=*",
        "R_HOME=/rhome",
      ],
      30_000,
      { cwd: "/work" },
    );
    expect(
      result.exitCode,
      [
        "shell-wrapper node --help exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain("Usage: node");
  } finally {
    runtime.destroy();
  }
}, 40_000);
