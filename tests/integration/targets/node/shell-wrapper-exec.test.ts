import { existsSync, readFileSync } from "node:fs";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../../support/runtime-workload.js";
import { buildNodeRuntimeSnapshot, readNodeBinary } from "./support.js";

const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";

test("node shell-wrapper exec chain: /bin/sh wrapper -> /bin/node --help", async () => {
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
    guestMemoryPolicyLimitBytes: 2 * 1024 * 1024 * 1024,
    kernelWorkerMaxMemoryPages: 7168,
    maxProcessMemoryPages: 40960,
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
        "PATH=/bin:/usr/bin",
        "PS1=$ ",
      ],
      120_000,
      { cwd: "/work" },
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
    });
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
}, 140_000);
