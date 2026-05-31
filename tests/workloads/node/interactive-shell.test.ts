import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import type { FsSnapshotEntry } from "../../../src/fs/types.js";
import {
  Runtime,
  decoder,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
} from "../../support/runtime-workload.js";
import { buildNodeRuntimeSnapshot, readNodeBinary } from "./support.js";

const BUSYBOX_BINARY_PATH = process.env.TIDEMARK_TEST_BUSYBOX_BINARY
  ?? "";
const GO_BINARY_PATH = process.env.TIDEMARK_TEST_GO_BINARY
  ?? "";

function readBytes(path: string, label: string): Uint8Array | null {
  if (!existsSync(path)) {
    return logSkip(`missing ${label}: ${path}`);
  }
  return new Uint8Array(readFileSync(path));
}

function demoShellEnv(): string[] {
  return [
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
  ];
}

function demoShellRc(): Uint8Array {
  return new TextEncoder().encode([
    "alias cat='/bin/busybox cat'",
    "alias env='/bin/busybox env'",
    "alias head='/bin/busybox head'",
    "alias ls='/bin/busybox ls'",
    "alias ls2='/bin/busybox ls'",
    "alias mkdir='/bin/busybox mkdir'",
    "alias rm='/bin/busybox rm'",
    "alias sleep='/bin/busybox sleep'",
    "alias lua='/usr/bin/lua5.4'",
    "alias php='/usr/bin/php8.4'",
    "alias python='/usr/bin/python3.12'",
    "alias perl='/usr/bin/perl'",
    "alias elixir='/usr/bin/elixir'",
    "alias erl='/usr/bin/erl'",
    "alias R='/usr/bin/R'",
    "alias uname='/bin/busybox uname'",
    "",
  ].join("\n"));
}

function buildInteractiveShellSnapshot(
  busyboxBytes: Uint8Array,
  nodeBytes: Uint8Array,
  goBytes?: Uint8Array,
): FsSnapshotEntry[] | null {
  const nodeSnapshot = buildNodeRuntimeSnapshot(nodeBytes);
  if (!nodeSnapshot) {
    return null;
  }
  const entries: FsSnapshotEntry[] = [
    ...nodeSnapshot,
    { kind: "dir", path: "/root", mode: 0o755 },
    { kind: "dir", path: "/root/.config", mode: 0o755 },
    { kind: "dir", path: "/root/.config/go", mode: 0o755 },
    { kind: "dir", path: "/root/.config/go/telemetry", mode: 0o755 },
    { kind: "dir", path: "/tmp/gopath", mode: 0o755 },
    { kind: "dir", path: "/tmp/gocache", mode: 0o755 },
    { kind: "dir", path: "/usr", mode: 0o755 },
    { kind: "dir", path: "/usr/local", mode: 0o755 },
    { kind: "dir", path: "/usr/local/go", mode: 0o755 },
    { kind: "dir", path: "/usr/local/go/bin", mode: 0o755 },
    { kind: "file", path: "/bin/busybox", data: busyboxBytes, mode: 0o755 },
    { kind: "file", path: "/bin/sh", data: busyboxBytes, mode: 0o755 },
    { kind: "file", path: "/root/.shrc", data: demoShellRc(), mode: 0o644 },
    { kind: "file", path: "/root/.config/go/telemetry/mode", data: new TextEncoder().encode("off 2000-01-01\n"), mode: 0o644 },
  ];
  if (goBytes) {
    entries.push(
      { kind: "file", path: "/usr/local/go/bin/go", data: goBytes, mode: 0o755 },
      { kind: "file", path: "/usr/local/go/go.env", data: new TextEncoder().encode("GOTOOLCHAIN=local\n"), mode: 0o644 },
      { kind: "file", path: "/usr/local/go/VERSION", data: new TextEncoder().encode("go1.26.1\n"), mode: 0o644 },
    );
  }
  return entries;
}

async function waitForOutput(
  outputRef: { value: string },
  predicate: (output: string) => boolean,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate(outputRef.value)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timeout waiting for ${label}`);
}

async function startShell(runtime: Runtime, busyboxBytes: Uint8Array) {
  const proc = await runtime.spawn(
    busyboxBytes,
    ["/bin/sh", "-i"],
    demoShellEnv(),
    { cwd: "/work" },
  );
  const outputRef = { value: "" };
  proc.onStdout((data) => {
    outputRef.value += decoder.decode(data);
  });
  await waitForOutput(outputRef, (output) => output.includes("$ "), 10_000, "shell prompt");
  return { proc, outputRef };
}

test("Runtime interactive shell executes node -v with demo-style PATH lookup from char-by-char stdin", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readNodeBinary();
  const snapshot = busyboxBytes && nodeBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes, ramSize: 384 * 1024 * 1024 });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const { proc, outputRef } = await startShell(runtime, busyboxBytes);
    for (const ch of "node -v\r") {
      proc.writeStdin(new TextEncoder().encode(ch));
    }

    await waitForOutput(
      outputRef,
      (output) => /\nv\d+\.\d+\.\d+\r?\n|\nv\d+\.\d+\.\d+\n/.test(output),
      10_000,
      "node version output",
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, `output=${JSON.stringify(outputRef.value)}`, await runtimeDebugSummary(runtime)].join("\n"));
    });

    expect(outputRef.value).toMatch(/v\d+\.\d+\.\d+/);
    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("Runtime interactive shell executes go version with demo-style PATH lookup", async () => {
  const wasmBytes = readKernelWasm();
  const busyboxBytes = readBytes(BUSYBOX_BINARY_PATH, "BusyBox binary");
  const nodeBytes = readNodeBinary();
  const goBytes = readBytes(GO_BINARY_PATH, "Go binary");
  const snapshot = busyboxBytes && nodeBytes && goBytes
    ? buildInteractiveShellSnapshot(busyboxBytes, nodeBytes, goBytes)
    : null;
  if (!wasmBytes || !busyboxBytes || !nodeBytes || !goBytes || !snapshot) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes, ramSize: 384 * 1024 * 1024 });
  try {
    await runtime.applyFsSnapshot(snapshot);
    const { proc, outputRef } = await startShell(runtime, busyboxBytes);
    proc.writeStdin(new TextEncoder().encode("go version\n"));

    await waitForOutput(
      outputRef,
      (output) => output.includes("go version"),
      10_000,
      "go version output",
    ).catch(async (error) => {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, `output=${JSON.stringify(outputRef.value)}`, await runtimeDebugSummary(runtime)].join("\n"));
    });

    expect(outputRef.value).toContain("go version");
    proc.writeStdin(new TextEncoder().encode("exit\n"));
    await expect(proc.waitForExit()).resolves.toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);
