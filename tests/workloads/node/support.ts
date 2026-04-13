import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { FsSnapshotEntry } from "../../../src/fs/types.js";
import {
  Runtime,
  type RuntimeInstance,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

export const NODE_BINARY_PATH = process.env.TIDEMARK_TEST_NODE_BINARY ?? "";
export const NODE_SYSROOT_DIR = process.env.TIDEMARK_TEST_NODE_SYSROOT_DIR ?? "";
const FIXTURES_DIR = resolve(__dirname, "./fixtures");
const NODE_RUNTIME_RAM_SIZE = 256 * 1024 * 1024;

const NODE_RUNTIME_LIBS = [
  ["ld-linux-riscv64-lp64d.so.1", "ld-linux-riscv64-lp64d.so.1"],
  ["libc.so.6", "libc.so.6"],
  ["libgcc_s.so.1", "libgcc_s.so.1"],
  ["libm.so.6", "libm.so.6"],
  ["libstdc++.so.6", "libstdc++.so.6.0.34"],
] as const;

export function readNodeBinary(): Uint8Array | null {
  if (!NODE_BINARY_PATH) {
    return logSkip("missing Node binary: set TIDEMARK_TEST_NODE_BINARY");
  }
  if (!existsSync(NODE_BINARY_PATH)) {
    return logSkip(`missing Node binary: ${NODE_BINARY_PATH}`);
  }
  return new Uint8Array(readFileSync(NODE_BINARY_PATH));
}

function readNodeRuntimeLibs(): FsSnapshotEntry[] | null {
  if (!NODE_SYSROOT_DIR) {
    return logSkip("missing Node sysroot dir: set TIDEMARK_TEST_NODE_SYSROOT_DIR");
  }
  if (!existsSync(NODE_SYSROOT_DIR)) {
    return logSkip(`missing Node sysroot dir: ${NODE_SYSROOT_DIR}`);
  }

  const entries: FsSnapshotEntry[] = [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/bin", mode: 0o755 },
    { kind: "dir", path: "/lib", mode: 0o755 },
    { kind: "dir", path: "/tmp", mode: 0o777 },
    { kind: "dir", path: "/work", mode: 0o755 },
  ];
  for (const [guestName, hostName] of NODE_RUNTIME_LIBS) {
    const hostPath = resolve(NODE_SYSROOT_DIR, hostName);
    if (!existsSync(hostPath)) {
      return logSkip(`missing Node runtime lib: ${hostPath}`);
    }
    entries.push({
      kind: "file",
      path: `/lib/${guestName}`,
      data: new Uint8Array(readFileSync(hostPath)),
      mode: 0o755,
    });
  }
  return entries;
}

export function buildNodeRuntimeSnapshot(nodeBytes: Uint8Array): FsSnapshotEntry[] | null {
  const libs = readNodeRuntimeLibs();
  if (!libs) {
    return null;
  }
  return [
    ...libs,
    { kind: "file", path: "/bin/node", data: nodeBytes, mode: 0o755 },
  ];
}

export async function prepareNodeRuntime(): Promise<{
  runtime: RuntimeInstance;
  nodeBytes: Uint8Array;
} | null> {
  const wasmBytes = readKernelWasm();
  const nodeBytes = readNodeBinary();
  const snapshot = nodeBytes ? buildNodeRuntimeSnapshot(nodeBytes) : null;
  if (!wasmBytes || !nodeBytes || !snapshot) {
    return null;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: NODE_RUNTIME_RAM_SIZE,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
  } catch (error) {
    runtime.destroy();
    throw error;
  }

  return { runtime, nodeBytes };
}

export async function runNodeEval(
  runtime: RuntimeInstance,
  nodeBytes: Uint8Array,
  code: string,
  timeoutMs: number,
): Promise<{ exitCode: number; output: string; debugInfo?: string }> {
  try {
    return await spawnWithOutput(
      runtime,
      nodeBytes,
      ["/bin/node", "-e", code],
      [],
      timeoutMs,
    );
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
  }
}

export async function writeNodeFixture(
  runtime: RuntimeInstance,
  path: string,
  data: Uint8Array,
  mode = 0o644,
): Promise<void> {
  const parts = path.split("/").filter(Boolean);
  for (let i = 0; i < parts.length - 1; i += 1) {
    const dirPath = `/${parts.slice(0, i + 1).join("/")}`;
    await runtime.fs.mkdir(dirPath, 0o755).catch(() => {});
  }
  await runtime.fs.writeFile(path, data, mode);
}

export function readNodeFixture(name: string): Uint8Array | null {
  const fixturePath = resolve(FIXTURES_DIR, name);
  if (!existsSync(fixturePath)) {
    return logSkip(`missing Node fixture: ${fixturePath}`);
  }
  return new Uint8Array(readFileSync(fixturePath));
}

export async function runNodeFixture(
  runtime: RuntimeInstance,
  nodeBytes: Uint8Array,
  fixtureName: string,
  timeoutMs: number,
  argv: readonly string[] = [],
): Promise<{ exitCode: number; output: string; debugInfo?: string } | null> {
  const fixture = readNodeFixture(fixtureName);
  if (!fixture) {
    return null;
  }
  await writeNodeFixture(runtime, "/test.js", fixture, 0o644);
  try {
    return await spawnWithOutput(
      runtime,
      nodeBytes,
      ["/bin/node", "/test.js", ...argv],
      [],
      timeoutMs,
    );
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
  }
}
