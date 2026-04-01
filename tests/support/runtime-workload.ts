import { afterAll, beforeAll } from "vite-plus/test";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import type { KernelExports, KernelRuntimeState } from "../../src/abi.js";
import type { FsSnapshotEntry } from "../../src/fs/types.js";
import type { Runtime as RuntimeClass } from "../../src/index.js";

export type RuntimeInstance = RuntimeClass;
export type RuntimeProcess = Awaited<ReturnType<RuntimeInstance["spawn"]>>;
export type SpawnOptions = Parameters<RuntimeInstance["spawn"]>[3];

export const KERNEL_WASM_PATH = resolve(
  __dirname,
  "../../../kernel/target/wasm32-unknown-unknown/release/tidemark_kernel.wasm",
);
export const RUNTIME_DIST_PATH = resolve(__dirname, "../../dist/index.mjs");
export const GO_PATH = resolve(__dirname, "../../../core/images/go");
export const BUSYBOX_PATH = resolve(__dirname, "../../../core/images/busybox");
export const GO_TOOLCHAIN_DIR = resolve(__dirname, "../../../core/images/go-toolchain");
export const GO_EXEC_CAPTURE_HELPER_PATH = resolve(
  __dirname,
  "../workloads/go/fixtures/exec-capture-helper.riscv64",
);
export const SAMPLE_MODULE_DIR = "/work/hello";
export const encoder = new TextEncoder();
export const decoder = new TextDecoder();

const originalWorker = globalThis.Worker;
export let Runtime: typeof import("../../dist/index.mjs").Runtime;

function rewriteRuntimeWorkerUrl(value: string | URL): string | URL {
  if (!(value instanceof URL) || value.protocol !== "file:") {
    return value;
  }
  const workerPath = value.pathname;
  if (existsSync(workerPath)) {
    return value;
  }
  const workerName = workerPath.endsWith("/thread-worker.mjs")
    ? "thread-worker.mjs"
    : workerPath.endsWith("/kernel-worker.mjs")
      ? "kernel-worker.mjs"
      : null;
  if (workerName === null) {
    return value;
  }
  const distPath = resolve(__dirname, "../../dist", workerName);
  return existsSync(distPath) ? pathToFileURL(distPath) : value;
}

beforeAll(async () => {
  globalThis.Worker = class extends Worker {
    constructor(filename: string | URL, options?: WorkerOptions) {
      super(rewriteRuntimeWorkerUrl(filename), options);
    }
  } as unknown as typeof globalThis.Worker;
  ({ Runtime } = await import(RUNTIME_DIST_PATH));
});

afterAll(() => {
  globalThis.Worker = originalWorker;
});

export function logSkip(reason: string): null {
  console.log(`skip runtime workload: ${reason}`);
  return null;
}

export function readKernelWasm(): Uint8Array | null {
  if (!existsSync(KERNEL_WASM_PATH)) {
    return logSkip("kernel release wasm not found");
  }
  return new Uint8Array(readFileSync(KERNEL_WASM_PATH));
}

export function findRiscvElf(name: string): Uint8Array | null {
  const buildDir = resolve(
    __dirname,
    "../../../kernel/target/wasm32-unknown-unknown/release/build",
  );
  if (!existsSync(buildDir)) {
    return logSkip("kernel release build outputs not found");
  }

  for (const entry of readdirSync(buildDir)) {
    const elfPath = resolve(buildDir, entry, "out/rv64ui", `${name}.elf`);
    if (existsSync(elfPath)) {
      return new Uint8Array(readFileSync(elfPath));
    }
  }
  return logSkip(`riscv test ELF not found: ${name}`);
}

function normalizeGuestPath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

function ensureSnapshotDir(
  entries: Map<string, FsSnapshotEntry>,
  path: string,
  mode = 0o755,
): void {
  const normalized = normalizeGuestPath(path);
  if (normalized !== "/") {
    const parent = normalized.split("/").slice(0, -1).join("/") || "/";
    ensureSnapshotDir(entries, parent, 0o755);
  }
  if (!entries.has(normalized)) {
    entries.set(normalized, { kind: "dir", path: normalized, mode });
  }
}

function setSnapshotFile(
  entries: Map<string, FsSnapshotEntry>,
  path: string,
  data: Uint8Array,
  mode = 0o644,
): void {
  const normalized = normalizeGuestPath(path);
  ensureSnapshotDir(entries, normalized.split("/").slice(0, -1).join("/") || "/", 0o755);
  entries.set(normalized, {
    kind: "file",
    path: normalized,
    data,
    mode,
  });
}

function appendHostDirSnapshot(
  entries: Map<string, FsSnapshotEntry>,
  hostDir: string,
  guestDir: string,
): void {
  ensureSnapshotDir(entries, guestDir, 0o755);

  for (const entry of readdirSync(hostDir, { withFileTypes: true })) {
    const hostPath = resolve(hostDir, entry.name);
    const guestPath = `${guestDir}/${entry.name}`;
    if (entry.isSymbolicLink()) {
      continue;
    }
    if (entry.isDirectory()) {
      appendHostDirSnapshot(entries, hostPath, guestPath);
      continue;
    }
    const st = statSync(hostPath);
    const mode = (st.mode & 0o111) !== 0 ? 0o755 : 0o644;
    setSnapshotFile(entries, guestPath, new Uint8Array(readFileSync(hostPath)), mode);
  }
}

export function buildGoRuntimeSnapshot(options: {
  includeToolchain?: boolean;
  includeSampleModule?: boolean;
} = {}): FsSnapshotEntry[] {
  const {
    includeToolchain = true,
    includeSampleModule = true,
  } = options;

  const entries = new Map<string, FsSnapshotEntry>();
  ensureSnapshotDir(entries, "/");
  ensureSnapshotDir(entries, "/usr");
  ensureSnapshotDir(entries, "/usr/local");
  ensureSnapshotDir(entries, "/usr/local/go");
  ensureSnapshotDir(entries, "/usr/local/go/bin");
  ensureSnapshotDir(entries, "/usr/local/go/pkg");
  ensureSnapshotDir(entries, "/usr/local/go/pkg/tool");
  ensureSnapshotDir(entries, "/usr/local/go/pkg/tool/linux_riscv64");
  setSnapshotFile(entries, "/usr/local/go/bin/go", new Uint8Array(readFileSync(GO_PATH)), 0o755);
  setSnapshotFile(entries, "/usr/local/go/go.env", encoder.encode("GOTOOLCHAIN=local\n"), 0o644);

  if (includeToolchain) {
    appendHostDirSnapshot(entries, GO_TOOLCHAIN_DIR, "/usr/local/go");
    setSnapshotFile(entries, "/usr/local/go/bin/go", new Uint8Array(readFileSync(GO_PATH)), 0o755);
    setSnapshotFile(entries, "/usr/local/go/go.env", encoder.encode("GOTOOLCHAIN=local\n"), 0o644);
  }

  if (existsSync(BUSYBOX_PATH)) {
    const busybox = new Uint8Array(readFileSync(BUSYBOX_PATH));
    ensureSnapshotDir(entries, "/bin");
    setSnapshotFile(entries, "/bin/busybox", busybox, 0o755);
    setSnapshotFile(entries, "/bin/sh", busybox, 0o755);
  }

  ensureSnapshotDir(entries, "/root", 0o755);
  ensureSnapshotDir(entries, "/root/.config", 0o755);
  ensureSnapshotDir(entries, "/root/.config/go", 0o755);
  ensureSnapshotDir(entries, "/root/.config/go/telemetry", 0o755);
  setSnapshotFile(
    entries,
    "/root/.config/go/telemetry/mode",
    encoder.encode("off 2000-01-01\n"),
    0o644,
  );
  ensureSnapshotDir(entries, "/tmp", 0o777);
  ensureSnapshotDir(entries, "/tmp/gopath", 0o755);
  ensureSnapshotDir(entries, "/tmp/gocache", 0o755);
  ensureSnapshotDir(entries, "/work");
  if (includeSampleModule) {
    ensureSnapshotDir(entries, SAMPLE_MODULE_DIR);
    setSnapshotFile(
      entries,
      `${SAMPLE_MODULE_DIR}/main.go`,
      encoder.encode('package main\nfunc main() { println("Hello from Go!") }\n'),
      0o644,
    );
    setSnapshotFile(
      entries,
      `${SAMPLE_MODULE_DIR}/go.mod`,
      encoder.encode("module hello\ngo 1.21\n"),
      0o644,
    );
  }

  return [...entries.values()];
}

export async function spawnWithOutput(
  runtime: RuntimeInstance,
  elfBytes: Uint8Array,
  argv: string[],
  env: string[],
  timeoutMs: number,
  options?: SpawnOptions,
): Promise<{ exitCode: number; output: string; debugInfo?: string }> {
  const proc = await runtime.spawn(new Uint8Array(elfBytes), argv, env, options);
  let output = "";
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  proc.onStdout((data) => {
    output += decoder.decode(data);
  });

  try {
    const exitCode = await Promise.race([
      proc.waitForExit(),
      new Promise<never>((_resolve, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs);
        if (typeof timeoutId === "object" && timeoutId !== null && "unref" in timeoutId) {
          timeoutId.unref();
        }
      }),
    ]);
    return { exitCode, output, debugInfo: proc.debugInfo?.() };
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
  }
}

type RuntimeDebugHandle = {
  blockedWorkers?: Map<unknown, { vmPtr: number; status: number }>;
  kernel: KernelExports;
  kernelState?: KernelRuntimeState;
  memory?: WebAssembly.Memory;
  stdinPipe?: number;
  stdoutPipe?: number;
  stderrPipe?: number;
  vmPtr?: number;
  lastDebugMessage?: string;
};

type RuntimeDebugView = RuntimeInstance & {
  processes?: Map<number, RuntimeDebugHandle>;
  sendKernelRequest?: (message: { type: "debug-processes" }) => Promise<{
    pids: number[];
    canonicalPids: number[];
  }>;
  debugSnapshot?: () => string;
};

function formatThreadDebug(handle: RuntimeDebugHandle, vmPtr: number): string {
  const syscallNr = handle.kernel.debug_syscall_trace_nr?.();
  const status = handle.kernel.debug_syscall_trace_status?.();
  const result = handle.kernel.debug_syscall_trace_result?.();
  return [
    `tid=${handle.kernel.thread_tid(vmPtr)}`,
    `pc=0x${handle.kernel.thread_pc(vmPtr).toString(16)}`,
    `ra=0x${handle.kernel.thread_ra(vmPtr).toString(16)}`,
    `sp=0x${handle.kernel.thread_sp(vmPtr).toString(16)}`,
    `a0=${handle.kernel.thread_a0(vmPtr).toString()}`,
    `a1=${handle.kernel.thread_a1(vmPtr).toString()}`,
    `status=${handle.kernel.thread_status(vmPtr)}`,
    syscallNr !== undefined ? `syscall=${syscallNr.toString()}` : "",
    status !== undefined ? `sys_status=${status}` : "",
    result !== undefined ? `sys_result=${result.toString()}` : "",
  ].filter(Boolean).join(" ");
}

export async function runtimeDebugSummary(runtime: RuntimeInstance): Promise<string> {
  const runtimeView = runtime as RuntimeDebugView;
  const processes = runtimeView.processes;
  const localSummary = !(processes instanceof Map) || processes.size === 0
    ? ""
    : [...processes.entries()]
      .map(([pid, handle]) => {
        const pipeSummary = (() => {
          if (!handle.memory?.buffer || !handle.kernelState) {
            return "";
          }
          const pipeSlots = new Set<number>();
          for (let fd = 0; fd < 16; fd += 1) {
            const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr ?? 0, fd);
            if (ofdId === 0 || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
              continue;
            }
            pipeSlots.add(handle.kernel.kernel_ofd_ino(ofdId));
          }
          const i32 = new Int32Array(handle.memory.buffer);
          const u32 = new Uint32Array(handle.memory.buffer);
          return [...pipeSlots]
            .sort((left, right) => left - right)
            .map((slot) => {
              const base = handle.kernelState!.pipeBase + slot * (32 + 4096);
              return [
                `${slot}`,
                `ro=${Atomics.load(i32, (base + 8) >>> 2)}`,
                `wo=${Atomics.load(i32, (base + 12) >>> 2)}`,
                `head=${Atomics.load(u32, base >>> 2)}`,
                `tail=${Atomics.load(u32, (base + 4) >>> 2)}`,
              ].join(":");
            })
            .join(" ");
        })();
        const blocked = handle.blockedWorkers instanceof Map
          ? [...handle.blockedWorkers.values()].map((state) => {
              const vmPtr = state.vmPtr;
              const fd = handle.kernel.thread_blocked_fd?.(vmPtr) ?? -1;
              const ofdId = handle.kernel.thread_blocked_ofd_id?.(vmPtr) ?? 0;
              const ofdType = handle.kernel.kernel_ofd_fd_type?.(ofdId) ?? 0;
              const ofdIno = handle.kernel.kernel_ofd_ino?.(ofdId) ?? 0;
              return `${state.status}:vm=0x${vmPtr.toString(16)}:fd=${fd}:ofd=${ofdId}:type=${ofdType}:ino=${ofdIno}`;
            }).join(",")
          : "";
        const debug = handle.lastDebugMessage ?? "n/a";
        const vmPtr = handle.vmPtr ?? 0;
        const threadSummary = vmPtr !== 0
          ? Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
              const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
              if (threadVmPtr === 0) {
                return null;
              }
              return `${slot}:${formatThreadDebug(handle, threadVmPtr)}`;
            }).filter(Boolean).join(" | ")
          : "";
        const fdSummary = vmPtr !== 0
          ? Array.from({ length: 16 }, (_value, fd) => {
              const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
              if (ofdId === 0) {
                return null;
              }
              const type = handle.kernel.kernel_ofd_fd_type(ofdId);
              const ino = handle.kernel.kernel_ofd_ino(ofdId);
              const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
              return `${fd}->${ofdId}/${type}/${ino}/${hostFd}`;
            }).filter(Boolean).join(" ")
          : "";
        const ofdSummary = Array.from({ length: 16 }, (_value, ofdId) => {
          if (ofdId === 0) {
            return null;
          }
          const type = handle.kernel.kernel_ofd_fd_type(ofdId);
          if (type === 0) {
            return null;
          }
          const ino = handle.kernel.kernel_ofd_ino(ofdId);
          const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
          const refcount = handle.kernel.kernel_ofd_refcount(ofdId);
          return `${ofdId}:${type}/${ino}/${hostFd}/rc=${refcount}`;
        }).filter(Boolean).join(" ");
        return [
          `pid=${pid}`,
          `stdin=${handle.stdinPipe ?? 0}`,
          `stdout=${handle.stdoutPipe ?? 0}`,
          `stderr=${handle.stderrPipe ?? 0}`,
          `blocked=[${blocked}]`,
          `threads=[${threadSummary}]`,
          `fds=[${fdSummary}]`,
          `ofds=[${ofdSummary}]`,
          `pipes=[${pipeSummary}]`,
          `debug=${debug}`,
        ].join(" ");
      })
      .join("\n");

  const kernelSummary = await runtimeView.sendKernelRequest?.({
    type: "debug-processes",
  }).then((response) => (
    `kernel_pids=[${response.pids.join(",")}] canonical_pids=[${response.canonicalPids.join(",")}]`
  )).catch(() => "");

  const internalSummary = runtimeView.debugSnapshot?.() ?? "";

  return [
    localSummary ? `local:\n${localSummary}` : "",
    internalSummary ? `internal:\n${internalSummary}` : "",
    kernelSummary,
  ].filter(Boolean).join("\n");
}

export async function spawnWithOutputOrDebug(
  runtime: RuntimeInstance,
  elfBytes: Uint8Array,
  argv: string[],
  env: string[],
  timeoutMs: number,
  options?: SpawnOptions,
): Promise<{ exitCode: number; output: string; debugInfo?: string }> {
  try {
    return await spawnWithOutput(runtime, elfBytes, argv, env, timeoutMs, options);
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${await runtimeDebugSummary(runtime)}`);
  }
}

export async function prepareGoRuntime(options: {
  includeToolchain?: boolean;
  includeSampleModule?: boolean;
} = {}): Promise<{ runtime: RuntimeInstance; goBytes: Uint8Array } | null> {
  const {
    includeToolchain = true,
    includeSampleModule = true,
  } = options;
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!existsSync(GO_PATH)) {
    return logSkip(`missing Go binary: ${GO_PATH}`);
  }
  if (includeToolchain && !existsSync(GO_TOOLCHAIN_DIR)) {
    return logSkip(`missing Go toolchain dir: ${GO_TOOLCHAIN_DIR}`);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 512 * 1024 * 1024,
  });
  await runtime.applyFsSnapshot(buildGoRuntimeSnapshot({
    includeToolchain,
    includeSampleModule,
  }));

  return {
    runtime,
    goBytes: new Uint8Array(readFileSync(GO_PATH)),
  };
}

export function goEnv(gomaxprocs: number): string[] {
  const env = [
    "GOROOT=/usr/local/go",
    "GOPATH=/tmp/gopath",
    "GOCACHE=/tmp/gocache",
    "PATH=/usr/local/go/bin:/bin",
    `GOMAXPROCS=${gomaxprocs}`,
    "GOTELEMETRY=off",
    "GOTELEMETRYDIR=/dev/null",
    "GONOSUMDB=*",
    "GOFLAGS=-mod=mod",
    "HOME=/root",
    "GOGC=off",
    "GOEXPERIMENT=nodwarf5",
  ];
  if (process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GODEBUG) {
    env.push(`GODEBUG=${process.env.TIDEMARK_ACCEPTANCE_GO_BUILD_GODEBUG}`);
  }
  return env;
}

export function goVersionEnv(gomaxprocs: number): string[] {
  return [
    "GOROOT=/usr/local/go",
    `GOMAXPROCS=${gomaxprocs}`,
  ];
}

export function readGoExecCaptureHelperBinary(): Uint8Array | null {
  if (!existsSync(GO_EXEC_CAPTURE_HELPER_PATH)) {
    return logSkip(`missing Go exec capture helper: ${GO_EXEC_CAPTURE_HELPER_PATH}`);
  }
  return new Uint8Array(readFileSync(GO_EXEC_CAPTURE_HELPER_PATH));
}

export function readGoToolBinary(relativePath: string): Uint8Array | null {
  const absolutePath = resolve(GO_TOOLCHAIN_DIR, relativePath);
  if (!existsSync(absolutePath)) {
    return logSkip(`missing Go tool binary: ${absolutePath}`);
  }
  return new Uint8Array(readFileSync(absolutePath));
}
