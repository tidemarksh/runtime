import { afterAll, beforeAll, expect } from "vite-plus/test";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { delimiter, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import type { KernelExports, KernelRuntimeState } from "../../src/abi.js";
import type { Runtime as RuntimeClass } from "../../src/index.js";

export type RuntimeInstance = RuntimeClass;
export type RuntimeProcess = Awaited<ReturnType<RuntimeInstance["spawn"]>>;
export type SpawnOptions = Parameters<RuntimeInstance["spawn"]>[3];

export const KERNEL_WASM_PATH = process.env.TIDEMARK_TEST_KERNEL_WASM ?? "";
export const RUNTIME_DIST_PATH = resolve(__dirname, "../../dist/index.mjs");
export const RUNTIME_WORKER_DIST_DIR = resolve(__dirname, "../../dist");
export const encoder = new TextEncoder();
export const decoder = new TextDecoder();
const TIMEOUT_OUTPUT_PREVIEW_BYTES = 4096;

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
  const distPath = resolve(RUNTIME_WORKER_DIST_DIR, workerName);
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

function isIntegrationRun(): boolean {
  if ((process.env.npm_lifecycle_event ?? "").startsWith("test:integration")) {
    return true;
  }
  const testPath = ((expect.getState() as { testPath?: string }).testPath ?? "").replaceAll("\\", "/");
  if (testPath.includes("/tests/integration/") || testPath.startsWith("tests/integration/")) {
    return true;
  }
  const args = process.argv.map((arg) => arg.replaceAll("\\", "/"));
  return args.some((arg) => (
    arg === "tests/integration"
    || arg.startsWith("tests/integration/")
    || arg.endsWith("/tests/integration")
    || arg.includes("/tests/integration/")
  ));
}

export function logSkip(reason: string): null {
  const integrationRun = isIntegrationRun();
  if (integrationRun) {
    console.log(`integration requirement missing: ${reason}`);
    throw new Error(`integration requirement missing: ${reason}`);
  }
  console.log(`test input unavailable: ${reason}`);
  return null;
}

function detectKernelWasmPath(): string | null {
  return KERNEL_WASM_PATH && existsSync(KERNEL_WASM_PATH) ? KERNEL_WASM_PATH : null;
}

export function readKernelWasm(): Uint8Array | null {
  const wasmPath = detectKernelWasmPath();
  if (!wasmPath) {
    return logSkip("missing kernel wasm path: set TIDEMARK_TEST_KERNEL_WASM");
  }
  return new Uint8Array(readFileSync(wasmPath));
}

function detectRiscvElfBuildDirs(): string[] {
  const candidates = (process.env.TIDEMARK_TEST_RISCV_ELF_BUILD_DIR ?? "")
    .split(delimiter)
    .filter(Boolean);
  return candidates.filter((candidate, index) => (
    candidate
    && existsSync(candidate)
    && statSync(candidate).isDirectory()
    && candidates.indexOf(candidate) === index
  ));
}

function findRiscvElfPath(buildDir: string, name: string): string | null {
  for (const entry of readdirSync(buildDir)) {
    const outDir = resolve(buildDir, entry, "out");
    if (!existsSync(outDir)) {
      continue;
    }
    for (const suite of readdirSync(outDir)) {
      const elfPath = resolve(outDir, suite, `${name}.elf`);
      if (existsSync(elfPath)) {
        return elfPath;
      }
    }
  }
  return null;
}

export function findRiscvElf(name: string): Uint8Array | null {
  const buildDirs = detectRiscvElfBuildDirs();
  if (buildDirs.length === 0) {
    return logSkip("missing riscv elf build dir: set TIDEMARK_TEST_RISCV_ELF_BUILD_DIR");
  }
  for (const buildDir of buildDirs) {
    const elfPath = findRiscvElfPath(buildDir, name);
    if (elfPath) {
      return new Uint8Array(readFileSync(elfPath));
    }
  }
  return logSkip(`riscv test ELF not found: ${name}`);
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
        timeoutId = setTimeout(() => {
          const preview = output.length > TIMEOUT_OUTPUT_PREVIEW_BYTES
            ? `${output.slice(0, TIMEOUT_OUTPUT_PREVIEW_BYTES)}<truncated ${output.length - TIMEOUT_OUTPUT_PREVIEW_BYTES} chars>`
            : output;
          reject(new Error([
            `timeout after ${timeoutMs}ms`,
            preview ? `capturedOutput=${JSON.stringify(preview)}` : "",
          ].filter(Boolean).join("\n")));
        }, timeoutMs);
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
  const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const futexWaitAddr = handle.kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n;
  const futexWaitExpected = handle.kernel.thread_futex_wait_expected?.(vmPtr);
  const futexWaitTimeout = handle.kernel.thread_futex_wait_timeout?.(vmPtr) ?? 0n;
  const blockedAddr = handle.kernel.thread_record_blocked_addr?.(vmPtr) ?? 0n;
  return [
    `tid=${tid}`,
    `pc=0x${handle.kernel.thread_pc(vmPtr).toString(16)}`,
    `ra=0x${ra.toString(16)}`,
    `sp=0x${handle.kernel.thread_sp(vmPtr).toString(16)}`,
    `a0=${handle.kernel.thread_a0(vmPtr).toString()}`,
    `a1=${handle.kernel.thread_a1(vmPtr).toString()}`,
    `status=${handle.kernel.thread_status(vmPtr)}`,
    `blocked_addr=0x${blockedAddr.toString(16)}`,
    `futex_wait_addr=0x${futexWaitAddr.toString(16)}`,
    futexWaitExpected !== undefined ? `futex_wait_expected=${futexWaitExpected}` : "",
    `futex_wait_timeout=${futexWaitTimeout.toString()}`,
    syscallNr !== undefined ? `syscall=${syscallNr.toString()}` : "",
    status !== undefined ? `sys_status=${status}` : "",
    result !== undefined ? `sys_result=${result.toString()}` : "",
  ].filter(Boolean).join(" ");
}

function formatSyscallTrace(handle: RuntimeDebugHandle): string {
  const len = handle.kernel.debug_syscall_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_syscall_trace_entry_vm_ptr?.(index) ?? 0;
    const nr = handle.kernel.debug_syscall_trace_entry_nr?.(index);
    const status = handle.kernel.debug_syscall_trace_entry_status?.(index) ?? 0;
    const result = handle.kernel.debug_syscall_trace_entry_result?.(index);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:nr=${nr?.toString() ?? "n/a"}:status=${status}:result=${result?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
}

function formatSelectedSyscallProfile(handle: RuntimeDebugHandle): string {
  const count = handle.kernel.debug_syscall_profile_count;
  if (!count) {
    return "";
  }
  const nonOk = handle.kernel.debug_syscall_profile_non_ok_status_count;
  const error = handle.kernel.debug_syscall_profile_error_count;
  const selected = [
    ["exit_group", 94],
    ["futex", 98],
    ["clone", 220],
    ["execve", 221],
    ["epoll_create1", 20],
    ["epoll_ctl", 21],
    ["epoll_pwait", 22],
    ["wait4", 260],
    ["futex_time64", 422],
    ["clone3", 435],
  ] as const;
  return selected
    .map(([name, nr]) => (
      `${name}/${nr}=${count(nr).toString()}/${nonOk?.(nr).toString() ?? "0"}/${error?.(nr).toString() ?? "0"}`
    ))
    .join(" ");
}

function formatExecveStageTrace(handle: RuntimeDebugHandle): string {
  const len = handle.kernel.debug_execve_stage_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const stage = handle.kernel.debug_execve_stage_trace_stage?.(index) ?? 0;
    const value0 = handle.kernel.debug_execve_stage_trace_value0?.(index) ?? 0n;
    const value1 = handle.kernel.debug_execve_stage_trace_value1?.(index) ?? 0n;
    entries.push(`${stage}:${value0.toString(16)}:${value1.toString(16)}`);
  }
  return entries.join(" ");
}

function safeDebugValue(read: () => string): string {
  try {
    return read();
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return `debug-error:${detail}`;
  }
}

export async function runtimeDebugSummary(runtime: RuntimeInstance): Promise<string> {
  const runtimeView = runtime as RuntimeDebugView;
  const processes = runtimeView.processes;
  const localSummary = !(processes instanceof Map) || processes.size === 0
    ? ""
    : [...processes.entries()]
      .map(([pid, handle]) => {
        const pipeSummary = safeDebugValue(() => {
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
        });
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
          ? safeDebugValue(() => Array.from({ length: handle.kernel.abi_max_threads() }, (_value, slot) => {
              const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
              if (threadVmPtr === 0) {
                return null;
              }
              return `${slot}:${formatThreadDebug(handle, threadVmPtr)}`;
            }).filter(Boolean).join(" | "))
          : "";
        const fdSummary = vmPtr !== 0
          ? safeDebugValue(() => Array.from({ length: 16 }, (_value, fd) => {
              const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
              if (ofdId === 0) {
                return null;
              }
              const type = handle.kernel.kernel_ofd_fd_type(ofdId);
              const ino = handle.kernel.kernel_ofd_ino(ofdId);
              const hostFd = handle.kernel.kernel_ofd_host_fd(ofdId);
              return `${fd}->${ofdId}/${type}/${ino}/${hostFd}`;
            }).filter(Boolean).join(" "))
          : "";
        const ofdSummary = safeDebugValue(() => Array.from({ length: 16 }, (_value, ofdId) => {
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
        }).filter(Boolean).join(" "));
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
          `execve_stage_trace=[${formatExecveStageTrace(handle)}]`,
          `syscalls=[${formatSyscallTrace(handle)}]`,
          `syscall_profile=[${formatSelectedSyscallProfile(handle)}]`,
        ].join(" ");
      })
      .join("\n");

  const kernelSummary = await Promise.race([
    runtimeView.sendKernelRequest?.({
      type: "debug-processes",
    }).then((response) => (
      `kernel_pids=[${response.pids.join(",")}] canonical_pids=[${response.canonicalPids.join(",")}]`
    )) ?? Promise.resolve(""),
    new Promise<string>((resolve) => {
      setTimeout(() => resolve("kernel_debug=timeout"), 1_000);
    }),
  ]).catch(() => "");

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
