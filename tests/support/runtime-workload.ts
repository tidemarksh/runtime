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
  const candidates = [
    KERNEL_WASM_PATH,
    ...detectKernelTargetRoots().flatMap((targetDir) => detectKernelWasmCandidates(targetDir)),
  ];
  return uniqueExistingFiles(candidates)[0] ?? null;
}

export function readKernelWasm(): Uint8Array | null {
  const wasmPath = detectKernelWasmPath();
  if (!wasmPath) {
    return logSkip("missing kernel wasm path: set TIDEMARK_TEST_KERNEL_WASM");
  }
  return new Uint8Array(readFileSync(wasmPath));
}

function isDirectory(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

function isFile(path: string): boolean {
  try {
    return statSync(path).isFile();
  } catch {
    return false;
  }
}

function uniqueExistingFiles(candidates: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    const resolved = resolve(candidate);
    if (seen.has(resolved) || !isFile(resolved)) {
      continue;
    }
    seen.add(resolved);
    result.push(resolved);
  }
  return result;
}

function uniqueExistingDirectories(candidates: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    const resolved = resolve(candidate);
    if (seen.has(resolved) || !isDirectory(resolved)) {
      continue;
    }
    seen.add(resolved);
    result.push(resolved);
  }
  return result;
}

function detectKernelTargetRoots(): string[] {
  return uniqueExistingDirectories([
    resolve(process.cwd(), "../kernel/target"),
    resolve(process.cwd(), "kernel/target"),
    resolve(__dirname, "../../../kernel/target"),
  ]);
}

function detectKernelWasmCandidates(targetDir: string): string[] {
  return [
    resolve(targetDir, "wasm64-unknown-unknown/release/tidemark_kernel.wasm"),
    resolve(targetDir, "wasm32-unknown-unknown/release/tidemark_kernel.wasm"),
    resolve(targetDir, "wasm32-wasip1/release/tidemark_kernel.wasm"),
    resolve(targetDir, "release/tidemark_kernel.wasm"),
    resolve(targetDir, "wasm64-unknown-unknown/debug/tidemark_kernel.wasm"),
    resolve(targetDir, "wasm32-unknown-unknown/debug/tidemark_kernel.wasm"),
    resolve(targetDir, "wasm32-wasip1/debug/tidemark_kernel.wasm"),
    resolve(targetDir, "debug/tidemark_kernel.wasm"),
  ];
}

function detectKernelTargetBuildDirs(targetDir: string): string[] {
  const candidates = [
    resolve(targetDir, "release/build"),
    resolve(targetDir, "debug/build"),
  ];
  for (const entry of readdirSync(targetDir)) {
    const entryPath = resolve(targetDir, entry);
    if (!isDirectory(entryPath)) {
      continue;
    }
    candidates.push(resolve(entryPath, "release/build"));
    candidates.push(resolve(entryPath, "debug/build"));
  }
  return uniqueExistingDirectories(candidates);
}

function detectRiscvElfBuildDirs(): string[] {
  const envCandidates = (process.env.TIDEMARK_TEST_RISCV_ELF_BUILD_DIR ?? "")
    .split(delimiter)
    .filter(Boolean);
  const builtArtifactCandidates = detectKernelTargetRoots()
    .flatMap((targetDir) => detectKernelTargetBuildDirs(targetDir));
  return uniqueExistingDirectories([...envCandidates, ...builtArtifactCandidates]);
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
  debugWorkerLogFocus?: () => unknown;
};

function readGuestU64(handle: RuntimeDebugHandle, vmPtr: number, addr: bigint): bigint | null {
  if (!handle.memory?.buffer) {
    return null;
  }
  const guestBase = handle.kernel.thread_guest_base?.(vmPtr) ?? 0;
  const ptr = Number(addr);
  if (!Number.isFinite(ptr) || ptr < 0) {
    return null;
  }
  const absolute = guestBase + ptr;
  if (absolute < 0 || absolute + 8 > handle.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(handle.memory.buffer).getBigUint64(absolute, true);
}

function formatGuestU64(handle: RuntimeDebugHandle, vmPtr: number, addr: bigint): string {
  const value = readGuestU64(handle, vmPtr, addr);
  return value === null ? "n/a" : `0x${value.toString(16)}`;
}

function formatStackPreview(handle: RuntimeDebugHandle, vmPtr: number, sp: bigint): string {
  if (sp === 0n) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = 0; index < 16; index += 1) {
    const offset = BigInt(index * 8);
    entries.push(`+${offset.toString(16)}=${formatGuestU64(handle, vmPtr, sp + offset)}`);
  }
  return `[${entries.join(",")}]`;
}

function formatFrameChain(handle: RuntimeDebugHandle, vmPtr: number, fp: bigint): string {
  if (fp === 0n) {
    return "[]";
  }
  const frames: string[] = [];
  let current = fp;
  for (let index = 0; index < 8; index += 1) {
    if (current < 16n) {
      break;
    }
    const previous = readGuestU64(handle, vmPtr, current - 16n);
    const ra = readGuestU64(handle, vmPtr, current - 8n);
    frames.push(
      `${index}:fp=0x${current.toString(16)}`
      + `:prev=${previous === null ? "n/a" : `0x${previous.toString(16)}`}`
      + `:ra=${ra === null ? "n/a" : `0x${ra.toString(16)}`}`,
    );
    if (previous === null || previous === 0n || previous <= current) {
      break;
    }
    current = previous;
  }
  return `[${frames.join(",")}]`;
}

function readGuestI32(handle: RuntimeDebugHandle, vmPtr: number, addr: bigint): number | null {
  if (!handle.memory?.buffer) {
    return null;
  }
  const guestBase = handle.kernel.thread_guest_base?.(vmPtr) ?? 0;
  const ptr = Number(addr);
  if (!Number.isFinite(ptr) || ptr < 0) {
    return null;
  }
  const absolute = guestBase + ptr;
  if (absolute < 0 || absolute + 4 > handle.memory.buffer.byteLength) {
    return null;
  }
  return new DataView(handle.memory.buffer).getInt32(absolute, true);
}

function formatGuestI32(handle: RuntimeDebugHandle, vmPtr: number, addr: bigint): string {
  const value = readGuestI32(handle, vmPtr, addr);
  return value === null ? "n/a" : value.toString();
}

function formatI32Window(
  handle: RuntimeDebugHandle,
  vmPtr: number,
  center: bigint,
  beforeWords = 4,
  afterWords = 12,
): string {
  if (center === 0n) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = -beforeWords; index <= afterWords; index += 1) {
    const offset = BigInt(index * 4);
    const label = offset < 0n ? offset.toString() : `+${offset.toString()}`;
    entries.push(`${label}=${formatGuestI32(handle, vmPtr, center + offset)}`);
  }
  return `[${entries.join(",")}]`;
}

function formatMemoryVma(handle: RuntimeDebugHandle, index: number): string {
  const start = BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
  const end = BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
  const prot = handle.kernel.debug_memory_vma_prot?.(index) ?? 0;
  const flags = handle.kernel.debug_memory_vma_flags?.(index) ?? 0;
  const fd = handle.kernel.debug_memory_vma_fd?.(index) ?? -1;
  const offset = BigInt(handle.kernel.debug_memory_vma_offset?.(index) ?? 0);
  return `0x${start.toString(16)}-0x${end.toString(16)}:${prot}/${flags}/fd=${fd}/off=0x${offset.toString(16)}`;
}

function findContainingMemoryVmaIndex(handle: RuntimeDebugHandle, addr: bigint): number | null {
  if (addr === 0n) {
    return null;
  }
  const len = handle.kernel.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < len; index += 1) {
    const start = BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
    const end = BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
    if (start <= addr && addr < end) {
      return index;
    }
  }
  return null;
}

function findContainingMemoryVma(handle: RuntimeDebugHandle, addr: bigint): string | null {
  const index = findContainingMemoryVmaIndex(handle, addr);
  return index === null ? null : formatMemoryVma(handle, index);
}

function findAdjacentMemoryVmas(handle: RuntimeDebugHandle, addr: bigint): { prev: string | null; next: string | null } {
  let prev: string | null = null;
  let next: string | null = null;
  if (addr === 0n) {
    return { prev, next };
  }
  const len = handle.kernel.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < len; index += 1) {
    const start = BigInt(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
    const end = BigInt(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
    if (end <= addr) {
      prev = formatMemoryVma(handle, index);
      continue;
    }
    if (addr < start) {
      next = formatMemoryVma(handle, index);
      break;
    }
  }
  return { prev, next };
}

function formatAddressVmaDebug(handle: RuntimeDebugHandle, addr: bigint): string {
  const current = findContainingMemoryVma(handle, addr);
  if (current !== null) {
    return current;
  }
  const adjacent = findAdjacentMemoryVmas(handle, addr);
  return `none:prev=${adjacent.prev ?? "none"}:next=${adjacent.next ?? "none"}`;
}

function formatStackTextCandidates(handle: RuntimeDebugHandle, vmPtr: number, sp: bigint): string {
  if (sp === 0n) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = 0; index < 256; index += 1) {
    const stackOffset = BigInt(index * 8);
    const value = readGuestU64(handle, vmPtr, sp + stackOffset);
    if (value === null || value === 0n) {
      continue;
    }
    const vmaIndex = findContainingMemoryVmaIndex(handle, value);
    if (vmaIndex === null) {
      continue;
    }
    const prot = handle.kernel.debug_memory_vma_prot?.(vmaIndex) ?? 0;
    if ((prot & 4) === 0) {
      continue;
    }
    const start = BigInt(handle.kernel.debug_memory_vma_start?.(vmaIndex) ?? 0);
    const vmaOffset = BigInt(handle.kernel.debug_memory_vma_offset?.(vmaIndex) ?? 0);
    const fileOffset = vmaOffset + (value - start);
    entries.push(
      `+${stackOffset.toString(16)}=0x${value.toString(16)}`
      + `:file=0x${fileOffset.toString(16)}`
      + `:vma=${formatMemoryVma(handle, vmaIndex)}`,
    );
    if (entries.length >= 64) {
      break;
    }
  }
  return `[${entries.join(",")}]`;
}

function formatPointerWords64(handle: RuntimeDebugHandle, vmPtr: number, addr: bigint): string {
  if (addr === 0n || findContainingMemoryVmaIndex(handle, addr) === null) {
    return "[]";
  }
  const entries: string[] = [];
  for (let index = 0; index < 6; index += 1) {
    const offset = BigInt(index * 8);
    entries.push(`+${offset.toString(16)}=${formatGuestU64(handle, vmPtr, addr + offset)}`);
  }
  return `[${entries.join(",")}]`;
}

function formatRegisterPointerWindows(
  handle: RuntimeDebugHandle,
  vmPtr: number,
  regs: Array<[string, bigint]>,
): string {
  const entries = regs
    .filter(([, addr]) => addr !== 0n && findContainingMemoryVmaIndex(handle, addr) !== null)
    .map(([name, addr]) => `${name}=0x${addr.toString(16)}:${formatPointerWords64(handle, vmPtr, addr)}`);
  return `[${entries.join(",")}]`;
}

function formatThreadDebug(handle: RuntimeDebugHandle, vmPtr: number): string {
  const syscallNr = handle.kernel.debug_syscall_trace_nr?.();
  const status = handle.kernel.debug_syscall_trace_status?.();
  const result = handle.kernel.debug_syscall_trace_result?.();
  const tid = handle.kernel.thread_tid?.(vmPtr) ?? 0;
  const ra = handle.kernel.thread_ra?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp(vmPtr);
  const s0 = handle.kernel.thread_reg?.(vmPtr, 8) ?? 0n;
  const s1 = handle.kernel.thread_reg?.(vmPtr, 9) ?? 0n;
  const s2 = handle.kernel.thread_reg?.(vmPtr, 18) ?? 0n;
  const s3 = handle.kernel.thread_reg?.(vmPtr, 19) ?? 0n;
  const s4 = handle.kernel.thread_reg?.(vmPtr, 20) ?? 0n;
  const s5 = handle.kernel.thread_reg?.(vmPtr, 21) ?? 0n;
  const s6 = handle.kernel.thread_reg?.(vmPtr, 22) ?? 0n;
  const s7 = handle.kernel.thread_reg?.(vmPtr, 23) ?? 0n;
  const s8 = handle.kernel.thread_reg?.(vmPtr, 24) ?? 0n;
  const s9 = handle.kernel.thread_reg?.(vmPtr, 25) ?? 0n;
  const s10 = handle.kernel.thread_reg?.(vmPtr, 26) ?? 0n;
  const s11 = handle.kernel.thread_reg?.(vmPtr, 27) ?? 0n;
  const futexWaitAddr = handle.kernel.thread_futex_wait_addr?.(vmPtr) ?? 0n;
  const futexWaitExpected = handle.kernel.thread_futex_wait_expected?.(vmPtr);
  const futexWaitTimeout = handle.kernel.thread_futex_wait_timeout?.(vmPtr) ?? 0n;
  const blockedAddr = handle.kernel.thread_record_blocked_addr?.(vmPtr) ?? 0n;
  return [
    `tid=${tid}`,
    `pc=0x${handle.kernel.thread_pc(vmPtr).toString(16)}`,
    `ra=0x${ra.toString(16)}`,
    `sp=0x${sp.toString(16)}`,
    `s0=0x${s0.toString(16)}`,
    `s1=0x${s1.toString(16)}`,
    `s2=0x${s2.toString(16)}`,
    `s3=0x${s3.toString(16)}`,
    `s4=0x${s4.toString(16)}`,
    `s5=0x${s5.toString(16)}`,
    `s6=0x${s6.toString(16)}`,
    `s7=0x${s7.toString(16)}`,
    `s8=0x${s8.toString(16)}`,
    `s9=0x${s9.toString(16)}`,
    `s10=0x${s10.toString(16)}`,
    `s11=0x${s11.toString(16)}`,
    `a0=${handle.kernel.thread_a0(vmPtr).toString()}`,
    `a1=${handle.kernel.thread_a1(vmPtr).toString()}`,
    `status=${handle.kernel.thread_status(vmPtr)}`,
    `blocked_addr=0x${blockedAddr.toString(16)}`,
    `futex_wait_addr=0x${futexWaitAddr.toString(16)}`,
    futexWaitExpected !== undefined ? `futex_wait_expected=${futexWaitExpected}` : "",
    `futex_wait_timeout=${futexWaitTimeout.toString()}`,
    `futex_word=${futexWaitAddr === 0n ? "n/a" : formatGuestU64(handle, vmPtr, futexWaitAddr)}`,
    `futex_words32=${futexWaitAddr === 0n ? "[]" : formatI32Window(handle, vmPtr, futexWaitAddr)}`,
    `startup_sync_words32=${futexWaitAddr === 0n ? "[]" : formatI32Window(handle, vmPtr, futexWaitAddr - 0x4cn, 0, 24)}`,
    `futex_vma=${futexWaitAddr === 0n ? "n/a" : formatAddressVmaDebug(handle, futexWaitAddr)}`,
    `stack64=${formatStackPreview(handle, vmPtr, sp)}`,
    `stack_text64=${formatStackTextCandidates(handle, vmPtr, sp)}`,
    `reg_ptr64=${formatRegisterPointerWindows(handle, vmPtr, [
      ["s0", s0], ["s1", s1], ["s2", s2], ["s3", s3],
      ["s4", s4], ["s5", s5], ["s6", s6], ["s7", s7],
      ["s8", s8], ["s9", s9], ["s10", s10], ["s11", s11],
    ])}`,
    `frame64=${formatFrameChain(handle, vmPtr, s0)}`,
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

function formatFutexTrace(handle: RuntimeDebugHandle): string {
  const len = handle.kernel.debug_futex_trace_len?.() ?? 0;
  if (len <= 0) {
    return "";
  }
  const entries: string[] = [];
  for (let index = 0; index < len; index += 1) {
    const traceVmPtr = handle.kernel.debug_futex_trace_entry_vm_ptr?.(index) ?? 0;
    const tid = handle.kernel.debug_futex_trace_entry_tid?.(index) ?? 0;
    const pc = handle.kernel.debug_futex_trace_entry_pc?.(index) ?? 0n;
    const ra = handle.kernel.debug_futex_trace_entry_ra?.(index) ?? 0n;
    const sp = handle.kernel.debug_futex_trace_entry_sp?.(index) ?? 0n;
    const s0 = handle.kernel.debug_futex_trace_entry_s0?.(index) ?? 0n;
    const s1 = handle.kernel.debug_futex_trace_entry_s1?.(index) ?? 0n;
    const s2 = handle.kernel.debug_futex_trace_entry_s2?.(index) ?? 0n;
    const s3 = handle.kernel.debug_futex_trace_entry_s3?.(index) ?? 0n;
    const a0 = handle.kernel.debug_futex_trace_entry_a0?.(index) ?? 0n;
    const a1 = handle.kernel.debug_futex_trace_entry_a1?.(index) ?? 0n;
    const a2 = handle.kernel.debug_futex_trace_entry_a2?.(index) ?? 0n;
    const a3 = handle.kernel.debug_futex_trace_entry_a3?.(index) ?? 0n;
    const a4 = handle.kernel.debug_futex_trace_entry_a4?.(index) ?? 0n;
    const a5 = handle.kernel.debug_futex_trace_entry_a5?.(index) ?? 0n;
    const op = handle.kernel.debug_futex_trace_entry_op?.(index) ?? 0;
    const addr = handle.kernel.debug_futex_trace_entry_addr?.(index) ?? 0n;
    const addr2 = handle.kernel.debug_futex_trace_entry_addr2?.(index) ?? 0n;
    const val = handle.kernel.debug_futex_trace_entry_val?.(index) ?? 0n;
    const val3 = handle.kernel.debug_futex_trace_entry_val3?.(index) ?? 0n;
    const current = handle.kernel.debug_futex_trace_entry_current?.(index) ?? 0;
    const local = handle.kernel.debug_futex_trace_entry_local?.(index) ?? 0;
    const external = handle.kernel.debug_futex_trace_entry_external?.(index) ?? 0;
    const requeued = handle.kernel.debug_futex_trace_entry_requeued?.(index) ?? 0;
    const status = handle.kernel.debug_futex_trace_entry_status?.(index) ?? 0;
    const result = handle.kernel.debug_futex_trace_entry_result?.(index);
    entries.push(
      `${index}:vm=0x${traceVmPtr.toString(16)}:tid=${tid}:op=${op}`
      + `:pc=0x${pc.toString(16)}:ra=0x${ra.toString(16)}`
      + `:sp=0x${sp.toString(16)}:s0=0x${s0.toString(16)}`
      + `:s1=0x${s1.toString(16)}:s2=0x${s2.toString(16)}`
      + `:s3=0x${s3.toString(16)}`
      + `:a0=0x${a0.toString(16)}:a1=0x${a1.toString(16)}`
      + `:a2=0x${a2.toString(16)}:a3=0x${a3.toString(16)}`
      + `:a4=0x${a4.toString(16)}:a5=0x${a5.toString(16)}`
      + `:addr=0x${addr.toString(16)}:addr2=0x${addr2.toString(16)}`
      + `:val=${val.toString()}:val3=${val3.toString()}:current=${current}`
      + `:local=${local}:external=${external}:requeued=${requeued}`
      + `:status=${status}:result=${result?.toString() ?? "n/a"}`,
    );
  }
  return entries.join(" ");
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
          `futex_trace=[${formatFutexTrace(handle)}]`,
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
  const workerLogFocus = safeDebugValue(() => {
    const focus = runtimeView.debugWorkerLogFocus?.();
    return focus === undefined ? "" : JSON.stringify(focus);
  });

  return [
    localSummary ? `local:\n${localSummary}` : "",
    internalSummary ? `internal:\n${internalSummary}` : "",
    workerLogFocus ? `worker_log_focus:\n${workerLogFocus}` : "",
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
