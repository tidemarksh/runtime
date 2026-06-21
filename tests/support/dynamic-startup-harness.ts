import { existsSync, readFileSync } from "node:fs";
import {
  Runtime,
  type RuntimeInstance,
  decoder,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  testStageDebug,
} from "./runtime-workload.js";
import {
  type SnapshotBuildOptions,
  type SnapshotSource,
  applySnapshotSourcesToRuntime,
  buildSnapshotFromSources,
} from "./snapshot-sources.js";

export type DynamicRunnerKind = "direct-exec" | "shell-script";

export interface DynamicStartupRuntimeOptions {
  guestMemoryPolicyLimitBytes?: number;
  kernelWorkerMaxMemoryPages?: number;
  maxProcessMemoryPages?: number;
  maxForkMemoryPages?: number;
}

export interface DynamicStartupEntry {
  id: string;
  targetClass: string;
  runnerKind: DynamicRunnerKind;
  hostExecutablePath: string;
  guestExecutablePath: string;
  argv: readonly string[];
  env: readonly string[];
  timeoutMs: number;
  stdio?: "pipe" | "pty";
  guestMemoryPolicyLimitBytes: number;
  runtimeOptions?: DynamicStartupRuntimeOptions;
  hostFileBackingMode?: SnapshotBuildOptions["hostFileBackingMode"];
  diagnosticArtifactPaths?: readonly string[];
  snapshotSources: readonly SnapshotSource[];
  expectedSubstrings: readonly string[];
  forbiddenSubstrings?: readonly string[];
}

export interface DynamicStartupResult {
  exitCode: number;
  output: string;
  debugInfo?: string;
}

interface GuestWriteWatchRegion {
  slot: number;
  start: number;
  end: number;
}

const DYNAMIC_STARTUP_TIMEOUT_ENV = "TIDEMARK_TEST_DYNAMIC_STARTUP_TIMEOUT_MS";
const DYNAMIC_STARTUP_TEST_TIMEOUT_ENV = "TIDEMARK_TEST_DYNAMIC_STARTUP_TEST_TIMEOUT_MS";
const TIMEOUT_OUTPUT_PREVIEW_BYTES = 4096;

export type DynamicStartupFailureBucket =
  | "foundation"
  | "runtime-ownership"
  | "consumer-runtime"
  | "unknown";

export type DynamicStartupFailureStatus =
  | "foundation-suspected"
  | "runtime-ownership-suspected"
  | "consumer-runtime-suspected"
  | "unclassified";

export function buildDynamicStartupSnapshot(entry: DynamicStartupEntry) {
  const result = buildSnapshotFromSources(entry.snapshotSources, {
    missingLabel: "dynamic startup artifact",
  });
  if (result.skipReason) {
    return logSkip(result.skipReason);
  }
  return result.entries ?? null;
}

function readPositiveIntegerEnv(name: string): number | null {
  const raw = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.[name];
  const parsed = raw === undefined ? NaN : Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

export function resolveDynamicStartupTimeoutMs(entry: DynamicStartupEntry): number {
  const overrideMs = readPositiveIntegerEnv(DYNAMIC_STARTUP_TIMEOUT_ENV);
  if (overrideMs !== null) {
    return overrideMs;
  }
  return entry.timeoutMs;
}

export function resolveDynamicStartupTestTimeoutMs(entry: DynamicStartupEntry): number {
  const timeoutMs = resolveDynamicStartupTimeoutMs(entry);
  const explicitTestTimeoutMs = readPositiveIntegerEnv(DYNAMIC_STARTUP_TEST_TIMEOUT_ENV);
  if (explicitTestTimeoutMs !== null) {
    return Math.max(20_000, explicitTestTimeoutMs);
  }
  const marginMs = timeoutMs === entry.timeoutMs ? 15_000 : 180_000;
  return Math.max(20_000, timeoutMs + marginMs);
}

function parseAddress(value: string): number | null {
  try {
    const parsed = BigInt(value);
    if (parsed < 0n || parsed > BigInt(Number.MAX_SAFE_INTEGER)) {
      return null;
    }
    return Number(parsed);
  } catch {
    return null;
  }
}

export function parseDebugGuestWriteWatchRegions(raw: string | undefined): GuestWriteWatchRegion[] {
  if (!raw || raw.trim() === "") {
    return [];
  }
  const regions: GuestWriteWatchRegion[] = [];
  for (const part of raw.split(/[\s,]+/)) {
    if (part.length === 0) {
      continue;
    }
    const [startRaw, endRaw, extra] = part.split(":");
    if (!startRaw || !endRaw || extra !== undefined) {
      continue;
    }
    const start = parseAddress(startRaw);
    const end = parseAddress(endRaw);
    if (start === null || end === null || end <= start) {
      continue;
    }
    regions.push({ slot: regions.length, start, end });
    if (regions.length >= 2) {
      break;
    }
  }
  return regions;
}

async function logDynamicStartupArtifactDiagnostics(
  runtime: RuntimeInstance,
  entry: DynamicStartupEntry,
): Promise<void> {
  for (const path of entry.diagnosticArtifactPaths ?? []) {
    try {
      const [stat, data] = await Promise.all([
        runtime.fs.stat(path),
        runtime.fs.readFile(path),
      ]);
      testStageDebug(
        `dynamic-startup:prepare:artifact path=${path} statSize=${stat.size} readBytes=${data.byteLength}`,
      );
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      testStageDebug(`dynamic-startup:prepare:artifact path=${path} error=${JSON.stringify(detail)}`);
    }
  }
}

export async function prepareDynamicStartupRuntime(
  entry: DynamicStartupEntry,
): Promise<{ runtime: RuntimeInstance; elfBytes: Uint8Array } | null> {
  testStageDebug(`dynamic-startup:prepare:start id=${entry.id}`);
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    testStageDebug(`dynamic-startup:prepare:skip-missing-wasm id=${entry.id}`);
    return null;
  }
  if (!existsSync(entry.hostExecutablePath)) {
    testStageDebug(`dynamic-startup:prepare:skip-missing-executable id=${entry.id}`);
    return logSkip(`missing dynamic startup executable: ${entry.hostExecutablePath}`);
  }
  testStageDebug(`dynamic-startup:prepare:runtime-create:start id=${entry.id}`);
  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: entry.guestMemoryPolicyLimitBytes,
    ...entry.runtimeOptions,
  });
  testStageDebug(`dynamic-startup:prepare:runtime-create:done id=${entry.id}`);
  try {
    testStageDebug(`dynamic-startup:prepare:snapshot:start id=${entry.id}`);
    const applied = await applySnapshotSourcesToRuntime(runtime, entry.snapshotSources, {
      missingLabel: "dynamic startup artifact",
      hostFileBackingMode: entry.hostFileBackingMode,
    });
    if (applied.skipReason) {
      runtime.destroy();
      testStageDebug(`dynamic-startup:prepare:snapshot:skip id=${entry.id}`);
      return logSkip(applied.skipReason);
    }
    testStageDebug(`dynamic-startup:prepare:snapshot:done id=${entry.id}`);
    await logDynamicStartupArtifactDiagnostics(runtime, entry);
  } catch (error) {
    runtime.destroy();
    testStageDebug(`dynamic-startup:prepare:snapshot:error id=${entry.id}`);
    throw error;
  }

  testStageDebug(`dynamic-startup:prepare:read-elf id=${entry.id}`);
  return {
    runtime,
    elfBytes: new Uint8Array(readFileSync(entry.hostExecutablePath)),
  };
}

export async function runDynamicStartupEntry(
  runtime: RuntimeInstance,
  entry: DynamicStartupEntry,
  elfBytes: Uint8Array,
): Promise<DynamicStartupResult> {
  let proc: Awaited<ReturnType<RuntimeInstance["spawn"]>> | null = null;
  let output = "";
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  const timeoutMs = resolveDynamicStartupTimeoutMs(entry);

  try {
    testStageDebug(`dynamic-startup:timeout-arm timeoutMs=${timeoutMs}`);
    const timeoutPromise = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(() => {
        testStageDebug(`dynamic-startup:timeout outputBytes=${output.length}`);
        const preview = output.length > TIMEOUT_OUTPUT_PREVIEW_BYTES
          ? `${output.slice(0, TIMEOUT_OUTPUT_PREVIEW_BYTES)}<truncated ${output.length - TIMEOUT_OUTPUT_PREVIEW_BYTES} chars>`
          : output;
        reject(new Error([
          `timeout after ${timeoutMs}ms`,
          preview ? `capturedOutput=${JSON.stringify(preview)}` : "",
        ].filter(Boolean).join("\n")));
      }, timeoutMs);
    });
    const runPromise = (async (): Promise<DynamicStartupResult> => {
      testStageDebug("dynamic-startup:spawn:start");
      const spawnOptions: Parameters<RuntimeInstance["spawn"]>[3] = {
        guestExecutablePath: entry.guestExecutablePath,
      };
      if (entry.stdio !== undefined) {
        spawnOptions.stdio = entry.stdio;
      }
      proc = await runtime.spawn(new Uint8Array(elfBytes), [...entry.argv], [...entry.env], spawnOptions);
      testStageDebug(`dynamic-startup:spawn:ready pid=${proc.pid}`);
      proc.onStdout((data) => {
        output += decoder.decode(data);
      });
      testStageDebug(`dynamic-startup:wait:start pid=${proc.pid}`);
      const exitCode = await proc.waitForExit();
      testStageDebug(`dynamic-startup:wait:exit pid=${proc.pid} code=${exitCode}`);
      const debugParts: string[] = [];
      const baseDebug = proc.debugInfo?.();
      if (baseDebug) {
        debugParts.push(baseDebug);
      }
      return { exitCode, output, debugInfo: debugParts.join(" ") || undefined };
    })();
    runPromise.catch(() => undefined);
    return await Promise.race([runPromise, timeoutPromise]);
  } catch (error) {
    const summary = await runtimeDebugSummary(runtime);
    const procDebug = proc?.debugInfo?.();
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error([detail, procDebug, summary].filter(Boolean).join("\n"));
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
  }
}

export function validateDynamicStartupOutput(
  entry: DynamicStartupEntry,
  output: string,
): string[] {
  const mismatches: string[] = [];
  for (const expected of entry.expectedSubstrings) {
    if (!output.includes(expected)) {
      mismatches.push(`missing substring: ${expected}`);
    }
  }
  for (const forbidden of entry.forbiddenSubstrings ?? []) {
    if (output.includes(forbidden)) {
      mismatches.push(`forbidden substring present: ${forbidden}`);
    }
  }
  return mismatches;
}

function hasGuestFatalSignalEvidence(text: string): boolean {
  return (
    /A fatal error has been detected by the Java Runtime Environment|Illegal instruction|SIGILL|Segmentation fault|SIGSEGV|debug_collect_error=/i.test(text)
    || /\bsig(?:_deliver)?=(?:4|6|11)\b/i.test(text)
    || /\bterm[_-]?signal=(?:4|6|11)\b/i.test(text)
    || /\btermSignal["']?\s*[:=]\s*(?:4|6|11)\b/i.test(text)
    || /\b(?:child-)?exit:\d+:"[^"]*":\d+:(?:4|6|11)\b/.test(text)
    || /\bexit=\d+:(?:4|6|11)\b/.test(text)
  );
}

export function bucketDynamicStartupFailure(input: {
  result?: DynamicStartupResult;
  error?: unknown;
}): DynamicStartupFailureBucket {
  const text = [
    input.result?.output ?? "",
    input.result?.debugInfo ?? "",
    input.error instanceof Error ? (input.error.stack ?? input.error.message) : String(input.error ?? ""),
  ].join("\n");
  if (/PT_INTERP|PT_TLS|auxv|inspect_elf|ELF inspection|interpreter fault|missing interpreter|getcpu\(2\) system call not supported by kernel/i.test(text)) {
    return "foundation";
  }
  if (
    /ENOENT|realpathSync|lstatSync|missing dynamic startup artifact|missing dynamic startup executable|error while loading shared libraries|cannot open shared object file/i.test(text)
  ) {
    return "consumer-runtime";
  }
  if (hasGuestFatalSignalEvidence(text)) {
    return "unknown";
  }
  if (
    /timeout after \d+ms/i.test(text)
    && (
      /\bstatus=6\b|\bSTATUS_FS\b/i.test(text)
      || /fs_lifecycle_tail=.*op=\d+\/(?:READ|MMAP)/is.test(text)
      || /fs-start:.*op=\d+\/(?:READ|MMAP)/is.test(text)
    )
  ) {
    return "runtime-ownership";
  }
  if (
    /timeout after \d+ms/i.test(text)
    && (
      /\bSTATUS_PIPE_WAIT\b/i.test(text)
      || /\bstatus=21\b/.test(text)
      || /\bkernelStatus=21\b/.test(text)
      || /\blast=21\b/.test(text)
      || /blocked=\[21@/.test(text)
    )
  ) {
    return "runtime-ownership";
  }
  if (
    /timeout after \d+ms/i.test(text)
    && (
      /"ownerState":"wait:20"/.test(text)
      || /\bowner=wait:kernelStatus=20\b/.test(text)
      || /"waitingInWorkerStatus":20/.test(text)
    )
    && (
      /FUTEX_WAIT/.test(text)
      || /\bstatus=20\b/.test(text)
      || /\bkernelStatus=20\b/.test(text)
      || /\blast=20\b/.test(text)
      || /thread-futex-wait:/i.test(text)
    )
    && (
      /"ownerState":"(?:running|ready|blocked|async:\d+)"/.test(text)
      || /\bowner=(?:running|ready|blocked|async:\d+)\b/.test(text)
    )
  ) {
    return "runtime-ownership";
  }
  if (hasGuestFatalSignalEvidence(text) || /timeout after \d+ms/i.test(text)) {
    return "unknown";
  }
  if (
    /process_create|plain-fork-child-ready|child-ready|attach|publish|ownership|kernel-worker|handoff|failed to write file|loadFsSnapshot|Memory\.grow\(\): Maximum memory size exceeded/i.test(text)
  ) {
    return "runtime-ownership";
  }
  if (text.length > 0) {
    return "consumer-runtime";
  }
  return "unknown";
}

export function classifyDynamicStartupFailureStatus(input: {
  result?: DynamicStartupResult;
  error?: unknown;
}): DynamicStartupFailureStatus {
  const bucket = bucketDynamicStartupFailure(input);
  if (bucket === "foundation") {
    return "foundation-suspected";
  }
  if (bucket === "runtime-ownership") {
    return "runtime-ownership-suspected";
  }
  if (bucket === "consumer-runtime") {
    return "consumer-runtime-suspected";
  }
  return "unclassified";
}
