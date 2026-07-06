import { existsSync, readFileSync } from "node:fs";
import {
  Runtime,
  type RuntimeInstance,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
} from "./runtime-workload.js";
import {
  type SnapshotBuildOptions,
  type SnapshotSource,
  applySnapshotSourcesToRuntime,
  buildSnapshotFromSources,
} from "./snapshot-sources.js";

export type ScriptRunnerKind = "direct-exec" | "shell-script";

export interface ScriptRuntimeEntry {
  id: string;
  targetClass: string;
  runnerKind: ScriptRunnerKind;
  hostExecutablePath: string;
  guestExecutablePath: string;
  argv: readonly string[];
  env: readonly string[];
  timeoutMs: number;
  stdin?: "pipe" | "closed";
  guestMemoryPolicyLimitBytes: number;
  hostFileBackingMode?: SnapshotBuildOptions["hostFileBackingMode"];
  snapshotSources: readonly SnapshotSource[];
  expectedSubstrings: readonly string[];
  forbiddenSubstrings?: readonly string[];
}

export interface ScriptRuntimeResult {
  exitCode: number;
  output: string;
  debugInfo?: string;
}

export type ScriptRuntimeFailureBucket =
  | "foundation"
  | "runtime-ownership"
  | "consumer-runtime"
  | "unknown";

const SCRIPT_RUNTIME_TIMEOUT_ENV = "TIDEMARK_TEST_SCRIPT_RUNTIME_TIMEOUT_MS";
const SCRIPT_RUNTIME_TEST_TIMEOUT_ENV = "TIDEMARK_TEST_SCRIPT_RUNTIME_TEST_TIMEOUT_MS";

function readPositiveIntegerEnv(name: string): number | null {
  const raw = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.[name];
  const parsed = raw === undefined ? NaN : Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

export function resolveScriptRuntimeTimeoutMs(entry: ScriptRuntimeEntry): number {
  return readPositiveIntegerEnv(SCRIPT_RUNTIME_TIMEOUT_ENV) ?? entry.timeoutMs;
}

export function resolveScriptRuntimeTestTimeoutMs(
  entry: ScriptRuntimeEntry,
  prepareHeadroomMs = 60_000,
): number {
  const timeoutMs = resolveScriptRuntimeTimeoutMs(entry);
  const explicitTestTimeoutMs = readPositiveIntegerEnv(SCRIPT_RUNTIME_TEST_TIMEOUT_ENV);
  if (explicitTestTimeoutMs !== null) {
    return Math.max(20_000, explicitTestTimeoutMs);
  }
  const marginMs = timeoutMs === entry.timeoutMs ? prepareHeadroomMs : 180_000;
  return Math.max(20_000, timeoutMs + marginMs);
}

export function buildScriptRuntimeSnapshot(entry: ScriptRuntimeEntry) {
  const result = buildSnapshotFromSources(entry.snapshotSources, {
    missingLabel: "script runtime artifact",
  });
  if (result.skipReason) {
    return logSkip(result.skipReason);
  }
  return result.entries ?? null;
}

export async function prepareScriptRuntime(
  entry: ScriptRuntimeEntry,
): Promise<{ runtime: RuntimeInstance; elfBytes: Uint8Array } | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!existsSync(entry.hostExecutablePath)) {
    return logSkip(`missing script runtime executable: ${entry.hostExecutablePath}`);
  }
  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: entry.guestMemoryPolicyLimitBytes,
  });
  try {
    const applied = await applySnapshotSourcesToRuntime(runtime, entry.snapshotSources, {
      missingLabel: "script runtime artifact",
      hostFileBackingMode: entry.hostFileBackingMode,
    });
    if (applied.skipReason) {
      runtime.destroy();
      return logSkip(applied.skipReason);
    }
  } catch (error) {
    runtime.destroy();
    throw error;
  }

  return {
    runtime,
    elfBytes: new Uint8Array(readFileSync(entry.hostExecutablePath)),
  };
}

export async function runScriptRuntimeEntry(
  runtime: RuntimeInstance,
  entry: ScriptRuntimeEntry,
  elfBytes: Uint8Array,
): Promise<ScriptRuntimeResult> {
  const proc = await runtime.spawn(
    new Uint8Array(elfBytes),
    [...entry.argv],
    [...entry.env],
    { stdin: entry.stdin ?? "closed" },
  );
  let output = "";
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  const timeoutMs = resolveScriptRuntimeTimeoutMs(entry);
  proc.onStdout((data) => {
    output += new TextDecoder().decode(data);
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
  } catch (error) {
    const summary = await runtimeDebugSummary(runtime);
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${summary}`);
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
  }
}

export function validateScriptRuntimeOutput(
  entry: ScriptRuntimeEntry,
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

export function bucketScriptRuntimeFailure(input: {
  result?: ScriptRuntimeResult;
  error?: unknown;
}): ScriptRuntimeFailureBucket {
  const text = [
    input.result?.output ?? "",
    input.result?.debugInfo ?? "",
    input.error instanceof Error ? (input.error.stack ?? input.error.message) : String(input.error ?? ""),
  ].join("\n");
  if (/PT_INTERP|PT_TLS|auxv|ELF inspection|interpreter fault|missing interpreter|getcpu\(2\) system call not supported by kernel/i.test(text)) {
    return "foundation";
  }
  if (
    /process_create|plain-fork-child-ready|child-ready|attach|publish|ownership|kernel-worker|handoff|failed to write file|loadFsSnapshot|Memory\.grow\(\): Maximum memory size exceeded/i.test(text)
  ) {
    return "runtime-ownership";
  }
  if (
    /missing script runtime artifact|missing script runtime executable|cannot open shared object file|error while loading shared libraries|Can't locate|cannot find system Renviron|No such file or directory|LoadError|ModuleNotFoundError|UndefinedFunctionError/i.test(text)
  ) {
    return "consumer-runtime";
  }
  if (
    /Illegal instruction|SIGILL|Segmentation fault|Exec format error|sig=4|sig=11|sig=6|timeout after \d+ms/i.test(text)
  ) {
    return "unknown";
  }
  if (text.length > 0) {
    return "consumer-runtime";
  }
  return "unknown";
}
