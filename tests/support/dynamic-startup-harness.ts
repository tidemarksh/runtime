import { existsSync, readFileSync } from "node:fs";
import {
  Runtime,
  type RuntimeInstance,
  decoder,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
} from "./runtime-workload.js";
import {
  type SnapshotSource,
  buildSnapshotFromSources,
} from "./snapshot-sources.js";

export type DynamicRunnerKind = "direct-exec" | "shell-script";

export interface DynamicStartupEntry {
  id: string;
  targetClass: string;
  runnerKind: DynamicRunnerKind;
  hostExecutablePath: string;
  guestExecutablePath: string;
  argv: readonly string[];
  env: readonly string[];
  timeoutMs: number;
  ramSize: number;
  snapshotSources: readonly SnapshotSource[];
  expectedSubstrings: readonly string[];
  forbiddenSubstrings?: readonly string[];
}

export interface DynamicStartupResult {
  exitCode: number;
  output: string;
  debugInfo?: string;
}

export type DynamicStartupFailureBucket =
  | "foundation"
  | "runtime-ownership"
  | "consumer-runtime"
  | "unknown";

export function buildDynamicStartupSnapshot(entry: DynamicStartupEntry) {
  const result = buildSnapshotFromSources(entry.snapshotSources, {
    missingLabel: "dynamic startup artifact",
  });
  if (result.skipReason) {
    return logSkip(result.skipReason);
  }
  return result.entries ?? null;
}

export async function prepareDynamicStartupRuntime(
  entry: DynamicStartupEntry,
): Promise<{ runtime: RuntimeInstance; elfBytes: Uint8Array } | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!existsSync(entry.hostExecutablePath)) {
    return logSkip(`missing dynamic startup executable: ${entry.hostExecutablePath}`);
  }
  const snapshot = buildDynamicStartupSnapshot(entry);
  if (!snapshot) {
    return null;
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: entry.ramSize,
  });
  try {
    await runtime.applyFsSnapshot(snapshot);
  } catch (error) {
    runtime.destroy();
    throw error;
  }

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
  const proc = await runtime.spawn(new Uint8Array(elfBytes), [...entry.argv], [...entry.env]);
  let output = "";
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  proc.onStdout((data) => {
    output += decoder.decode(data);
  });

  try {
    const exitCode = await Promise.race([
      proc.waitForExit(),
      new Promise<never>((_resolve, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`timeout after ${entry.timeoutMs}ms`)), entry.timeoutMs);
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
  if (/Illegal instruction|SIGILL|Segmentation fault|sig=4|sig=6|sig=11|sig_deliver=11|:\d+:11\b|debug_collect_error=|timeout after \d+ms/i.test(text)) {
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
