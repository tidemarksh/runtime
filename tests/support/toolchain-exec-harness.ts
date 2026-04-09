import { existsSync, readFileSync } from "node:fs";
import type { RuntimeStat } from "../../src/fs/types.js";
import {
  Runtime,
  type RuntimeInstance,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "./runtime-workload.js";
import {
  type SnapshotSource,
  buildSnapshotFromSources,
} from "./snapshot-sources.js";

export interface ToolchainArtifactCheck {
  path: string;
  minSize?: number;
}

export interface ToolchainRunCheck {
  path: string;
  argv?: readonly string[];
  env?: readonly string[];
  timeoutMs: number;
  expectedSubstrings: readonly string[];
  forbiddenSubstrings?: readonly string[];
}

export interface ToolchainExecEntry {
  id: string;
  targetClass: string;
  workflowKind: string;
  hostExecutablePath: string;
  guestExecutablePath: string;
  argv: readonly string[];
  env: readonly string[];
  cwd?: string;
  timeoutMs: number;
  ramSize: number;
  snapshotSources: readonly SnapshotSource[];
  expectedDriverSubstrings?: readonly string[];
  forbiddenDriverSubstrings?: readonly string[];
  artifactChecks: readonly ToolchainArtifactCheck[];
  runChecks?: readonly ToolchainRunCheck[];
}

export interface ToolchainExecResult {
  exitCode: number;
  output: string;
  debugInfo?: string;
}

export interface ToolchainRunResult extends ToolchainExecResult {
  path: string;
}

type RuntimeProcessDebugView = RuntimeInstance & {
  processes?: Map<number, { lastDebugMessage?: string }>;
};

export type ToolchainExecFailureBucket =
  | "foundation"
  | "runtime-ownership"
  | "consumer-runtime"
  | "unknown";

export async function prepareToolchainExecRuntime(
  entry: ToolchainExecEntry,
): Promise<{ runtime: RuntimeInstance; executableBytes: Uint8Array } | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!existsSync(entry.hostExecutablePath)) {
    return logSkip(`missing toolchain exec driver: ${entry.hostExecutablePath}`);
  }
  const snapshot = buildSnapshotFromSources(entry.snapshotSources, {
    missingLabel: "toolchain exec artifact",
  });
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: entry.ramSize,
  });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
  } catch (error) {
    runtime.destroy();
    throw error;
  }

  return {
    runtime,
    executableBytes: new Uint8Array(readFileSync(entry.hostExecutablePath)),
  };
}

export async function runToolchainExecEntry(
  runtime: RuntimeInstance,
  entry: ToolchainExecEntry,
  executableBytes: Uint8Array,
): Promise<ToolchainExecResult> {
  const runtimeView = runtime as RuntimeProcessDebugView;
  let markerTimer: ReturnType<typeof setTimeout> | null = null;
  let markerDone = false;
  const markerPromise = new Promise<never>((_resolve, reject) => {
    const poll = (): void => {
      if (markerDone) {
        return;
      }
      const processes = runtimeView.processes;
      if (processes instanceof Map) {
        for (const handle of processes.values()) {
          const detail = handle?.lastDebugMessage ?? "";
          if (
            detail.includes("vfork-execve:handoff-failed")
            || detail.includes("vfork-execve:handoff-trap")
          ) {
            markerDone = true;
            reject(new Error(detail));
            return;
          }
        }
      }
      markerTimer = setTimeout(poll, 25);
      if (typeof markerTimer === "object" && markerTimer !== null && "unref" in markerTimer) {
        markerTimer.unref();
      }
    };
    poll();
  });
  try {
    return await Promise.race([
      spawnWithOutput(
        runtime,
        executableBytes,
        [...entry.argv],
        [...entry.env],
        entry.timeoutMs,
        entry.cwd ? { cwd: entry.cwd } : undefined,
      ),
      markerPromise,
    ]);
  } catch (error) {
    const summary = await runtimeDebugSummary(runtime);
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${summary}`);
  } finally {
    markerDone = true;
    if (markerTimer !== null) {
      clearTimeout(markerTimer);
    }
  }
}

export function validateToolchainDriverOutput(
  entry: ToolchainExecEntry,
  output: string,
): string[] {
  const mismatches: string[] = [];
  for (const expected of entry.expectedDriverSubstrings ?? []) {
    if (!output.includes(expected)) {
      mismatches.push(`missing substring: ${expected}`);
    }
  }
  for (const forbidden of entry.forbiddenDriverSubstrings ?? []) {
    if (output.includes(forbidden)) {
      mismatches.push(`forbidden substring present: ${forbidden}`);
    }
  }
  return mismatches;
}

async function statToolchainArtifact(
  runtime: RuntimeInstance,
  artifact: ToolchainArtifactCheck,
): Promise<RuntimeStat | null> {
  try {
    return await runtime.fs.stat(artifact.path);
  } catch {
    return null;
  }
}

export async function validateToolchainArtifacts(
  runtime: RuntimeInstance,
  entry: ToolchainExecEntry,
): Promise<string[]> {
  const mismatches: string[] = [];
  for (const artifact of entry.artifactChecks) {
    const stat = await statToolchainArtifact(runtime, artifact);
    if (!stat) {
      mismatches.push(`missing artifact: ${artifact.path}`);
      continue;
    }
    if (stat.size < (artifact.minSize ?? 1)) {
      mismatches.push(`artifact too small: ${artifact.path} size=${stat.size}`);
    }
  }
  return mismatches;
}

export async function runToolchainArtifactChecks(
  runtime: RuntimeInstance,
  entry: ToolchainExecEntry,
): Promise<ToolchainRunResult[]> {
  const results: ToolchainRunResult[] = [];
  for (const check of entry.runChecks ?? []) {
    let elfBytes: Uint8Array;
    try {
      elfBytes = await runtime.fs.readFile(check.path);
    } catch (error) {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`missing generated artifact: ${check.path}\n${detail}`);
    }
    const runArgv = check.argv ? [...check.argv] : [check.path];
    const result = await spawnWithOutput(
      runtime,
      elfBytes,
      runArgv,
      [...(check.env ?? [])],
      check.timeoutMs,
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error(`${detail}\n${summary}`);
    });
    results.push({
      path: check.path,
      exitCode: result.exitCode,
      output: result.output,
      debugInfo: result.debugInfo,
    });
  }
  return results;
}

export function validateToolchainRunOutput(
  check: ToolchainRunCheck,
  output: string,
): string[] {
  const mismatches: string[] = [];
  for (const expected of check.expectedSubstrings) {
    if (!output.includes(expected)) {
      mismatches.push(`missing substring: ${expected}`);
    }
  }
  for (const forbidden of check.forbiddenSubstrings ?? []) {
    if (output.includes(forbidden)) {
      mismatches.push(`forbidden substring present: ${forbidden}`);
    }
  }
  return mismatches;
}

export function bucketToolchainExecFailure(input: {
  result?: ToolchainExecResult;
  error?: unknown;
}): ToolchainExecFailureBucket {
  const text = [
    input.result?.output ?? "",
    input.result?.debugInfo ?? "",
    input.error instanceof Error ? (input.error.stack ?? input.error.message) : String(input.error ?? ""),
  ].join("\n");
  if (
    /PT_INTERP|PT_TLS|auxv|inspect_elf|ELF inspection|interpreter fault|missing interpreter|getcpu\(2\) system call not supported by kernel|RISCV_FLUSH_ICACHE not supported|system call not supported by kernel/i.test(text)
  ) {
    return "foundation";
  }
  if (
    /process_create|plain-fork-child-ready|child-ready|attach|publish|ownership|kernel-worker|handoff|failed to write file|loadFsSnapshot|Memory\.grow\(\): Maximum memory size exceeded|Invalid atomic access index|thread_count=|process_threads=|thread_slots=\[/i.test(text)
  ) {
    return "runtime-ownership";
  }
  if (
    /ENOENT|missing toolchain exec artifact|missing toolchain exec driver|error while loading shared libraries|cannot open shared object file|No such file or directory|not found|missing generated artifact/i.test(text)
  ) {
    return "consumer-runtime";
  }
  if (/Illegal instruction|SIGILL|timeout after \d+ms/i.test(text)) {
    return "unknown";
  }
  if (text.length > 0) {
    return "consumer-runtime";
  }
  return "unknown";
}
