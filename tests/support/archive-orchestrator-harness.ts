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
  type SnapshotBuildOptions,
  type SnapshotSource,
  applySnapshotSourcesToRuntime,
} from "./snapshot-sources.js";

export interface ArchiveOrchestratorArtifactCheck {
  path: string;
  minSize?: number;
  expectedTextSubstrings?: readonly string[];
  expectedBinaryPrefix?: readonly number[];
}

export interface ArchiveOrchestratorStep {
  id: string;
  hostExecutablePath: string;
  guestExecutablePath: string;
  argv: readonly string[];
  env: readonly string[];
  cwd?: string;
  timeoutMs: number;
  expectedOutputSubstrings?: readonly string[];
  forbiddenOutputSubstrings?: readonly string[];
}

export interface ArchiveOrchestratorEntry {
  id: string;
  targetClass: string;
  workflowKind: string;
  guestMemoryPolicyLimitBytes: number;
  hostFileBackingMode?: SnapshotBuildOptions["hostFileBackingMode"];
  snapshotSources: readonly SnapshotSource[];
  steps: readonly ArchiveOrchestratorStep[];
  artifactChecks?: readonly ArchiveOrchestratorArtifactCheck[];
}

export interface ArchiveOrchestratorStepResult {
  exitCode: number;
  output: string;
  debugInfo?: string;
}

export type ArchiveOrchestratorFailureBucket =
  | "foundation"
  | "runtime-ownership"
  | "consumer-runtime"
  | "unknown";

export async function prepareArchiveOrchestratorRuntime(
  entry: ArchiveOrchestratorEntry,
): Promise<RuntimeInstance | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  for (const step of entry.steps) {
    if (!existsSync(step.hostExecutablePath)) {
      return logSkip(`missing archive orchestrator driver: ${step.hostExecutablePath}`);
    }
  }
  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: entry.guestMemoryPolicyLimitBytes,
  });
  try {
    const snapshot = await applySnapshotSourcesToRuntime(runtime, entry.snapshotSources, {
      missingLabel: "archive orchestrator artifact",
      hostFileBackingMode: entry.hostFileBackingMode,
    });
    if (snapshot.skipReason) {
      runtime.destroy();
      return logSkip(snapshot.skipReason);
    }
  } catch (error) {
    runtime.destroy();
    throw error;
  }
  return runtime;
}

export async function runArchiveOrchestratorStep(
  runtime: RuntimeInstance,
  step: ArchiveOrchestratorStep,
): Promise<ArchiveOrchestratorStepResult> {
  try {
    return await spawnWithOutput(
      runtime,
      new Uint8Array(readFileSync(step.hostExecutablePath)),
      [...step.argv],
      [...step.env],
      step.timeoutMs,
      step.cwd ? { cwd: step.cwd } : undefined,
    );
  } catch (error) {
    const summary = await runtimeDebugSummary(runtime);
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${summary}`);
  }
}

export function validateArchiveOrchestratorStepOutput(
  step: ArchiveOrchestratorStep,
  output: string,
): string[] {
  const mismatches: string[] = [];
  for (const expected of step.expectedOutputSubstrings ?? []) {
    if (!output.includes(expected)) {
      mismatches.push(`missing substring: ${expected}`);
    }
  }
  for (const forbidden of step.forbiddenOutputSubstrings ?? []) {
    if (output.includes(forbidden)) {
      mismatches.push(`forbidden substring present: ${forbidden}`);
    }
  }
  return mismatches;
}

async function statArchiveArtifact(
  runtime: RuntimeInstance,
  artifact: ArchiveOrchestratorArtifactCheck,
): Promise<RuntimeStat | null> {
  try {
    return await runtime.fs.stat(artifact.path);
  } catch {
    return null;
  }
}

export async function validateArchiveOrchestratorArtifacts(
  runtime: RuntimeInstance,
  entry: ArchiveOrchestratorEntry,
): Promise<string[]> {
  const mismatches: string[] = [];
  for (const artifact of entry.artifactChecks ?? []) {
    const stat = await statArchiveArtifact(runtime, artifact);
    if (!stat) {
      mismatches.push(`missing artifact: ${artifact.path}`);
      continue;
    }
    if (stat.size < (artifact.minSize ?? 1)) {
      mismatches.push(`artifact too small: ${artifact.path} size=${stat.size}`);
      continue;
    }
    let bytes: Uint8Array;
    try {
      bytes = await runtime.fs.readFile(artifact.path);
    } catch (error) {
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      mismatches.push(`failed to read artifact: ${artifact.path}: ${detail}`);
      continue;
    }
    if (artifact.expectedBinaryPrefix) {
      const actualPrefix = [...bytes.slice(0, artifact.expectedBinaryPrefix.length)];
      const expectedPrefix = [...artifact.expectedBinaryPrefix];
      if (
        actualPrefix.length !== expectedPrefix.length
        || actualPrefix.some((value, index) => value !== expectedPrefix[index])
      ) {
        mismatches.push(
          `artifact prefix mismatch: ${artifact.path} expected=${expectedPrefix.join(",")} actual=${actualPrefix.join(",")}`,
        );
      }
    }
    if (artifact.expectedTextSubstrings?.length) {
      const text = new TextDecoder().decode(bytes);
      for (const expected of artifact.expectedTextSubstrings) {
        if (!text.includes(expected)) {
          mismatches.push(`artifact missing substring: ${artifact.path}: ${expected}`);
        }
      }
    }
  }
  return mismatches;
}

export function bucketArchiveOrchestratorFailure(input: {
  result?: ArchiveOrchestratorStepResult;
  error?: unknown;
}): ArchiveOrchestratorFailureBucket {
  const text = [
    input.result?.output ?? "",
    input.result?.debugInfo ?? "",
    input.error instanceof Error ? (input.error.stack ?? input.error.message) : String(input.error ?? ""),
  ].join("\n");
  if (
    /PT_INTERP|PT_TLS|auxv|inspect_elf|ELF inspection|interpreter fault|missing interpreter|getcpu\(2\) system call not supported by kernel|RISCV_FLUSH_ICACHE not supported/i.test(text)
  ) {
    return "foundation";
  }
  if (
    /process_create|plain-fork-child-ready|child-ready|attach|publish|ownership|kernel-worker|handoff|failed to write file|loadFsSnapshot|Memory\.grow\(\): Maximum memory size exceeded|Invalid atomic access index|thread_count=|process_threads=|thread_slots=\[/i.test(text)
  ) {
    return "runtime-ownership";
  }
  if (
    /ENOENT|missing archive orchestrator artifact|missing archive orchestrator driver|error while loading shared libraries|cannot open shared object file|No such file or directory|not found|failed to read artifact|missing artifact/i.test(text)
  ) {
    return "consumer-runtime";
  }
  if (/Illegal instruction|SIGILL|Segmentation fault|sig=4|sig=11|sig=6|sig_deliver=11|timeout after \d+ms/i.test(text)) {
    return "unknown";
  }
  if (text.length > 0) {
    return "consumer-runtime";
  }
  return "unknown";
}
