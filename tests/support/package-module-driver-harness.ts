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

export interface PackageModuleArtifactCheck {
  path: string;
  minSize?: number;
  expectedTextSubstrings?: readonly string[];
  expectedBinaryPrefix?: readonly number[];
}

export interface PackageModuleDriverEntry {
  id: string;
  targetClass: string;
  workflowKind: string;
  hostExecutablePath: string;
  guestExecutablePath: string;
  argv: readonly string[];
  env: readonly string[];
  cwd?: string;
  timeoutMs: number;
  guestMemoryPolicyLimitBytes: number;
  hostFileBackingMode?: SnapshotBuildOptions["hostFileBackingMode"];
  snapshotSources: readonly SnapshotSource[];
  expectedDriverSubstrings?: readonly string[];
  forbiddenDriverSubstrings?: readonly string[];
  artifactChecks?: readonly PackageModuleArtifactCheck[];
}

export interface PackageModuleDriverResult {
  exitCode: number;
  output: string;
  debugInfo?: string;
}

export type PackageModuleDriverFailureBucket =
  | "foundation"
  | "runtime-ownership"
  | "consumer-runtime"
  | "unknown";

export async function preparePackageModuleDriverRuntime(
  entry: PackageModuleDriverEntry,
): Promise<{ runtime: RuntimeInstance; executableBytes: Uint8Array } | null> {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  if (!existsSync(entry.hostExecutablePath)) {
    return logSkip(`missing package/module driver executable: ${entry.hostExecutablePath}`);
  }
  const runtime = await Runtime.create({
    wasmBytes,
    guestMemoryPolicyLimitBytes: entry.guestMemoryPolicyLimitBytes,
  });
  try {
    const snapshot = await applySnapshotSourcesToRuntime(runtime, entry.snapshotSources, {
      missingLabel: "package/module driver artifact",
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

  return {
    runtime,
    executableBytes: new Uint8Array(readFileSync(entry.hostExecutablePath)),
  };
}

export async function runPackageModuleDriverEntry(
  runtime: RuntimeInstance,
  entry: PackageModuleDriverEntry,
  executableBytes: Uint8Array,
): Promise<PackageModuleDriverResult> {
  try {
    return await spawnWithOutput(
      runtime,
      executableBytes,
      [...entry.argv],
      [...entry.env],
      entry.timeoutMs,
      entry.cwd ? { cwd: entry.cwd } : undefined,
    );
  } catch (error) {
    const summary = await runtimeDebugSummary(runtime);
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${summary}`);
  }
}

export function validatePackageModuleDriverOutput(
  entry: PackageModuleDriverEntry,
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

async function statArtifact(
  runtime: RuntimeInstance,
  artifact: PackageModuleArtifactCheck,
): Promise<RuntimeStat | null> {
  try {
    return await runtime.fs.stat(artifact.path);
  } catch {
    return null;
  }
}

export async function validatePackageModuleArtifacts(
  runtime: RuntimeInstance,
  entry: PackageModuleDriverEntry,
): Promise<string[]> {
  const mismatches: string[] = [];
  for (const artifact of entry.artifactChecks ?? []) {
    const stat = await statArtifact(runtime, artifact);
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
      if (actualPrefix.length !== expectedPrefix.length || actualPrefix.some((value, index) => value !== expectedPrefix[index])) {
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

export function bucketPackageModuleDriverFailure(input: {
  result?: PackageModuleDriverResult;
  error?: unknown;
}): PackageModuleDriverFailureBucket {
  const text = [
    input.result?.output ?? "",
    input.result?.debugInfo ?? "",
    input.error instanceof Error ? (input.error.stack ?? input.error.message) : String(input.error ?? ""),
  ].join("\n");
  if (
    /PT_INTERP|PT_TLS|auxv|inspect_elf|ELF inspection|interpreter fault|missing interpreter|getcpu\(2\) system call not supported by kernel|Exec format error/i.test(text)
  ) {
    return "foundation";
  }
  if (
    /process_create|plain-fork-child-ready|child-ready|attach|publish|ownership|kernel-worker|handoff|failed to write file|loadFsSnapshot|Memory\.grow\(\): Maximum memory size exceeded/i.test(text)
  ) {
    return "runtime-ownership";
  }
  if (
    /missing package\/module driver artifact|missing package\/module driver executable|error while loading shared libraries|cannot open shared object file|No such file or directory|LoadError|Can't locate|Could not find or load main class|No such file or directory/i.test(text)
  ) {
    return "consumer-runtime";
  }
  if (
    /Illegal instruction|SIGILL|Segmentation fault|sig=4|sig=11|sig=6|sig_deliver=11|:\d+:11\b|debug_collect_error=|timeout after \d+ms/i.test(text)
  ) {
    return "unknown";
  }
  if (text.length > 0) {
    return "consumer-runtime";
  }
  return "unknown";
}
