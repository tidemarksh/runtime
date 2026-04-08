import {
  existsSync,
  lstatSync,
  readFileSync,
  readlinkSync,
  readdirSync,
  realpathSync,
  statSync,
} from "node:fs";
import type { FsSnapshotEntry } from "../../src/fs/types.js";
import {
  Runtime,
  type RuntimeInstance,
  decoder,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
} from "./runtime-workload.js";

export type SnapshotSource =
  | { kind: "file"; hostPath: string; guestPath: string; mode?: number }
  | { kind: "tree"; hostPath: string; guestPath: string }
  | { kind: "busyboxApplets"; hostPath: string; applets: readonly string[] };

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

type SnapshotMap = Map<string, FsSnapshotEntry>;

function normalizeGuestPath(value: string): string {
  const normalized = `/${value.split("/").filter(Boolean).join("/")}`;
  return normalized || "/";
}

function ensureSnapshotDir(entries: SnapshotMap, path: string, mode = 0o755): void {
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
  entries: SnapshotMap,
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

function appendFileSnapshot(
  entries: SnapshotMap,
  hostPath: string,
  guestPath: string,
  mode?: number,
): void {
  const fileMode = mode ?? ((statSync(hostPath).mode & 0o111) !== 0 ? 0o755 : 0o644);
  setSnapshotFile(entries, guestPath, new Uint8Array(readFileSync(hostPath)), fileMode);
}

function appendHostTreeSnapshot(
  entries: SnapshotMap,
  hostPath: string,
  guestPath: string,
  seenRealDirs = new Set<string>(),
): void {
  const normalizedGuest = normalizeGuestPath(guestPath);
  const stat = lstatSync(hostPath);
  if (stat.isSymbolicLink()) {
    let linkTarget: string;
    try {
      linkTarget = realpathSync(hostPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return;
      }
      throw error;
    }
    if (!existsSync(linkTarget)) {
      return;
    }
    const targetStat = statSync(linkTarget);
    if (targetStat.isDirectory()) {
      if (seenRealDirs.has(linkTarget)) {
        return;
      }
      const nextSeen = new Set(seenRealDirs);
      nextSeen.add(linkTarget);
      ensureSnapshotDir(entries, normalizedGuest, 0o755);
      for (const child of readdirSync(linkTarget)) {
        appendHostTreeSnapshot(entries, `${linkTarget}/${child}`, `${normalizedGuest}/${child}`, nextSeen);
      }
      return;
    }
    appendFileSnapshot(entries, linkTarget, normalizedGuest, (targetStat.mode & 0o111) !== 0 ? 0o755 : 0o644);
    return;
  }
  if (stat.isDirectory()) {
    const realDir = realpathSync(hostPath);
    if (seenRealDirs.has(realDir)) {
      return;
    }
    const nextSeen = new Set(seenRealDirs);
    nextSeen.add(realDir);
    ensureSnapshotDir(entries, normalizedGuest, 0o755);
    for (const child of readdirSync(hostPath)) {
      appendHostTreeSnapshot(entries, `${hostPath}/${child}`, `${normalizedGuest}/${child}`, nextSeen);
    }
    return;
  }
  appendFileSnapshot(entries, hostPath, normalizedGuest, (stat.mode & 0o111) !== 0 ? 0o755 : 0o644);
}

function appendBusyboxApplets(
  entries: SnapshotMap,
  hostPath: string,
  applets: readonly string[],
): void {
  appendFileSnapshot(entries, hostPath, "/bin/busybox", 0o755);
  for (const applet of applets) {
    appendFileSnapshot(entries, hostPath, `/bin/${applet}`, 0o755);
  }
}

export function buildDynamicStartupSnapshot(entry: DynamicStartupEntry): FsSnapshotEntry[] | null {
  const entries = new Map<string, FsSnapshotEntry>();
  ensureSnapshotDir(entries, "/", 0o755);
  ensureSnapshotDir(entries, "/tmp", 0o777);
  ensureSnapshotDir(entries, "/work", 0o755);

  for (const source of entry.snapshotSources) {
    if (source.kind === "busyboxApplets") {
      if (!existsSync(source.hostPath)) {
        return logSkip(`missing BusyBox binary: ${source.hostPath}`);
      }
      appendBusyboxApplets(entries, source.hostPath, source.applets);
      continue;
    }
    if (!existsSync(source.hostPath)) {
      return logSkip(`missing dynamic startup artifact: ${source.hostPath}`);
    }
    if (source.kind === "file") {
      appendFileSnapshot(entries, source.hostPath, source.guestPath, source.mode);
      continue;
    }
    appendHostTreeSnapshot(entries, source.hostPath, source.guestPath);
  }

  return [...entries.values()];
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
  if (/Illegal instruction|SIGILL|timeout after \d+ms/i.test(text)) {
    return "unknown";
  }
  if (
    /process_create|plain-fork-child-ready|attach|publish|ready|step|ownership|kernel-worker|handoff|failed to write file|loadFsSnapshot|Memory\.grow\(\): Maximum memory size exceeded/i.test(text)
  ) {
    return "runtime-ownership";
  }
  if (text.length > 0) {
    return "consumer-runtime";
  }
  return "unknown";
}
