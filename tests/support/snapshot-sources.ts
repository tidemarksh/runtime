import {
  existsSync,
  lstatSync,
  readFileSync,
  realpathSync,
  readdirSync,
  statSync,
} from "node:fs";
import type { FsSnapshotEntry } from "../../src/fs/types.js";
import type { RuntimeInstance } from "./runtime-workload.js";

export type SnapshotSource =
  | { kind: "dir"; guestPath: string; mode?: number }
  | { kind: "file"; hostPath: string; guestPath: string; mode?: number }
  | { kind: "tree"; hostPath: string; guestPath: string }
  | { kind: "inlineFile"; guestPath: string; data: string | Uint8Array; mode?: number }
  | { kind: "fileCopies"; hostPath: string; guestPaths: readonly string[]; mode?: number };

export interface SnapshotBuildOptions {
  baseDirs?: readonly string[];
  missingLabel?: string;
  artifactMaterializationBudgetBytes?: number;
  hostFileBackingMode?: "write" | "source";
}

export interface SnapshotBuildResult {
  entries?: FsSnapshotEntry[];
  skipReason?: string;
}

type SnapshotMap = Map<string, FsSnapshotEntry>;
type HostFileSnapshotTask = {
  hostPath: string;
  guestPath: string;
  mode?: number;
  size: number;
};

interface SnapshotPlan {
  entries: SnapshotMap;
  hostFiles: HostFileSnapshotTask[];
  skipReason?: string;
}

type HostFileBackingSourceTask = HostFileSnapshotTask & {
  sourceIdentity: string;
};

let nextSnapshotSourceId = 1;

function isSkippableHostFsError(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  return code === "ENOENT" || code === "EACCES" || code === "EPERM";
}

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

function appendHostFileTask(
  entries: SnapshotMap,
  hostFiles: HostFileSnapshotTask[],
  hostPath: string,
  guestPath: string,
  mode?: number,
): void {
  let stat;
  try {
    stat = statSync(hostPath);
  } catch (error) {
    if (isSkippableHostFsError(error)) {
      return;
    }
    throw error;
  }
  const fileMode = mode ?? ((stat.mode & 0o111) !== 0 ? 0o755 : 0o644);
  const normalized = normalizeGuestPath(guestPath);
  ensureSnapshotDir(entries, normalized.split("/").slice(0, -1).join("/") || "/", 0o755);
  hostFiles.push({ hostPath, guestPath: normalized, mode: fileMode, size: stat.size });
}

function appendFileSnapshot(
  entries: SnapshotMap,
  hostPath: string,
  guestPath: string,
  mode?: number,
): void {
  let stat;
  try {
    stat = statSync(hostPath);
  } catch (error) {
    if (isSkippableHostFsError(error)) {
      return;
    }
    throw error;
  }
  let data: Uint8Array;
  try {
    data = new Uint8Array(readFileSync(hostPath));
  } catch (error) {
    if (isSkippableHostFsError(error)) {
      return;
    }
    throw error;
  }
  const fileMode = mode ?? ((stat.mode & 0o111) !== 0 ? 0o755 : 0o644);
  setSnapshotFile(entries, guestPath, data, fileMode);
}

function appendHostTreePlan(
  entries: SnapshotMap,
  hostFiles: HostFileSnapshotTask[],
  hostPath: string,
  guestPath: string,
  seenRealDirs = new Set<string>(),
): void {
  const normalizedGuest = normalizeGuestPath(guestPath);
  let stat;
  try {
    stat = lstatSync(hostPath);
  } catch (error) {
    if (isSkippableHostFsError(error)) {
      return;
    }
    throw error;
  }
  if (stat.isSymbolicLink()) {
    let linkTarget: string;
    try {
      linkTarget = realpathSync(hostPath);
    } catch (error) {
      if (isSkippableHostFsError(error)) {
        return;
      }
      throw error;
    }
    if (!existsSync(linkTarget)) {
      return;
    }
    let targetStat;
    try {
      targetStat = statSync(linkTarget);
    } catch (error) {
      if (isSkippableHostFsError(error)) {
        return;
      }
      throw error;
    }
    if (targetStat.isDirectory()) {
      if (seenRealDirs.has(linkTarget)) {
        return;
      }
      const nextSeen = new Set(seenRealDirs);
      nextSeen.add(linkTarget);
      ensureSnapshotDir(entries, normalizedGuest, 0o755);
      let children: string[];
      try {
        children = readdirSync(linkTarget);
      } catch (error) {
        if (isSkippableHostFsError(error)) {
          return;
        }
        throw error;
      }
      for (const child of children) {
        appendHostTreePlan(entries, hostFiles, `${linkTarget}/${child}`, `${normalizedGuest}/${child}`, nextSeen);
      }
      return;
    }
    appendHostFileTask(
      entries,
      hostFiles,
      linkTarget,
      normalizedGuest,
      (targetStat.mode & 0o111) !== 0 ? 0o755 : 0o644,
    );
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
    let children: string[];
    try {
      children = readdirSync(hostPath);
    } catch (error) {
      if (isSkippableHostFsError(error)) {
        return;
      }
      throw error;
    }
    for (const child of children) {
      appendHostTreePlan(entries, hostFiles, `${hostPath}/${child}`, `${normalizedGuest}/${child}`, nextSeen);
    }
    return;
  }
  appendHostFileTask(entries, hostFiles, hostPath, normalizedGuest, (stat.mode & 0o111) !== 0 ? 0o755 : 0o644);
}

function appendFileCopyTasks(
  entries: SnapshotMap,
  hostFiles: HostFileSnapshotTask[],
  hostPath: string,
  guestPaths: readonly string[],
  mode?: number,
): void {
  for (const guestPath of guestPaths) {
    appendHostFileTask(entries, hostFiles, hostPath, guestPath, mode);
  }
}

function encodeInlineData(data: string | Uint8Array): Uint8Array {
  return typeof data === "string" ? new TextEncoder().encode(data) : data;
}

function normalizeArtifactMaterializationBudget(value: number | undefined): number | null {
  if (value === undefined) {
    return null;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`invalid artifact materialization budget: ${value}`);
  }
  return Math.floor(value);
}

function snapshotEntryMaterializedBytes(entry: FsSnapshotEntry): number {
  if (entry.kind === "file") {
    return entry.data?.byteLength ?? 0;
  }
  if (entry.kind === "symlink") {
    return entry.target?.byteLength ?? 0;
  }
  return 0;
}

function totalSnapshotMaterializedBytes(plan: SnapshotPlan): number {
  let total = 0;
  for (const entry of plan.entries.values()) {
    total += snapshotEntryMaterializedBytes(entry);
  }
  for (const file of plan.hostFiles) {
    total += file.size;
  }
  return total;
}

function assertArtifactMaterializationBudget(plan: SnapshotPlan, budgetValue: number | undefined): void {
  const budget = normalizeArtifactMaterializationBudget(budgetValue);
  if (budget === null) {
    return;
  }
  const materializedBytes = totalSnapshotMaterializedBytes(plan);
  if (materializedBytes > budget) {
    throw new Error(
      `snapshot artifact materialization budget exceeded: materializedBytes=${materializedBytes} budgetBytes=${budget}`,
    );
  }
}

function createHostFileBackingSourceTasks(plan: SnapshotPlan): HostFileBackingSourceTask[] {
  return plan.hostFiles.map((file) => {
    const sourceIdentity = `snapshot-source:${nextSnapshotSourceId++}`;
    plan.entries.set(file.guestPath, {
      kind: "file",
      path: file.guestPath,
      size: file.size,
      mode: file.mode,
      backingExtent: {
        sourceIdentity,
        length: file.size,
      },
    });
    return { ...file, sourceIdentity };
  });
}

function buildSnapshotPlan(
  sources: readonly SnapshotSource[],
  options: SnapshotBuildOptions = {},
): SnapshotPlan {
  const entries = new Map<string, FsSnapshotEntry>();
  const hostFiles: HostFileSnapshotTask[] = [];
  for (const path of options.baseDirs ?? ["/", "/tmp", "/work"]) {
    ensureSnapshotDir(entries, path, path === "/tmp" ? 0o777 : 0o755);
  }

  for (const source of sources) {
    if (source.kind === "dir") {
      ensureSnapshotDir(entries, source.guestPath, source.mode ?? 0o755);
      continue;
    }
    if (source.kind === "inlineFile") {
      setSnapshotFile(entries, source.guestPath, encodeInlineData(source.data), source.mode ?? 0o644);
      continue;
    }
    if (!existsSync(source.hostPath)) {
      return {
        entries,
        hostFiles,
        skipReason: `missing ${options.missingLabel ?? "snapshot artifact"}: ${source.hostPath}`,
      };
    }
    if (source.kind === "fileCopies") {
      appendFileCopyTasks(entries, hostFiles, source.hostPath, source.guestPaths, source.mode);
      continue;
    }
    if (source.kind === "file") {
      appendHostFileTask(entries, hostFiles, source.hostPath, source.guestPath, source.mode);
      continue;
    }
    appendHostTreePlan(entries, hostFiles, source.hostPath, source.guestPath);
  }

  return { entries, hostFiles };
}

export function buildSnapshotFromSources(
  sources: readonly SnapshotSource[],
  options: SnapshotBuildOptions = {},
): SnapshotBuildResult {
  const plan = buildSnapshotPlan(sources, options);
  if (plan.skipReason) {
    return { skipReason: plan.skipReason };
  }
  assertArtifactMaterializationBudget(plan, options.artifactMaterializationBudgetBytes);
  for (const file of plan.hostFiles) {
    appendFileSnapshot(plan.entries, file.hostPath, file.guestPath, file.mode);
  }

  return { entries: [...plan.entries.values()] };
}

export async function applySnapshotSourcesToRuntime(
  runtime: RuntimeInstance,
  sources: readonly SnapshotSource[],
  options: SnapshotBuildOptions = {},
): Promise<{ skipReason?: string }> {
  const plan = buildSnapshotPlan(sources, options);
  if (plan.skipReason) {
    return { skipReason: plan.skipReason };
  }
  assertArtifactMaterializationBudget(plan, options.artifactMaterializationBudgetBytes);
  const sourceFiles = options.hostFileBackingMode === "source"
    ? createHostFileBackingSourceTasks(plan)
    : [];

  await runtime.applyFsSnapshot([...plan.entries.values()]);
  if (options.hostFileBackingMode === "source") {
    if (typeof runtime.registerFsBackingSource !== "function") {
      throw new Error("runtime does not support source-backed snapshot host files");
    }
    for (const file of sourceFiles) {
      let data: Uint8Array;
      try {
        data = new Uint8Array(readFileSync(file.hostPath));
      } catch (error) {
        if (isSkippableHostFsError(error)) {
          continue;
        }
        throw error;
      }
      await runtime.registerFsBackingSource(file.sourceIdentity, data);
    }
    return {};
  }

  for (const file of plan.hostFiles) {
    let data: Uint8Array;
    try {
      data = new Uint8Array(readFileSync(file.hostPath));
    } catch (error) {
      if (isSkippableHostFsError(error)) {
        continue;
      }
      throw error;
    }
    if ("writeFileOwned" in runtime && typeof runtime.writeFileOwned === "function") {
      await runtime.writeFileOwned(file.guestPath, data, file.mode);
    } else {
      await runtime.fs.writeFile(file.guestPath, data, file.mode);
    }
  }

  return {};
}
