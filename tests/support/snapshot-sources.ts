import {
  existsSync,
  lstatSync,
  readFileSync,
  realpathSync,
  readdirSync,
  statSync,
} from "node:fs";
import type { FsSnapshotEntry } from "../../src/fs/types.js";

export type SnapshotSource =
  | { kind: "dir"; guestPath: string; mode?: number }
  | { kind: "file"; hostPath: string; guestPath: string; mode?: number }
  | { kind: "tree"; hostPath: string; guestPath: string }
  | { kind: "inlineFile"; guestPath: string; data: string | Uint8Array; mode?: number }
  | { kind: "fileCopies"; hostPath: string; guestPaths: readonly string[]; mode?: number };

export interface SnapshotBuildOptions {
  baseDirs?: readonly string[];
  missingLabel?: string;
}

export interface SnapshotBuildResult {
  entries?: FsSnapshotEntry[];
  skipReason?: string;
}

type SnapshotMap = Map<string, FsSnapshotEntry>;

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

function appendHostTreeSnapshot(
  entries: SnapshotMap,
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
      appendHostTreeSnapshot(entries, `${hostPath}/${child}`, `${normalizedGuest}/${child}`, nextSeen);
    }
    return;
  }
  appendFileSnapshot(entries, hostPath, normalizedGuest, (stat.mode & 0o111) !== 0 ? 0o755 : 0o644);
}

function appendFileCopies(
  entries: SnapshotMap,
  hostPath: string,
  guestPaths: readonly string[],
  mode?: number,
): void {
  for (const guestPath of guestPaths) {
    appendFileSnapshot(entries, hostPath, guestPath, mode);
  }
}

function encodeInlineData(data: string | Uint8Array): Uint8Array {
  return typeof data === "string" ? new TextEncoder().encode(data) : data;
}

export function buildSnapshotFromSources(
  sources: readonly SnapshotSource[],
  options: SnapshotBuildOptions = {},
): SnapshotBuildResult {
  const entries = new Map<string, FsSnapshotEntry>();
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
        skipReason: `missing ${options.missingLabel ?? "snapshot artifact"}: ${source.hostPath}`,
      };
    }
    if (source.kind === "fileCopies") {
      appendFileCopies(entries, source.hostPath, source.guestPaths, source.mode);
      continue;
    }
    if (source.kind === "file") {
      appendFileSnapshot(entries, source.hostPath, source.guestPath, source.mode);
      continue;
    }
    appendHostTreeSnapshot(entries, source.hostPath, source.guestPath);
  }

  return { entries: [...entries.values()] };
}
