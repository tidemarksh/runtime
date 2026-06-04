import { writeKernelScratch } from "./kernel-scratch.js";
import { normalizePath } from "./process-fs.js";
import type { ProcessHandle } from "./types.js";

const encoder = new TextEncoder();

function deletedPathSet(handle: ProcessHandle): Set<string> {
  if (!handle.processMemfsDeletedPaths) {
    handle.processMemfsDeletedPaths = new Set();
  }
  return handle.processMemfsDeletedPaths;
}

export function resolveLocalMemfsPath(handle: ProcessHandle, path: string): number {
  const pathBytes = encoder.encode(normalizePath(path));
  const pathPtr = writeKernelScratch(handle.kernel, pathBytes, 0x1c000);
  return Number(handle.kernel.memfs_resolve_path(pathPtr, pathBytes.length));
}

export function removeLocalMemfsPath(handle: ProcessHandle, path: string): boolean {
  if (!path.startsWith("/")) {
    return false;
  }
  const normalized = normalizePath(path);
  const parts = normalized.split("/").filter(Boolean);
  const name = parts.pop();
  if (!name) {
    return false;
  }
  const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
  const parentIno = resolveLocalMemfsPath(handle, parentPath);
  if (parentIno === 0) {
    return true;
  }
  const nameBytes = encoder.encode(name);
  const namePtr = writeKernelScratch(handle.kernel, nameBytes, 0x1d000);
  const unlinkResult = Number(handle.kernel.memfs_unlink(parentIno, namePtr, nameBytes.length));
  if (unlinkResult === 0 || unlinkResult === -2) {
    return true;
  }
  const rmdirResult = Number(handle.kernel.memfs_rmdir(parentIno, namePtr, nameBytes.length));
  return rmdirResult === 0 || rmdirResult === -2;
}

export function rememberMemfsDeletedPath(handle: ProcessHandle, path: string): void {
  deletedPathSet(handle).add(normalizePath(path));
}

export function forgetMemfsDeletedPath(handle: ProcessHandle, path: string): void {
  handle.processMemfsDeletedPaths?.delete(normalizePath(path));
}

export function reapplyMemfsDeletedPaths(
  handle: ProcessHandle,
): Array<{ path: string; removed: boolean; postIno: number }> {
  const paths = handle.processMemfsDeletedPaths;
  if (!paths || paths.size === 0) {
    return [];
  }
  const results: Array<{ path: string; removed: boolean; postIno: number }> = [];
  for (const path of paths) {
    const removed = removeLocalMemfsPath(handle, path);
    results.push({
      path,
      removed,
      postIno: resolveLocalMemfsPath(handle, path),
    });
  }
  return results;
}
