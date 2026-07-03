import { readKernelRuntimeState, type KernelExports, type KernelRuntimeState } from "../abi.js";
import { publishInode } from "../fs/sab-page-cache.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import {
  ensureMemoryCapacityFor,
  normalizePath,
  normalizePermMode,
  pathParts,
  scratchBaseFor,
  writeScratchFor,
} from "./common.js";

interface KernelWorkerFsUtilsDeps {
  getCanonicalExports(): KernelExports;
  getSabPageCache(): SharedArrayBuffer;
  processKernels: Map<number, KernelExports>;
  seedEntries: Map<string, FsSnapshotEntry>;
  encoder: TextEncoder;
  decoder: TextDecoder;
  copyMemfsState(
    sourceExports: KernelExports,
    sourceState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options?: { includeData?: boolean },
  ): void;
}

export function createKernelWorkerFsUtils(deps: KernelWorkerFsUtilsDeps) {
  function resolvePathIn(targetExports: KernelExports, path: string): number {
    const pathBytes = deps.encoder.encode(normalizePath(path));
    const pathPtr = writeScratchFor(targetExports, pathBytes);
    return targetExports.memfs_resolve_path(pathPtr, pathBytes.length);
  }

  function snapshotProcSelfExe(targetExports: KernelExports): Uint8Array | null {
    const ino = resolvePathIn(targetExports, "/proc/self/exe");
    if (ino === 0 || (targetExports.memfs_inode_mode(ino) & 0o170000) !== 0o120000) {
      return null;
    }
    const bufPtr = scratchBaseFor(targetExports) + 0x4000;
    const bufLen = 1024;
    ensureMemoryCapacityFor(targetExports.memory, bufPtr + bufLen);
    const len = Number(targetExports.memfs_read_link(ino, bufPtr, bufLen));
    if (len <= 0) {
      return null;
    }
    return new Uint8Array(targetExports.memory.buffer.slice(bufPtr, bufPtr + len));
  }

  function restoreProcSelfExe(targetExports: KernelExports, target: Uint8Array | null): void {
    const existingSelfIno = resolvePathIn(targetExports, "/proc/self");
    if (existingSelfIno !== 0) {
      const exeName = deps.encoder.encode("exe");
      const exeNamePtr = writeScratchFor(targetExports, exeName, 0x200);
      targetExports.memfs_unlink(existingSelfIno, exeNamePtr, exeName.length);
    }
    if (!target || target.length === 0) {
      return;
    }
    const procName = deps.encoder.encode("proc");
    const selfName = deps.encoder.encode("self");
    const exeName = deps.encoder.encode("exe");
    const procNamePtr = writeScratchFor(targetExports, procName);
    const selfNamePtr = writeScratchFor(targetExports, selfName, 0x100);
    const exeNamePtr = writeScratchFor(targetExports, exeName, 0x200);
    const targetPtr = writeScratchFor(targetExports, target, 0x300);
    const procIno = targetExports.memfs_ensure_dir(1, procNamePtr, procName.length, 0o555);
    if (procIno === 0) {
      return;
    }
    const selfIno = targetExports.memfs_ensure_dir(procIno, selfNamePtr, selfName.length, 0o555);
    if (selfIno === 0) {
      return;
    }
    targetExports.memfs_create_symlink(selfIno, exeNamePtr, exeName.length, targetPtr, target.length);
  }

  function restoreProcfsSnapshot(
    targetExports: KernelExports,
    target: Uint8Array | null,
    guestMemoryPolicyLimit: bigint,
  ): void {
    if (!target || target.length === 0) {
      return;
    }
    if (typeof targetExports.process_install_procfs_snapshot === "function") {
      const targetPtr = writeScratchFor(targetExports, target, 0x300);
      targetExports.process_install_procfs_snapshot(targetPtr, target.length, guestMemoryPolicyLimit);
      return;
    }
    restoreProcSelfExe(targetExports, target);
  }

  function guestMemoryPolicyLimitFor(targetExports: KernelExports, targetVmPtr?: number): bigint {
    if (targetVmPtr === undefined || targetVmPtr === 0) {
      return 0n;
    }
    const read = targetExports.thread_guest_memory_policy_limit;
    if (typeof read !== "function") {
      throw new Error("kernel ABI missing thread_guest_memory_policy_limit");
    }
    const limit = read(targetVmPtr);
    return limit < 0n ? 0n : limit;
  }

  function copyCanonicalMemfsToProcess(
    canonicalState: KernelRuntimeState,
    targetExports: KernelExports,
    targetState: KernelRuntimeState,
    options: { includeData?: boolean; targetVmPtr?: number } = {},
  ): void {
    const procSelfExe = snapshotProcSelfExe(targetExports);
    const guestMemoryPolicyLimit = guestMemoryPolicyLimitFor(targetExports, options.targetVmPtr);
    deps.copyMemfsState(
      deps.getCanonicalExports(),
      canonicalState,
      targetExports,
      targetState,
      options,
    );
    restoreProcfsSnapshot(targetExports, procSelfExe, guestMemoryPolicyLimit);
  }

  function formatProcessDebug(processExports: KernelExports, vmPtr: number): string {
    const parts = [`vm=0x${vmPtr.toString(16)}`];
    try {
      parts.push(`status=${processExports.thread_status(vmPtr)}`);
    } catch {
      parts.push("status=unavailable");
    }
    try {
      const fd = Number(processExports.thread_a0(vmPtr));
      parts.push(`fd=${fd}`);
      parts.push(`buf=${processExports.thread_a1(vmPtr).toString()}`);
      parts.push(`process_ptr=0x${processExports.thread_process_ptr(vmPtr).toString(16)}`);
      parts.push(`ofd_table_ptr=0x${processExports.kernel_ofd_table_ptr().toString(16)}`);
      if (fd >= 0) {
        const ofdId = processExports.process_fd_ofd_id(vmPtr, fd);
        parts.push(`ofd_id=${ofdId}`);
        if (ofdId > 0) {
          parts.push(`ofd_type=${processExports.kernel_ofd_fd_type(ofdId)}`);
          parts.push(`ofd_host_fd=${processExports.kernel_ofd_host_fd(ofdId)}`);
          parts.push(`ofd_flags=${processExports.kernel_ofd_status_flags(ofdId)}`);
          parts.push(`ofd_ino=${processExports.kernel_ofd_ino(ofdId)}`);
          parts.push(`ofd_offset=${processExports.kernel_ofd_offset(ofdId).toString()}`);
        }
      }
    } catch {
      parts.push("fd=unavailable");
    }
    try {
      parts.push(`syscall_a7=${processExports.thread_a7?.(vmPtr)?.toString() ?? "n/a"}`);
      parts.push(`syscall_a0=${processExports.thread_a0(vmPtr).toString()}`);
      parts.push(`poll_pipe=0x${(processExports.thread_poll_wait_first_pipe_revents?.(vmPtr) ?? 0).toString(16)}`);
      const blockedFd = processExports.thread_blocked_fd(vmPtr);
      const blockedOfdId = processExports.thread_blocked_ofd_id(vmPtr);
      parts.push(`blocked_fd=${blockedFd}`);
      parts.push(`blocked_ofd_id=${blockedOfdId}`);
      if (blockedOfdId > 0) {
        parts.push(`blocked_ofd_type=${processExports.kernel_ofd_fd_type(blockedOfdId)}`);
        parts.push(`blocked_ofd_host_fd=${processExports.kernel_ofd_host_fd(blockedOfdId)}`);
        parts.push(`blocked_ofd_flags=${processExports.kernel_ofd_status_flags(blockedOfdId)}`);
        parts.push(`blocked_ofd_ino=${processExports.kernel_ofd_ino(blockedOfdId)}`);
        parts.push(`blocked_ofd_offset=${processExports.kernel_ofd_offset(blockedOfdId).toString()}`);
      }
    } catch {
      parts.push("blocked_ofd=unavailable");
    }
    parts.push(`pipe_base=0x${processExports.kernel_pipe_base().toString(16)}`);
    parts.push(`pipe_used=${processExports.kernel_pipe_used().toString()}`);
    let syscallNr: bigint | undefined;
    try {
      syscallNr = processExports.debug_syscall_trace_nr?.();
    } catch {
      syscallNr = undefined;
    }
    if (syscallNr !== undefined && syscallNr !== 0n) {
      parts.push(`syscall=${syscallNr.toString()}`);
      try {
        parts.push(`sys_result=${processExports.debug_syscall_trace_result?.().toString() ?? "n/a"}`);
        parts.push(`sys_status=${processExports.debug_syscall_trace_status?.() ?? "n/a"}`);
      } catch {
        parts.push("sys_debug=unavailable");
      }
    }
    let memoryNr: bigint | undefined;
    try {
      memoryNr = processExports.debug_memory_trace_nr?.();
    } catch {
      memoryNr = undefined;
    }
    if (memoryNr !== undefined && memoryNr !== 0n) {
      parts.push(`mem_nr=${memoryNr.toString()}`);
      try {
        parts.push(`mem_a0=${processExports.debug_memory_trace_arg0?.().toString() ?? "n/a"}`);
        parts.push(`mem_a1=${processExports.debug_memory_trace_arg1?.().toString() ?? "n/a"}`);
        parts.push(`mem_a2=${processExports.debug_memory_trace_arg2?.().toString() ?? "n/a"}`);
        parts.push(`mem_a3=${processExports.debug_memory_trace_arg3?.().toString() ?? "n/a"}`);
        parts.push(`mem_result=${processExports.debug_memory_trace_result?.().toString() ?? "n/a"}`);
      } catch {
        parts.push("mem_debug=unavailable");
      }
    }
    const liveThreads: string[] = [];
    try {
      for (let slot = 0; slot < processExports.abi_max_threads(); slot++) {
        const threadVmPtr = processExports.process_thread_vm_ptr(vmPtr, slot);
        if (threadVmPtr !== 0) {
          liveThreads.push(`0x${threadVmPtr.toString(16)}`);
        }
      }
    } catch {
      parts.push("live_threads=unavailable");
    }
    if (liveThreads.length > 0) {
      parts.push(`live_threads=[${liveThreads.join(", ")}]`);
    }
    try {
      const procSelfExe = snapshotProcSelfExe(processExports);
      if (procSelfExe === null) {
        parts.push("proc_self_exe=missing");
      } else {
        parts.push(`proc_self_exe=${JSON.stringify(deps.decoder.decode(procSelfExe))}`);
      }
    } catch {
      parts.push("proc_self_exe=unavailable");
    }
    return parts.join(" ");
  }

  function fanoutCanonicalMemfs(includeData = true): void {
    const canonicalState = readKernelRuntimeState(deps.getCanonicalExports());
    for (const processExports of deps.processKernels.values()) {
      const processState = readKernelRuntimeState(processExports);
      copyCanonicalMemfsToProcess(canonicalState, processExports, processState, { includeData });
    }
  }

  function publishRootInode(mode = 0o755): void {
    publishInode(deps.getSabPageCache(), 1, {
      mode: 0o040000 | normalizePermMode(mode, 0o755),
      size: 0,
      parentIno: 1,
      name: new Uint8Array(),
      nextSibling: 0,
    });
  }

  function ensureSeedDirEntries(path: string, mode = 0o755): void {
    let current = "";
    for (const part of pathParts(path)) {
      current += `/${part}`;
      if (!deps.seedEntries.has(current)) {
        deps.seedEntries.set(current, {
          kind: "dir",
          path: current,
          mode: normalizePermMode(mode, 0o755),
        });
      }
    }
  }

  function upsertFileSeed(
    path: string,
    data: Uint8Array,
    mode = 0o644,
    options: { size?: number } = {},
  ): void {
    const normalized = normalizePath(path);
    const parent = normalized.split("/").slice(0, -1).join("/") || "/";
    ensureSeedDirEntries(parent);
    const size = options.size ?? data.byteLength;
    deps.seedEntries.set(normalized, {
      kind: "file",
      path: normalized,
      size,
      mode: normalizePermMode(mode, 0o644),
    });
  }

  function upsertDirSeed(path: string, mode = 0o755): void {
    const normalized = normalizePath(path);
    const perms = normalizePermMode(mode, 0o755);
    ensureSeedDirEntries(normalized, perms);
    deps.seedEntries.set(normalized, { kind: "dir", path: normalized, mode: perms });
  }

  function upsertSymlinkSeed(path: string, target: Uint8Array, mode = 0o777): void {
    const normalized = normalizePath(path);
    const parent = normalized.split("/").slice(0, -1).join("/") || "/";
    ensureSeedDirEntries(parent);
    deps.seedEntries.set(normalized, {
      kind: "symlink",
      path: normalized,
      target: new Uint8Array(target),
      mode: normalizePermMode(mode, 0o777),
    });
  }

  return {
    copyCanonicalMemfsToProcess,
    ensureSeedDirEntries,
    fanoutCanonicalMemfs,
    formatProcessDebug,
    publishRootInode,
    resolvePathIn,
    restoreProcSelfExe,
    snapshotProcSelfExe,
    upsertDirSeed,
    upsertFileSeed,
    upsertSymlinkSeed,
  };
}
