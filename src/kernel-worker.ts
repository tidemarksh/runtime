/**
 * Dedicated kernel-worker RPC endpoint.
 *
 * Runtime-owned FS state is canonical here, and registered process memories are
 * rehydrated from this worker for MemFS/control-plane ownership and fork-shared
 * OFD/MemFS fanout. The current kernel still runs per-process instances, so
 * this remains a staged ownership boundary rather than a single in-worker
 * kernel instance.
 */

import type { KernelExports, KernelRuntimeState } from "./abi.js";
import { attachKernelRuntimeState, readKernelRuntimeState } from "./abi.js";
import type { FsSnapshotEntry, RuntimeStat } from "./fs-types.js";
import type {
  KernelWorkerInbound,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerOutbound,
  KernelWorkerPidStateSnapshot,
} from "./messages.js";
import { bumpDirEpoch, publishInode, resetPageCache } from "./sab-page-cache.js";
import { createKernelEnv } from "./wasm-env.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./worker-scope.js";

let exports: KernelExports;
let wasmModule: WebAssembly.Module | null = null;
let sabPageCache: SharedArrayBuffer;
const processKernels = new Map<number, KernelExports>();
let canonicalNextPid = 3;
const canonicalProcesses = new Map<number, {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
}>();

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const seedEntries = new Map<string, FsSnapshotEntry>();

function normalizePermMode(mode: number | undefined, fallback: number): number {
  return (mode ?? fallback) & 0o7777;
}

function normalizePath(path: string): string {
  const normalized = `/${path.split("/").filter(Boolean).join("/")}`;
  return normalized === "//" ? "/" : normalized || "/";
}

function pathParts(path: string): string[] {
  return normalizePath(path).split("/").filter(Boolean);
}

function postResponse(msg: KernelWorkerOutbound): void {
  workerPostMessage(msg);
}

function cloneSnapshotEntry(entry: FsSnapshotEntry): FsSnapshotEntry {
  return entry.kind === "file"
    ? {
        kind: "file",
        path: entry.path,
        data: new Uint8Array(entry.data ?? new Uint8Array()),
        mode: entry.mode,
      }
    : {
        kind: "dir",
        path: entry.path,
        mode: entry.mode,
      };
}

function scratchBase(): number {
  return exports.kernel_heap_ptr() + 0x10000;
}

function ensureMemoryCapacityFor(memory: WebAssembly.Memory, endOffset: number): void {
  const pageSize = 65536;
  const currentBytes = memory.buffer.byteLength;
  if (endOffset <= currentBytes) {
    return;
  }
  const additionalPages = Math.ceil((endOffset - currentBytes) / pageSize);
  memory.grow(additionalPages);
}

function ensureMemoryCapacity(endOffset: number): void {
  ensureMemoryCapacityFor(exports.memory, endOffset);
}

function writeScratch(bytes: Uint8Array, offset = 0): number {
  const scratch = scratchBase() + offset;
  ensureMemoryCapacity(scratch + bytes.length);
  const mem = new Uint8Array(exports.memory.buffer);
  mem.set(bytes, scratch);
  return scratch;
}

function copyMemoryRange(
  sourceMemory: WebAssembly.Memory,
  sourceOffset: number,
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacityFor(targetMemory, targetOffset + byteLength);
  const source = new Uint8Array(sourceMemory.buffer, sourceOffset, byteLength);
  new Uint8Array(targetMemory.buffer).set(source, targetOffset);
}

function clearMemoryRange(
  targetMemory: WebAssembly.Memory,
  targetOffset: number,
  byteLength: number,
): void {
  if (byteLength <= 0) {
    return;
  }
  ensureMemoryCapacityFor(targetMemory, targetOffset + byteLength);
  new Uint8Array(targetMemory.buffer, targetOffset, byteLength).fill(0);
}

type MemfsLayout = {
  inodeCount: number;
  dirEntryCount: number;
  dataUsed: number;
  dataCap: number;
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataPtr: number;
};

function readMemfsLayout(memory: WebAssembly.Memory, memfsPtr: number): MemfsLayout {
  const view = new DataView(memory.buffer, memfsPtr, 32);
  return {
    inodeCount: view.getUint32(0, true),
    dirEntryCount: view.getUint32(4, true),
    dataUsed: view.getUint32(8, true),
    dataCap: view.getUint32(12, true),
    inodesPtr: view.getUint32(16, true),
    firstChildPtr: view.getUint32(20, true),
    dirEntriesPtr: view.getUint32(24, true),
    dataPtr: view.getUint32(28, true),
  };
}

function copyOfdState(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
): void {
  copyMemoryRange(
    sourceExports.memory,
    sourceState.ofdTablePtr,
    targetExports.memory,
    targetState.ofdTablePtr,
    sourceExports.kernel_ofd_table_bytes(),
  );
}

function copySelectedOfdState(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  ofdIds: Iterable<number>,
): void {
  const maxFds = sourceExports.abi_max_fds();
  const tableBytes = sourceExports.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    copyMemoryRange(
      sourceExports.memory,
      sourceState.ofdTablePtr + ofdId * slotBytes,
      targetExports.memory,
      targetState.ofdTablePtr + ofdId * slotBytes,
      slotBytes,
    );
  }
}

function clearSelectedOfdState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  ofdIds: Iterable<number>,
): void {
  const maxFds = targetExports.abi_max_fds();
  const tableBytes = targetExports.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const bytes = new Uint8Array(targetExports.memory.buffer);
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    bytes.fill(0, targetState.ofdTablePtr + ofdId * slotBytes, targetState.ofdTablePtr + (ofdId + 1) * slotBytes);
  }
}

function applySelectedOfdSnapshots(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  snapshots: KernelWorkerOfdSlotSnapshot[],
): void {
  const maxFds = targetExports.abi_max_fds();
  const tableBytes = targetExports.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const bytes = new Uint8Array(targetExports.memory.buffer);
  for (const snapshot of snapshots) {
    if (snapshot.ofdId <= 0 || snapshot.ofdId >= maxFds || snapshot.slot.length !== slotBytes) {
      continue;
    }
    bytes.set(snapshot.slot, targetState.ofdTablePtr + snapshot.ofdId * slotBytes);
  }
}

function copyMemfsState(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  _targetState: KernelRuntimeState,
): void {
  const sourceLayout = readMemfsLayout(sourceExports.memory, sourceState.memfsPtr);
  copyMemoryRange(
    sourceExports.memory,
    sourceLayout.inodesPtr,
    targetExports.memory,
    sourceLayout.inodesPtr,
    sourceExports.kernel_memfs_inodes_bytes(),
  );
  copyMemoryRange(
    sourceExports.memory,
    sourceLayout.firstChildPtr,
    targetExports.memory,
    sourceLayout.firstChildPtr,
    sourceExports.kernel_memfs_first_child_bytes(),
  );
  copyMemoryRange(
    sourceExports.memory,
    sourceLayout.dirEntriesPtr,
    targetExports.memory,
    sourceLayout.dirEntriesPtr,
    sourceExports.kernel_memfs_dir_entries_bytes(),
  );
  copyMemoryRange(
    sourceExports.memory,
    sourceLayout.dataPtr,
    targetExports.memory,
    sourceLayout.dataPtr,
    sourceLayout.dataCap,
  );
  copyMemoryRange(
    sourceExports.memory,
    sourceState.memfsPtr,
    targetExports.memory,
    _targetState.memfsPtr,
    sourceExports.kernel_memfs_header_bytes(),
  );
}

function clearMemfsState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
): void {
  clearMemoryRange(
    targetExports.memory,
    targetExports.kernel_memfs_inodes_ptr(),
    targetExports.kernel_memfs_inodes_bytes(),
  );
  clearMemoryRange(
    targetExports.memory,
    targetExports.kernel_memfs_first_child_ptr(),
    targetExports.kernel_memfs_first_child_bytes(),
  );
  clearMemoryRange(
    targetExports.memory,
    targetExports.kernel_memfs_dir_entries_ptr(),
    targetExports.kernel_memfs_dir_entries_bytes(),
  );
  clearMemoryRange(
    targetExports.memory,
    targetExports.kernel_memfs_data_ptr(),
    targetExports.kernel_memfs_data_bytes(),
  );
  clearMemoryRange(
    targetExports.memory,
    targetState.memfsPtr,
    targetExports.kernel_memfs_header_bytes(),
  );
}

function copyEpollState(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
): void {
  const tableBytes = sourceExports.kernel_epoll_table_bytes();
  const instanceBytes = sourceExports.kernel_epoll_instance_bytes();

  copyMemoryRange(
    sourceExports.memory,
    sourceState.epollTablePtr,
    targetExports.memory,
    targetState.epollTablePtr,
    tableBytes,
  );

  const tableEntries = tableBytes >>> 2;
  const sourceTable = new Uint32Array(sourceExports.memory.buffer, sourceState.epollTablePtr, tableEntries);
  for (let i = 0; i < tableEntries; i++) {
    const ptr = sourceTable[i];
    if (ptr === 0) {
      continue;
    }
    copyMemoryRange(
      sourceExports.memory,
      ptr,
      targetExports.memory,
      ptr,
      instanceBytes,
    );
  }
}

function clearEpollState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
): void {
  const tableBytes = targetExports.kernel_epoll_table_bytes();
  const instanceBytes = targetExports.kernel_epoll_instance_bytes();
  const tableEntries = tableBytes >>> 2;
  const table = new Uint32Array(targetExports.memory.buffer, targetState.epollTablePtr, tableEntries);
  const instancePtrs = [...table].filter((ptr) => ptr !== 0);

  clearMemoryRange(targetExports.memory, targetState.epollTablePtr, tableBytes);
  for (const ptr of instancePtrs) {
    clearMemoryRange(targetExports.memory, ptr, instanceBytes);
  }
}

function copySocketState(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
): void {
  attachKernelRuntimeState(targetExports, {
    ...targetState,
    socketUsed: sourceState.socketUsed,
    netBufUsed: sourceState.netBufUsed,
  });
  copyMemoryRange(
    sourceExports.memory,
    sourceState.socketTablePtr,
    targetExports.memory,
    targetState.socketTablePtr,
    sourceExports.kernel_socket_table_bytes(),
  );
  copyMemoryRange(
    sourceExports.memory,
    sourceState.netBufBase,
    targetExports.memory,
    targetState.netBufBase,
    sourceExports.kernel_net_buf_pool_bytes(),
  );
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
  return parts.join(" ");
}

function fanoutCanonicalMemfs(): void {
  const canonicalState = readKernelRuntimeState(exports);
  for (const processExports of processKernels.values()) {
    const processState = readKernelRuntimeState(processExports);
    copyMemfsState(
      exports,
      canonicalState,
      processExports,
      processState,
    );
  }
}

function publishRootInode(mode = 0o755): void {
  publishInode(sabPageCache, 1, {
    mode: 0o040000 | normalizePermMode(mode, 0o755),
    size: 0,
    parentIno: 1,
    name: new Uint8Array(),
    nextSibling: 0,
  }, null);
}

function ensureSeedDirEntries(path: string, mode = 0o755): void {
  let current = "";
  for (const part of pathParts(path)) {
    current += `/${part}`;
    if (!seedEntries.has(current)) {
      seedEntries.set(current, {
        kind: "dir",
        path: current,
        mode: normalizePermMode(mode, 0o755),
      });
    }
  }
}

function upsertFileSeed(path: string, data: Uint8Array, mode = 0o644): void {
  const normalized = normalizePath(path);
  const parent = normalized.split("/").slice(0, -1).join("/") || "/";
  ensureSeedDirEntries(parent);
  seedEntries.set(normalized, {
    kind: "file",
    path: normalized,
    data: new Uint8Array(data),
    mode: normalizePermMode(mode, 0o644),
  });
}

function upsertDirSeed(path: string, mode = 0o755): void {
  const normalized = normalizePath(path);
  const perms = normalizePermMode(mode, 0o755);
  ensureSeedDirEntries(normalized, perms);
  seedEntries.set(normalized, { kind: "dir", path: normalized, mode: perms });
}

async function initKernel(
  module: WebAssembly.Module,
  pageCache: SharedArrayBuffer,
): Promise<KernelRuntimeState> {
  wasmModule = module;
  sabPageCache = pageCache;

  const privateMemory = new WebAssembly.Memory({
    initial: 1024,
    maximum: 32768,
    shared: true,
  });
  const instance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(privateMemory),
  });
  exports = instance.exports as unknown as KernelExports;

  const wasmHeapBase = (exports as { __heap_base?: { value: number } }).__heap_base?.value ?? 0x200000;
  exports.kernel_init(wasmHeapBase);

  const now = Date.now();
  exports.set_base_time(
    BigInt(Math.floor(now / 1000)),
    BigInt((now % 1000) * 1_000_000),
  );

  seedEntries.clear();
  seedEntries.set("/", { kind: "dir", path: "/", mode: 0o755 });
  resetPageCache(sabPageCache);
  publishRootInode();
  canonicalNextPid = 3;
  canonicalProcesses.clear();
  canonicalProcesses.set(1, {
    pid: 1,
    ppid: 0,
    pgid: 1,
    sid: 1,
    didExec: false,
  });

  return readKernelRuntimeState(exports);
}

async function registerProcess(
  pid: number,
  memory: WebAssembly.Memory,
  kernelState: KernelRuntimeState,
  seedMemfsFromCanonical = true,
): Promise<KernelRuntimeState> {
  if (wasmModule === null) {
    throw new Error("kernel worker is not initialized");
  }
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: createKernelEnv(memory),
  });
  const processExports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(processExports, kernelState);
  processKernels.set(pid, processExports);
  canonicalNextPid = Math.max(canonicalNextPid, pid + 1);
  if (processKernels.size === 1) {
    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(exports);
    copyOfdState(
      processExports,
      sourceState,
      exports,
      canonicalState,
    );
    copyMemfsState(
      processExports,
      sourceState,
      exports,
      canonicalState,
    );
    const pipeState = syncPipeState(pid, readKernelRuntimeState(processExports));
    const epollState = syncEpollState(pid, pipeState);
    return syncSocketState(pid, epollState);
  }

  const canonicalState = readKernelRuntimeState(exports);
  const processState = readKernelRuntimeState(processExports);
  attachKernelRuntimeState(processExports, {
    ...processState,
    pipeUsed: canonicalState.pipeUsed,
    socketUsed: canonicalState.socketUsed,
    netBufUsed: canonicalState.netBufUsed,
  });
  copyMemoryRange(
    exports.memory,
    canonicalState.pipeBase,
    processExports.memory,
    processState.pipeBase,
    processExports.kernel_pipe_region_bytes(),
  );
  if (seedMemfsFromCanonical) {
    copyMemfsState(
      exports,
      canonicalState,
      processExports,
      processState,
    );
  }
  copyEpollState(
    exports,
    canonicalState,
    processExports,
    processState,
  );
  copySocketState(
    exports,
    canonicalState,
    processExports,
    processState,
  );
  return readKernelRuntimeState(processExports);
}

function unregisterProcess(pid: number): void {
  processKernels.delete(pid);
  canonicalProcesses.delete(pid);
  for (const processExports of processKernels.values()) {
    processExports.process_remove_process_entry(pid);
  }
}

function debugProcesses(): { pids: number[]; canonicalPids: number[] } {
  return {
    pids: [...processKernels.keys()].sort((left, right) => left - right),
    canonicalPids: [...canonicalProcesses.keys()].sort((left, right) => left - right),
  };
}

function requireProcessKernel(pid: number): KernelExports {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    throw new Error(`process ${pid} is not registered`);
  }
  return processExports;
}

function processFs(
  pid: number,
  vmPtr: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  processExports.thread_process_fs(vmPtr);
  processExports.thread_resume_fs(vmPtr);
  const sourceState = readKernelRuntimeState(processExports);
  const canonicalState = readKernelRuntimeState(exports);
  copyMemfsState(
    processExports,
    sourceState,
    exports,
    canonicalState,
  );
  fanoutCanonicalMemfs();
  return readKernelRuntimeState(processExports);
}

function syncForkSharedState(
  pid: number,
  targetPids: number[],
  targetKernelStates: KernelWorkerPidStateSnapshot[],
  ofdIds: number[],
  ofdSlots: KernelWorkerOfdSlotSnapshot[],
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);

  const sourceState = readKernelRuntimeState(processExports);
  const snappedOfdIds = new Set<number>();
  if (ofdSlots.length > 0) {
    applySelectedOfdSnapshots(processExports, sourceState, ofdSlots);
    const canonicalState = readKernelRuntimeState(exports);
    applySelectedOfdSnapshots(exports, canonicalState, ofdSlots);
    for (const snapshot of ofdSlots) {
      snappedOfdIds.add(snapshot.ofdId);
    }
  }
  const canonicalState = readKernelRuntimeState(exports);
  copySelectedOfdState(
    processExports,
    sourceState,
    exports,
    canonicalState,
    ofdIds.filter((ofdId) => !snappedOfdIds.has(ofdId)),
  );
  copyMemfsState(
    processExports,
    sourceState,
    exports,
    canonicalState,
  );

  const targetStateByPid = new Map<number, KernelRuntimeState>();
  for (const target of targetKernelStates) {
    targetStateByPid.set(target.pid, target.kernelState);
  }
  const uniqueTargets = new Set(targetPids);
  uniqueTargets.delete(pid);
  for (const targetPid of uniqueTargets) {
    const targetExports = requireProcessKernel(targetPid);
    const targetKernelState = targetStateByPid.get(targetPid);
    if (targetKernelState) {
      attachKernelRuntimeState(targetExports, targetKernelState);
    }
    const targetState = readKernelRuntimeState(targetExports);
    copySelectedOfdState(exports, canonicalState, targetExports, targetState, ofdIds);
    copyMemfsState(
      exports,
      canonicalState,
      targetExports,
      targetState,
    );
  }

  return readKernelRuntimeState(processExports);
}

function rehydrateForkSharedState(
  targetPids: number[],
  targetKernelStates: KernelWorkerPidStateSnapshot[],
  ofdIds: number[],
): void {
  const canonicalState = readKernelRuntimeState(exports);
  const targetStateByPid = new Map<number, KernelRuntimeState>();
  for (const target of targetKernelStates) {
    targetStateByPid.set(target.pid, target.kernelState);
  }
  const uniqueTargets = new Set(targetPids);
  for (const targetPid of uniqueTargets) {
    const targetExports = requireProcessKernel(targetPid);
    const targetKernelState = targetStateByPid.get(targetPid);
    if (targetKernelState) {
      attachKernelRuntimeState(targetExports, targetKernelState);
    }
    const targetState = readKernelRuntimeState(targetExports);
    clearSelectedOfdState(targetExports, targetState, ofdIds);
    clearMemfsState(targetExports, targetState);
    clearMemoryRange(targetExports.memory, targetState.pipeBase, targetExports.kernel_pipe_region_bytes());
    clearEpollState(targetExports, targetState);
    clearMemoryRange(
      targetExports.memory,
      targetState.socketTablePtr,
      targetExports.kernel_socket_table_bytes(),
    );
    clearMemoryRange(
      targetExports.memory,
      targetState.netBufBase,
      targetExports.kernel_net_buf_pool_bytes(),
    );
    copySelectedOfdState(exports, canonicalState, targetExports, targetState, ofdIds);
    copyMemfsState(
      exports,
      canonicalState,
      targetExports,
      targetState,
    );
    copyMemoryRange(
      exports.memory,
      canonicalState.pipeBase,
      targetExports.memory,
      targetState.pipeBase,
      exports.kernel_pipe_region_bytes(),
    );
    copyEpollState(
      exports,
      canonicalState,
      targetExports,
      targetState,
    );
    copySocketState(
      exports,
      canonicalState,
      targetExports,
      targetState,
    );
  }
}

function recordChildExit(
  pid: number,
  childPid: number,
  waitStatus: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  processExports.process_record_child_exit(childPid, waitStatus);
  return readKernelRuntimeState(processExports);
}

function resumeConnect(
  pid: number,
  vmPtr: number,
  result: bigint,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  processExports.thread_resume_connect_result(vmPtr, result);
  return readKernelRuntimeState(processExports);
}

function resumeWait4(
  pid: number,
  vmPtr: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  processExports.thread_resume_wait4(vmPtr);
  return readKernelRuntimeState(processExports);
}

function resumeEpollWait(
  pid: number,
  vmPtr: number,
  timeoutExpired: boolean,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  if (timeoutExpired) {
    processExports.thread_resume_epoll_wait_timeout(vmPtr);
  } else {
    processExports.thread_resume_blocked_syscall(vmPtr);
  }
  return readKernelRuntimeState(processExports);
}

function resumeBlockedSyscall(
  pid: number,
  vmPtr: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  try {
    processExports.thread_resume_blocked_syscall(vmPtr);
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw new Error(`${detail}\n${formatProcessDebug(processExports, vmPtr)}`);
  }
  return readKernelRuntimeState(processExports);
}

function resumeVforkParent(
  pid: number,
  vmPtr: number,
  waitStatus: number,
  kernelState: KernelRuntimeState,
): { childPid: number; kernelState: KernelRuntimeState } {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  const childPid = processExports.thread_vfork_child_pid();
  if (childPid > 0) {
    processExports.process_record_child_exit(childPid, waitStatus);
    processExports.thread_resume_vfork_parent(vmPtr, childPid);
  }
  return {
    childPid,
    kernelState: readKernelRuntimeState(processExports),
  };
}

function reserveCanonicalPid(provisionalChildPid: number): number {
  const childPid = Math.max(canonicalNextPid, provisionalChildPid);
  canonicalNextPid = childPid + 1;
  return childPid;
}

function upsertCanonicalProcess(meta: {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
}): void {
  canonicalProcesses.set(meta.pid, meta);
  for (const processExports of processKernels.values()) {
    processExports.process_upsert_process_entry(
      meta.pid,
      meta.ppid,
      meta.pgid,
      meta.sid,
      meta.didExec ? 1 : 0,
    );
  }
}

function canonicalizeChildPid(
  pid: number,
  vmPtr: number,
  provisionalChildPid: number,
  kernelState: KernelRuntimeState,
): { childPid: number; nextPid: number; pgid: number; sid: number; kernelState: KernelRuntimeState } {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  const childPid = reserveCanonicalPid(provisionalChildPid);
  processExports.process_rewrite_child_pid(vmPtr, provisionalChildPid, childPid, canonicalNextPid);
  upsertCanonicalProcess({
    pid: childPid,
    ppid: processExports.process_current_ppid(vmPtr),
    pgid: processExports.process_current_pgid(vmPtr),
    sid: processExports.process_current_sid(vmPtr),
    didExec: false,
  });
  return {
    childPid,
    nextPid: canonicalNextPid,
    pgid: processExports.process_current_pgid(vmPtr),
    sid: processExports.process_current_sid(vmPtr),
    kernelState: readKernelRuntimeState(processExports),
  };
}

function initializeProcessLifecycle(
  pid: number,
  vmPtr: number,
  ppid: number,
  nextPid: number,
  pgid: number,
  sid: number,
  didExec: boolean,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  canonicalNextPid = Math.max(canonicalNextPid, nextPid, pid + 1);
  processExports.process_reset_lifecycle(
    vmPtr,
    pid,
    ppid,
    canonicalNextPid,
    pgid,
    sid,
    didExec ? 1 : 0,
  );
  upsertCanonicalProcess({
    pid,
    ppid,
    pgid,
    sid,
    didExec,
  });
  return readKernelRuntimeState(processExports);
}

function resumeVforkExecveParent(
  pid: number,
  vmPtr: number,
  childPid: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  processExports.thread_resume_vfork_parent(vmPtr, childPid);
  return readKernelRuntimeState(processExports);
}

function syncProcessLifecycle(
  pid: number,
  vmPtr: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  upsertCanonicalProcess({
    pid,
    ppid: processExports.process_current_ppid(vmPtr),
    pgid: processExports.process_current_pgid(vmPtr),
    sid: processExports.process_current_sid(vmPtr),
    didExec: processExports.process_current_did_exec(vmPtr) !== 0,
  });
  return readKernelRuntimeState(processExports);
}

function syncPipeState(
  pid: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    return kernelState;
  }
  attachKernelRuntimeState(processExports, kernelState);

  const sourceState = readKernelRuntimeState(processExports);
  const canonicalState = readKernelRuntimeState(exports);
  attachKernelRuntimeState(exports, {
    ...canonicalState,
    pipeUsed: sourceState.pipeUsed,
  });
  copyMemoryRange(
    processExports.memory,
    sourceState.pipeBase,
    exports.memory,
    canonicalState.pipeBase,
    processExports.kernel_pipe_region_bytes(),
  );

  for (const memberExports of processKernels.values()) {
    const memberState = readKernelRuntimeState(memberExports);
    attachKernelRuntimeState(memberExports, {
      ...memberState,
      pipeUsed: sourceState.pipeUsed,
    });
    copyMemoryRange(
      exports.memory,
      canonicalState.pipeBase,
      memberExports.memory,
      memberState.pipeBase,
      memberExports.kernel_pipe_region_bytes(),
    );
  }

  return readKernelRuntimeState(processExports);
}

function syncEpollState(
  pid: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    return kernelState;
  }
  attachKernelRuntimeState(processExports, kernelState);

  const sourceState = readKernelRuntimeState(processExports);
  const canonicalState = readKernelRuntimeState(exports);
  copyEpollState(
    processExports,
    sourceState,
    exports,
    canonicalState,
  );

  for (const memberExports of processKernels.values()) {
    const memberState = readKernelRuntimeState(memberExports);
    copyEpollState(
      exports,
      canonicalState,
      memberExports,
      memberState,
    );
  }

  return readKernelRuntimeState(processExports);
}

function syncSocketState(
  pid: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    return kernelState;
  }
  attachKernelRuntimeState(processExports, kernelState);

  const sourceState = readKernelRuntimeState(processExports);
  const canonicalState = readKernelRuntimeState(exports);
  copySocketState(
    processExports,
    sourceState,
    exports,
    canonicalState,
  );

  for (const memberExports of processKernels.values()) {
    const memberState = readKernelRuntimeState(memberExports);
    copySocketState(
      exports,
      canonicalState,
      memberExports,
      memberState,
    );
  }

  return readKernelRuntimeState(processExports);
}

function releaseHostStdio(
  pid: number,
  vmPtr: number,
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = processKernels.get(pid);
  if (!processExports) {
    return kernelState;
  }
  attachKernelRuntimeState(processExports, kernelState);
  processExports.process_release_host_stdio(vmPtr);
  return syncPipeState(pid, readKernelRuntimeState(processExports));
}

function ensureDirectory(path: string, mode = 0o755): number {
  const parts = pathParts(path);
  let parentIno = 1;
  const perms = normalizePermMode(mode, 0o755);

  for (const dir of parts) {
    const dirBytes = encoder.encode(dir);
    const dirPtr = writeScratch(dirBytes);
    const prevParent = parentIno;
    parentIno = exports.memfs_ensure_dir(parentIno, dirPtr, dirBytes.length, perms);
    if (parentIno === 0) {
      throw new Error(`failed to create directory: ${path}`);
    }
    publishInode(sabPageCache, parentIno, {
      mode: 0o40000 | perms,
      size: 0,
      parentIno: prevParent,
      name: dirBytes,
      nextSibling: 0,
    }, null);
  }

  if (parts.length > 0) {
    bumpDirEpoch(sabPageCache);
  }

  return parentIno;
}

function resolvePath(path: string): number {
  const normalized = normalizePath(path);
  const pathBytes = encoder.encode(normalized);
  const pathPtr = writeScratch(pathBytes);
  return exports.memfs_resolve_path(pathPtr, pathBytes.length);
}

function writeFile(path: string, data: Uint8Array, mode = 0o644, fanout = true): void {
  const normalized = normalizePath(path);
  const parts = pathParts(normalized);
  const fileName = parts.pop();
  if (!fileName) {
    throw new Error(`invalid file path: ${path}`);
  }

  const parentPath = parts.length === 0 ? "/" : `/${parts.join("/")}`;
  const parentIno = ensureDirectory(parentPath);
  const fileMode = normalizePermMode(mode, 0o644);

  const existingIno = resolvePath(normalized);
  const fileBytes = encoder.encode(fileName);
  const namePtr = writeScratch(fileBytes);

  const ino = existingIno !== 0
    ? existingIno
    : exports.memfs_create_file(parentIno, namePtr, fileBytes.length);
  if (ino === 0) {
    throw new Error(`failed to create file: ${path}`);
  }

  const truncateResult = Number(exports.memfs_truncate_file(ino, BigInt(data.length)));
  if (truncateResult < 0) {
    throw new Error(`failed to truncate file: ${path}`);
  }

  if (data.length > 0) {
    const dataPtr = writeScratch(data, fileBytes.length + 64);
    const written = Number(exports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
    if (written < 0 || written !== data.length) {
      throw new Error(`failed to write file: ${path}`);
    }
  }
  const chmodResult = Number(exports.memfs_set_mode(ino, fileMode));
  if (chmodResult < 0) {
    throw new Error(`failed to set mode on file: ${path}`);
  }

  publishInode(sabPageCache, ino, {
    mode: 0o100000 | fileMode,
    size: data.length,
    parentIno,
    name: fileBytes,
    nextSibling: 0,
  }, data);
  bumpDirEpoch(sabPageCache);
  upsertFileSeed(normalized, data, fileMode);
  if (fanout) {
    fanoutCanonicalMemfs();
  }
}

function readFile(path: string): Uint8Array {
  const ino = resolvePath(path);
  if (ino === 0) {
    throw new Error(`file not found: ${path}`);
  }

  const size = Number(exports.memfs_file_size(ino));
  const bufPtr = scratchBase() + 0x4000;
  ensureMemoryCapacity(bufPtr + size);
  const read = Number(exports.memfs_read_file_to(ino, 0n, bufPtr, size));
  if (read < 0) {
    throw new Error(`failed to read file: ${path}`);
  }

  return new Uint8Array(exports.memory.buffer.slice(bufPtr, bufPtr + read));
}

function mkdir(path: string, mode = 0o755, fanout = true): void {
  const normalized = normalizePath(path);
  ensureDirectory(normalized, mode);
  upsertDirSeed(normalized, mode);
  if (fanout) {
    fanoutCanonicalMemfs();
  }
}

function loadFsSnapshot(entries: FsSnapshotEntry[]): void {
  exports.kernel_reset_memfs();
  resetPageCache(sabPageCache);
  seedEntries.clear();

  const rootEntry = entries.find((entry) => entry.kind === "dir" && normalizePath(entry.path) === "/");
  const rootMode = normalizePermMode(rootEntry?.mode, 0o755);
  seedEntries.set("/", { kind: "dir", path: "/", mode: rootMode });
  const rootModeResult = Number(exports.memfs_set_mode(1, rootMode));
  if (rootModeResult < 0) {
    throw new Error("failed to set mode on root directory");
  }
  publishRootInode(rootMode);

  const ordered = entries
    .filter((entry) => !(entry.kind === "dir" && normalizePath(entry.path) === "/"))
    .sort((left, right) => {
      if (left.kind !== right.kind) {
        return left.kind === "dir" ? -1 : 1;
      }
      return left.path.localeCompare(right.path);
    });

  for (const entry of ordered) {
    if (entry.kind === "dir") {
      mkdir(entry.path, entry.mode ?? 0o755, false);
      continue;
    }
    writeFile(entry.path, entry.data ?? new Uint8Array(), entry.mode ?? 0o644, false);
  }

  fanoutCanonicalMemfs();
}

function statPath(path: string): RuntimeStat {
  const ino = resolvePath(path);
  if (ino === 0) {
    throw new Error(`path not found: ${path}`);
  }

  return {
    ino,
    mode: exports.memfs_inode_mode(ino),
    nlink: exports.memfs_inode_nlink(ino),
    uid: exports.memfs_inode_uid(ino),
    gid: exports.memfs_inode_gid(ino),
    size: Number(exports.memfs_file_size(ino)),
  };
}

function readdir(path: string): string[] {
  const ino = resolvePath(path);
  if (ino === 0) {
    throw new Error(`directory not found: ${path}`);
  }

  const bufPtr = scratchBase() + 0x8000;
  const bufLen = 64 * 1024;
  const byteLen = Number(exports.memfs_readdir_names(ino, bufPtr, bufLen));
  if (byteLen < 0) {
    throw new Error(`failed to read directory: ${path}`);
  }

  const bytes = new Uint8Array(exports.memory.buffer, bufPtr, byteLen);
  const names: string[] = [];
  let start = 0;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] !== 0) {
      continue;
    }
    if (i > start) {
      names.push(decoder.decode(bytes.subarray(start, i)));
    }
    start = i + 1;
  }
  return names;
}

await bindWorkerMessageHandler<KernelWorkerInbound>(async (event) => {
  const msg = event.data;

  try {
    switch (msg.type) {
      case "init-kernel": {
        const kernelState = await initKernel(msg.wasmModule, msg.sabPageCache);
        postResponse({ type: "ready", requestId: msg.requestId, kernelState });
        break;
      }
      case "write-file":
        writeFile(msg.path, msg.data, msg.mode);
        postResponse({ type: "file-written", requestId: msg.requestId });
        break;
      case "read-file":
        postResponse({
          type: "file-read",
          requestId: msg.requestId,
          data: readFile(msg.path),
        });
        break;
      case "mkdir":
        mkdir(msg.path, msg.mode ?? 0o755);
        postResponse({ type: "dir-created", requestId: msg.requestId });
        break;
      case "stat":
        postResponse({
          type: "stat-result",
          requestId: msg.requestId,
          stat: statPath(msg.path),
        });
        break;
      case "readdir":
        postResponse({
          type: "readdir-result",
          requestId: msg.requestId,
          entries: readdir(msg.path),
        });
        break;
      case "snapshot-fs":
        postResponse({
          type: "snapshot-fs-result",
          requestId: msg.requestId,
          entries: Array.from(seedEntries.values(), cloneSnapshotEntry),
        });
        break;
      case "load-fs-snapshot":
        loadFsSnapshot(msg.entries);
        postResponse({ type: "fs-loaded", requestId: msg.requestId });
        break;
      case "register-process": {
        const kernelState = await registerProcess(
          msg.pid,
          msg.memory,
          msg.kernelState,
          msg.seedMemfsFromCanonical ?? true,
        );
        postResponse({
          type: "process-registered",
          requestId: msg.requestId,
          kernelState,
        });
        break;
      }
      case "unregister-process":
        unregisterProcess(msg.pid);
        postResponse({ type: "process-unregistered", requestId: msg.requestId });
        break;
      case "process-fs":
        {
          const kernelState = processFs(msg.pid, msg.vmPtr, msg.kernelState);
          postResponse({
            type: "fs-processed",
            requestId: msg.requestId,
            kernelState,
          });
        }
        break;
      case "record-child-exit":
        postResponse({
          type: "child-exit-recorded",
          requestId: msg.requestId,
          kernelState: recordChildExit(
            msg.pid,
            msg.childPid,
            msg.waitStatus,
            msg.kernelState,
          ),
        });
        break;
      case "resume-connect":
        postResponse({
          type: "connect-resumed",
          requestId: msg.requestId,
          kernelState: resumeConnect(
            msg.pid,
            msg.vmPtr,
            msg.result,
            msg.kernelState,
          ),
        });
        break;
      case "resume-wait4":
        postResponse({
          type: "wait4-resumed",
          requestId: msg.requestId,
          kernelState: resumeWait4(
            msg.pid,
            msg.vmPtr,
            msg.kernelState,
          ),
        });
        break;
      case "resume-epoll-wait":
        postResponse({
          type: "epoll-wait-resumed",
          requestId: msg.requestId,
          kernelState: resumeEpollWait(
            msg.pid,
            msg.vmPtr,
            msg.timeoutExpired,
            msg.kernelState,
          ),
        });
        break;
      case "resume-blocked-syscall":
        postResponse({
          type: "blocked-syscall-resumed",
          requestId: msg.requestId,
          kernelState: resumeBlockedSyscall(
            msg.pid,
            msg.vmPtr,
            msg.kernelState,
          ),
        });
        break;
      case "resume-vfork-parent": {
        const { childPid, kernelState } = resumeVforkParent(
          msg.pid,
          msg.vmPtr,
          msg.waitStatus,
          msg.kernelState,
        );
        postResponse({
          type: "vfork-parent-resumed",
          requestId: msg.requestId,
          childPid,
          kernelState,
        });
        break;
      }
      case "canonicalize-child-pid": {
        const { childPid, nextPid, pgid, sid, kernelState } = canonicalizeChildPid(
          msg.pid,
          msg.vmPtr,
          msg.provisionalChildPid,
          msg.kernelState,
        );
        postResponse({
          type: "child-pid-canonicalized",
          requestId: msg.requestId,
          childPid,
          nextPid,
          pgid,
          sid,
          kernelState,
        });
        break;
      }
      case "initialize-process-lifecycle":
        postResponse({
          type: "process-lifecycle-initialized",
          requestId: msg.requestId,
          kernelState: initializeProcessLifecycle(
            msg.pid,
            msg.vmPtr,
            msg.ppid,
            msg.nextPid,
            msg.pgid,
            msg.sid,
            msg.didExec,
            msg.kernelState,
          ),
        });
        break;
      case "resume-vfork-execve-parent":
        postResponse({
          type: "vfork-execve-parent-resumed",
          requestId: msg.requestId,
          kernelState: resumeVforkExecveParent(
            msg.pid,
            msg.vmPtr,
            msg.childPid,
            msg.kernelState,
          ),
        });
        break;
      case "sync-process-lifecycle":
        postResponse({
          type: "process-lifecycle-synced",
          requestId: msg.requestId,
          kernelState: syncProcessLifecycle(
            msg.pid,
            msg.vmPtr,
            msg.kernelState,
          ),
        });
        break;
      case "sync-pipe-state":
        postResponse({
          type: "pipe-state-synced",
          requestId: msg.requestId,
          kernelState: syncPipeState(
            msg.pid,
            msg.kernelState,
          ),
        });
        break;
      case "sync-epoll-state":
        postResponse({
          type: "epoll-state-synced",
          requestId: msg.requestId,
          kernelState: syncEpollState(
            msg.pid,
            msg.kernelState,
          ),
        });
        break;
      case "sync-socket-state":
        postResponse({
          type: "socket-state-synced",
          requestId: msg.requestId,
          kernelState: syncSocketState(
            msg.pid,
            msg.kernelState,
          ),
        });
        break;
      case "sync-fork-shared-state":
        postResponse({
          type: "fork-shared-state-synced",
          requestId: msg.requestId,
          kernelState: syncForkSharedState(
            msg.pid,
            msg.targetPids,
            msg.targetKernelStates,
            msg.ofdIds,
            msg.ofdSlots,
            msg.kernelState,
          ),
        });
        break;
      case "rehydrate-fork-shared-state":
        rehydrateForkSharedState(msg.targetPids, msg.targetKernelStates, msg.ofdIds);
        postResponse({
          type: "fork-shared-state-rehydrated",
          requestId: msg.requestId,
        });
        break;
      case "debug-processes": {
        const { pids, canonicalPids } = debugProcesses();
        postResponse({
          type: "processes-debugged",
          requestId: msg.requestId,
          pids,
          canonicalPids,
        });
        break;
      }
      case "release-host-stdio":
        postResponse({
          type: "host-stdio-released",
          requestId: msg.requestId,
          kernelState: releaseHostStdio(
            msg.pid,
            msg.vmPtr,
            msg.kernelState,
          ),
        });
        break;
    }
  } catch (error) {
      postResponse({
        type: "error",
        requestId: msg.requestId,
        message: error instanceof Error ? (error.stack ?? error.message) : String(error),
      });
  }
});
