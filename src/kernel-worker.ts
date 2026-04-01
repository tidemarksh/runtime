/**
 * Dedicated kernel-worker RPC endpoint.
 *
 * Kernel-worker-owned FS state is canonical here, and registered process
 * memories are rehydrated from this worker for MemFS/control-plane ownership
 * and fork-shared OFD/MemFS fanout. The current kernel still runs per-process
 * instances, so this remains a staged ownership boundary rather than a single
 * in-worker kernel instance.
 */

import type { KernelExports, KernelRuntimeState } from "./abi.js";
import { attachKernelRuntimeState, readKernelRuntimeState } from "./abi.js";
import type { FsSnapshotEntry, RuntimeStat } from "./fs-types.js";
import type {
  KernelWorkerInbound,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerOutbound,
  KernelWorkerPidBytesSnapshot,
  KernelWorkerPidStateSnapshot,
} from "./messages.js";
import { bumpDirEpoch, publishInode, readInodeRecord, resetPageCache } from "./sab-page-cache.js";
import { createKernelEnv } from "./wasm-env.js";
import { bindWorkerMessageHandler, workerPostMessage } from "./worker-scope.js";

let exports: KernelExports;
let wasmModule: WebAssembly.Module | null = null;
let sabPageCache: SharedArrayBuffer;
const processKernels = new Map<number, KernelExports>();
const lastProcessPipeSlotModes = new Map<number, Map<number, number>>();
let canonicalNextPid = 3;
const canonicalProcesses = new Map<number, {
  pid: number;
  ppid: number;
  pgid: number;
  sid: number;
  didExec: boolean;
  vmPtr: number;
}>();

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const seedEntries = new Map<string, FsSnapshotEntry>();
const FS_EFFECT_MEMFS_MUTATION = 1 << 0;
const FS_EFFECT_SHARED_OFD_STATE = 1 << 1;
const FS_EFFECT_MEMFS_FILE_DATA = 1 << 2;
const FS_OP_OPENAT = 3;
const FS_OP_MKDIR = 6;
type MemfsSeedMode = "full" | "metadata" | "none";
type PipeSeedMode = "canonical" | "process";
const FD_TYPE_PIPE = 6;
const PIPE_SLOT_BYTES = 32 + 4096;
const PIPE_SLOT_MODE_READ = 1 << 0;
const PIPE_SLOT_MODE_WRITE = 1 << 1;
const PIPE_OFF_HEAD = 0;
const PIPE_OFF_TAIL = 4;
const PIPE_OFF_READ_OPEN = 8;
const PIPE_OFF_WRITE_OPEN = 12;
const PIPE_OFF_DATA = 32;
const materializedMemfsFiles = new WeakMap<KernelExports, Set<number>>();

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
        ino: entry.ino,
        data: entry.data ? new Uint8Array(entry.data) : undefined,
        size: entry.size ?? entry.data?.length ?? 0,
        mode: entry.mode,
      }
    : {
        kind: "dir",
        path: entry.path,
        ino: entry.ino,
        mode: entry.mode,
      };
}

function scratchBase(): number {
  return exports.kernel_heap_ptr() + 0x10000;
}

function scratchBaseFor(targetExports: KernelExports): number {
  return targetExports.kernel_heap_ptr() + 0x10000;
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

function writeScratchFor(targetExports: KernelExports, bytes: Uint8Array, offset = 0): number {
  const scratch = scratchBaseFor(targetExports) + offset;
  ensureMemoryCapacityFor(targetExports.memory, scratch + bytes.length);
  const mem = new Uint8Array(targetExports.memory.buffer);
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

function markMaterializedMemfsFile(targetExports: KernelExports, ino: number): void {
  let inos = materializedMemfsFiles.get(targetExports);
  if (!inos) {
    inos = new Set<number>();
    materializedMemfsFiles.set(targetExports, inos);
  }
  inos.add(ino);
}

function clearMaterializedMemfsFiles(targetExports: KernelExports): void {
  materializedMemfsFiles.delete(targetExports);
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

const MEMFS_S_IFMT = 0o170000;
const MEMFS_S_IFREG = 0o100000;
const MEMFS_HEADER_OFF_INODE_COUNT = 0;
const MEMFS_HEADER_OFF_DIR_ENTRY_COUNT = 4;
const MEMFS_HEADER_OFF_DATA_USED = 8;
const MEMFS_HEADER_OFF_DATA_CAP = 12;
const MEMFS_HEADER_OFF_INODES_PTR = 16;
const MEMFS_HEADER_OFF_FIRST_CHILD_PTR = 20;
const MEMFS_HEADER_OFF_DIR_ENTRIES_PTR = 24;
const MEMFS_HEADER_OFF_DATA_PTR = 28;
const MEMFS_INODE_OFF_MODE = 0;
const MEMFS_INODE_OFF_DATA_OFFSET = 24;
const MEMFS_INODE_OFF_DATA_CAP = 28;

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

function normalizeMetadataOnlyMemfsState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  sourceLayout: MemfsLayout,
): void {
  const header = new DataView(targetExports.memory.buffer, targetState.memfsPtr, 32);
  header.setUint32(MEMFS_HEADER_OFF_INODE_COUNT, sourceLayout.inodeCount, true);
  header.setUint32(MEMFS_HEADER_OFF_DIR_ENTRY_COUNT, sourceLayout.dirEntryCount, true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_USED, 8, true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_CAP, targetExports.kernel_memfs_data_bytes(), true);
  header.setUint32(MEMFS_HEADER_OFF_INODES_PTR, targetExports.kernel_memfs_inodes_ptr(), true);
  header.setUint32(MEMFS_HEADER_OFF_FIRST_CHILD_PTR, targetExports.kernel_memfs_first_child_ptr(), true);
  header.setUint32(MEMFS_HEADER_OFF_DIR_ENTRIES_PTR, targetExports.kernel_memfs_dir_entries_ptr(), true);
  header.setUint32(MEMFS_HEADER_OFF_DATA_PTR, targetExports.kernel_memfs_data_ptr(), true);

  const maxInodes = targetExports.kernel_memfs_first_child_bytes() / 4;
  const inodeBytes = targetExports.kernel_memfs_inodes_bytes() / maxInodes;
  const inodeView = new DataView(targetExports.memory.buffer);
  for (let ino = 1; ino < sourceLayout.inodeCount; ino++) {
    const inodeBase = targetExports.kernel_memfs_inodes_ptr() + ino * inodeBytes;
    const mode = inodeView.getUint32(inodeBase + MEMFS_INODE_OFF_MODE, true);
    if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
      continue;
    }
    inodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_OFFSET, 0, true);
    inodeView.setUint32(inodeBase + MEMFS_INODE_OFF_DATA_CAP, 0, true);
  }
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

function collectProcessHeldOfdRefcounts(processExports: KernelExports, vmPtr: number): Int32Array {
  const maxFds = processExports.abi_max_fds();
  const heldOfdRefcounts = new Int32Array(maxFds);
  const processHeldOfds = new Set<number>();

  for (let fd = 0; fd < maxFds; fd += 1) {
    const ofdId = processExports.process_fd_ofd_id(vmPtr, fd);
    if (ofdId !== 0 && ofdId < maxFds) {
      processHeldOfds.add(ofdId);
    }
  }

  for (const ofdId of processHeldOfds) {
    heldOfdRefcounts[ofdId] += 1;
  }

  for (let slot = 0; slot < processExports.abi_max_threads(); slot += 1) {
    const threadVmPtr = processExports.process_thread_vm_ptr(vmPtr, slot);
    if (threadVmPtr === 0) {
      continue;
    }
    const blockedOfdId = processExports.thread_blocked_ofd_id(threadVmPtr);
    if (blockedOfdId !== 0 && blockedOfdId < maxFds) {
      // Blocked waits pin the OFD independently of the fd table.
      heldOfdRefcounts[blockedOfdId] += 1;
    }
  }

  return heldOfdRefcounts;
}

function collectProcessHeldPipeSlots(processExports: KernelExports, vmPtr: number): Set<number> {
  const maxFds = processExports.abi_max_fds();
  const maxPipes = processExports.abi_max_pipes();
  const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
  const privatePipeSlots = collectProcessPrivatePipeSlots(processExports, vmPtr);
  const pipeSlots = new Set<number>();

  for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
    if (heldRefcounts[ofdId] <= 0 || processExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
      continue;
    }
    const pipeIdx = processExports.kernel_ofd_ino(ofdId);
    if (pipeIdx > 0 && pipeIdx < maxPipes && !privatePipeSlots.has(pipeIdx)) {
      pipeSlots.add(pipeIdx);
    }
  }

  return pipeSlots;
}

function collectProcessPrivatePipeSlots(processExports: KernelExports, vmPtr: number): Set<number> {
  const maxPipes = processExports.abi_max_pipes();
  const privatePipeSlots = new Set<number>();
  const candidatePipeSlots = [
    processExports.process_stdin_pipe(vmPtr),
    processExports.process_stdout_pipe(vmPtr),
    processExports.process_stderr_pipe(vmPtr),
  ];

  for (const pipeSlot of candidatePipeSlots) {
    if (pipeSlot > 0 && pipeSlot < maxPipes) {
      privatePipeSlots.add(pipeSlot);
    }
  }

  return privatePipeSlots;
}

function composeProcessPipeUsed(
  processExports: KernelExports,
  vmPtr: number,
  sharedPipeUsed: bigint,
): bigint {
  let pipeUsed = sharedPipeUsed | 1n;
  for (const pipeSlot of collectProcessPrivatePipeSlots(processExports, vmPtr)) {
    pipeUsed |= 1n << BigInt(pipeSlot);
  }
  return pipeUsed;
}

function collectProcessPipeSlotModes(processExports: KernelExports, vmPtr: number): Map<number, number> {
  const maxFds = processExports.abi_max_fds();
  const maxPipes = processExports.abi_max_pipes();
  const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
  const privatePipeSlots = collectProcessPrivatePipeSlots(processExports, vmPtr);
  const pipeSlotModes = new Map<number, number>();

  for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
    if (heldRefcounts[ofdId] <= 0 || processExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
      continue;
    }
    const pipeIdx = processExports.kernel_ofd_ino(ofdId);
    if (pipeIdx <= 0 || pipeIdx >= maxPipes || privatePipeSlots.has(pipeIdx)) {
      continue;
    }
    const currentMode = pipeSlotModes.get(pipeIdx) ?? 0;
    const nextMode = processExports.kernel_ofd_host_fd(ofdId) >= 0
      ? currentMode | PIPE_SLOT_MODE_READ
      : currentMode | PIPE_SLOT_MODE_WRITE;
    pipeSlotModes.set(pipeIdx, nextMode);
  }

  return pipeSlotModes;
}

function reconcileCanonicalOfdRefcounts(
  memberPids: Iterable<number>,
  memberVmPtrs: ReadonlyMap<number, number>,
  ofdIds: Iterable<number>,
): void {
  const maxFds = exports.abi_max_fds();
  const trackedOfdIds = [...new Set(ofdIds)].filter((ofdId) => ofdId > 0 && ofdId < maxFds);
  if (trackedOfdIds.length === 0) {
    return;
  }

  const tracked = new Set<number>(trackedOfdIds);
  const expectedRefcounts = new Int32Array(maxFds);
  for (const pid of memberPids) {
    const vmPtr = memberVmPtrs.get(pid) ?? 0;
    if (vmPtr === 0) {
      continue;
    }
    const processExports = processKernels.get(pid);
    if (!processExports) {
      continue;
    }
    const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
    for (const ofdId of trackedOfdIds) {
      if (tracked.has(ofdId)) {
        expectedRefcounts[ofdId] += heldRefcounts[ofdId] ?? 0;
      }
    }
  }

  for (const ofdId of trackedOfdIds) {
    let currentRefcount = exports.kernel_ofd_refcount(ofdId);
    const expectedRefcount = expectedRefcounts[ofdId] ?? 0;
    while (currentRefcount < expectedRefcount) {
      exports.kernel_retain_ofd(ofdId);
      currentRefcount += 1;
    }
    while (currentRefcount > expectedRefcount) {
      exports.kernel_release_ofd(ofdId);
      currentRefcount -= 1;
    }
  }
}

function recomputeCanonicalPipeControlState(): {
  kernelState: KernelRuntimeState;
  readOpen: Int32Array;
  writeOpen: Int32Array;
  managedPipeSlots: number[];
} {
  const maxFds = exports.abi_max_fds();
  const maxPipes = exports.abi_max_pipes();
  const readOpen = new Int32Array(maxPipes);
  const writeOpen = new Int32Array(maxPipes);
  const managedPipeSlots = new Set<number>();
  const priorPipeUsed = readKernelRuntimeState(exports).pipeUsed;

  for (let pipeIdx = 1; pipeIdx < maxPipes; pipeIdx += 1) {
    if ((priorPipeUsed & (1n << BigInt(pipeIdx))) !== 0n) {
      managedPipeSlots.add(pipeIdx);
    }
  }

  for (const [pid, processExports] of processKernels) {
    const vmPtr = canonicalProcesses.get(pid)?.vmPtr ?? 0;
    if (vmPtr === 0) {
      continue;
    }

    const heldRefcounts = collectProcessHeldOfdRefcounts(processExports, vmPtr);
    for (let ofdId = 1; ofdId < maxFds; ofdId += 1) {
      const refcount = heldRefcounts[ofdId] ?? 0;
      if (refcount <= 0 || processExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
        continue;
      }
      const pipeIdx = processExports.kernel_ofd_ino(ofdId);
      if (pipeIdx <= 0 || pipeIdx >= maxPipes) {
        continue;
      }
      managedPipeSlots.add(pipeIdx);
      if (processExports.kernel_ofd_host_fd(ofdId) >= 0) {
        readOpen[pipeIdx] += refcount;
      } else {
        writeOpen[pipeIdx] += refcount;
      }
    }
  }

  const bytes = new DataView(exports.memory.buffer);
  let pipeUsed = 1n;
  for (const pipeIdx of managedPipeSlots) {
    const slotBase = exports.kernel_pipe_base() + pipeIdx * PIPE_SLOT_BYTES;
    bytes.setInt32(slotBase + PIPE_OFF_READ_OPEN, readOpen[pipeIdx] ?? 0, true);
    bytes.setInt32(slotBase + PIPE_OFF_WRITE_OPEN, writeOpen[pipeIdx] ?? 0, true);
    if ((readOpen[pipeIdx] ?? 0) > 0 || (writeOpen[pipeIdx] ?? 0) > 0) {
      pipeUsed |= 1n << BigInt(pipeIdx);
    }
  }

  const kernelState = {
    ...readKernelRuntimeState(exports),
    pipeUsed,
  };
  attachKernelRuntimeState(exports, kernelState);
  return {
    kernelState,
    readOpen,
    writeOpen,
    managedPipeSlots: [...managedPipeSlots].sort((left, right) => left - right),
  };
}

function applyPipeControlState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  readOpen: Int32Array,
  writeOpen: Int32Array,
  pipeUsed: bigint,
  managedPipeSlots: Iterable<number>,
): void {
  attachKernelRuntimeState(targetExports, {
    ...targetState,
    pipeUsed,
  });
  const bytes = new DataView(targetExports.memory.buffer);
  for (const pipeIdx of managedPipeSlots) {
    const slotBase = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
    bytes.setInt32(slotBase + PIPE_OFF_READ_OPEN, readOpen[pipeIdx] ?? 0, true);
    bytes.setInt32(slotBase + PIPE_OFF_WRITE_OPEN, writeOpen[pipeIdx] ?? 0, true);
  }
}

function collectPipeSlotsForOfdIds(targetExports: KernelExports, ofdIds: Iterable<number>): number[] {
  const maxPipes = targetExports.abi_max_pipes();
  const pipeSlots = new Set<number>();
  for (const ofdId of ofdIds) {
    if (targetExports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE) {
      continue;
    }
    const pipeSlot = targetExports.kernel_ofd_ino(ofdId);
    if (pipeSlot > 0 && pipeSlot < maxPipes) {
      pipeSlots.add(pipeSlot);
    }
  }
  return [...pipeSlots].sort((left, right) => left - right);
}

function copySelectedPipeSlots(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  pipeSlots: Iterable<number>,
): void {
  const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
  const targetBytes = new Uint8Array(targetExports.memory.buffer);
  for (const pipeSlot of pipeSlots) {
    if (pipeSlot <= 0 || pipeSlot >= sourceExports.abi_max_pipes()) {
      continue;
    }
    const sourceOffset = sourceState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
    const targetOffset = targetState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
    targetBytes.set(sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES), targetOffset);
  }
}

function clearSelectedPipeSlots(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  pipeSlots: Iterable<number>,
): void {
  const targetBytes = new Uint8Array(targetExports.memory.buffer);
  for (const pipeSlot of pipeSlots) {
    if (pipeSlot <= 0 || pipeSlot >= targetExports.abi_max_pipes()) {
      continue;
    }
    const targetOffset = targetState.pipeBase + pipeSlot * PIPE_SLOT_BYTES;
    targetBytes.fill(0, targetOffset, targetOffset + PIPE_SLOT_BYTES);
  }
}

function mergePipeSlotsFromProcess(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  sourceModes: ReadonlyMap<number, number>,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  pipeSlots: Iterable<number>,
): void {
  const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
  const targetBytes = new Uint8Array(targetExports.memory.buffer);

  for (const pipeIdx of pipeSlots) {
    if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
      continue;
    }
    const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
    const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
    const mode = sourceModes.get(pipeIdx) ?? 0;
    if ((mode & PIPE_SLOT_MODE_READ) !== 0 && (mode & PIPE_SLOT_MODE_WRITE) !== 0) {
      targetBytes.set(sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES), targetOffset);
      continue;
    }
    if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
      targetBytes.set(
        sourceBytes.subarray(sourceOffset + PIPE_OFF_HEAD, sourceOffset + PIPE_OFF_TAIL),
        targetOffset + PIPE_OFF_HEAD,
      );
    }
    if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
      targetBytes.set(
        sourceBytes.subarray(sourceOffset + PIPE_OFF_TAIL, sourceOffset + PIPE_OFF_READ_OPEN),
        targetOffset + PIPE_OFF_TAIL,
      );
      targetBytes.set(
        sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
        targetOffset + PIPE_OFF_DATA,
      );
    }
  }
}

function mergePipeSlotsFromCanonical(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  targetVmPtr: number,
  pipeSlots: Iterable<number>,
): void {
  const sourceBytes = new Uint8Array(sourceExports.memory.buffer);
  const targetBytes = new Uint8Array(targetExports.memory.buffer);
  const targetModes = targetVmPtr !== 0
    ? collectProcessPipeSlotModes(targetExports, targetVmPtr)
    : new Map<number, number>();

  for (const pipeIdx of pipeSlots) {
    if (pipeIdx <= 0 || pipeIdx >= sourceExports.abi_max_pipes()) {
      continue;
    }
    const sourceOffset = sourceState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
    const targetOffset = targetState.pipeBase + pipeIdx * PIPE_SLOT_BYTES;
    const mode = targetModes.get(pipeIdx) ?? 0;
    if ((mode & PIPE_SLOT_MODE_READ) !== 0 && (mode & PIPE_SLOT_MODE_WRITE) !== 0) {
      targetBytes.set(sourceBytes.subarray(sourceOffset, sourceOffset + PIPE_SLOT_BYTES), targetOffset);
      continue;
    }
    if ((mode & PIPE_SLOT_MODE_READ) !== 0) {
      targetBytes.set(
        sourceBytes.subarray(sourceOffset + PIPE_OFF_TAIL, sourceOffset + PIPE_OFF_READ_OPEN),
        targetOffset + PIPE_OFF_TAIL,
      );
      targetBytes.set(
        sourceBytes.subarray(sourceOffset + PIPE_OFF_DATA, sourceOffset + PIPE_SLOT_BYTES),
        targetOffset + PIPE_OFF_DATA,
      );
    }
    if ((mode & PIPE_SLOT_MODE_WRITE) !== 0) {
      targetBytes.set(
        sourceBytes.subarray(sourceOffset + PIPE_OFF_HEAD, sourceOffset + PIPE_OFF_TAIL),
        targetOffset + PIPE_OFF_HEAD,
      );
    }
  }
}

function collectCanonicalManagedPipeSlots(kernelState: KernelRuntimeState): number[] {
  const managedPipeSlots = new Set<number>();
  const maxPipes = exports.abi_max_pipes();

  for (let pipeIdx = 1; pipeIdx < maxPipes; pipeIdx += 1) {
    if ((kernelState.pipeUsed & (1n << BigInt(pipeIdx))) !== 0n) {
      managedPipeSlots.add(pipeIdx);
    }
  }

  for (let ofdId = 1; ofdId < exports.abi_max_fds(); ofdId += 1) {
    if (exports.kernel_ofd_fd_type(ofdId) !== FD_TYPE_PIPE || exports.kernel_ofd_refcount(ofdId) <= 0) {
      continue;
    }
    const pipeIdx = exports.kernel_ofd_ino(ofdId);
    if (pipeIdx > 0 && pipeIdx < maxPipes) {
      managedPipeSlots.add(pipeIdx);
    }
  }

  return [...managedPipeSlots].sort((left, right) => left - right);
}

function copyMemfsState(
  sourceExports: KernelExports,
  sourceState: KernelRuntimeState,
  targetExports: KernelExports,
  _targetState: KernelRuntimeState,
  options: { includeData?: boolean } = {},
): void {
  const includeData = options.includeData ?? true;
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
  if (includeData) {
    copyMemoryRange(
      sourceExports.memory,
      sourceLayout.dataPtr,
      targetExports.memory,
      sourceLayout.dataPtr,
      sourceLayout.dataCap,
    );
  } else {
    clearMaterializedMemfsFiles(targetExports);
    clearMemoryRange(
      targetExports.memory,
      targetExports.kernel_memfs_data_ptr(),
      targetExports.kernel_memfs_data_bytes(),
    );
  }
  copyMemoryRange(
    sourceExports.memory,
    sourceState.memfsPtr,
    targetExports.memory,
    _targetState.memfsPtr,
    sourceExports.kernel_memfs_header_bytes(),
  );
  if (!includeData) {
    normalizeMetadataOnlyMemfsState(targetExports, _targetState, sourceLayout);
  }
}

function clearMemfsState(
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
): void {
  clearMaterializedMemfsFiles(targetExports);
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
  const tableEntries = tableBytes >>> 2;
  const sourceTable = new Uint32Array(sourceExports.memory.buffer, sourceState.epollTablePtr, tableEntries);
  const targetTable = new Uint32Array(targetExports.memory.buffer, targetState.epollTablePtr, tableEntries);
  const targetPtrs = [...targetTable];
  for (let i = 0; i < tableEntries; i += 1) {
    const sourcePtr = sourceTable[i] ?? 0;
    const targetPtr = targetPtrs[i] ?? 0;
    if (sourcePtr === 0) {
      targetTable[i] = 0;
      if (targetPtr !== 0) {
        clearMemoryRange(targetExports.memory, targetPtr, instanceBytes);
      }
      continue;
    }
    const instancePtr = targetPtr !== 0 ? targetPtr : sourcePtr;
    targetTable[i] = instancePtr;
    copyMemoryRange(
      sourceExports.memory,
      sourcePtr,
      targetExports.memory,
      instancePtr,
      instanceBytes,
    );
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

function resolvePathIn(targetExports: KernelExports, path: string): number {
  const pathBytes = encoder.encode(normalizePath(path));
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
  if (!target || target.length === 0 || resolvePathIn(targetExports, "/proc/self/exe") !== 0) {
    return;
  }
  const procName = encoder.encode("proc");
  const selfName = encoder.encode("self");
  const exeName = encoder.encode("exe");
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

function copyCanonicalMemfsToProcess(
  canonicalState: KernelRuntimeState,
  targetExports: KernelExports,
  targetState: KernelRuntimeState,
  options: { includeData?: boolean } = {},
): void {
  const procSelfExe = snapshotProcSelfExe(targetExports);
  copyMemfsState(
    exports,
    canonicalState,
    targetExports,
    targetState,
    options,
  );
  restoreProcSelfExe(targetExports, procSelfExe);
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
  try {
    const procSelfExe = snapshotProcSelfExe(processExports);
    if (procSelfExe === null) {
      parts.push("proc_self_exe=missing");
    } else {
      parts.push(`proc_self_exe=${JSON.stringify(decoder.decode(procSelfExe))}`);
    }
  } catch {
    parts.push("proc_self_exe=unavailable");
  }
  return parts.join(" ");
}

function fanoutCanonicalMemfs(includeData = false): void {
  const canonicalState = readKernelRuntimeState(exports);
  for (const processExports of processKernels.values()) {
    const processState = readKernelRuntimeState(processExports);
    copyCanonicalMemfsToProcess(canonicalState, processExports, processState, { includeData });
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
    env: createKernelEnv(privateMemory, { sabPageCache }),
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
    vmPtr: 0,
  });

  return readKernelRuntimeState(exports);
}

async function registerProcess(
  pid: number,
  vmPtr: number,
  memory: WebAssembly.Memory,
  kernelState: KernelRuntimeState,
  seedMemfsMode: MemfsSeedMode = "full",
  pipeSeedMode: PipeSeedMode = "canonical",
): Promise<KernelRuntimeState> {
  if (wasmModule === null) {
    throw new Error("kernel worker is not initialized");
  }
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: createKernelEnv(memory, { sabPageCache }),
  });
  const processExports = instance.exports as unknown as KernelExports;
  attachKernelRuntimeState(processExports, kernelState);
  processKernels.set(pid, processExports);
  const existingProcess = canonicalProcesses.get(pid);
  canonicalProcesses.set(pid, {
    pid,
    ppid: existingProcess?.ppid ?? 0,
    pgid: existingProcess?.pgid ?? pid,
    sid: existingProcess?.sid ?? pid,
    didExec: existingProcess?.didExec ?? false,
    vmPtr,
  });
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
    if (seedMemfsMode === "full") {
      copyMemfsState(
        processExports,
        sourceState,
        exports,
        canonicalState,
      );
      rebuildPageCacheFromCanonical();
    }
    const pipeState = syncPipeState(pid, readKernelRuntimeState(processExports));
    const epollState = syncEpollState(pid, pipeState);
    const socketState = syncSocketState(pid, epollState);
    if (seedMemfsMode === "metadata") {
      const memberState = readKernelRuntimeState(processExports);
      const updatedCanonicalState = readKernelRuntimeState(exports);
      copyCanonicalMemfsToProcess(updatedCanonicalState, processExports, memberState, {
        includeData: false,
      });
    }
    return socketState;
  }

  const canonicalState = readKernelRuntimeState(exports);
  const processState = readKernelRuntimeState(processExports);
  if (pipeSeedMode === "process") {
    syncPipeState(pid, processState);
  } else {
    attachKernelRuntimeState(processExports, {
      ...processState,
      pipeUsed: composeProcessPipeUsed(processExports, vmPtr, canonicalState.pipeUsed),
      socketUsed: canonicalState.socketUsed,
      netBufUsed: canonicalState.netBufUsed,
    });
    const registeredProcessState = readKernelRuntimeState(processExports);
    const sharedPipeSlots = collectProcessHeldPipeSlots(processExports, vmPtr);
    mergePipeSlotsFromCanonical(
      exports,
      canonicalState,
      processExports,
      registeredProcessState,
      vmPtr,
      sharedPipeSlots,
    );
    const {
      readOpen,
      writeOpen,
    } = recomputeCanonicalPipeControlState();
    applyPipeControlState(
      processExports,
      registeredProcessState,
      readOpen,
      writeOpen,
      composeProcessPipeUsed(processExports, vmPtr, readKernelRuntimeState(exports).pipeUsed),
      sharedPipeSlots,
    );
  }
  const registeredCanonicalState = readKernelRuntimeState(exports);
  const registeredProcessState = readKernelRuntimeState(processExports);
  if (seedMemfsMode !== "none") {
    copyCanonicalMemfsToProcess(registeredCanonicalState, processExports, registeredProcessState, {
      includeData: seedMemfsMode === "full",
    });
  }
  copyEpollState(
    exports,
    registeredCanonicalState,
    processExports,
    registeredProcessState,
  );
  copySocketState(
    exports,
    registeredCanonicalState,
    processExports,
    registeredProcessState,
  );
  return readKernelRuntimeState(processExports);
}

function unregisterProcess(pid: number): void {
  processKernels.delete(pid);
  lastProcessPipeSlotModes.delete(pid);
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
): {
  kernelState: KernelRuntimeState;
  memfsMutated: boolean;
  ofdStateChanged: boolean;
} {
  const processExports = requireProcessKernel(pid);
  attachKernelRuntimeState(processExports, kernelState);
  const opcode = processExports.thread_pending_fs_opcode(vmPtr);
  const effects = processExports.thread_pending_fs_effects(vmPtr);
  const fileDataTargetIno = (effects & FS_EFFECT_MEMFS_FILE_DATA) !== 0
    ? processExports.thread_pending_fs_target_ino(vmPtr)
    : 0;
  processExports.thread_process_fs(vmPtr);
  const result = Number(processExports.thread_resume_fs(vmPtr));
  const fsSucceeded = result >= 0;
  const treeMutated = fsSucceeded && (effects & FS_EFFECT_MEMFS_MUTATION) !== 0;
  const fileDataMutated = fsSucceeded && (effects & FS_EFFECT_MEMFS_FILE_DATA) !== 0;
  let handledFileDataMutation = false;
  if (!treeMutated && fileDataMutated && fileDataTargetIno !== 0) {
    handledFileDataMutation = syncFileDataMutation(processExports, fileDataTargetIno);
  }
  const needsFullSync = treeMutated || (fileDataMutated && !handledFileDataMutation);
  if (needsFullSync) {
    const sourceState = readKernelRuntimeState(processExports);
    const canonicalState = readKernelRuntimeState(exports);
    const metadataOnlyTreeSync = treeMutated
      && !fileDataMutated
      && (opcode === FS_OP_MKDIR || opcode === FS_OP_OPENAT);
    copyMemfsState(processExports, sourceState, exports, canonicalState, {
      includeData: !metadataOnlyTreeSync,
    });
    if (metadataOnlyTreeSync) {
      bumpDirEpoch(sabPageCache);
    } else {
      rebuildPageCacheFromCanonical();
    }
    fanoutCanonicalMemfs();
  }
  return {
    kernelState: readKernelRuntimeState(processExports),
    memfsMutated: treeMutated || fileDataMutated,
    ofdStateChanged: fsSucceeded && (effects & FS_EFFECT_SHARED_OFD_STATE) !== 0,
  };
}

function syncForkSharedState(
  pid: number,
  vmPtr: number,
  targetPids: number[],
  targetKernelStates: KernelWorkerPidStateSnapshot[],
  ofdIds: number[],
  ofdSlots: KernelWorkerOfdSlotSnapshot[],
  includeMemfs: boolean,
  preserveSourceRefcounts: boolean,
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
  const sourceHeldRefcounts = vmPtr !== 0
    ? collectProcessHeldOfdRefcounts(processExports, vmPtr)
    : new Int32Array(processExports.abi_max_fds());
  const sourceHeldOfdIds = ofdIds.filter((ofdId) => {
    if (ofdId <= 0 || ofdId >= processExports.abi_max_fds()) {
      return false;
    }
    return (sourceHeldRefcounts[ofdId] ?? 0) > 0;
  });
  const canonicalState = readKernelRuntimeState(exports);
  copySelectedOfdState(
    processExports,
    sourceState,
    exports,
    canonicalState,
    sourceHeldOfdIds.filter((ofdId) => !snappedOfdIds.has(ofdId)),
  );
  if (includeMemfs) {
    copyMemfsState(
      processExports,
      sourceState,
      exports,
      canonicalState,
    );
  }

  const memberVmPtrs = new Map<number, number>([[pid, vmPtr]]);
  const memberPids = new Set<number>([pid]);
  for (const target of targetKernelStates) {
    memberVmPtrs.set(target.pid, target.vmPtr);
  }
  for (const targetPid of targetPids) {
    memberPids.add(targetPid);
  }
  if (!preserveSourceRefcounts) {
    reconcileCanonicalOfdRefcounts(memberPids, memberVmPtrs, ofdIds);
  }
  const {
    kernelState: reconciledCanonicalState,
    readOpen,
    writeOpen,
  } = recomputeCanonicalPipeControlState();
  const sharedPipeSlots = collectPipeSlotsForOfdIds(exports, ofdIds);
  attachKernelRuntimeState(processExports, sourceState);
  copySelectedOfdState(exports, reconciledCanonicalState, processExports, sourceState, ofdIds);
  copySelectedPipeSlots(exports, reconciledCanonicalState, processExports, sourceState, sharedPipeSlots);
  applyPipeControlState(
    processExports,
    sourceState,
    readOpen,
    writeOpen,
    composeProcessPipeUsed(processExports, vmPtr, reconciledCanonicalState.pipeUsed),
    sharedPipeSlots,
  );

  const targetStateByPid = new Map<number, KernelRuntimeState>();
  const targetVmPtrByPid = new Map<number, number>();
  for (const target of targetKernelStates) {
    targetStateByPid.set(target.pid, target.kernelState);
    targetVmPtrByPid.set(target.pid, target.vmPtr);
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
    attachKernelRuntimeState(targetExports, targetState);
    copySelectedOfdState(exports, reconciledCanonicalState, targetExports, targetState, ofdIds);
    copySelectedPipeSlots(exports, reconciledCanonicalState, targetExports, targetState, sharedPipeSlots);
    applyPipeControlState(
      targetExports,
      targetState,
      readOpen,
      writeOpen,
      composeProcessPipeUsed(
        targetExports,
        targetVmPtrByPid.get(targetPid) ?? 0,
        reconciledCanonicalState.pipeUsed,
      ),
      sharedPipeSlots,
    );
    if (includeMemfs) {
      copyCanonicalMemfsToProcess(reconciledCanonicalState, targetExports, targetState);
    }
  }

  return readKernelRuntimeState(processExports);
}

function rehydrateForkSharedState(
  targetPids: number[],
  targetKernelStates: KernelWorkerPidStateSnapshot[],
  ofdIds: number[],
  procSelfExeTargets: KernelWorkerPidBytesSnapshot[] = [],
  includeMemfs = true,
): void {
  const {
    kernelState: canonicalState,
    readOpen,
    writeOpen,
  } = recomputeCanonicalPipeControlState();
  const targetStateByPid = new Map<number, KernelRuntimeState>();
  const targetVmPtrByPid = new Map<number, number>();
  for (const target of targetKernelStates) {
    targetStateByPid.set(target.pid, target.kernelState);
    targetVmPtrByPid.set(target.pid, target.vmPtr);
  }
  const procSelfExeTargetByPid = new Map<number, Uint8Array>();
  for (const target of procSelfExeTargets) {
    procSelfExeTargetByPid.set(target.pid, target.bytes);
  }
  const sharedPipeSlots = collectPipeSlotsForOfdIds(exports, ofdIds);
  const uniqueTargets = new Set(targetPids);
  for (const targetPid of uniqueTargets) {
    const targetExports = requireProcessKernel(targetPid);
    const targetKernelState = targetStateByPid.get(targetPid);
    if (targetKernelState) {
      attachKernelRuntimeState(targetExports, targetKernelState);
    }
    const targetState = readKernelRuntimeState(targetExports);
    const procSelfExe = includeMemfs
      ? (procSelfExeTargetByPid.get(targetPid) ?? snapshotProcSelfExe(targetExports))
      : null;
    clearSelectedOfdState(targetExports, targetState, ofdIds);
    if (includeMemfs) {
      clearMemfsState(targetExports, targetState);
    }
    clearSelectedPipeSlots(targetExports, targetState, sharedPipeSlots);
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
    if (includeMemfs) {
      copyMemfsState(
        exports,
        canonicalState,
        targetExports,
        targetState,
      );
      restoreProcSelfExe(targetExports, procSelfExe);
    }
    copySelectedPipeSlots(exports, canonicalState, targetExports, targetState, sharedPipeSlots);
    applyPipeControlState(
      targetExports,
      targetState,
      readOpen,
      writeOpen,
      composeProcessPipeUsed(
        targetExports,
        targetVmPtrByPid.get(targetPid) ?? 0,
        canonicalState.pipeUsed,
      ),
      sharedPipeSlots,
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
  vmPtr?: number;
}): void {
  const previous = canonicalProcesses.get(meta.pid);
  canonicalProcesses.set(meta.pid, {
    ...meta,
    vmPtr: meta.vmPtr ?? previous?.vmPtr ?? 0,
  });
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
    vmPtr,
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
    vmPtr,
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
  processExports.thread_clear_spawn_handoff(vmPtr);
  if (processExports.thread_restore_vfork_parent_without_cleanup) {
    processExports.thread_restore_vfork_parent_without_cleanup(vmPtr, childPid);
  } else {
    processExports.thread_resume_vfork_parent(vmPtr, childPid);
  }
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
    vmPtr,
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
  const sourceVmPtr = canonicalProcesses.get(pid)?.vmPtr ?? 0;
  const canonicalManagedPipeSlots = new Set<number>(collectCanonicalManagedPipeSlots(canonicalState));
  if (sourceVmPtr !== 0) {
    for (const pipeSlot of collectProcessHeldPipeSlots(processExports, sourceVmPtr)) {
      canonicalManagedPipeSlots.add(pipeSlot);
    }
  }
  const currentSourcePipeSlotModes = sourceVmPtr !== 0
    ? collectProcessPipeSlotModes(processExports, sourceVmPtr)
    : new Map<number, number>();
  const previousSourcePipeSlotModes = lastProcessPipeSlotModes.get(pid) ?? new Map<number, number>();
  const sourcePipeSlotModes = new Map(currentSourcePipeSlotModes);
  const sourceSyncPipeSlots = new Set<number>(sourcePipeSlotModes.keys());
  if (sourceVmPtr !== 0) {
    const privatePipeSlots = collectProcessPrivatePipeSlots(processExports, sourceVmPtr);
    for (const pipeSlot of collectProcessHeldPipeSlots(processExports, sourceVmPtr)) {
      sourceSyncPipeSlots.add(pipeSlot);
    }
    for (let pipeSlot = 1; pipeSlot < processExports.abi_max_pipes(); pipeSlot += 1) {
      if (
        privatePipeSlots.has(pipeSlot)
        || sourcePipeSlotModes.has(pipeSlot)
      ) {
        continue;
      }
      const previousMode = previousSourcePipeSlotModes.get(pipeSlot);
      if (previousMode !== undefined) {
        if (
          (sourceState.pipeUsed & (1n << BigInt(pipeSlot))) === 0n
          && !canonicalManagedPipeSlots.has(pipeSlot)
        ) {
          continue;
        }
        sourcePipeSlotModes.set(pipeSlot, previousMode);
        sourceSyncPipeSlots.add(pipeSlot);
      }
    }
  }
  mergePipeSlotsFromProcess(
    processExports,
    sourceState,
    sourcePipeSlotModes,
    exports,
    canonicalState,
    canonicalManagedPipeSlots,
  );
  const {
    kernelState: reconciledCanonicalState,
    readOpen,
    writeOpen,
    managedPipeSlots,
  } = recomputeCanonicalPipeControlState();

  for (const [memberPid, memberExports] of processKernels) {
    const memberState = readKernelRuntimeState(memberExports);
    const memberVmPtr = canonicalProcesses.get(memberPid)?.vmPtr ?? 0;
    const memberPipeUsed = memberVmPtr !== 0
      ? composeProcessPipeUsed(memberExports, memberVmPtr, reconciledCanonicalState.pipeUsed)
      : reconciledCanonicalState.pipeUsed;
    attachKernelRuntimeState(memberExports, {
      ...memberState,
      pipeUsed: memberPipeUsed,
    });
    mergePipeSlotsFromCanonical(
      exports,
      reconciledCanonicalState,
      memberExports,
      memberState,
      memberVmPtr,
      [...sourceSyncPipeSlots].sort((left, right) => left - right),
    );
    applyPipeControlState(
      memberExports,
      memberState,
      readOpen,
      writeOpen,
      memberPipeUsed,
      managedPipeSlots,
    );
  }

  if (currentSourcePipeSlotModes.size === 0) {
    lastProcessPipeSlotModes.delete(pid);
  } else {
    lastProcessPipeSlotModes.set(pid, currentSourcePipeSlotModes);
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

function syncOfdState(
  pid: number,
  ofdIds: number[],
  kernelState: KernelRuntimeState,
): KernelRuntimeState {
  const processExports = processKernels.get(pid);
  if (!processExports || ofdIds.length === 0) {
    return kernelState;
  }
  attachKernelRuntimeState(processExports, kernelState);

  const sourceState = readKernelRuntimeState(processExports);
  const canonicalState = readKernelRuntimeState(exports);
  copySelectedOfdState(
    processExports,
    sourceState,
    exports,
    canonicalState,
    ofdIds,
  );

  for (const memberExports of processKernels.values()) {
    const memberState = readKernelRuntimeState(memberExports);
    copySelectedOfdState(
      exports,
      canonicalState,
      memberExports,
      memberState,
      ofdIds,
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

function readFileByInode(sourceExports: KernelExports, ino: number): Uint8Array {
  const size = Number(sourceExports.memfs_file_size(ino));
  if (size <= 0) {
    return new Uint8Array();
  }

  const bufPtr = scratchBaseFor(sourceExports) + 0x4000;
  ensureMemoryCapacityFor(sourceExports.memory, bufPtr + size);
  const read = Number(sourceExports.memfs_read_file_to(ino, 0n, bufPtr, size));
  if (read < 0) {
    throw new Error(`failed to read inode: ${ino}`);
  }

  return new Uint8Array(sourceExports.memory.buffer.slice(bufPtr, bufPtr + read));
}

function writeFileByInode(targetExports: KernelExports, ino: number, data: Uint8Array): void {
  const truncateResult = Number(targetExports.memfs_truncate_file(ino, BigInt(data.length)));
  if (truncateResult < 0) {
    throw new Error(`failed to truncate inode: ${ino}`);
  }

  if (data.length === 0) {
    return;
  }

  const dataPtr = writeScratchFor(targetExports, data);
  const written = Number(targetExports.memfs_write_file_at(ino, 0n, dataPtr, data.length));
  if (written < 0 || written !== data.length) {
    throw new Error(`failed to write inode: ${ino}`);
  }
  markMaterializedMemfsFile(targetExports, ino);
}

function syncFileDataIntoTarget(
  targetExports: KernelExports,
  ino: number,
  mode: number,
  data: Uint8Array,
): void {
  const shallowResult = Number(targetExports.memfs_set_file_size_shallow(ino, BigInt(data.length)));
  if (shallowResult < 0) {
    writeFileByInode(targetExports, ino, data);
  }
  const modeResult = Number(targetExports.memfs_set_mode(ino, mode));
  if (modeResult < 0) {
    throw new Error(`failed to set inode mode: ${ino}`);
  }
}

function syncFileDataMutation(sourceExports: KernelExports, ino: number): boolean {
  const mode = sourceExports.memfs_inode_mode(ino);
  if ((mode & MEMFS_S_IFMT) !== MEMFS_S_IFREG) {
    return false;
  }

  const published = readInodeRecord(sabPageCache, ino) ?? {
    dataOff: 0,
    size: 0,
    mode,
    parentIno: 0,
    name: new Uint8Array(),
    nextSibling: 0,
  };

  const data = readFileByInode(sourceExports, ino);
  writeFileByInode(exports, ino, data);
  publishInode(sabPageCache, ino, {
    mode,
    size: data.length,
    parentIno: published.parentIno,
    name: published.name,
    nextSibling: published.nextSibling,
  }, data);

  syncFileDataIntoTarget(exports, ino, mode, data);
  for (const memberExports of processKernels.values()) {
    if (memberExports === sourceExports) {
      continue;
    }
    syncFileDataIntoTarget(memberExports, ino, mode, data);
  }

  return true;
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

function snapshotFsTree(
  ino: number,
  path: string,
  out: FsSnapshotEntry[],
  includeData: boolean,
): void {
  const normalized = normalizePath(path);
  const mode = exports.memfs_inode_mode(ino);
  const perms = mode & 0o7777;
  const fileType = mode & 0o170000;

  if (fileType === 0o040000) {
    out.push({ kind: "dir", path: normalized, ino, mode: perms });
    for (const name of readdir(normalized).sort()) {
      const childPath = normalized === "/" ? `/${name}` : `${normalized}/${name}`;
      const childIno = resolvePath(childPath);
      if (childIno === 0) {
        continue;
      }
      snapshotFsTree(childIno, childPath, out, includeData);
    }
    return;
  }

  if (fileType !== 0o100000) {
    return;
  }

  out.push({
    kind: "file",
    path: normalized,
    ino,
    data: includeData ? readFile(normalized) : undefined,
    size: Number(exports.memfs_file_size(ino)),
    mode: perms,
  });
}

function snapshotCanonicalFs(includeData: boolean): FsSnapshotEntry[] {
  const entries: FsSnapshotEntry[] = [];
  snapshotFsTree(1, "/", entries, includeData);
  return entries;
}

function rebuildPageCacheFromCanonical(): void {
  const entries = snapshotCanonicalFs(true);
  const rootMode = exports.memfs_inode_mode(1) & 0o7777;
  resetPageCache(sabPageCache);
  publishRootInode(rootMode);

  for (const entry of entries) {
    if (entry.path === "/") {
      continue;
    }
    const normalized = normalizePath(entry.path);
    const name = normalized.split("/").pop();
    if (!name || entry.ino === undefined) {
      continue;
    }
    const parentPath = normalized.split("/").slice(0, -1).join("/") || "/";
    const parentIno = resolvePath(parentPath);
    if (parentIno === 0) {
      continue;
    }
    const perms = normalizePermMode(entry.mode, entry.kind === "dir" ? 0o755 : 0o644);
    publishInode(sabPageCache, entry.ino, {
      mode: (entry.kind === "dir" ? 0o040000 : 0o100000) | perms,
      size: entry.size ?? entry.data?.length ?? 0,
      parentIno,
      name: encoder.encode(name),
      nextSibling: 0,
    }, entry.kind === "file" ? (entry.data ?? new Uint8Array()) : null);
  }
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
          entries: snapshotCanonicalFs(false).map(cloneSnapshotEntry),
        });
        break;
      case "load-fs-snapshot":
        loadFsSnapshot(msg.entries);
        postResponse({ type: "fs-loaded", requestId: msg.requestId });
        break;
      case "register-process": {
        const kernelState = await registerProcess(
          msg.pid,
          msg.vmPtr,
          msg.memory,
          msg.kernelState,
          msg.seedMemfsMode ?? "full",
          msg.pipeSeedMode ?? "canonical",
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
          const result = processFs(msg.pid, msg.vmPtr, msg.kernelState);
          postResponse({
            type: "fs-processed",
            requestId: msg.requestId,
            kernelState: result.kernelState,
            memfsMutated: result.memfsMutated,
            ofdStateChanged: result.ofdStateChanged,
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
      case "sync-ofd-state":
        postResponse({
          type: "ofd-state-synced",
          requestId: msg.requestId,
          kernelState: syncOfdState(
            msg.pid,
            msg.ofdIds,
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
            msg.vmPtr,
            msg.targetPids,
            msg.targetKernelStates,
          msg.ofdIds,
          msg.ofdSlots,
          msg.includeMemfs ?? true,
          msg.preserveSourceRefcounts ?? false,
          msg.kernelState,
        ),
      });
        break;
      case "rehydrate-fork-shared-state":
        rehydrateForkSharedState(
          msg.targetPids,
          msg.targetKernelStates,
          msg.ofdIds,
          msg.procSelfExeTargets ?? [],
          msg.includeMemfs ?? true,
        );
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
