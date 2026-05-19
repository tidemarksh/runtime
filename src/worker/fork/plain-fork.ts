import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { createKernelEnv } from "../../wasm/env.js";
import {
  configureKernelInstanceStack,
  MAIN_THREAD_KERNEL_STACK_SLOT,
} from "../../wasm/stack.js";
import {
  adoptKernelShare,
  discardCopiedSharedKernelObjects,
  reconcileKernelSharePipeControl,
  restoreLocalOfdSlots,
  restoreLocalPipeSlots,
  snapshotLocalPipeSlots,
  trackForkSharedOfds,
} from "../kernel-share.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  syncKernelShareOfdReservations,
} from "../fork-shared.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { adoptKernelStateForHandle } from "../sync.js";
import { createForkChildHandle } from "./common.js";
import { detachInheritedHostStdioFromChild } from "./common.js";
import { computeGuestRamLimitBytes } from "./common.js";
import type { WorkerForkDeps, ProcessHandle, ThreadWorkerState } from "./types.js";

type ParentProcessSnapshot = {
  processPtr: number;
  processState: Uint8Array;
  pid: number;
  ppid: number;
};

type MemoryRange = {
  start: number;
  end: number;
};

type ForkMemorySnapshot = {
  ranges: Array<MemoryRange & { bytes: Uint8Array }>;
  endOffset: number;
};

const FORK_CHILD_EXECVE_HEADROOM_BYTES = 16 * 1024 * 1024;
const FORK_CHILD_FIRST_SCHEDULING_TURNS = 32;

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function acquireWorkerMemoryFence(handle: ProcessHandle): void {
  Atomics.load(new Int32Array(handle.runtimeBridge.notifySab), 0);
}

const OFD_OFFSET_OFFSET = 16;
const PIPE_GENERATION_OFFSET = 20;

function currentPipeGeneration(handle: ProcessHandle, pipeSlot: number): number | null {
  const pipeSlotBytes = handle.kernel.kernel_pipe_region_bytes() / handle.kernel.abi_max_pipes();
  if (
    !Number.isInteger(pipeSlotBytes)
    || pipeSlotBytes <= PIPE_GENERATION_OFFSET
    || pipeSlot <= 0
    || pipeSlot >= handle.kernel.abi_max_pipes()
  ) {
    return null;
  }
  return new DataView(handle.memory.buffer).getUint32(
    handle.kernelState.pipeBase + pipeSlot * pipeSlotBytes + PIPE_GENERATION_OFFSET,
    true,
  );
}

function pipeOfdGenerationDiffers(handle: ProcessHandle, ofdId: number): boolean {
  if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()) {
    return false;
  }
  const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
  return pipeGeneration !== null && handle.kernel.kernel_ofd_offset(ofdId) !== BigInt(pipeGeneration);
}

function localOfdSlotDiffersFromHandle(handle: ProcessHandle, ofdId: number, slot: Uint8Array): boolean {
  if (ofdId <= 0 || ofdId >= handle.kernel.abi_max_fds()) {
    return false;
  }
  const view = new DataView(slot.buffer, slot.byteOffset, slot.byteLength);
  const preservedType = view.getInt32(0, true);
  if (preservedType === 0) {
    return false;
  }
  const currentType = handle.kernel.kernel_ofd_fd_type(ofdId);
  if (currentType === 0 || currentType !== preservedType) {
    return true;
  }
  if (pipeOfdGenerationDiffers(handle, ofdId)) {
    return true;
  }
  return handle.kernel.kernel_ofd_host_fd(ofdId) !== view.getInt32(4, true)
    || handle.kernel.kernel_ofd_ino(ofdId) !== view.getUint32(24, true);
}

function snapshotLocalOfdSlots(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): Map<number, Uint8Array> {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const snapshots = new Map<number, Uint8Array>();
  for (const ofdId of ofdIds) {
    if (ofdId <= 0 || ofdId >= maxFds || snapshots.has(ofdId)) {
      continue;
    }
    const fdType = handle.kernel.kernel_ofd_fd_type(ofdId);
    if (fdType <= 0 || fdType > 64) {
      continue;
    }
    const slotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    snapshots.set(
      ofdId,
      new Uint8Array(handle.memory.buffer, slotOffset, slotBytes).slice(),
    );
  }
  return snapshots;
}

function restoreIncompatibleLocalOfdSlots(
  handle: ProcessHandle,
  preservedOfdSlots: ReadonlyMap<number, Uint8Array>,
): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  const view = new DataView(handle.memory.buffer);
  for (const [ofdId, slot] of preservedOfdSlots) {
    if (slot.length !== slotBytes || !localOfdSlotDiffersFromHandle(handle, ofdId, slot)) {
      continue;
    }
    const ofdSlotOffset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    bytes.set(slot, ofdSlotOffset);
    if (
      handle.kernel.kernel_ofd_fd_type(ofdId) === handle.kernel.abi_fd_type_pipe()
    ) {
      const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
      if (pipeGeneration !== null) {
        view.setBigInt64(ofdSlotOffset + OFD_OFFSET_OFFSET, BigInt(pipeGeneration), true);
      }
    }
  }
}

function restoreLocalPipeSlotGenerations(
  handle: ProcessHandle,
  preservedPipeSlots: ReadonlyMap<number, Uint8Array>,
): void {
  if (preservedPipeSlots.size === 0) {
    return;
  }
  const maxPipes = handle.kernel.abi_max_pipes();
  const pipeSlotBytes = handle.kernel.kernel_pipe_region_bytes() / maxPipes;
  if (!Number.isInteger(pipeSlotBytes) || pipeSlotBytes <= PIPE_GENERATION_OFFSET) {
    return;
  }
  const bytes = new Uint8Array(handle.memory.buffer);
  for (const [pipeSlot, slot] of preservedPipeSlots) {
    if (
      pipeSlot <= 0
      || pipeSlot >= maxPipes
      || slot.length !== pipeSlotBytes
    ) {
      continue;
    }
    bytes.set(
      slot.subarray(PIPE_GENERATION_OFFSET, PIPE_GENERATION_OFFSET + 4),
      handle.kernelState.pipeBase + pipeSlot * pipeSlotBytes + PIPE_GENERATION_OFFSET,
    );
  }
}

async function waitForForkChildSchedulingPoint(childHandle: ProcessHandle): Promise<void> {
  for (let turn = 0; turn < FORK_CHILD_FIRST_SCHEDULING_TURNS; turn += 1) {
    if (
      childHandle.exitInProgress
      || childHandle.workers.size === 0
      || childHandle.blockedWorkers.size > 0
      || childHandle.runningWorker !== null
      || childHandle.readyWorkers.length > 0
    ) {
      return;
    }
    await nextTurn();
  }
}

function captureParentProcessSnapshot(
  handle: ProcessHandle,
  parentThread: ThreadWorkerState,
): ParentProcessSnapshot {
  const processPtr = parentThread.lastStepProcessPtr ?? handle.kernel.thread_process_ptr(parentThread.vmPtr);
  const processStateBytes = handle.kernel.process_state_bytes();
  const guestBytes = new Uint8Array(handle.memory.buffer);
  return {
    processPtr,
    processState: guestBytes.slice(
      handle.guestBase + processPtr,
      handle.guestBase + processPtr + processStateBytes,
    ),
    pid: handle.pid,
    ppid: handle.parentPid ?? 0,
  };
}

function restoreParentProcessSnapshot(
  handle: ProcessHandle,
  parentThread: ThreadWorkerState,
  snapshot: ParentProcessSnapshot,
): void {
  new Uint8Array(handle.memory.buffer).set(
    snapshot.processState,
    handle.guestBase + snapshot.processPtr,
  );
  handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
  handle.kernel.thread_restore_fork_parent?.(
    parentThread.vmPtr,
    snapshot.processPtr,
    snapshot.pid,
    snapshot.ppid,
  );
  const currentPid = handle.kernel.process_current_pid(parentThread.vmPtr);
  const currentProcessPtr = handle.kernel.thread_process_ptr(parentThread.vmPtr);
  if (currentPid !== snapshot.pid || currentProcessPtr !== snapshot.processPtr) {
    throw new Error(
      `plain-fork parent restore drift vm=0x${parentThread.vmPtr.toString(16)}`
        + ` expected_pid=${snapshot.pid} actual_pid=${currentPid}`
        + ` expected_process_ptr=0x${snapshot.processPtr.toString(16)}`
        + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`,
    );
  }
}

function addForkCopyRange(ranges: MemoryRange[], start: number, end: number, limit: number): void {
  const boundedStart = Math.max(0, Math.min(start, limit));
  const boundedEnd = Math.max(boundedStart, Math.min(end, limit));
  if (boundedEnd > boundedStart) {
    ranges.push({ start: boundedStart, end: boundedEnd });
  }
}

function coalesceRanges(ranges: MemoryRange[]): MemoryRange[] {
  const sorted = ranges
    .filter((range) => range.end > range.start)
    .sort((left, right) => left.start - right.start || left.end - right.end);
  const coalesced: MemoryRange[] = [];
  for (const range of sorted) {
    const last = coalesced[coalesced.length - 1];
    if (last && range.start <= last.end) {
      last.end = Math.max(last.end, range.end);
    } else {
      coalesced.push({ ...range });
    }
  }
  return coalesced;
}

function captureForkMemorySnapshot(handle: ProcessHandle, stackPointer = 0n): ForkMemorySnapshot {
  const source = new Uint8Array(handle.memory.buffer);
  const ranges: MemoryRange[] = [];
  const limit = source.byteLength;
  addForkCopyRange(ranges, 0, Math.max(handle.kernelState.heapPtr, handle.kernel.kernel_heap_ptr()), limit);
  addForkCopyRange(
    ranges,
    handle.kernelState.ofdTablePtr,
    handle.kernelState.ofdTablePtr + handle.kernel.kernel_ofd_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.memfsPtr,
    handle.kernelState.memfsPtr
      + handle.kernelState.memfsHeaderBytes
      + handle.kernelState.memfsInodesBytes
      + handle.kernelState.memfsFirstChildBytes
      + handle.kernelState.memfsDirEntriesBytes,
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_ptr(),
    handle.kernel.kernel_memfs_ptr() + handle.kernel.kernel_memfs_header_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_inodes_ptr(),
    handle.kernel.kernel_memfs_inodes_ptr() + handle.kernel.kernel_memfs_inodes_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_first_child_ptr(),
    handle.kernel.kernel_memfs_first_child_ptr() + handle.kernel.kernel_memfs_first_child_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_dir_entries_ptr(),
    handle.kernel.kernel_memfs_dir_entries_ptr() + handle.kernel.kernel_memfs_dir_entries_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernel.kernel_memfs_data_ptr(),
    handle.kernel.kernel_memfs_data_ptr() + Math.min(
      handle.kernel.memfs_data_used?.() ?? handle.kernel.kernel_memfs_data_bytes(),
      handle.kernel.kernel_memfs_data_bytes(),
    ),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.epollTablePtr,
    handle.kernelState.epollTablePtr + handle.kernel.kernel_epoll_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.socketTablePtr,
    handle.kernelState.socketTablePtr + handle.kernel.kernel_socket_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.pipeBase,
    handle.kernelState.pipeBase + handle.kernel.kernel_pipe_region_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.netBufBase,
    handle.kernelState.netBufBase + handle.kernel.kernel_net_buf_pool_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.sigSlotsPtr,
    handle.kernelState.sigSlotsPtr
      + handle.kernel.kernel_sig_slots_bytes()
      + handle.kernel.kernel_signal_action_table_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.processPendingPtr,
    handle.kernelState.processPendingPtr + handle.kernel.kernel_process_pending_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.processRuntimeStatePtr,
    handle.kernelState.processRuntimeStatePtr + handle.kernel.kernel_process_runtime_state_bytes(),
    limit,
  );
  addForkCopyRange(
    ranges,
    handle.kernelState.timeStatePtr,
    handle.kernelState.timeStatePtr + handle.kernel.kernel_time_state_bytes(),
    limit,
  );

  const lowGuestEnd = Math.max(
    Number(handle.kernel.debug_memory_brk_current?.() ?? 0),
    Number(handle.kernel.debug_memory_mmap_next?.() ?? 0),
    handle.kernel.thread_process_ptr(handle.vmPtr) + handle.kernel.process_state_bytes(),
  );
  addForkCopyRange(ranges, handle.guestBase, handle.guestBase + lowGuestEnd, limit);

  const vmaLen = handle.kernel.debug_memory_vma_len?.() ?? 0;
  for (let index = 0; index < vmaLen; index += 1) {
    const start = Number(handle.kernel.debug_memory_vma_start?.(index) ?? 0);
    const end = Number(handle.kernel.debug_memory_vma_end?.(index) ?? 0);
    addForkCopyRange(ranges, handle.guestBase + start, handle.guestBase + end, limit);
  }

  const stackGuestPtr = Number(stackPointer);
  const stackTop = handle.guestBase + handle.ramSize;
  if (Number.isFinite(stackGuestPtr) && stackGuestPtr > 0 && stackGuestPtr < handle.ramSize) {
    const stackStartGuest = Math.max(
      0,
      Math.floor(stackGuestPtr / 65536) * 65536 - 1024 * 1024,
    );
    addForkCopyRange(ranges, handle.guestBase + stackStartGuest, stackTop, limit);
  }

  const merged = coalesceRanges(ranges);
  return {
    ranges: merged.map((range) => ({
      ...range,
      bytes: source.slice(range.start, range.end),
    })),
    endOffset: merged.reduce((end, range) => Math.max(end, range.end), 0),
  };
}

function applyForkMemorySnapshot(snapshot: ForkMemorySnapshot, target: WebAssembly.Memory): void {
  const targetBytes = new Uint8Array(target.buffer);
  for (const range of snapshot.ranges) {
    targetBytes.set(range.bytes, range.start);
  }
}

export async function handlePlainFork(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  handle.pendingChildSpawnRecords += 1;
  try {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:start:${childPid}`);
    deps.appendWorkerDebugLog(
      handle,
      `plain-fork-parent:start:${childPid}:vm=0x${parentThread.vmPtr.toString(16)}:handle_vm=0x${handle.vmPtr.toString(16)}`,
    );
    const pendingForkSharedSync = handle.kernelShare.forkSharedSyncPromise ?? handle.forkSharedSyncPromise;
    if (pendingForkSharedSync !== null) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:await-fork-shared-sync:${handle.pid}`);
      await pendingForkSharedSync;
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:await-fork-shared-sync-done:${handle.pid}`);
    }
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.requestPipeStateSync(handle, false);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-pipe-synced:${handle.pid}`);
      if (handle.processMemfsDirty) {
        await deps.syncForkSharedStateViaKernelWorker(handle, [], {
          includeMemfs: true,
          memfsOnly: true,
          preservePublishedRegularFileData: true,
          sourceVmPtr: parentThread.vmPtr,
        });
        handle.processMemfsDirty = false;
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-memfs-published:${handle.pid}`);
      } else {
        deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-memfs-clean:${handle.pid}`);
      }
    }
    const parentProcessSnapshot = captureParentProcessSnapshot(handle, parentThread);
    const cloneFlags = Number(handle.kernel.thread_clone_flags(parentThread.vmPtr));
    const childPpid = (cloneFlags & Number(handle.kernel.abi_clone_parent())) !== 0
      ? (handle.parentPid ?? handle.pid)
      : handle.pid;
    const lifecycle = handle.kernelWorkerRequest && handle.pid > 0
      ? await deps.canonicalizeChildLifecycleViaKernelWorker(handle, parentThread.vmPtr, childPid)
      : {
          childPid,
          nextPid: childPid + 1,
          pgid: handle.kernel.process_current_pgid(parentThread.vmPtr),
          sid: handle.kernel.process_current_sid(parentThread.vmPtr),
        };
    childPid = lifecycle.childPid;

    trackForkSharedOfds(handle, parentThread.vmPtr, true);
    handle.kernelState = adoptKernelStateForHandle(handle, readKernelRuntimeState(handle.kernel));
    attachKernelRuntimeState(handle.kernel, handle.kernelState);

    const parentStackPointer = parentThread.lastStepSp ?? handle.kernel.thread_sp(parentThread.vmPtr);
    acquireWorkerMemoryFence(handle);
    const forkMemorySnapshot = captureForkMemorySnapshot(handle, parentStackPointer);
    if (parentThread.forkStackSnapshot) {
      const start = handle.guestBase + parentThread.forkStackSnapshot.startGuest;
      const end = start + parentThread.forkStackSnapshot.bytes.length;
      forkMemorySnapshot.ranges.push({
        start,
        end,
        bytes: parentThread.forkStackSnapshot.bytes,
      });
      forkMemorySnapshot.endOffset = Math.max(forkMemorySnapshot.endOffset, end);
      parentThread.forkStackSnapshot = undefined;
    }
    restoreParentProcessSnapshot(handle, parentThread, parentProcessSnapshot);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const childMemoryBytes = Math.max(
      forkMemorySnapshot.endOffset,
      handle.guestBase + handle.ramSize + FORK_CHILD_EXECVE_HEADROOM_BYTES,
    );
    const childMemory = new WebAssembly.Memory({
      initial: Math.ceil(childMemoryBytes / 65536),
      maximum: Math.max(handle.maxMemoryPages, handle.forkMaxMemoryPages),
      shared: true,
    });
    applyForkMemorySnapshot(forkMemorySnapshot, childMemory);

    const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
      env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
    });
    const childKernel = childInstance.exports as unknown as KernelExports;
    configureKernelInstanceStack(childKernel, MAIN_THREAD_KERNEL_STACK_SLOT);
    // Instantiating a kernel instance over imported child memory replays the
    // module's data segments. In browsers that can clobber copied guest text/data
    // below the probe's argv/envp strings, so restore the whole copied image here
    // and after kernel-worker registration before reattaching shared state.
    applyForkMemorySnapshot(forkMemorySnapshot, childMemory);
    attachKernelRuntimeState(childKernel, handle.kernelState);

    ensureEvictionTimer();
    const childHandle = createForkChildHandle({
      deps,
      parent: handle,
      childPid,
      childPpid,
      vmPtr: parentThread.vmPtr,
      worker: parentWorker,
      memory: childMemory,
      kernel: childKernel,
      kernelState: readKernelRuntimeState(childKernel),
      debugCommand: handle.debugCommand,
      workerDebugLabel: "spawn:plain-fork",
    });
    childHandle.onError = (error: Error) => {
      if (handle.pid <= 0 || deps.processHandles.get(handle.pid) !== handle) {
        return;
      }
      const childLogTail = childHandle.workerDebugLog.slice(-12).join(", ");
      deps.failProcess(handle, new Error(
        `child-debug:${childPid}:${childHandle.lastDebugMessage ?? "last-debug-unavailable"}`
        + ` worker_log=[${childLogTail}]`
        + ` cause=${error.message}`,
      ));
    };
    const childGuestRamLimit = computeGuestRamLimitBytes(
      childHandle.guestBase,
      childHandle.ramSize,
      childHandle.maxMemoryPages,
    );
    detachInheritedHostStdioFromChild(childHandle);
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const sharedOfdIds = activeKernelShareOfdIds(handle);
    const sharedOfdIdSet = new Set(sharedOfdIds);
    const childSharedOfdIds = sharedOfdIds.filter((ofdId) => childHeldOfdIds.has(ofdId));
    const childPrivateOfdIds = [...childHeldOfdIds].filter((ofdId) => !sharedOfdIdSet.has(ofdId));
    const preservedChildHeldOfdSlots = snapshotLocalOfdSlots(childHandle, childHeldOfdIds);
    const preservedOfdSlots = snapshotLocalOfdSlots(childHandle, childPrivateOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childPrivateOfdIds);
    const inheritedSharedPipeSlots = snapshotLocalPipeSlots(childHandle, sharedOfdIds);
    // A fork child inherits the parent's filesystem namespace. The linear
    // memory copy already carries MemFS, so only shared kernel objects are
    // discarded before rehydrating OFD/pipe/socket state.
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots, {
      clearMemfs: false,
    });
    await deps.registerProcessWithKernelWorker(childHandle, {
      seedMemfsMode: "none",
      pipeSeedMode: "process",
    });
    // Kernel-worker registration also instantiates over the shared child memory,
    // so restore the copied parent image again before rebuilding canonical shared
    // state for the child.
    applyForkMemorySnapshot(forkMemorySnapshot, childMemory);
    attachKernelRuntimeState(childHandle.kernel, childHandle.kernelState);
    await deps.initializeProcessLifecycleViaKernelWorker(
      childHandle,
      childHandle.vmPtr,
      childPpid,
      lifecycle.nextPid,
      lifecycle.pgid,
      lifecycle.sid,
      false,
    );
    childHandle.kernel.thread_prepare_fork_child?.(childHandle.vmPtr, childPid);
    detachInheritedHostStdioFromChild(childHandle);
    childHandle.kernel.thread_set_guest_ram_limit(childHandle.vmPtr, childGuestRamLimit);
    childHandle.kernelState = readKernelRuntimeState(childHandle.kernel);
    adoptKernelShare(handle, childHandle);
    deps.syncForkSharedStateLocally(handle, [childHandle], {
      includeMemfs: false,
      ofdIds: childSharedOfdIds,
    });
    await deps.syncForkSharedStateViaKernelWorker(handle, [childHandle], {
      includeMemfs: false,
      ofdIds: childSharedOfdIds,
    });
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], childSharedOfdIds, {
      includeMemfs: false,
    });
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedChildHeldOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    restoreLocalPipeSlots(childHandle, inheritedSharedPipeSlots);
    restoreLocalPipeSlotGenerations(childHandle, preservedPipeSlots);
    restoreLocalPipeSlotGenerations(childHandle, inheritedSharedPipeSlots);
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedChildHeldOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    restoreLocalPipeSlots(childHandle, inheritedSharedPipeSlots);
    restoreLocalPipeSlotGenerations(childHandle, preservedPipeSlots);
    restoreLocalPipeSlotGenerations(childHandle, inheritedSharedPipeSlots);
    syncKernelShareOfdReservations(childHandle);
    await deps.requestPipeStateSync(childHandle, false);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedChildHeldOfdSlots);
    syncKernelShareOfdReservations(childHandle);
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreIncompatibleLocalOfdSlots(childHandle, preservedChildHeldOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    restoreLocalPipeSlots(childHandle, inheritedSharedPipeSlots);
    restoreLocalPipeSlotGenerations(childHandle, preservedPipeSlots);
    restoreLocalPipeSlotGenerations(childHandle, inheritedSharedPipeSlots);
    syncKernelShareOfdReservations(childHandle);
    deps.assertProcessStandardFdIntegrity(childHandle, "plain-fork-child-ready");
    publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
    deps.processHandles.set(childPid, childHandle);
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.requestPipeStateSync(handle, true);
      if (childHandle.runningWorker === null) {
        await deps.requestPipeStateSync(childHandle, false);
        restoreIncompatibleLocalOfdSlots(childHandle, preservedChildHeldOfdSlots);
        syncKernelShareOfdReservations(childHandle);
      }
    }
    reconcileKernelSharePipeControl(handle);
    deps.appendWorkerDebugLog(
      handle,
      `plain-fork-parent:spawn-child:${childPid}:vm=0x${childHandle.vmPtr.toString(16)}`,
    );
    deps.attachThreadWorker(childHandle, {
      worker: parentWorker,
      vmPtr: childHandle.vmPtr,
      transferFrom: handle,
      preserveMemoryBytes: forkMemorySnapshot.endOffset,
    });
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const parentReplacementWorker = acquireWorker();
    deps.attachThreadWorker(handle, {
      worker: parentReplacementWorker,
      vmPtr: parentThread.vmPtr,
      preserveMemoryBytes: forkMemorySnapshot.endOffset,
    });
    deps.kickBlockedWorkers(handle);
    await waitForForkChildSchedulingPoint(childHandle);
    deps.kickBlockedWorkers(handle);
    deps.appendWorkerDebugLog(handle, `plain-fork-parent:done:${childPid}`);
  } finally {
    if (handle.pendingChildSpawnRecords > 0) {
      handle.pendingChildSpawnRecords -= 1;
    }
    deps.kickBlockedWorkers(handle);
  }
}
