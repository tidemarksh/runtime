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
import { activeKernelShareOfdIds, collectProcessHeldOfds, snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
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
const FORK_CHILD_FIRST_SCHEDULING_TIMEOUT_MS = 10_000;

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

async function waitForForkChildSchedulingPoint(childHandle: ProcessHandle): Promise<void> {
  const deadline = Date.now() + FORK_CHILD_FIRST_SCHEDULING_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (
      childHandle.exitInProgress
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

function captureForkMemorySnapshot(handle: ProcessHandle): ForkMemorySnapshot {
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
      + handle.kernelState.memfsDirEntriesBytes
      + handle.kernelState.memfsDataBytes,
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
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.syncProcessMemfsViaKernelWorker(handle);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-memfs-synced:${handle.pid}`);
      await deps.requestPipeStateSync(handle, false);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-pipe-synced:${handle.pid}`);
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
    handle.kernelState = readKernelRuntimeState(handle.kernel);

    const forkMemorySnapshot = captureForkMemorySnapshot(handle);
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
    const rehydratedSharedOfdIds = sharedOfdIds.filter((ofdId) => !childHeldOfdIds.has(ofdId));
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
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
    deps.syncForkSharedStateLocally(handle, [childHandle], { includeMemfs: false });
    await deps.syncForkSharedStateViaKernelWorker(handle, [childHandle], { includeMemfs: false });
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      includeMemfs: false,
    });
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    await deps.requestPipeStateSync(childHandle, false);
    deps.assertProcessStandardFdIntegrity(childHandle, "plain-fork-child-ready");
    publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
    deps.processHandles.set(childPid, childHandle);
    restoreParentProcessSnapshot(handle, parentThread, parentProcessSnapshot);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.requestPipeStateSync(handle, true);
      await deps.requestPipeStateSync(childHandle, false);
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
    });
    await waitForForkChildSchedulingPoint(childHandle);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const parentReplacementWorker = acquireWorker();
    deps.attachThreadWorker(handle, {
      worker: parentReplacementWorker,
      vmPtr: parentThread.vmPtr,
    });
    deps.kickBlockedWorkers(handle);
    deps.appendWorkerDebugLog(handle, `plain-fork-parent:done:${childPid}`);
  } finally {
    if (handle.pendingChildSpawnRecords > 0) {
      handle.pendingChildSpawnRecords -= 1;
    }
    deps.kickBlockedWorkers(handle);
  }
}
