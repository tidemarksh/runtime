import { hostExecutionState } from "../host-execution-state.js";
import { processExitInProgress } from "../process-lifecycle-state.js";
import {
  processDebugCommand,
  processLastDebugMessage,
} from "../process-debug-state.js";
import { setProcessOnError } from "../process-callback-state.js";
import { workerDebugLog } from "../worker-debug-log.js";
import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  setGuestMemoryPolicyLimit,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { instantiateKernelWithSharedMemory } from "../../wasm/kernel-memory.js";
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
} from "../kernel-share.js";
import {
  syncKernelShareOfdReservations,
} from "../fork-shared.js";
import {
  activeKernelShareOfdIds,
  collectProcessHeldOfds,
  trackForkSharedOfds,
} from "../ownership/ofd-table.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import {
  beginPendingChildSpawnRecord,
  finishPendingChildSpawnRecord,
} from "../pending-child-spawn-state.js";
import {
  isProcessMemfsDirty,
  setProcessMemfsDirty,
} from "../process-memfs-state.js";
import { adoptKernelStateForHandle } from "../sync.js";
import { getActiveForkSharedSync } from "../fork-shared-sync-state.js";
import { createForkChildHandle } from "./common.js";
import { detachInheritedHostStdioFromChild } from "./common.js";
import {
  appendPlainForkMemoryDebug,
  applyForkMemorySnapshot,
  captureForkMemorySnapshotFromRanges,
  captureParentProcessSnapshot,
  forkMemoryPreserveRanges,
  overwriteForkMemorySnapshotStrict,
  restoreParentProcessSnapshot,
} from "./memory-snapshot.js";
import {
  restoreIncompatibleLocalOfdSlots,
  restoreLocalPipeSlotGenerations,
  snapshotLocalOfdSlots,
} from "./shared-ofd.js";
import {
  assertForkChildReadyOwnership,
} from "../ownership/invariants.js";
import {
  applySupervisorProcessMemoryPlan,
  readSupervisorProcessMemoryPlan,
  supervisorProcessMemoryInitialBytes,
  supervisorProcessMemoryMaxPages,
} from "../supervisor-host-op.js";
import type { WorkerForkDeps, ProcessHandle, ThreadWorkerState } from "./types.js";

const FORK_CHILD_FIRST_SCHEDULING_TURNS = 32;

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function acquireWorkerMemoryFence(handle: ProcessHandle): void {
  Atomics.load(new Int32Array(handle.runtimeBridge.notifySab), 0);
}

async function waitForForkChildSchedulingPoint(childHandle: ProcessHandle): Promise<void> {
  for (let turn = 0; turn < FORK_CHILD_FIRST_SCHEDULING_TURNS; turn += 1) {
    if (
      processExitInProgress(childHandle)
      || childHandle.workers.size === 0
      || hostExecutionState(childHandle).hostBlockedWorkers.size > 0
      || hostExecutionState(childHandle).runningWorker !== null
      || hostExecutionState(childHandle).vforkReadyWorkers.length > 0
    ) {
      return;
    }
    await nextTurn();
  }
}

function forkChildGuestMemoryPolicyLimit(handle: ProcessHandle): bigint {
  const read = handle.kernel.thread_guest_memory_policy_limit;
  if (typeof read !== "function") {
    throw new Error("kernel ABI missing thread_guest_memory_policy_limit");
  }
  const inheritedLimit = read(handle.vmPtr);
  return inheritedLimit < 0n ? 0n : inheritedLimit;
}

function formatProcessThreadSlots(handle: ProcessHandle, vmPtr: number): string {
  const slots: string[] = [];
  const maxThreads = handle.kernel.abi_max_threads();
  for (let slot = 0; slot < maxThreads; slot += 1) {
    const tid = handle.kernel.process_thread_tid(vmPtr, slot);
    const state = handle.kernel.process_thread_state(vmPtr, slot);
    const threadVmPtr = handle.kernel.process_thread_vm_ptr(vmPtr, slot);
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    slots.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return slots.join(",");
}

function appendPlainForkProcessThreadsDebug(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  childPid: number,
  stage: string,
  vmPtr: number,
): void {
  const entry = `plain-fork-memory:process-${stage}:${childPid}:threads=[${formatProcessThreadSlots(handle, vmPtr)}]`;
  deps.appendWorkerDebugLog(handle, entry);
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, entry);
}

export async function handlePlainFork(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  beginPendingChildSpawnRecord(handle);
  try {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:start:${childPid}`);
    deps.appendWorkerDebugLog(
      handle,
      `plain-fork-parent:start:${childPid}:vm=0x${parentThread.vmPtr.toString(16)}:handle_vm=0x${handle.vmPtr.toString(16)}`,
    );
    acquireWorkerMemoryFence(handle);
    appendPlainForkProcessThreadsDebug(deps, handle, childPid, "before-capture", parentThread.vmPtr);
    const parentProcessSnapshot = captureParentProcessSnapshot(handle, parentThread);
    const pendingForkSharedSync = getActiveForkSharedSync(handle);
    if (pendingForkSharedSync !== null) {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:await-fork-shared-sync:${handle.pid}`);
      await pendingForkSharedSync;
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:await-fork-shared-sync-done:${handle.pid}`);
    }
    await deps.requestPipeStateSync(handle, false);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-pipe-synced:${handle.pid}`);
    if (isProcessMemfsDirty(handle)) {
      await deps.syncForkSharedStateViaKernelWorker(handle, [], {
        cause: { kind: "syscall", nr: -1n, name: "plain-fork-parent-memfs-publication" },
        includeMemfs: true,
        memfsOnly: true,
        sourceVmPtr: parentThread.vmPtr,
      });
      setProcessMemfsDirty(handle, false);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-memfs-published:${handle.pid}`);
    } else {
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-memfs-clean:${handle.pid}`);
    }
    const lifecycle = await deps.canonicalizeChildLifecycleViaKernelWorker(handle, parentThread.vmPtr, childPid);
    childPid = lifecycle.childPid;

    trackForkSharedOfds(handle, parentThread.vmPtr, true);
    handle.kernelState = adoptKernelStateForHandle(handle, readKernelRuntimeState(handle.kernel));
    attachKernelRuntimeState(handle.kernel, handle.kernelState);

    const processMemoryPlan = readSupervisorProcessMemoryPlan(handle, parentThread.vmPtr);
    const forkMemorySnapshot = captureForkMemorySnapshotFromRanges(
      handle,
      processMemoryPlan.copyRanges,
    );
    overwriteForkMemorySnapshotStrict(
      forkMemorySnapshot,
      handle.guestBase + parentProcessSnapshot.processPtr,
      parentProcessSnapshot.processState,
      "parent process snapshot",
    );
    appendPlainForkProcessThreadsDebug(deps, handle, childPid, "before-restore", parentThread.vmPtr);
    restoreParentProcessSnapshot(handle, parentThread, parentProcessSnapshot);
    appendPlainForkProcessThreadsDebug(deps, handle, childPid, "after-restore", parentThread.vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const childMemoryBytes = supervisorProcessMemoryInitialBytes(processMemoryPlan);
    const childMaxMemoryPages = supervisorProcessMemoryMaxPages(processMemoryPlan);
    appendPlainForkMemoryDebug(handle, childPid, forkMemorySnapshot, childMemoryBytes);
    const { instance: childInstance, memory: childMemory } = await instantiateKernelWithSharedMemory(
      handle.wasmModule,
      {
        initialPages: Math.ceil(childMemoryBytes / 65536),
        maximumPages: childMaxMemoryPages,
      },
      { sabPageCache: handle.sabPageCache },
      (memory) => applySupervisorProcessMemoryPlan(handle.memory, memory, processMemoryPlan),
    );
    const childKernel = childInstance.exports as unknown as KernelExports;
    configureKernelInstanceStack(childKernel, MAIN_THREAD_KERNEL_STACK_SLOT);
    // Instantiating a kernel instance over imported child memory replays the
    // module's data segments. In browsers that can clobber copied guest text/data
    // below the probe's argv/envp strings, so restore the whole copied image here
    // and after kernel-worker registration before reattaching shared state.
    applyForkMemorySnapshot(forkMemorySnapshot, childMemory);
    attachKernelRuntimeState(childKernel, handle.kernelState);

    ensureEvictionTimer();
    const childGuestMemoryPolicyLimit = forkChildGuestMemoryPolicyLimit(handle);
    const childHandle = createForkChildHandle({
      deps,
      parent: handle,
      childPid,
      childPpid: lifecycle.ppid,
      vmPtr: parentThread.vmPtr,
      worker: parentWorker,
      memory: childMemory,
      kernel: childKernel,
      kernelState: readKernelRuntimeState(childKernel),
      guestMemoryPolicyLimitBytes: childGuestMemoryPolicyLimit,
      maxMemoryPages: childMaxMemoryPages,
      debugCommand: processDebugCommand(handle),
      workerDebugLabel: "spawn:plain-fork",
    });
    setProcessOnError(childHandle, (error: Error) => {
      if (handle.pid <= 0 || deps.processHandles.get(handle.pid) !== handle) {
        return;
      }
      const childLogTail = workerDebugLog(childHandle).slice(-12).join(", ");
      deps.failProcess(handle, new Error(
        `child-debug:${childPid}:${processLastDebugMessage(childHandle) ?? "last-debug-unavailable"}`
        + ` worker_log=[${childLogTail}]`
        + ` cause=${error.message}`,
      ));
    });
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
    // Kernel-worker registration can instantiate over the shared child memory;
    // restore after discarding copied shared objects before reattaching state.
    applyForkMemorySnapshot(forkMemorySnapshot, childMemory);
    attachKernelRuntimeState(childHandle.kernel, childHandle.kernelState);
    await deps.initializeProcessLifecycleViaKernelWorker(
      childHandle,
      childHandle.vmPtr,
      lifecycle.ppid,
      lifecycle.nextPid,
      lifecycle.pgid,
      lifecycle.sid,
      false,
    );
    childHandle.kernel.thread_prepare_fork_child?.(childHandle.vmPtr, childPid);
    detachInheritedHostStdioFromChild(childHandle);
    setGuestMemoryPolicyLimit(childHandle.kernel, childHandle.vmPtr, childGuestMemoryPolicyLimit);
    childHandle.kernelState = readKernelRuntimeState(childHandle.kernel);
    adoptKernelShare(handle, childHandle);
    await deps.syncForkSharedStateViaKernelWorker(handle, [childHandle], {
      cause: { kind: "fork-child-ready", childPid },
      includeMemfs: false,
      ofdIds: childSharedOfdIds,
    });
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], childSharedOfdIds, {
      cause: { kind: "fork-child-ready", childPid },
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
    assertForkChildReadyOwnership(
      deps,
      childHandle,
      childHandle.vmPtr,
      "plain-fork-child-ready",
    );
    publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
    deps.processHandles.set(childPid, childHandle);
    await deps.requestPipeStateSync(handle, true);
    if (hostExecutionState(childHandle).runningWorker === null) {
      await deps.requestPipeStateSync(childHandle, false);
      restoreIncompatibleLocalOfdSlots(childHandle, preservedChildHeldOfdSlots);
      syncKernelShareOfdReservations(childHandle);
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
      preserveMemoryRanges: forkMemoryPreserveRanges(forkMemorySnapshot),
    });
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    const parentReplacementWorker = acquireWorker();
    deps.attachThreadWorker(handle, {
      worker: parentReplacementWorker,
      vmPtr: parentThread.vmPtr,
      preserveMemoryRanges: forkMemoryPreserveRanges(forkMemorySnapshot),
    });
    appendPlainForkProcessThreadsDebug(deps, handle, childPid, "after-parent-reattach", parentThread.vmPtr);
    deps.notifyForkProgress(handle);
    await waitForForkChildSchedulingPoint(childHandle);
    deps.notifyForkProgress(handle);
    deps.appendWorkerDebugLog(handle, `plain-fork-parent:done:${childPid}`);
  } finally {
    finishPendingChildSpawnRecord(handle);
    deps.notifyForkProgress(handle);
  }
}
