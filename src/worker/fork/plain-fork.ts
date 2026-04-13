import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { createKernelEnv } from "../../wasm/env.js";
import {
  adoptKernelShare,
  discardCopiedSharedKernelObjects,
  restoreLocalOfdSlots,
  restoreLocalPipeSlots,
  retainForkSharedOfds,
  snapshotLocalPipeSlots,
} from "../kernel-share.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds, snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { copyLinearMemoryImage } from "../kernel-scratch.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { createForkChildHandle } from "./common.js";
import { detachInheritedHostStdioFromChild } from "./common.js";
import { computeGuestRamLimitBytes } from "./common.js";
import type { WorkerForkDeps, ProcessHandle, ThreadWorkerState } from "./types.js";

type MemfsLayoutSnapshot = {
  inodeCount: number;
  dirEntryCount: number;
  dataUsed: number;
  dataCap: number;
  inodesPtr: number;
  firstChildPtr: number;
  dirEntriesPtr: number;
  dataPtr: number;
};

const FORK_CHILD_EXECVE_HEADROOM_BYTES = 16 * 1024 * 1024;

function readMemfsLayoutSnapshot(handle: ProcessHandle): MemfsLayoutSnapshot {
  const view = new DataView(handle.memory.buffer, handle.kernelState.memfsPtr, 32);
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

function restoreForkChildMemfs(
  source: ProcessHandle,
  target: ProcessHandle,
  targetLayout: MemfsLayoutSnapshot,
): void {
  const sourceLayout = readMemfsLayoutSnapshot(source);
  const sourceBytes = new Uint8Array(source.memory.buffer);
  const targetBytes = new Uint8Array(target.memory.buffer);

  targetBytes.set(
    sourceBytes.subarray(sourceLayout.inodesPtr, sourceLayout.inodesPtr + source.kernel.kernel_memfs_inodes_bytes()),
    targetLayout.inodesPtr,
  );
  targetBytes.set(
    sourceBytes.subarray(sourceLayout.firstChildPtr, sourceLayout.firstChildPtr + source.kernel.kernel_memfs_first_child_bytes()),
    targetLayout.firstChildPtr,
  );
  targetBytes.set(
    sourceBytes.subarray(sourceLayout.dirEntriesPtr, sourceLayout.dirEntriesPtr + source.kernel.kernel_memfs_dir_entries_bytes()),
    targetLayout.dirEntriesPtr,
  );
  const dataBytes = Math.min(sourceLayout.dataUsed, targetLayout.dataCap);
  targetBytes.set(
    sourceBytes.subarray(sourceLayout.dataPtr, sourceLayout.dataPtr + dataBytes),
    targetLayout.dataPtr,
  );
  if (targetLayout.dataCap > dataBytes) {
    targetBytes.fill(0, targetLayout.dataPtr + dataBytes, targetLayout.dataPtr + targetLayout.dataCap);
  }

  const header = new DataView(target.memory.buffer, target.kernelState.memfsPtr, 32);
  header.setUint32(0, sourceLayout.inodeCount, true);
  header.setUint32(4, sourceLayout.dirEntryCount, true);
  header.setUint32(8, dataBytes, true);
  header.setUint32(12, targetLayout.dataCap, true);
  header.setUint32(16, targetLayout.inodesPtr, true);
  header.setUint32(20, targetLayout.firstChildPtr, true);
  header.setUint32(24, targetLayout.dirEntriesPtr, true);
  header.setUint32(28, targetLayout.dataPtr, true);
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
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.requestPipeStateSync(handle, false);
      deps.appendRuntimeLifecycleEvent(handle.runtimeId, `plain-fork:parent-pipe-synced:${handle.pid}`);
    }
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

    retainForkSharedOfds(handle);
    handle.kernelState = readKernelRuntimeState(handle.kernel);

    const parentMem = new Uint8Array(handle.memory.buffer);
    const childMemoryBytes = Math.max(
      handle.memory.buffer.byteLength,
      handle.guestBase + handle.ramSize + FORK_CHILD_EXECVE_HEADROOM_BYTES,
    );
    const childMemory = new WebAssembly.Memory({
      initial: Math.ceil(childMemoryBytes / 65536),
      maximum: Math.max(handle.maxMemoryPages, handle.forkMaxMemoryPages),
      shared: true,
    });
    new Uint8Array(childMemory.buffer).set(parentMem);

    const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
      env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
    });
    const childKernel = childInstance.exports as unknown as KernelExports;
    // Instantiating a kernel instance over imported child memory replays the
    // module's data segments. In browsers that can clobber copied guest text/data
    // below the probe's argv/envp strings, so restore the whole copied image here
    // and after kernel-worker registration before reattaching shared state.
    copyLinearMemoryImage(handle.memory, childMemory);
    attachKernelRuntimeState(childKernel, handle.kernelState);

    ensureEvictionTimer();
    const childWorker = acquireWorker();
    const childHandle = createForkChildHandle({
      deps,
      parent: handle,
      childPid,
      childPpid,
      vmPtr: handle.vmPtr,
      worker: childWorker,
      memory: childMemory,
      kernel: childKernel,
      kernelState: readKernelRuntimeState(childKernel),
      debugCommand: handle.debugCommand,
      workerDebugLabel: "spawn:plain-fork",
    });
    const childGuestRamLimit = computeGuestRamLimitBytes(
      childHandle.guestBase,
      childHandle.ramSize,
      childHandle.maxMemoryPages,
    );
    detachInheritedHostStdioFromChild(childHandle);
    adoptKernelShare(handle, childHandle);
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const sharedOfdIds = activeKernelShareOfdIds(handle);
    const rehydratedSharedOfdIds = sharedOfdIds.filter((ofdId) => !childHeldOfdIds.has(ofdId));
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    const childMemfsLayout = readMemfsLayoutSnapshot(childHandle);

    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots);
    await deps.registerProcessWithKernelWorker(childHandle, {
      seedMemfsMode: "none",
      pipeSeedMode: "process",
    });
    // Kernel-worker registration also instantiates over the shared child memory,
    // so restore the copied parent image again before rebuilding canonical shared
    // state for the child.
    copyLinearMemoryImage(handle.memory, childMemory);
    attachKernelRuntimeState(childHandle.kernel, childHandle.kernelState);
    restoreForkChildMemfs(handle, childHandle, childMemfsLayout);
    deps.syncForkSharedStateLocally(handle, [childHandle], { includeMemfs: false });
    await deps.syncForkSharedStateViaKernelWorker(handle, [childHandle], { includeMemfs: false });
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      includeMemfs: false,
    });
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
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    childHandle.kernel.thread_set_guest_ram_limit(childHandle.vmPtr, childGuestRamLimit);
    childHandle.kernelState = readKernelRuntimeState(childHandle.kernel);
    deps.assertProcessStandardFdIntegrity(childHandle, "plain-fork-child-ready");
    publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState);
    deps.processHandles.set(childPid, childHandle);
    deps.attachThreadWorker(childHandle, childWorker, childHandle.vmPtr);

    handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    deps.enqueueWorker(handle, parentWorker);
  } finally {
    if (handle.pendingChildSpawnRecords > 0) {
      handle.pendingChildSpawnRecords -= 1;
    }
    deps.kickBlockedWorkers(handle);
  }
}
