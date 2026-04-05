import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readProcessSpawnHandoff,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { createKernelEnv } from "../../wasm/env.js";
import { captureExecveGuestHandoff, decodeExecveBytes, stageExecveGuestHandoff } from "../execve-handoff.js";
import { activeKernelShareOfdIds, captureVforkSnapshot, collectProcessHeldOfds, snapshotKernelShareOfdSlots } from "../fork-shared.js";
import {
  adoptKernelShare,
  discardCopiedSharedKernelObjects,
  formatOpenFdSummary,
  formatPipeCounterDebug,
  kernelShareOtherMembers,
  restoreLocalOfdSlots,
  restoreLocalPipeSlots,
  snapshotLocalPipeSlots,
} from "../kernel-share.js";
import { copyKernelPrivateRegion } from "../kernel-scratch.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { createForkChildHandle } from "./common.js";
import type { ProcessHandle, ThreadWorkerState, WorkerForkDeps } from "./types.js";

function reconcileInheritedSharedOfdsAfterVforkExecve(
  handle: ProcessHandle,
  inheritedSharedOfdIds: Iterable<number>,
): void {
  const heldByExistingMembers = new Set<number>();
  for (const member of handle.kernelShare.members) {
    for (const ofdId of collectProcessHeldOfds(member)) {
      heldByExistingMembers.add(ofdId);
    }
  }
  for (const ofdId of inheritedSharedOfdIds) {
    if (heldByExistingMembers.has(ofdId)) {
      handle.kernelShare.sharedOfdIds.add(ofdId);
    } else {
      handle.kernelShare.sharedOfdIds.delete(ofdId);
    }
  }
}

export async function handleVforkExecve(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:start:${childPid}`);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`,
  );
  deps.appendWorkerDebugLog(handle, `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`);
  handle.lastDebugMessage = `vfork-execve:start child=${childPid}`;
  const snapshot = handle.vforkSnapshot ?? captureVforkSnapshot(handle, parentThread.vmPtr);
  const inheritedSharedOfdIds = new Set(snapshot.inheritedSharedOfdIds);
  const handoff = readProcessSpawnHandoff(handle.kernel, parentThread.vmPtr);
  const execveGuestPayload = captureExecveGuestHandoff(
    handle,
    parentThread.vmPtr,
    Number(handoff.path),
    Number(handoff.argv),
    Number(handoff.envp),
  );
  handle.kernelState = readKernelRuntimeState(handle.kernel);
  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(
      Math.max(deps.bootStrapMemoryBytes, handle.guestBase + 0x10000) / deps.wasmPageSize,
    ),
    maximum: deps.wasmMaxPages,
    shared: true,
  });

  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
  });
  const childKernel = childInstance.exports as unknown as KernelExports;
  copyKernelPrivateRegion(handle.memory, childMemory, handle.guestBase);
  attachKernelRuntimeState(childKernel, handle.kernelState);

  const rollbackParentAfterHandoffFailure = async (): Promise<void> => {
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
      const parentRehydrateOfdIds = [...inheritedSharedOfdIds].sort((left, right) => left - right);
      if (parentRehydrateOfdIds.length > 0) {
        await deps.rehydrateForkSharedStateViaKernelWorker([handle], parentRehydrateOfdIds, {
          includeMemfs: false,
        });
      }
    } else {
      handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
      if (handle.kernel.thread_restore_vfork_parent_without_cleanup) {
        handle.kernel.thread_restore_vfork_parent_without_cleanup(parentThread.vmPtr, childPid);
      } else {
        handle.kernel.thread_resume_vfork_parent(parentThread.vmPtr, childPid);
      }
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    deps.clearVforkSnapshot(handle);
    deps.kickBlockedWorkers(handle);
    deps.enqueueWorker(handle, parentWorker);
  };

  const childVmPtr = parentThread.vmPtr;
  const stagedExecveHandoff = stageExecveGuestHandoff(
    childKernel,
    handle.guestBase,
    execveGuestPayload,
  );
  childKernel.thread_set_execve_handoff(
    childVmPtr,
    stagedExecveHandoff.processPtr,
    stagedExecveHandoff.pathPtr,
    stagedExecveHandoff.argvPtr,
    stagedExecveHandoff.envpPtr,
  );
  let execRet: number;
  try {
    execRet = Number(childKernel.thread_complete_execve_handoff(childVmPtr));
  } catch (error) {
    const debugStage = childKernel.debug_execve_stage?.();
    const debugValue0 = childKernel.debug_execve_value0?.();
    const debugValue1 = childKernel.debug_execve_value1?.();
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    await rollbackParentAfterHandoffFailure();
    handle.lastDebugMessage = `vfork-execve:handoff-trap child=${childPid} detail=${detail}`
      + ` stage=${debugStage ?? "n/a"} value0=${debugValue0 ?? "n/a"} value1=${debugValue1 ?? "n/a"}`;
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:handoff-trap:${childPid}`);
    return;
  }
  if (execRet < 0) {
    const debugStage = childKernel.debug_execve_stage?.();
    const debugValue0 = childKernel.debug_execve_value0?.();
    const debugValue1 = childKernel.debug_execve_value1?.();
    await rollbackParentAfterHandoffFailure();
    handle.lastDebugMessage = `vfork-execve:handoff-failed child=${childPid} ret=${execRet}`
      + ` stage=${debugStage ?? "n/a"} value0=${debugValue0 ?? "n/a"} value1=${debugValue1 ?? "n/a"}`;
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:handoff-failed:${childPid}:${execRet}`);
    return;
  }

  ensureEvictionTimer();
  const childWorker = acquireWorker();
  const childHandle = createForkChildHandle({
    deps,
    parent: handle,
    childPid,
    childPpid: handle.pid,
    vmPtr: childVmPtr,
    worker: childWorker,
    memory: childMemory,
    kernel: childKernel,
    kernelState: readKernelRuntimeState(childKernel),
    debugCommand: execveGuestPayload.argv[0]
      ? decodeExecveBytes(execveGuestPayload.argv[0])
      : decodeExecveBytes(execveGuestPayload.path),
    workerDebugLabel: "spawn:vfork-execve",
  });
  deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-execve");
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-kernel-ready:${childPid}`);
  handle.lastDebugMessage = `vfork-execve:child-kernel-ready child=${childPid}`;
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    await deps.resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
    const parentRehydrateOfdIds = [...inheritedSharedOfdIds].sort((left, right) => left - right);
    if (parentRehydrateOfdIds.length > 0) {
      await deps.rehydrateForkSharedStateViaKernelWorker([handle], parentRehydrateOfdIds, {
        includeMemfs: false,
      });
    }
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-parent-restored-fds:${childPid}:${formatOpenFdSummary(handle)}`,
    );
    deps.appendWorkerDebugLog(handle, `vfork-parent-restored-fds:${childPid}:${formatOpenFdSummary(handle)}`);
    deps.assertProcessStandardFdIntegrity(handle, "vfork-parent-restored");
  } else {
    handle.kernel.thread_clear_spawn_handoff(parentThread.vmPtr);
    handle.kernel.thread_resume_vfork_parent(parentThread.vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
  }
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

  reconcileInheritedSharedOfdsAfterVforkExecve(handle, inheritedSharedOfdIds);
  adoptKernelShare(handle, childHandle);
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    const sharedOfdIds = activeKernelShareOfdIds(handle);
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const rehydratedSharedOfdIds = sharedOfdIds.filter((ofdId) => !childHeldOfdIds.has(ofdId));
    const procSelfExeTarget = execveGuestPayload.path.length > 0
      && execveGuestPayload.path[execveGuestPayload.path.length - 1] === 0
      ? execveGuestPayload.path.slice(0, -1)
      : execveGuestPayload.path;
    await deps.registerProcessWithKernelWorker(childHandle, {
      seedMemfsMode: "none",
      pipeSeedMode: "process",
    });
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-registered:${childPid}`);
    handle.lastDebugMessage = `vfork-execve:child-registered child=${childPid}`;
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-register");
    await deps.syncForkSharedStateViaKernelWorker(childHandle, kernelShareOtherMembers(childHandle), {
      includeMemfs: false,
      trackHeldOfds: false,
    });
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-synced:${childPid}`);
    handle.lastDebugMessage = `vfork-execve:child-synced child=${childPid}`;
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-sync-fork-shared-state");
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots);
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-discard");
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      procSelfExeTargets: procSelfExeTarget.length > 0
        ? [{ pid: childHandle.pid, bytes: procSelfExeTarget }]
        : undefined,
    });
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-rehydrated:${childPid}`);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:child-fds:${childPid}:${formatPipeCounterDebug(childHandle, 1)}:${formatPipeCounterDebug(childHandle, 2)}`,
    );
    deps.appendWorkerDebugLog(childHandle, `vfork-child-fds:${childPid}:${formatOpenFdSummary(childHandle)}`);
    handle.lastDebugMessage = `vfork-execve:child-rehydrated child=${childPid}`;
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-rehydrate");
  } else {
    const childHeldOfdIds = collectProcessHeldOfds(childHandle);
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    deps.syncForkSharedStateLocally(childHandle, kernelShareOtherMembers(childHandle), {
      includeMemfs: false,
      trackHeldOfds: false,
    });
    discardCopiedSharedKernelObjects(childHandle, null, {
      clearMemfs: false,
    });
    deps.syncForkSharedStateLocally(handle, [childHandle], {
      includeMemfs: false,
    });
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
  }
  await deps.initializeProcessLifecycleViaKernelWorker(
    childHandle,
    childVmPtr,
    handle.pid,
    snapshot.nextPid ?? Math.max(3, childPid + 1),
    snapshot.pgid ?? childKernel.process_current_pgid(childVmPtr),
    snapshot.sid ?? childKernel.process_current_sid(childVmPtr),
    true,
  );
  const childThreadCount = childHandle.kernel.thread_process_thread_count(childVmPtr);
  if (childThreadCount !== 1) {
    const threadSlots = Array.from({ length: Math.min(childHandle.kernel.abi_max_threads(), 8) }, (_value, slot) => {
      const candidateVmPtr = childHandle.kernel.process_thread_vm_ptr(childVmPtr, slot);
      return candidateVmPtr === 0 ? `${slot}=0x0` : `${slot}=0x${candidateVmPtr.toString(16)}`;
    });
    throw new Error(
      `vfork-child-post-lifecycle-init thread_count=${childThreadCount}`
        + ` thread_slots=[${threadSlots.join(", ")}]`,
    );
  }
  deps.assertProcessStandardFdIntegrity(
    childHandle,
    `vfork-child-ready shared=[${activeKernelShareOfdIds(childHandle).join(",")}]`
      + ` private=[${[...collectProcessHeldOfds(childHandle)]
        .filter((ofdId) => !inheritedSharedOfdIds.has(ofdId))
        .join(",")}]`,
  );
  publishKernelRuntimeState(childHandle.runtimeBridge, childHandle.kernelState, true);
  deps.processHandles.set(childPid, childHandle);
  handle.lastDebugMessage = `vfork-execve:child-visible child=${childPid}`;
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `spawn:${childPid}:${JSON.stringify(childHandle.debugCommand)}`,
  );
  deps.attachThreadWorker(childHandle, childWorker, childVmPtr);

  deps.clearVforkSnapshot(handle);
  deps.kickBlockedWorkers(handle);
  deps.enqueueWorker(handle, parentWorker);
}
