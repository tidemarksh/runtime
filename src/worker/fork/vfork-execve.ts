import {
  attachKernelRuntimeState,
  readKernelRuntimeState,
  readProcessSpawnHandoff,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { createKernelEnv } from "../../wasm/env.js";
import {
  configureKernelInstanceStack,
  MAIN_THREAD_KERNEL_STACK_SLOT,
} from "../../wasm/stack.js";
import { captureExecveGuestHandoff, decodeExecveBytes, stageExecveGuestHandoff } from "../execve-handoff.js";
import { captureVforkSnapshot, snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds } from "../ownership/ofd-table.js";
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
import {
  copyKernelPrivateRegion,
  ensureMemoryCapacity,
} from "../kernel-scratch.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { createForkChildHandle } from "./common.js";
import { computeGuestRamLimitBytes } from "./common.js";
import type { ProcessHandle, ThreadWorkerState, WorkerForkDeps } from "./types.js";

const VFORK_EXECVE_HANDOFF_FAILURE_WAIT_STATUS = 127 << 8;
const OFD_OFFSET_OFFSET = 16;
const PIPE_GENERATION_OFFSET = 20;

function copyKernelRuntimeRegion(
  source: WebAssembly.Memory,
  target: WebAssembly.Memory,
  start: number,
  byteLength: number,
): void {
  if (start === 0 || byteLength <= 0) {
    return;
  }
  const end = start + byteLength;
  ensureMemoryCapacity(target, end);
  new Uint8Array(target.buffer).set(new Uint8Array(source.buffer, start, byteLength), start);
}

function copyExecveKernelRuntimeRegions(handle: ProcessHandle, target: WebAssembly.Memory): void {
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.ofdTablePtr,
    handle.kernel.kernel_ofd_table_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.memfsPtr,
    handle.kernelState.memfsHeaderBytes
      + handle.kernelState.memfsInodesBytes
      + handle.kernelState.memfsFirstChildBytes
      + handle.kernelState.memfsDirEntriesBytes
      + handle.kernelState.memfsDataBytes,
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.epollTablePtr,
    handle.kernel.kernel_epoll_table_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.socketTablePtr,
    handle.kernel.kernel_socket_table_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.pipeBase,
    handle.kernel.kernel_pipe_region_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.netBufBase,
    handle.kernel.kernel_net_buf_pool_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.sigSlotsPtr,
    handle.kernel.kernel_sig_slots_bytes() + handle.kernel.kernel_signal_action_table_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.processPendingPtr,
    handle.kernel.kernel_process_pending_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.processRuntimeStatePtr,
    handle.kernel.kernel_process_runtime_state_bytes(),
  );
  copyKernelRuntimeRegion(
    handle.memory,
    target,
    handle.kernelState.timeStatePtr,
    handle.kernel.kernel_time_state_bytes(),
  );
}

function debugVforkVmState(
  handle: ProcessHandle,
  vmPtr: number,
): string {
  return [
    `pid=${handle.kernel.process_current_pid(vmPtr)}`,
    `tid=${handle.kernel.thread_tid?.(vmPtr) ?? 0}`,
    `ppid=${handle.kernel.process_current_ppid(vmPtr)}`,
    `pc=0x${handle.kernel.thread_pc(vmPtr).toString(16)}`,
    `sp=0x${handle.kernel.thread_sp(vmPtr).toString(16)}`,
    `s11=0x${(handle.kernel.thread_s11?.(vmPtr) ?? 0n).toString(16)}`,
    `process_ptr=0x${handle.kernel.thread_process_ptr(vmPtr).toString(16)}`,
    `thread_count=${handle.kernel.thread_process_thread_count(vmPtr)}`,
  ].join(":");
}

interface ProcessFdEntrySnapshot {
  fd: number;
  ofdId: number;
  fdFlags: number;
}

function snapshotProcessFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
): ProcessFdEntrySnapshot[] {
  return Array.from({ length: handle.kernel.abi_max_fds() }, (_value, fd) => ({
    fd,
    ofdId: handle.kernel.process_fd_ofd_id(vmPtr, fd),
    fdFlags: handle.kernel.process_fd_flags(vmPtr, fd),
  }));
}

function restoreProcessFdEntries(
  handle: ProcessHandle,
  vmPtr: number,
  entries: readonly ProcessFdEntrySnapshot[],
): void {
  const maxFds = handle.kernel.abi_max_fds();
  for (const entry of entries) {
    if (entry.fd < 0 || entry.fd >= maxFds || entry.ofdId < 0 || entry.ofdId >= maxFds) {
      continue;
    }
    handle.kernel.process_set_fd_entry(vmPtr, entry.fd, entry.ofdId, entry.fdFlags);
  }
}

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

function alignPipeOfdGenerationOffsets(
  handle: ProcessHandle,
  ofdIds: Iterable<number>,
): void {
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= OFD_OFFSET_OFFSET) {
    throw new Error(`invalid OFD table layout: bytes=${tableBytes} maxFds=${maxFds}`);
  }

  const view = new DataView(handle.memory.buffer);
  for (const ofdId of ofdIds) {
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeGeneration = currentPipeGeneration(handle, handle.kernel.kernel_ofd_ino(ofdId));
    if (pipeGeneration === null || handle.kernel.kernel_ofd_offset(ofdId) === BigInt(pipeGeneration)) {
      continue;
    }
    view.setBigInt64(
      handle.kernelState.ofdTablePtr + ofdId * slotBytes + OFD_OFFSET_OFFSET,
      BigInt(pipeGeneration),
      true,
    );
  }
}

function assertVforkChildHandoffState(
  handle: ProcessHandle,
  vmPtr: number,
  childPid: number,
  stage: string,
): void {
  const currentPid = handle.kernel.process_current_pid(vmPtr);
  if (currentPid !== childPid) {
    throw new Error(
      `vfork-execve child handoff pid drift at ${stage}: expected=${childPid} actual=${currentPid}`
        + ` vm=0x${vmPtr.toString(16)}:${debugVforkVmState(handle, vmPtr)}`,
    );
  }
}

function assertVforkParentState(
  handle: ProcessHandle,
  vmPtr: number,
  stage: string,
): void {
  const snapshot = handle.vforkSnapshot;
  const currentPid = handle.kernel.process_current_pid(vmPtr);
  if (currentPid !== handle.pid || (snapshot && currentPid !== snapshot.parentPid)) {
    throw new Error(
        `vfork-execve parent pid drift at ${stage}: expected=${handle.pid}`
        + `${snapshot ? ` snapshot=${snapshot.parentPid}` : ""} actual=${currentPid}`
        + ` vm=0x${vmPtr.toString(16)}:${debugVforkVmState(handle, vmPtr)}`,
    );
  }
  if (snapshot) {
    const currentProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
    if (currentProcessPtr !== snapshot.parentProcessPtr) {
      throw new Error(
        `vfork-execve parent process drift at ${stage}`
          + ` expected_process_ptr=0x${snapshot.parentProcessPtr.toString(16)}`
          + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`
        + ` vm=0x${vmPtr.toString(16)}:${debugVforkVmState(handle, vmPtr)}`,
      );
    }
  }
}

function restoreVforkParentLocally(
  kernel: KernelExports,
  vmPtr: number,
  childPid: number,
): void {
  kernel.thread_clear_spawn_handoff(vmPtr);
  if (kernel.thread_restore_vfork_parent_without_cleanup) {
    kernel.thread_restore_vfork_parent_without_cleanup(vmPtr, childPid);
    kernel.thread_clear_vfork_runtime_state?.();
    return;
  }
  kernel.thread_resume_vfork_parent(vmPtr, childPid);
  kernel.thread_clear_vfork_runtime_state?.();
}

function reattachRestoredVforkParentWorker(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  vmPtr: number,
): void {
  deps.attachThreadWorker(handle, {
    worker: parentWorker,
    vmPtr,
  });
  deps.kickBlockedWorkers(handle);
}

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

function trackVforkExecveChildSharedOfds(
  childHandle: ProcessHandle,
  childExecFdEntries: readonly ProcessFdEntrySnapshot[],
): void {
  for (const entry of childExecFdEntries) {
    if (entry.ofdId > 0 && entry.ofdId < childHandle.kernel.abi_max_fds()) {
      childHandle.kernelShare.sharedOfdIds.add(entry.ofdId);
    }
  }
  for (const ofdId of collectProcessHeldOfds(childHandle)) {
    childHandle.kernelShare.sharedOfdIds.add(ofdId);
  }
}

export async function handleVforkExecve(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  handle.pendingChildSpawnRecords += 1;
  try {
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.syncProcessMemfsViaKernelWorker(handle);
      assertVforkChildHandoffState(handle, parentThread.vmPtr, childPid, "child-handoff-preflight");
    }
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:start:${childPid}`);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`,
    );
    deps.appendWorkerDebugLog(handle, `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`);
    handle.lastDebugMessage = `vfork-execve:start child=${childPid}`;
    const snapshot = handle.vforkSnapshot ?? captureVforkSnapshot(handle, parentThread);
    const inheritedSharedOfdIds = new Set(snapshot.inheritedSharedOfdIds);
    const handoff = readProcessSpawnHandoff(handle.kernel, parentThread.vmPtr);
    const execveGuestPayload = captureExecveGuestHandoff(
      handle,
      parentThread.vmPtr,
      Number(handoff.path),
      Number(handoff.argv),
      Number(handoff.envp),
      parentThread.execveProcessState,
    );
    const debugPath = decodeExecveBytes(execveGuestPayload.path);
    const debugArgv0 = execveGuestPayload.argv[0]
      ? decodeExecveBytes(execveGuestPayload.argv[0])
      : "";
    const debugArgv1 = execveGuestPayload.argv[1]
      ? decodeExecveBytes(execveGuestPayload.argv[1])
      : "";
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:guest:${childPid}:${JSON.stringify(debugPath)}:${JSON.stringify(debugArgv0)}:${JSON.stringify(debugArgv1)}`,
    );
    handle.kernelState = readKernelRuntimeState(handle.kernel);
  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(
      Math.max(
        deps.bootStrapMemoryBytes,
        handle.memory.buffer.byteLength,
        handle.guestBase + 0x10000,
      ) / deps.wasmPageSize,
    ),
    maximum: Math.max(handle.maxMemoryPages, handle.forkMaxMemoryPages),
    shared: true,
  });

  const childInstance = await WebAssembly.instantiate(handle.wasmModule, {
    env: createKernelEnv(childMemory, { sabPageCache: handle.sabPageCache }),
  });
  const childKernel = childInstance.exports as unknown as KernelExports;
  configureKernelInstanceStack(childKernel, MAIN_THREAD_KERNEL_STACK_SLOT);
  // vfork+execve does not need the pre-exec guest image. The kernel handoff
  // only requires the live kernel/private region; argv/envp/process state are
  // restaged explicitly into the child guest window below. Kernel runtime
  // regions must still be present so inherited fds, pipes, and MemFS-backed
  // dynamic loader reads remain valid before kernel-worker rehydration.
  copyKernelPrivateRegion(handle.memory, childMemory, handle.kernelState.heapPtr);
  copyExecveKernelRuntimeRegions(handle, childMemory);
  attachKernelRuntimeState(childKernel, handle.kernelState);

  const rollbackParentAfterHandoffFailure = async (): Promise<void> => {
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      await deps.recordChildExitViaKernelWorker(
        handle,
        childPid,
        VFORK_EXECVE_HANDOFF_FAILURE_WAIT_STATUS,
      );
      await deps.resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
    } else {
      handle.kernel.process_record_child_exit(childPid, VFORK_EXECVE_HANDOFF_FAILURE_WAIT_STATUS);
      restoreVforkParentLocally(handle.kernel, parentThread.vmPtr, childPid);
      handle.kernelState = readKernelRuntimeState(handle.kernel);
    }
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    reattachRestoredVforkParentWorker(deps, handle, parentWorker, parentThread.vmPtr);
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
    handle.ramSize,
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
    deps.appendWorkerDebugLog(
      handle,
      `vfork-execve:handoff-trap:${childPid}:${detail}:stage=${debugStage ?? "n/a"}`
      + `:value0=${debugValue0 ?? "n/a"}:value1=${debugValue1 ?? "n/a"}`,
    );
    handle.lastDebugMessage = `vfork-execve:handoff-trap child=${childPid} detail=${detail}`
      + ` stage=${debugStage ?? "n/a"} value0=${debugValue0 ?? "n/a"} value1=${debugValue1 ?? "n/a"}`;
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:handoff-trap:${childPid}:${debugStage ?? "n/a"}:${debugValue0 ?? "n/a"}:${debugValue1 ?? "n/a"}`,
    );
    return;
  }
  if (execRet < 0) {
    const debugStage = childKernel.debug_execve_stage?.();
    const debugValue0 = childKernel.debug_execve_value0?.();
    const debugValue1 = childKernel.debug_execve_value1?.();
    await rollbackParentAfterHandoffFailure();
    deps.appendWorkerDebugLog(
      handle,
      `vfork-execve:handoff-failed:${childPid}:ret=${execRet}:stage=${debugStage ?? "n/a"}`
      + `:value0=${debugValue0 ?? "n/a"}:value1=${debugValue1 ?? "n/a"}`,
    );
    handle.lastDebugMessage = `vfork-execve:handoff-failed child=${childPid} ret=${execRet}`
      + ` stage=${debugStage ?? "n/a"} value0=${debugValue0 ?? "n/a"} value1=${debugValue1 ?? "n/a"}`;
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:handoff-failed:${childPid}:${execRet}:${debugStage ?? "n/a"}:${debugValue0 ?? "n/a"}:${debugValue1 ?? "n/a"}`,
    );
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
    worker: parentWorker,
    memory: childMemory,
    kernel: childKernel,
    kernelState: readKernelRuntimeState(childKernel),
    debugCommand: execveGuestPayload.argv[0]
      ? decodeExecveBytes(execveGuestPayload.argv[0])
      : decodeExecveBytes(execveGuestPayload.path),
    workerDebugLabel: "spawn:vfork-execve",
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
  childHandle.kernel.thread_set_guest_ram_limit(
    childHandle.vmPtr,
    computeGuestRamLimitBytes(
      childHandle.guestBase,
      childHandle.ramSize,
      childHandle.maxMemoryPages,
    ),
  );
  const childExecFdEntries = snapshotProcessFdEntries(childHandle, childVmPtr);
  deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-execve");
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-exec-fds:${childPid}:${formatOpenFdSummary(childHandle)}`,
  );
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-kernel-ready:${childPid}`);
  handle.lastDebugMessage = `vfork-execve:child-kernel-ready child=${childPid}`;
  if (handle.kernelWorkerRequest && handle.pid > 0) {
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:resume-parent:start:${childPid}`);
    await deps.resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:resume-parent:done:${childPid}`);
    assertVforkParentState(handle, parentThread.vmPtr, "parent-post-resume");
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-parent-restored-fds:${childPid}:${formatOpenFdSummary(handle)}`,
    );
    deps.appendWorkerDebugLog(handle, `vfork-parent-restored-fds:${childPid}:${formatOpenFdSummary(handle)}`);
    deps.assertProcessStandardFdIntegrity(handle, "vfork-parent-restored");
  } else {
    restoreVforkParentLocally(handle.kernel, parentThread.vmPtr, childPid);
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
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-register:start:${childPid}`);
    await deps.registerProcessWithKernelWorker(childHandle, {
      seedMemfsMode: "none",
      pipeSeedMode: "process",
    });
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-register:done:${childPid}`);
    assertVforkParentState(handle, parentThread.vmPtr, "parent-post-child-register");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-registered:${childPid}`);
    handle.lastDebugMessage = `vfork-execve:child-registered child=${childPid}`;
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-register");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-sync:start:${childPid}`);
    await deps.syncForkSharedStateViaKernelWorker(childHandle, kernelShareOtherMembers(childHandle), {
      includeMemfs: false,
      trackHeldOfds: false,
    });
    trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-sync:done:${childPid}`);
    assertVforkParentState(handle, parentThread.vmPtr, "parent-post-child-sync");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-synced:${childPid}`);
    handle.lastDebugMessage = `vfork-execve:child-synced child=${childPid}`;
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-sync-fork-shared-state");
    const preservedOfdSlots = snapshotKernelShareOfdSlots(childHandle, childHeldOfdIds);
    const preservedPipeSlots = snapshotLocalPipeSlots(childHandle, childHeldOfdIds);
    // Keep the execve-completed MemFS image intact. Unlike the plain fork path,
    // vfork+execve has already rebuilt the child process in-place, so clearing
    // MemFS here drops executable/toolchain file bodies and makes later execve
    // lookups fail with ENOENT.
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots, {
      clearMemfs: false,
    });
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-discard");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-rehydrate:start:${childPid}`);
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      procSelfExeTargets: procSelfExeTarget.length > 0
        ? [{ pid: childHandle.pid, bytes: procSelfExeTarget }]
        : undefined,
      includeMemfs: false,
    });
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-rehydrate:done:${childPid}`);
    assertVforkParentState(handle, parentThread.vmPtr, "parent-post-child-rehydrate");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-rehydrated:${childPid}`);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:child-fds:${childPid}:${formatPipeCounterDebug(childHandle, 1)}:${formatPipeCounterDebug(childHandle, 2)}`,
    );
    deps.appendWorkerDebugLog(childHandle, `vfork-child-fds:${childPid}:${formatOpenFdSummary(childHandle)}`);
    handle.lastDebugMessage = `vfork-execve:child-rehydrated child=${childPid}`;
    restoreLocalOfdSlots(childHandle, preservedOfdSlots);
    restoreLocalPipeSlots(childHandle, preservedPipeSlots);
    restoreProcessFdEntries(childHandle, childVmPtr, childExecFdEntries);
    alignPipeOfdGenerationOffsets(childHandle, collectProcessHeldOfds(childHandle));
    trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:child-fds-restored:${childPid}:${formatOpenFdSummary(childHandle)}`,
    );
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
    restoreProcessFdEntries(childHandle, childVmPtr, childExecFdEntries);
    alignPipeOfdGenerationOffsets(childHandle, collectProcessHeldOfds(childHandle));
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:child-fds-restored:${childPid}:${formatOpenFdSummary(childHandle)}`,
    );
  }
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-lifecycle:start:${childPid}`);
  await deps.initializeProcessLifecycleViaKernelWorker(
    childHandle,
    childVmPtr,
    handle.pid,
    snapshot.nextPid ?? Math.max(3, childPid + 1),
    snapshot.pgid ?? childKernel.process_current_pgid(childVmPtr),
    snapshot.sid ?? childKernel.process_current_sid(childVmPtr),
    true,
  );
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-lifecycle:done:${childPid}`);
  restoreProcessFdEntries(childHandle, childVmPtr, childExecFdEntries);
  alignPipeOfdGenerationOffsets(childHandle, collectProcessHeldOfds(childHandle));
  trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-fds-ready:${childPid}:${formatOpenFdSummary(childHandle)}`,
  );
  assertVforkParentState(handle, parentThread.vmPtr, "parent-post-child-lifecycle");
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
  handle.worker = childWorker;
  deps.attachThreadWorker(childHandle, {
    worker: parentWorker,
    vmPtr: childVmPtr,
    transferFrom: handle,
  });

  assertVforkParentState(handle, parentThread.vmPtr, "parent-pre-enqueue");
  deps.attachThreadWorker(handle, {
    worker: childWorker,
    vmPtr: parentThread.vmPtr,
  });
  deps.kickBlockedWorkers(handle);
  } finally {
    if (handle.pendingChildSpawnRecords > 0) {
      handle.pendingChildSpawnRecords -= 1;
    }
    deps.kickBlockedWorkers(handle);
  }
}
