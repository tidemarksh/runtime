import {
  attachKernelRuntimeState,
  guestAddressProfileForProcessCreation,
  readKernelRuntimeState,
  readProcessSpawnHandoff,
  setExecveMemoryHandoff,
  setGuestMemoryPolicyLimit,
  type KernelExports,
} from "../../abi.js";
import { publishKernelRuntimeState } from "../../bridge/runtime-state.js";
import { instantiateKernelWithSharedMemory } from "../../wasm/kernel-memory.js";
import {
  configureKernelInstanceStack,
  MAIN_THREAD_KERNEL_STACK_SLOT,
} from "../../wasm/stack.js";
import { captureExecveGuestHandoff, decodeExecveBytes, stageExecveGuestHandoff } from "./execve-handoff.js";
import { captureVforkSnapshot, snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds } from "../ownership/ofd-table.js";
import { snapshotProcessFdEntries, restoreProcessFdEntries } from "../ownership/fd-table.js";
import {
  assertExecveChildReadyOwnership,
  assertVforkParentResumeOwnership,
} from "../ownership/invariants.js";
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
import { normalizeGuestMemoryPolicyLimitBytes } from "./common.js";
import {
  alignPipeOfdGenerationOffsets,
  reconcileInheritedSharedOfdsAfterVforkExecve,
  trackVforkExecveChildSharedOfds,
} from "./shared-ofd.js";
import type { ProcessHandle, ThreadWorkerState, WorkerForkDeps } from "./types.js";

const VFORK_EXECVE_HANDOFF_FAILURE_WAIT_STATUS = 127 << 8;
const HANDOFF_DEBUG_STRING_LIMIT = 128;

function formatGuestCStringForDebug(
  handle: ProcessHandle,
  guestPtr: bigint | number,
): string {
  const ptr = Number(guestPtr);
  if (ptr === 0) {
    return "null";
  }
  const start = handle.guestBase + ptr;
  const bytes = new Uint8Array(handle.memory.buffer);
  if (start < 0 || start >= bytes.length) {
    return `out-of-range:0x${ptr.toString(16)}`;
  }
  let end = start;
  const limit = Math.min(bytes.length, start + HANDOFF_DEBUG_STRING_LIMIT);
  while (end < limit && bytes[end] !== 0) {
    end += 1;
  }
  const terminated = end < bytes.length && bytes[end] === 0;
  const decoded = decodeExecveBytes(bytes.slice(start, terminated ? end + 1 : end));
  return `${JSON.stringify(decoded)}${terminated ? "" : ":unterminated"}`;
}

function formatGuestStrvecForDebug(
  handle: ProcessHandle,
  guestPtr: bigint | number,
): string {
  const ptr = Number(guestPtr);
  if (ptr === 0) {
    return "null";
  }
  const start = handle.guestBase + ptr;
  if (start < 0 || start + 8 > handle.memory.buffer.byteLength) {
    return `out-of-range:0x${ptr.toString(16)}`;
  }
  const view = new DataView(handle.memory.buffer);
  const values: string[] = [];
  for (let index = 0; index < 2; index += 1) {
    const entryOffset = start + index * 8;
    if (entryOffset + 8 > handle.memory.buffer.byteLength) {
      values.push(`out-of-range@${index}`);
      break;
    }
    const entryPtr = view.getBigUint64(entryOffset, true);
    values.push(`0x${entryPtr.toString(16)}=${formatGuestCStringForDebug(handle, entryPtr)}`);
    if (entryPtr === 0n) {
      break;
    }
  }
  return `[${values.join(",")}]`;
}

function formatThreadSyscallArgsForDebug(
  handle: ProcessHandle,
  vmPtr: number,
): string {
  type RegisterAccessorName = "thread_a7" | "thread_a0" | "thread_a1" | "thread_a2" | "thread_a3" | "thread_a4" | "thread_a5";
  const format = (value: bigint | undefined): string => (
    value === undefined ? "n/a" : `0x${value.toString(16)}`
  );
  const read = (name: RegisterAccessorName): bigint | undefined => {
    const accessor = handle.kernel[name] as (((vmPtr: number) => bigint) | undefined);
    return typeof accessor === "function" ? accessor(vmPtr) : undefined;
  };
  return [
    `a7=${format(read("thread_a7"))}`,
    `a0=${format(read("thread_a0"))}`,
    `a1=${format(read("thread_a1"))}`,
    `a2=${format(read("thread_a2"))}`,
    `a3=${format(read("thread_a3"))}`,
    `a4=${format(read("thread_a4"))}`,
    `a5=${format(read("thread_a5"))}`,
  ].join(":");
}

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

function currentGuestMemoryPolicyLimit(
  handle: ProcessHandle,
  kernel: KernelExports,
  vmPtr: number,
  fallbackPolicyLimit?: bigint,
): bigint {
  const policyLimit = kernel.thread_guest_memory_policy_limit?.(vmPtr);
  if (policyLimit !== undefined) {
    return policyLimit < 0n ? 0n : policyLimit;
  }
  if (fallbackPolicyLimit !== undefined) {
    return fallbackPolicyLimit < 0n ? 0n : fallbackPolicyLimit;
  }
  return BigInt(normalizeGuestMemoryPolicyLimitBytes(handle.ramSize));
}

function normalizeProcessMaxMemoryPages(processMaxMemoryPages: number, fallbackPages: number): number {
  if (Number.isFinite(processMaxMemoryPages) && processMaxMemoryPages > 0) {
    return Math.trunc(processMaxMemoryPages);
  }
  return fallbackPages;
}

function currentProcessMaxMemoryPages(
  handle: ProcessHandle,
  kernel: KernelExports,
  vmPtr: number,
  fallbackPages = handle.maxMemoryPages,
): number {
  return normalizeProcessMaxMemoryPages(
    kernel.thread_process_max_memory_pages?.(vmPtr) ?? 0,
    fallbackPages,
  );
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

export async function handleVforkExecve(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  handle.pendingChildSpawnRecords += 1;
  try {
    const handoffBeforePreflight = readProcessSpawnHandoff(handle.kernel, parentThread.vmPtr);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:handoff-before-preflight:${childPid}`
        + `:kind=${handoffBeforePreflight.kind}`
        + `:child=${handoffBeforePreflight.childPid}`
        + `:path=0x${handoffBeforePreflight.path.toString(16)}:${formatGuestCStringForDebug(handle, handoffBeforePreflight.path)}`
        + `:argv=0x${handoffBeforePreflight.argv.toString(16)}:${formatGuestStrvecForDebug(handle, handoffBeforePreflight.argv)}`
        + `:envp=0x${handoffBeforePreflight.envp.toString(16)}`
        + `:${formatThreadSyscallArgsForDebug(handle, parentThread.vmPtr)}`,
    );
    const execveGuestPayload = captureExecveGuestHandoff(
      handle,
      parentThread.vmPtr,
      Number(handoffBeforePreflight.path),
      Number(handoffBeforePreflight.argv),
      Number(handoffBeforePreflight.envp),
      parentThread.execveProcessState,
    );
    if (handle.kernelWorkerRequest && handle.pid > 0) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `vfork-execve:memfs-preflight-skipped:${childPid}`,
      );
      assertVforkChildHandoffState(handle, parentThread.vmPtr, childPid, "child-handoff-no-preflight");
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
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:handoff:${childPid}`
        + `:kind=${handoff.kind}`
        + `:child=${handoff.childPid}`
        + `:path=0x${handoff.path.toString(16)}:${formatGuestCStringForDebug(handle, handoff.path)}`
        + `:argv=0x${handoff.argv.toString(16)}:${formatGuestStrvecForDebug(handle, handoff.argv)}`
        + `:envp=0x${handoff.envp.toString(16)}`
        + `:vfork_child=${handle.kernel.thread_vfork_child_pid?.() ?? "n/a"}`
        + `:${debugVforkVmState(handle, parentThread.vmPtr)}`
        + `:${formatThreadSyscallArgsForDebug(handle, parentThread.vmPtr)}`
        + `:last_pc=${parentThread.lastStatusPc === null ? "n/a" : `0x${parentThread.lastStatusPc.toString(16)}`}`
        + `:last_sp=${parentThread.lastStatusSp === null ? "n/a" : `0x${parentThread.lastStatusSp.toString(16)}`}`,
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
  const childProcessMaxMemoryPages = normalizeProcessMaxMemoryPages(
    handoff.processMaxMemoryPages,
    handle.maxMemoryPages,
  );
  const { instance: childInstance, memory: childMemory } = await instantiateKernelWithSharedMemory(
    handle.wasmModule,
    {
      initialPages: Math.ceil(
        Math.max(
          deps.bootStrapMemoryBytes,
          handle.memory.buffer.byteLength,
          handle.guestBase + 0x10000,
        ) / deps.wasmPageSize,
      ),
      maximumPages: Math.max(childProcessMaxMemoryPages, handle.forkMaxMemoryPages),
    },
    { sabPageCache: handle.sabPageCache },
    (memory) => {
      copyKernelPrivateRegion(handle.memory, memory, handle.kernelState.heapPtr);
      copyExecveKernelRuntimeRegions(handle, memory);
    },
  );
  const childKernel = childInstance.exports as unknown as KernelExports;
  configureKernelInstanceStack(childKernel, MAIN_THREAD_KERNEL_STACK_SLOT);
  // vfork+execve does not need the pre-exec guest image. The kernel handoff
  // only requires the live kernel/private region; argv/envp/process state are
  // restaged explicitly into the child guest window below. Kernel runtime
  // regions must still be present so inherited fds, pipes, and MemFS-backed
  // dynamic loader reads remain valid before kernel-worker rehydration.
  attachKernelRuntimeState(childKernel, handle.kernelState);

  const rollbackParentAfterHandoffFailure = async (): Promise<void> => {
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:rollback:start:${childPid}`
        + `:vfork_child=${handle.kernel.thread_vfork_child_pid?.() ?? "n/a"}`
        + `:${debugVforkVmState(handle, parentThread.vmPtr)}`,
    );
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
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:rollback:restored:${childPid}`
        + `:vfork_child=${handle.kernel.thread_vfork_child_pid?.() ?? "n/a"}`
        + `:${debugVforkVmState(handle, parentThread.vmPtr)}`,
    );
    assertVforkParentResumeOwnership(
      deps,
      handle,
      parentThread.vmPtr,
      "vfork-parent-restored-rollback",
    );
    publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
    reattachRestoredVforkParentWorker(deps, handle, parentWorker, parentThread.vmPtr);
  };

  const childVmPtr = parentThread.vmPtr;
  const stagedExecveHandoff = stageExecveGuestHandoff(
    childKernel,
    handle.guestBase,
    execveGuestPayload,
  );
  const threadAddressProfile = childKernel.thread_guest_address_profile?.(childVmPtr) ?? 0;
  const guestAddressProfile = handoff.addressProfile
    || threadAddressProfile
    || guestAddressProfileForProcessCreation(childKernel);
  const guestMemoryPolicyLimit = currentGuestMemoryPolicyLimit(
    handle,
    childKernel,
    childVmPtr,
    handoff.guestMemoryPolicyLimit,
  );
  const processMaxMemoryPages = currentProcessMaxMemoryPages(
    handle,
    childKernel,
    childVmPtr,
    childProcessMaxMemoryPages,
  );
  setExecveMemoryHandoff(
    childKernel,
    childVmPtr,
    stagedExecveHandoff.processPtr,
    stagedExecveHandoff.pathPtr,
    stagedExecveHandoff.argvPtr,
    stagedExecveHandoff.envpPtr,
    guestAddressProfile,
    guestMemoryPolicyLimit,
    processMaxMemoryPages,
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
  childHandle.maxMemoryPages = processMaxMemoryPages;
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
  setGuestMemoryPolicyLimit(
    childHandle.kernel,
    childHandle.vmPtr,
    guestMemoryPolicyLimit,
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
    assertVforkParentResumeOwnership(
      deps,
      handle,
      parentThread.vmPtr,
      "vfork-parent-restored",
    );
  } else {
    restoreVforkParentLocally(handle.kernel, parentThread.vmPtr, childPid);
    handle.kernelState = readKernelRuntimeState(handle.kernel);
    assertVforkParentResumeOwnership(
      deps,
      handle,
      parentThread.vmPtr,
      "vfork-parent-restored-local",
    );
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
      cause: { kind: "fork-child-ready", childPid },
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
    // MemFS here drops executable file bodies and makes later execve
    // lookups fail with ENOENT.
    discardCopiedSharedKernelObjects(childHandle, preservedOfdSlots, {
      clearMemfs: false,
    });
    deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-discard");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-rehydrate:start:${childPid}`);
    await deps.rehydrateForkSharedStateViaKernelWorker([childHandle], rehydratedSharedOfdIds, {
      cause: { kind: "fork-child-ready", childPid },
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
      cause: { kind: "fork-child-ready", childPid },
      includeMemfs: false,
      trackHeldOfds: false,
    });
    discardCopiedSharedKernelObjects(childHandle, null, {
      clearMemfs: false,
    });
    deps.syncForkSharedStateLocally(handle, [childHandle], {
      cause: { kind: "fork-child-ready", childPid },
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
  assertExecveChildReadyOwnership(
    deps,
    childHandle,
    childVmPtr,
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
