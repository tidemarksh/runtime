import { hostExecutionState } from "../host-execution-state.js";
import { workerDebugLog } from "../worker-debug-log.js";
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
import {
  DEFAULT_RESIDENT_PAGE_SIZE,
  publishResidentPage,
  readResidentFileData,
} from "../../fs/sab-page-cache.js";
import {
  clearSupervisorPendingBackingFill,
  readSupervisorPendingBackingFill,
  supervisorBackingFillSpan,
  supervisorBackingRetryLimit,
  type SupervisorPendingBackingFillRequest,
} from "../../fs/supervisor-backing-fill.js";
import { supervisorResidentPageTableAuthority } from "../../fs/supervisor-page-table.js";
import { captureExecveGuestHandoff, decodeExecveBytes, stageExecveGuestHandoff } from "./execve-handoff.js";
import {
  captureVforkSnapshot,
  reconcileForkSharedOfdRefcounts,
  snapshotKernelShareOfdSlots,
} from "../fork-shared.js";
import { getThreadExecveProcessState } from "../thread-execve-state.js";
import { getThreadActivityState } from "../thread-activity-state.js";
import { activeKernelShareOfdIds, collectProcessHeldOfds } from "../ownership/ofd-table.js";
import { snapshotProcessFdEntries, restoreProcessFdEntries } from "../ownership/fd-table.js";
import {
  beginPendingChildSpawnRecord,
  finishPendingChildSpawnRecord,
} from "../pending-child-spawn-state.js";
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
  applySupervisorProcessMemoryPlan,
  readSupervisorProcessMemoryPlan,
  supervisorProcessMemoryInitialBytes,
  supervisorProcessMemoryMaxPages,
} from "../supervisor-host-op.js";
import {
  supervisorProcessIdentityMatches,
  supervisorProcessOwnerMatches,
} from "../process-identity.js";
import {
  processDebugCommand,
  processLastDebugMessage,
  setProcessLastDebugMessage,
} from "../process-debug-state.js";
import { processKernelWorkerRequest } from "../process-kernel-worker-request-state.js";
import {
  processMemfsNeedsPublication,
  setProcessMemfsDirty,
} from "../process-memfs-state.js";
import { setProcessOnError } from "../process-callback-state.js";
import { setProcessInheritedParentOutput } from "../process-stdio-state.js";
import { acquireWorker, ensureEvictionTimer } from "../pool.js";
import { createForkChildHandle } from "./common.js";
import {
  alignPipeOfdGenerationOffsets,
  reconcileInheritedSharedOfdsAfterVforkExecve,
  trackVforkExecveChildSharedOfds,
} from "./shared-ofd.js";
import type { ProcessHandle, ThreadWorkerState, WorkerForkDeps } from "./types.js";

const HANDOFF_DEBUG_STRING_LIMIT = 128;
const EAGAIN = -11;

type ExecveBackingMiss = SupervisorPendingBackingFillRequest;

interface KernelChildLifecycle {
  ppid: number;
  nextPid: number;
  pgid: number;
  sid: number;
}

function formatProcessStateFdEntries(processState: Uint8Array, limit = 12): string {
  const view = new DataView(
    processState.buffer,
    processState.byteOffset,
    processState.byteLength,
  );
  return Array.from({ length: limit }, (_value, fd) => {
    const offset = fd * 8;
    if (offset + 8 > view.byteLength) {
      return null;
    }
    const ofdId = view.getUint32(offset, true);
    if (ofdId === 0) {
      return null;
    }
    return `${fd}->${ofdId}/${view.getUint32(offset + 4, true)}`;
  }).filter(Boolean).join(" ");
}

export function fdEntriesDifferingFromSnapshot<
  T extends { fd: number; ofdId: number; fdFlags: number; generation?: number },
>(
  entries: readonly T[],
  otherEntries: readonly { fd: number; ofdId: number; fdFlags: number; generation?: number }[],
): T[] {
  const otherByFd = new Map(otherEntries.map((entry) => [entry.fd, entry]));
  return entries.filter((entry) => {
    const other = otherByFd.get(entry.fd);
    return other === undefined
      || other.ofdId !== entry.ofdId
      || other.fdFlags !== entry.fdFlags
      || other.generation !== entry.generation;
  });
}

export function fdAuthorityEntriesChangedBetweenSnapshots(
  entries: readonly { fd: number; ofdId: number; fdFlags: number; generation?: number }[],
  otherEntries: readonly { fd: number; ofdId: number; fdFlags: number; generation?: number }[],
): Array<{ fd: number }> {
  const changedFds = new Set([
    ...fdEntriesDifferingFromSnapshot(entries, otherEntries).map((entry) => entry.fd),
    ...fdEntriesDifferingFromSnapshot(otherEntries, entries).map((entry) => entry.fd),
  ]);
  return [...changedFds].sort((left, right) => left - right).map((fd) => ({ fd }));
}

export function fdAuthorityEntriesMutatedInBothSnapshots(
  entries: readonly { fd: number; ofdId: number; fdFlags: number; generation?: number }[],
  otherEntries: readonly { fd: number; ofdId: number; fdFlags: number; generation?: number }[],
): Array<{ fd: number }> {
  const otherByFd = new Map(otherEntries.map((entry) => [entry.fd, entry]));
  return entries.filter((entry) => {
    const other = otherByFd.get(entry.fd);
    return other !== undefined
      && (
        other.ofdId !== entry.ofdId
        || other.fdFlags !== entry.fdFlags
        || other.generation !== entry.generation
      );
  }).map((entry) => ({ fd: entry.fd }));
}

function readKernelChildLifecycle(kernel: KernelExports, childPid: number): KernelChildLifecycle {
  const ppid = kernel.process_child_ppid?.(childPid);
  const pgid = kernel.process_child_pgid?.(childPid);
  const sid = kernel.process_child_sid?.(childPid);
  const nextPid = kernel.process_next_pid?.();
  if (ppid === undefined || pgid === undefined || sid === undefined || nextPid === undefined) {
    throw new Error("kernel process lifecycle ABI missing child lifecycle export");
  }
  return { ppid, nextPid, pgid, sid };
}

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

async function publishExecveBackingMissForHandoff(
  handle: ProcessHandle,
  miss: ExecveBackingMiss,
): Promise<boolean> {
  const sab = handle.sabPageCache;
  const kernelWorkerRequest = processKernelWorkerRequest(handle);
  if (!sab || !kernelWorkerRequest) {
    return false;
  }
  const span = supervisorBackingFillSpan(handle.kernel, miss);
  if (!span) {
    return false;
  }
  const response = await kernelWorkerRequest({
    type: "read-file-range-by-inode",
    ino: miss.ino,
    offset: span.offset,
    length: span.length,
    expectedSize: miss.expectedSize,
  });
  if (response.type !== "file-range-read" || response.data.length !== span.length) {
    return false;
  }
  for (let pageStart = span.offset; pageStart < span.end; pageStart += DEFAULT_RESIDENT_PAGE_SIZE) {
    const pageLen = Math.min(DEFAULT_RESIDENT_PAGE_SIZE, miss.expectedSize - pageStart);
    if (pageLen <= 0 || readResidentFileData(sab, miss.ino, pageStart, pageLen)) {
      continue;
    }
    const backingOffset = pageStart - span.offset;
    const page = response.data.subarray(backingOffset, backingOffset + pageLen);
    if (
      page.length !== pageLen
      || !publishResidentPage(sab, miss.ino, pageStart / DEFAULT_RESIDENT_PAGE_SIZE, page, {
        authority: supervisorResidentPageTableAuthority(handle.kernel),
      })
    ) {
      return false;
    }
  }
  return true;
}

async function completeExecveHandoffWithBackingRetry(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  childPid: number,
  childKernel: KernelExports,
  childVmPtr: number,
): Promise<number> {
  let lastRet = EAGAIN;
  let maxAttempts = 1;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    lastRet = Number(childKernel.thread_complete_execve_handoff(childVmPtr));
    if (lastRet !== EAGAIN) {
      return lastRet;
    }
    const miss = readSupervisorPendingBackingFill(childKernel, childVmPtr);
    if (!miss) {
      return lastRet;
    }
    maxAttempts = Math.max(maxAttempts, supervisorBackingRetryLimit(childKernel, miss));
    if (!await publishExecveBackingMissForHandoff(handle, miss)) {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `vfork-execve:backing-publish-failed:${childPid}`
          + `:ino=${miss.ino}:off=${miss.offset}:len=${miss.length}:size=${miss.expectedSize}`,
      );
      return lastRet;
    }
    clearSupervisorPendingBackingFill(childKernel, childVmPtr);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:backing-published:${childPid}`
        + `:attempt=${attempt + 1}:ino=${miss.ino}:off=${miss.offset}:len=${miss.length}:size=${miss.expectedSize}`,
    );
  }
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:backing-retry-exhausted:${childPid}:attempts=${maxAttempts}`,
  );
  return lastRet;
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
  kernel: KernelExports,
  vmPtr: number,
  spawnHandoffPolicyLimit?: bigint,
): bigint {
  const policyLimit = kernel.thread_guest_memory_policy_limit?.(vmPtr);
  if (policyLimit !== undefined) {
    return policyLimit < 0n ? 0n : policyLimit;
  }
  if (spawnHandoffPolicyLimit !== undefined) {
    return spawnHandoffPolicyLimit < 0n ? 0n : spawnHandoffPolicyLimit;
  }
  throw new Error("kernel ABI missing thread_guest_memory_policy_limit");
}

function normalizeProcessMaxMemoryPages(processMaxMemoryPages: number, source: string): number {
  if (Number.isFinite(processMaxMemoryPages) && processMaxMemoryPages > 0) {
    return Math.trunc(processMaxMemoryPages);
  }
  throw new Error(`${source} invalid process max memory pages: ${processMaxMemoryPages}`);
}

function currentProcessMaxMemoryPages(
  kernel: KernelExports,
  vmPtr: number,
  supervisorPlanMaxPages: number,
): number {
  const maxPages = kernel.thread_process_max_memory_pages?.(vmPtr);
  if (maxPages !== undefined && maxPages > 0) {
    return normalizeProcessMaxMemoryPages(maxPages, "kernel thread_process_max_memory_pages");
  }
  return normalizeProcessMaxMemoryPages(supervisorPlanMaxPages, "supervisor process memory plan");
}

function assertVforkChildHandoffState(
  handle: ProcessHandle,
  vmPtr: number,
  childPid: number,
  stage: string,
): void {
  if (!supervisorProcessOwnerMatches(handle.kernel, vmPtr, childPid)) {
    const currentPid = handle.kernel.process_current_pid(vmPtr);
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
  const snapshot = hostExecutionState(handle).vforkSnapshot;
  if (!supervisorProcessOwnerMatches(handle.kernel, vmPtr, handle.pid)) {
    const currentPid = handle.kernel.process_current_pid(vmPtr);
    throw new Error(
        `vfork-execve parent pid drift at ${stage}: expected=${handle.pid}`
        + `${snapshot ? ` snapshot=${snapshot.parentPid}` : ""} actual=${currentPid}`
        + ` vm=0x${vmPtr.toString(16)}:${debugVforkVmState(handle, vmPtr)}`,
    );
  }
  if (snapshot) {
    if (!supervisorProcessIdentityMatches(
      handle.kernel,
      vmPtr,
      snapshot.parentPid,
      snapshot.parentProcessPtr,
    )) {
      const currentProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      throw new Error(
        `vfork-execve parent process drift at ${stage}`
          + ` expected_process_ptr=0x${snapshot.parentProcessPtr.toString(16)}`
          + ` actual_process_ptr=0x${currentProcessPtr.toString(16)}`
        + ` vm=0x${vmPtr.toString(16)}:${debugVforkVmState(handle, vmPtr)}`,
      );
    }
  }
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
  deps.notifyForkProgress(handle);
}

export function transferVforkExecveWorkers(
  deps: Pick<
    WorkerForkDeps,
    "attachThreadWorker" | "markCanonicalFdEntriesForNextStep" | "notifyForkProgress"
  >,
  parentHandle: ProcessHandle,
  childHandle: ProcessHandle,
  parentWorker: Worker,
  childWorker: Worker,
  vmPtr: number,
  parentFdEntries: readonly { fd: number }[],
  childFdEntries: readonly { fd: number }[],
): void {
  parentHandle.worker = childWorker;
  deps.attachThreadWorker(childHandle, {
    worker: parentWorker,
    vmPtr,
    transferFrom: parentHandle,
  });
  deps.attachThreadWorker(parentHandle, {
    worker: childWorker,
    vmPtr,
  });

  // FD authority is worker-local, so publish it only after both workers have
  // reached their final process owners.
  deps.markCanonicalFdEntriesForNextStep?.(parentHandle, parentFdEntries);
  deps.markCanonicalFdEntriesForNextStep?.(childHandle, childFdEntries);
  deps.notifyForkProgress(parentHandle);
}

export async function handleVforkExecve(
  deps: WorkerForkDeps,
  handle: ProcessHandle,
  parentWorker: Worker,
  parentThread: ThreadWorkerState,
  childPid: number,
): Promise<void> {
  beginPendingChildSpawnRecord(handle);
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
        + `:cwd_ino=${handle.kernel.process_cwd_ino(parentThread.vmPtr)}`
        + `:${formatThreadSyscallArgsForDebug(handle, parentThread.vmPtr)}`,
    );
    const execveGuestPayload = captureExecveGuestHandoff(
      handle,
      parentThread.vmPtr,
      handoffBeforePreflight.path,
      handoffBeforePreflight.argv,
      handoffBeforePreflight.envp,
      getThreadExecveProcessState(parentThread),
    );
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-execve:captured-child-fds:${childPid}`
        + `:${formatProcessStateFdEntries(execveGuestPayload.processState)}`,
    );
    if (processMemfsNeedsPublication(handle)) {
      await deps.syncForkSharedStateViaKernelWorker(handle, [], {
        cause: { kind: "syscall", nr: -1n, name: "vfork-execve-parent-memfs-publication" },
        includeMemfs: true,
        memfsOnly: true,
        sourceVmPtr: parentThread.vmPtr,
      });
      setProcessMemfsDirty(handle, false);
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `vfork-execve:parent-memfs-published:${handle.pid}`,
      );
    } else {
      deps.appendRuntimeLifecycleEvent(
        handle.runtimeId,
        `vfork-execve:parent-memfs-clean:${handle.pid}`,
      );
    }
    assertVforkChildHandoffState(handle, parentThread.vmPtr, childPid, "child-handoff-preflight");
    deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:start:${childPid}`);
    deps.appendRuntimeLifecycleEvent(
      handle.runtimeId,
      `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`,
    );
    deps.appendWorkerDebugLog(handle, `vfork-parent-fds:${childPid}:${formatOpenFdSummary(handle)}`);
    setProcessLastDebugMessage(handle, `vfork-execve:start child=${childPid}`);
    const snapshot = hostExecutionState(handle).vforkSnapshot ?? captureVforkSnapshot(handle, parentThread);
    const parentActivity = getThreadActivityState(parentThread);
    const childLifecycle = readKernelChildLifecycle(handle.kernel, childPid);
    const inheritedSharedOfdIds = new Set(snapshot.inheritedSharedOfdIds);
    const parentResumeFdEntries = snapshot.parentFdEntries
      ?? snapshotProcessFdEntries(handle, parentThread.vmPtr);
    const parentResumeHeldOfdIds = snapshot.parentHeldOfdIds
      ?? [...new Set([
        ...collectProcessHeldOfds(handle),
        ...parentResumeFdEntries
          .map((entry) => entry.ofdId)
          .filter((ofdId) => ofdId > 0 && ofdId < handle.kernel.abi_max_fds()),
      ])];
    const parentResumeOfdSlots = snapshot.parentOfdSlots
      ?? snapshotKernelShareOfdSlots(handle, parentResumeHeldOfdIds);
    const parentResumePipeSlots = snapshot.parentPipeSlots
      ?? snapshotLocalPipeSlots(handle, parentResumeHeldOfdIds);
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
        + `:last_pc=${parentActivity.lastStatusPc === null ? "n/a" : `0x${parentActivity.lastStatusPc.toString(16)}`}`
        + `:last_sp=${parentActivity.lastStatusSp === null ? "n/a" : `0x${parentActivity.lastStatusSp.toString(16)}`}`,
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
  const processMemoryPlan = readSupervisorProcessMemoryPlan(handle, parentThread.vmPtr);
  const childProcessMaxMemoryPages = supervisorProcessMemoryMaxPages(processMemoryPlan);
  const childInitialMemoryBytes = supervisorProcessMemoryInitialBytes(processMemoryPlan);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-memory:${childPid}`
      + `:initial=${childInitialMemoryBytes}`
      + `:parent=${handle.memory.buffer.byteLength}`
      + `:guestBase=${handle.guestBase}`
      + `:heap=${Math.max(handle.kernelState.heapPtr, handle.kernel.kernel_heap_ptr?.() ?? 0)}`
      + `:sp=${parentActivity.lastStatusSp ?? parentActivity.lastStepSp ?? handle.kernel.thread_sp(parentThread.vmPtr)}`,
  );
  const { instance: childInstance, memory: childMemory } = await instantiateKernelWithSharedMemory(
    handle.wasmModule,
    {
      initialPages: Math.ceil(childInitialMemoryBytes / deps.wasmPageSize),
      maximumPages: childProcessMaxMemoryPages,
    },
    {
      sabPageCache: handle.sabPageCache,
    },
    (memory) => applySupervisorProcessMemoryPlan(handle.memory, memory, processMemoryPlan),
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
    await deps.rollbackVforkExecveHandoffViaKernelWorker(handle, parentThread.vmPtr, childPid);
    restoreLocalOfdSlots(handle, parentResumeOfdSlots);
    restoreLocalPipeSlots(handle, parentResumePipeSlots);
    const parentAuthorityFdEntries = fdEntriesDifferingFromSnapshot(
      parentResumeFdEntries,
      snapshotProcessFdEntries(handle, parentThread.vmPtr),
    );
    alignPipeOfdGenerationOffsets(handle, collectProcessHeldOfds(handle));
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
    deps.markCanonicalFdEntriesForNextStep?.(handle, parentAuthorityFdEntries);
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
    childKernel,
    childVmPtr,
    handoff.guestMemoryPolicyLimit,
  );
  const processMaxMemoryPages = currentProcessMaxMemoryPages(
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
  const childMemfsDataPtr = childKernel.kernel_memfs_data_ptr?.() ?? 0;
  const childMemfsDataBytes = childKernel.kernel_memfs_data_bytes?.() ?? 0;
  const childPreCompleteDebug =
    `memory=${childMemory.buffer.byteLength}`
    + `:heap=${childKernel.kernel_heap_ptr?.() ?? 0}`
    + `:memfsData=${childMemfsDataPtr}+${childMemfsDataBytes}`
    + `:process=0x${stagedExecveHandoff.processPtr.toString(16)}`
    + `:path=0x${stagedExecveHandoff.pathPtr.toString(16)}`
    + `:argv=0x${stagedExecveHandoff.argvPtr.toString(16)}`
    + `:envp=0x${stagedExecveHandoff.envpPtr.toString(16)}`
    + `:cwd_ino=${childKernel.process_cwd_ino(childVmPtr)}`
    + `:policy=${guestMemoryPolicyLimit}`
    + `:maxPages=${processMaxMemoryPages}`;
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-pre-complete:${childPid}:${childPreCompleteDebug}`,
  );
  let execRet: number;
  try {
    execRet = await completeExecveHandoffWithBackingRetry(
      deps,
      handle,
      childPid,
      childKernel,
      childVmPtr,
    );
  } catch (error) {
    const debugStage = childKernel.debug_execve_stage?.();
    const debugValue0 = childKernel.debug_execve_value0?.();
    const debugValue1 = childKernel.debug_execve_value1?.();
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    await rollbackParentAfterHandoffFailure();
    deps.appendWorkerDebugLog(
      handle,
      `vfork-execve:handoff-trap:${childPid}:${detail}:stage=${debugStage ?? "n/a"}`
      + `:value0=${debugValue0 ?? "n/a"}:value1=${debugValue1 ?? "n/a"}`
      + `:${childPreCompleteDebug}`,
    );
    setProcessLastDebugMessage(
      handle,
      `vfork-execve:handoff-trap child=${childPid} detail=${detail}`
        + ` stage=${debugStage ?? "n/a"} value0=${debugValue0 ?? "n/a"} value1=${debugValue1 ?? "n/a"}`
        + ` ${childPreCompleteDebug}`,
    );
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
      + `:value0=${debugValue0 ?? "n/a"}:value1=${debugValue1 ?? "n/a"}`
      + `:processCreateLastError=${childKernel.process_create_last_error?.() ?? "n/a"}`,
    );
    setProcessLastDebugMessage(
      handle,
      `vfork-execve:handoff-failed child=${childPid} ret=${execRet}`
        + ` stage=${debugStage ?? "n/a"} value0=${debugValue0 ?? "n/a"} value1=${debugValue1 ?? "n/a"}`
        + ` processCreateLastError=${childKernel.process_create_last_error?.() ?? "n/a"}`,
    );
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
    guestMemoryPolicyLimitBytes: guestMemoryPolicyLimit,
    maxMemoryPages: processMaxMemoryPages,
    debugCommand: execveGuestPayload.argv[0]
      ? decodeExecveBytes(execveGuestPayload.argv[0])
      : decodeExecveBytes(execveGuestPayload.path),
    workerDebugLabel: "spawn:vfork-execve",
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
  setGuestMemoryPolicyLimit(
    childHandle.kernel,
    childHandle.vmPtr,
    guestMemoryPolicyLimit,
  );
  const childExecFdEntries = snapshotProcessFdEntries(childHandle, childVmPtr);
  const parentAuthorityFdEntries = fdAuthorityEntriesChangedBetweenSnapshots(
    parentResumeFdEntries,
    childExecFdEntries,
  );
  const childAuthorityFdEntries = fdAuthorityEntriesChangedBetweenSnapshots(
    parentResumeFdEntries,
    childExecFdEntries,
  );
  deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-execve");
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-exec-fds:${childPid}:${formatOpenFdSummary(childHandle)}`,
  );
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-kernel-ready:${childPid}`);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-cwd-ready:${childPid}:${childKernel.process_cwd_ino(childVmPtr)}`,
  );
  setProcessLastDebugMessage(handle, `vfork-execve:child-kernel-ready child=${childPid}`);
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-register:start:${childPid}`);
  await deps.registerProcessWithKernelWorker(childHandle, {
    seedMemfsMode: "none",
    pipeSeedMode: "process",
  });
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-register:done:${childPid}`);
  deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-register");
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:resume-parent:start:${childPid}`);
  await deps.resumeVforkExecveParentViaKernelWorker(handle, parentThread.vmPtr, childPid);
  restoreLocalOfdSlots(handle, parentResumeOfdSlots);
  restoreLocalPipeSlots(handle, parentResumePipeSlots);
  alignPipeOfdGenerationOffsets(handle, collectProcessHeldOfds(handle));
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
  publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);

  reconcileInheritedSharedOfdsAfterVforkExecve(handle, inheritedSharedOfdIds);
  adoptKernelShare(handle, childHandle);
  trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
  const sharedOfdIds = activeKernelShareOfdIds(handle);
  const childHeldOfdIds = collectProcessHeldOfds(childHandle);
  const rehydratedSharedOfdIds = sharedOfdIds.filter((ofdId) => !childHeldOfdIds.has(ofdId));
  const procSelfExeTarget = execveGuestPayload.path.length > 0
    && execveGuestPayload.path[execveGuestPayload.path.length - 1] === 0
    ? execveGuestPayload.path.slice(0, -1)
    : execveGuestPayload.path;
  assertVforkParentState(handle, parentThread.vmPtr, "parent-post-child-register");
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-registered:${childPid}`);
  setProcessLastDebugMessage(handle, `vfork-execve:child-registered child=${childPid}`);
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-sync:start:${childPid}`);
  trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
  await deps.syncForkSharedStateViaKernelWorker(childHandle, kernelShareOtherMembers(childHandle), {
    cause: { kind: "fork-child-ready", childPid },
    includeMemfs: false,
    trackHeldOfds: false,
    ofdIds: [...childHeldOfdIds],
  });
  trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-sync:done:${childPid}`);
  assertVforkParentState(handle, parentThread.vmPtr, "parent-post-child-sync");
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-synced:${childPid}`);
  setProcessLastDebugMessage(handle, `vfork-execve:child-synced child=${childPid}`);
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
  setProcessLastDebugMessage(handle, `vfork-execve:child-rehydrated child=${childPid}`);
  restoreLocalOfdSlots(childHandle, preservedOfdSlots);
  restoreLocalPipeSlots(childHandle, preservedPipeSlots);
  restoreProcessFdEntries(childHandle, childVmPtr, childExecFdEntries);
  alignPipeOfdGenerationOffsets(childHandle, collectProcessHeldOfds(childHandle));
  trackVforkExecveChildSharedOfds(childHandle, childExecFdEntries);
  reconcileForkSharedOfdRefcounts(childHandle, activeKernelShareOfdIds(childHandle));
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `vfork-execve:child-fds-restored:${childPid}:${formatOpenFdSummary(childHandle)}`,
  );
  deps.assertProcessStandardFdIntegrity(childHandle, "vfork-child-post-rehydrate");
  deps.appendRuntimeLifecycleEvent(handle.runtimeId, `vfork-execve:child-lifecycle:start:${childPid}`);
  await deps.initializeProcessLifecycleViaKernelWorker(
    childHandle,
    childVmPtr,
    childLifecycle.ppid,
    childLifecycle.nextPid,
    childLifecycle.pgid,
    childLifecycle.sid,
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
  setProcessInheritedParentOutput(
    childHandle,
    [1, 2].every((fd) => {
      const childOfdId = childHandle.kernel.process_fd_ofd_id(childVmPtr, fd);
      const parentOfdId = handle.kernel.process_fd_ofd_id(parentThread.vmPtr, fd);
      return childOfdId > 0 && childOfdId === parentOfdId;
    }),
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
  setProcessLastDebugMessage(handle, `vfork-execve:child-visible child=${childPid}`);
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `spawn:${childPid}:${JSON.stringify(processDebugCommand(childHandle))}`,
  );
  assertVforkParentState(handle, parentThread.vmPtr, "parent-pre-enqueue");
  transferVforkExecveWorkers(
    deps,
    handle,
    childHandle,
    parentWorker,
    childWorker,
    parentThread.vmPtr,
    parentAuthorityFdEntries,
    childAuthorityFdEntries,
  );
  } finally {
    finishPendingChildSpawnRecord(handle);
    deps.notifyForkProgress(handle);
  }
}
