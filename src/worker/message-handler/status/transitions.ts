import { readKernelRuntimeState } from "../../../abi.js";
import { publishKernelRuntimeState, wakeRuntimeBridge } from "../../../bridge/runtime-state.js";
import {
  acquireWorker,
  releaseWorker,
} from "../../pool.js";
import { ensureMemoryCapacity, kernelScratchBase } from "../../kernel-scratch.js";
import { kernelShareOtherMembers } from "../../kernel-share.js";
import { collectLiveThreadVmPtrs, processHasLiveThreads } from "../../live-threads.js";
import { assertHandleWorkerProcessIdentity } from "../../process-identity.js";
import { blockedStatusUsesForkSharedStateSync } from "../../sync.js";
import type {
  BlockedWorkerState,
  ProcessHandle,
  ThreadWorkerState,
} from "../../types.js";
import type {
  ThreadWorkerStatusMessage,
  WorkerMessageHandlerDeps,
} from "../types.js";
import { resolveWorkerMessageHandlerSyncEffects } from "../sync-effects.js";
import type { KernelRpcResumeOutcome } from "../../kernel-rpc/outcomes.js";
import { createWorkerOwnershipGuard } from "../../ownership/process-state.js";
import {
  refreshPipeControlFromKernelShare,
  shouldRefreshPipeStateBeforeFsResume,
  snapshotProcessPipeSlotsForThreadStep,
} from "../../ownership/pipe-control.js";
import { restoreRuntimeNetworkSidecarForHandle } from "../../ownership/socket-control.js";
import { formatCompactDebugSuffix } from "../../diagnostics/format.js";
import {
  pollSupervisorFdControlDiagnostic,
  pollSupervisorFsControlDiagnostic,
  pollSupervisorSignalControlDiagnostic,
  readSupervisorFaultExitDecision,
  supervisorFaultExitKindProcessExit,
} from "../../supervisor-shadow.js";

const CLONE_SPAWN_DEBUG_ENABLED = ((globalThis as {
  process?: { env?: Record<string, string | undefined> };
}).process?.env?.TIDEMARK_CLONE_SPAWN_DEBUG ?? "") === "1";
const SUPERVISOR_HOST_OP_RECORD_BYTES = 64;
const SUPERVISOR_HOST_OP_RECORD_MAGIC = 0x4f48_5354;
const SUPERVISOR_HOST_OP_KIND_SPAWN_THREAD_WORKER = 1;
const SUPERVISOR_HOST_OP_KIND_ARM_HOST_TIMER = 12;
const SUPERVISOR_HOST_OP_SCRATCH_OFFSET = 0x2c000;
const SUPERVISOR_HOST_COMPLETION_RECORD_BYTES = 64;
const SUPERVISOR_HOST_COMPLETION_RECORD_MAGIC = 0x4348_5354;
const SUPERVISOR_HOST_COMPLETION_OUTCOME_SUCCESS = 1;
const SUPERVISOR_HOST_COMPLETION_OUTCOME_FAILURE = 2;
const SUPERVISOR_HOST_COMPLETION_SCRATCH_OFFSET = 0x2d000;

const cloneSpawnAsyncTokens = new WeakMap<ThreadWorkerState, number>();

interface SupervisorHostOpRecord {
  kind: number;
  seq: bigint;
  parentVmPtr: number;
  childVmPtr: number;
  tid: number;
  processPtr: number;
  status: number;
  flags: bigint;
  childTidPtr: bigint;
}

type SupervisorCloneSpawnHostOp = SupervisorHostOpRecord;

interface StatusTransitionArgs {
  handle: ProcessHandle;
  worker: Worker;
  thread: ThreadWorkerState;
  msg: ThreadWorkerStatusMessage;
  status: number;
  wroteToPipe: boolean;
  drainedPipe: boolean;
  completedSyncEffects: number;
}

const VFORK_CHILD_FAULT_DEBUG_KEYS = [
  "pc",
  "pc_insn",
  "pc_vma",
  "pc_vma_prev",
  "pc_vma_next",
  "ra",
  "sp",
  "s11",
  "syscall",
  "syscall_a7",
  "syscall_a0",
  "syscall_a1",
  "syscall_a2",
  "syscall_a3",
  "syscall_a4",
  "syscall_a5",
  "process_pid_field",
  "process_thread_count",
  "fault_pc",
  "fault_insn",
  "fault_vma",
  "fault_vma_prev",
  "fault_vma_next",
  "fault_addr",
  "fault_signal",
  "fault_code",
  "mem_brk_base",
  "mem_brk_current",
] as const;

function formatProcessThreadSlots(handle: ProcessHandle, vmPtr: number): string {
  const slots: string[] = [];
  for (let slot = 0; slot < handle.kernel.abi_max_threads(); slot += 1) {
    const tid = handle.kernel.process_thread_tid?.(vmPtr, slot) ?? 0;
    const state = handle.kernel.process_thread_state?.(vmPtr, slot) ?? 0;
    const threadVmPtr = handle.kernel.process_thread_vm_ptr?.(vmPtr, slot) ?? 0;
    if (tid === 0 && state === 0 && threadVmPtr === 0) {
      continue;
    }
    slots.push(`${slot}:${tid}/${state}/0x${threadVmPtr.toString(16)}`);
  }
  return slots.join(",");
}

function formatFaultThreadSummary(handle: ProcessHandle, label: string, vmPtr: number): string {
  return `${label}=0x${vmPtr.toString(16)}`
    + `/status=${handle.kernel.thread_status?.(vmPtr) ?? -1}`
    + `/exit=${handle.kernel.thread_exit_code?.(vmPtr) ?? 0}:${handle.kernel.thread_term_signal?.(vmPtr) ?? 0}`
    + `/sig=${handle.kernel.thread_fault_signal?.(vmPtr) ?? 0}`
    + `/kind=${handle.kernel.thread_fault_kind?.(vmPtr) ?? 0}`
    + `/pc=0x${(handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n).toString(16)}`
    + `/addr=0x${(handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n).toString(16)}`;
}

function faultExitVmPriority(handle: ProcessHandle, vmPtr: number): number {
  if ((handle.kernel.thread_fault_signal?.(vmPtr) ?? 0) !== 0) {
    return 4;
  }
  if ((handle.kernel.thread_fault_kind?.(vmPtr) ?? 0) !== 0) {
    return 3;
  }
  if (
    (handle.kernel.thread_fault_pc?.(vmPtr) ?? 0n) !== 0n
    || (handle.kernel.thread_fault_addr?.(vmPtr) ?? 0n) !== 0n
  ) {
    return 2;
  }
  if ((handle.kernel.thread_term_signal?.(vmPtr) ?? 0) !== 0) {
    return 1;
  }
  return 0;
}

function collectFaultExitVmCandidates(
  handle: ProcessHandle,
  primaryVmPtr: number,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): Set<number> {
  const candidates = new Set<number>();
  if (primaryVmPtr !== 0) {
    candidates.add(primaryVmPtr);
    for (const liveVmPtr of collectLiveThreadVmPtrs(handle.kernel, primaryVmPtr)) {
      candidates.add(liveVmPtr);
    }
  }
  if (handle.vmPtr !== 0) {
    candidates.add(handle.vmPtr);
    for (const liveVmPtr of collectLiveThreadVmPtrs(handle.kernel, handle.vmPtr)) {
      candidates.add(liveVmPtr);
    }
  }
  for (const worker of handle.workers) {
    const thread = threadWorkerStates.get(worker);
    if (thread?.vmPtr) {
      candidates.add(thread.vmPtr);
    }
  }
  return candidates;
}

function selectFaultExitVmPtr(
  handle: ProcessHandle,
  primaryVmPtr: number,
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
): number {
  let selectedVmPtr = primaryVmPtr;
  let selectedPriority = faultExitVmPriority(handle, selectedVmPtr);
  for (const candidate of collectFaultExitVmCandidates(handle, primaryVmPtr, threadWorkerStates)) {
    const priority = faultExitVmPriority(handle, candidate);
    if (priority > selectedPriority) {
      selectedVmPtr = candidate;
      selectedPriority = priority;
    }
  }
  return selectedVmPtr;
}

function supervisorHostOpScratchBase(handle: ProcessHandle): number {
  return kernelScratchBase(handle.kernel) + SUPERVISOR_HOST_OP_SCRATCH_OFFSET;
}

function supervisorHostOpKindSpawnThreadWorker(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_op_kind_spawn_thread_worker?.()
    ?? SUPERVISOR_HOST_OP_KIND_SPAWN_THREAD_WORKER;
}

function supervisorHostOpKindArmHostTimer(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_op_kind_arm_host_timer?.()
    ?? SUPERVISOR_HOST_OP_KIND_ARM_HOST_TIMER;
}

function decodeSupervisorHostOpRecord(
  handle: ProcessHandle,
  view: DataView,
  context: string,
): SupervisorHostOpRecord {
  if (view.byteLength < SUPERVISOR_HOST_OP_RECORD_BYTES) {
    throw new Error(`${context} record too short: ${view.byteLength}`);
  }
  const magic = view.getUint32(0, true);
  const version = view.getUint32(4, true);
  const bytes = view.getUint32(8, true);
  const kind = view.getUint32(12, true);
  if (magic !== SUPERVISOR_HOST_OP_RECORD_MAGIC) {
    throw new Error(`${context} bad magic: 0x${magic.toString(16)}`);
  }
  const expectedVersion = handle.kernel.supervisor_abi_version?.() ?? version;
  if (version !== expectedVersion) {
    throw new Error(`${context} bad version: ${version}`);
  }
  if (bytes < SUPERVISOR_HOST_OP_RECORD_BYTES || bytes > view.byteLength) {
    throw new Error(`${context} bad size: ${bytes}`);
  }
  const record: SupervisorCloneSpawnHostOp = {
    kind,
    seq: view.getBigUint64(16, true),
    parentVmPtr: view.getUint32(24, true),
    childVmPtr: view.getUint32(28, true),
    tid: view.getUint32(32, true),
    processPtr: view.getUint32(36, true),
    status: view.getUint32(40, true),
    flags: view.getBigUint64(48, true),
    childTidPtr: view.getBigUint64(56, true),
  };
  return record;
}

function decodeSupervisorCloneSpawnHostOp(
  handle: ProcessHandle,
  view: DataView,
  parentVmPtr: number,
): SupervisorCloneSpawnHostOp {
  const record = decodeSupervisorHostOpRecord(handle, view, "supervisor clone spawn HostOp");
  if (record.kind !== supervisorHostOpKindSpawnThreadWorker(handle)) {
    throw new Error(`supervisor clone spawn HostOp bad kind: ${record.kind}`);
  }
  if (record.parentVmPtr !== parentVmPtr) {
    throw new Error(
      `supervisor clone spawn HostOp parent mismatch: `
        + `0x${record.parentVmPtr.toString(16)} != 0x${parentVmPtr.toString(16)}`,
    );
  }
  if (record.childVmPtr === 0) {
    throw new Error(`supervisor clone spawn HostOp missing child for parent=0x${parentVmPtr.toString(16)}`);
  }
  if (record.status !== handle.kernel.abi_status_ok()) {
    throw new Error(`supervisor clone spawn HostOp child status not OK: ${record.status}`);
  }
  return record;
}

function decodeSupervisorArmHostTimerOp(
  handle: ProcessHandle,
  view: DataView,
  vmPtr: number,
): SupervisorHostOpRecord {
  const record = decodeSupervisorHostOpRecord(handle, view, "supervisor arm host timer HostOp");
  if (record.kind !== supervisorHostOpKindArmHostTimer(handle)) {
    throw new Error(`supervisor arm host timer HostOp bad kind: ${record.kind}`);
  }
  if (record.parentVmPtr !== vmPtr) {
    throw new Error(
      `supervisor arm host timer HostOp vm mismatch: `
        + `0x${record.parentVmPtr.toString(16)} != 0x${vmPtr.toString(16)}`,
    );
  }
  return record;
}

function drainSupervisorCloneSpawnHostOp(
  handle: ProcessHandle,
  parentVmPtr: number,
): SupervisorCloneSpawnHostOp | null | undefined {
  const nextHostOp = handle.kernel.supervisor_next_host_op;
  const recordBytesExport = handle.kernel.supervisor_host_op_record_bytes;
  if (nextHostOp === undefined || recordBytesExport === undefined) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_HOST_OP_RECORD_BYTES) {
    throw new Error(`supervisor clone spawn HostOp record bytes too small: ${recordBytes}`);
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = supervisorHostOpScratchBase(handle);
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = nextHostOp(scratch, recordBytes);
  if (result === 0) {
    return null;
  }
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor clone spawn HostOp poll failed: ${result}`);
  }
  return decodeSupervisorCloneSpawnHostOp(
    handle,
    new DataView(memory.buffer, scratch, result),
    parentVmPtr,
  );
}

function drainSupervisorArmHostTimerHostOp(
  handle: ProcessHandle,
  vmPtr: number,
): SupervisorHostOpRecord | null | undefined {
  const nextHostOp = handle.kernel.supervisor_next_host_op;
  const recordBytesExport = handle.kernel.supervisor_host_op_record_bytes;
  if (nextHostOp === undefined || recordBytesExport === undefined) {
    return undefined;
  }
  const recordBytes = recordBytesExport();
  if (recordBytes < SUPERVISOR_HOST_OP_RECORD_BYTES) {
    throw new Error(`supervisor arm host timer HostOp record bytes too small: ${recordBytes}`);
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = supervisorHostOpScratchBase(handle);
  ensureMemoryCapacity(memory, scratch + recordBytes);
  const result = nextHostOp(scratch, recordBytes);
  if (result === 0) {
    return null;
  }
  if (result < 0 || result > recordBytes) {
    throw new Error(`supervisor arm host timer HostOp poll failed: ${result}`);
  }
  return decodeSupervisorArmHostTimerOp(
    handle,
    new DataView(memory.buffer, scratch, result),
    vmPtr,
  );
}

function supervisorHostCompletionOutcomeSuccess(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_completion_outcome_success?.()
    ?? SUPERVISOR_HOST_COMPLETION_OUTCOME_SUCCESS;
}

function supervisorHostCompletionOutcomeFailure(handle: ProcessHandle): number {
  return handle.kernel.supervisor_host_completion_outcome_failure?.()
    ?? SUPERVISOR_HOST_COMPLETION_OUTCOME_FAILURE;
}

function submitSupervisorHostOpCompletion(
  handle: ProcessHandle,
  record: SupervisorHostOpRecord,
  outcome: number,
  detail: number,
): number | undefined {
  const completeHostOp = handle.kernel.supervisor_complete_host_op;
  if (completeHostOp === undefined) {
    return undefined;
  }
  const memory = handle.kernel.memory ?? handle.memory;
  const scratch = kernelScratchBase(handle.kernel) + SUPERVISOR_HOST_COMPLETION_SCRATCH_OFFSET;
  ensureMemoryCapacity(memory, scratch + SUPERVISOR_HOST_COMPLETION_RECORD_BYTES);
  const view = new DataView(memory.buffer, scratch, SUPERVISOR_HOST_COMPLETION_RECORD_BYTES);
  view.setUint32(0, SUPERVISOR_HOST_COMPLETION_RECORD_MAGIC, true);
  view.setUint32(4, handle.kernel.supervisor_abi_version?.() ?? 1, true);
  view.setUint32(8, SUPERVISOR_HOST_COMPLETION_RECORD_BYTES, true);
  view.setUint32(12, record.kind, true);
  view.setBigUint64(16, record.seq, true);
  view.setUint32(24, record.parentVmPtr, true);
  view.setUint32(28, record.childVmPtr, true);
  view.setUint32(32, record.tid, true);
  view.setUint32(36, outcome, true);
  view.setUint32(40, detail, true);
  view.setUint32(44, 0, true);
  view.setBigUint64(48, record.flags, true);
  view.setBigUint64(56, record.childTidPtr, true);
  return completeHostOp(scratch, SUPERVISOR_HOST_COMPLETION_RECORD_BYTES);
}

function armSupervisorHostTimer(
  handle: ProcessHandle,
  record: SupervisorHostOpRecord,
  deps: Pick<
    WorkerMessageHandlerDeps,
    "appendRuntimeLifecycleEvent" | "appendWorkerDebugLog" | "failProcess" | "pumpProcess"
  >,
): void {
  const completeFutexWait = handle.kernel.supervisor_complete_futex_wait;
  const timedOut = handle.kernel.supervisor_futex_wait_completion_timed_out;
  if (completeFutexWait === undefined || timedOut === undefined) {
    throw new Error("kernel supervisor ABI missing futex timeout completion exports");
  }
  const delayMs = Number(
    record.flags > BigInt(0x7fff_ffff)
      ? 0x7fff_ffffn
      : record.flags,
  );
  deps.appendRuntimeLifecycleEvent(
    handle.runtimeId,
    `supervisor-host-timer-arm:${handle.pid}:vm=0x${record.parentVmPtr.toString(16)}`
      + `:seq=${record.seq.toString()}:delay=${delayMs}`,
  );
  globalThis.setTimeout(() => {
    try {
      const status = completeFutexWait(record.parentVmPtr, timedOut());
      submitSupervisorHostOpCompletion(
        handle,
        record,
        supervisorHostCompletionOutcomeSuccess(handle),
        status,
      );
      deps.appendWorkerDebugLog(
        handle,
        `supervisor-host-timer-fired:vm=0x${record.parentVmPtr.toString(16)}`
          + `:seq=${record.seq.toString()}:status=${status}`,
      );
      deps.pumpProcess(handle);
    } catch (error) {
      submitSupervisorHostOpCompletion(
        handle,
        record,
        supervisorHostCompletionOutcomeFailure(handle),
        0,
      );
      deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
    }
  }, delayMs);
}

function beginStatusTransitionRpc(blocked: BlockedWorkerState): number {
  blocked.rpcPending = true;
  blocked.rpcGeneration += 1;
  return blocked.rpcGeneration;
}

function finishStatusTransitionRpc(
  handle: ProcessHandle,
  worker: Worker,
  vmPtr: number,
  rpcGeneration: number,
): BlockedWorkerState | null {
  const latestBlocked = handle.blockedWorkers.get(worker);
  if (!latestBlocked || latestBlocked.vmPtr !== vmPtr) {
    return null;
  }
  if (latestBlocked.rpcGeneration === rpcGeneration) {
    latestBlocked.rpcPending = false;
  }
  return latestBlocked;
}

function hasPendingBlockedRpc(handle: ProcessHandle): boolean {
  for (const blocked of handle.blockedWorkers.values()) {
    if (blocked.rpcPending) {
      return true;
    }
  }
  return false;
}

function hasBlockedIoPressure(handle: ProcessHandle): boolean {
  for (const blocked of handle.blockedWorkers.values()) {
    const status = blocked.status;
    if (
      status === handle.kernel.abi_status_epoll_wait()
      || status === handle.kernel.abi_status_fs()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_wait4()
      || status === handle.kernel.abi_status_flock_wait()
      || status === handle.kernel.abi_status_connect()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
    ) {
      return true;
    }
  }
  return false;
}

export function createWorkerStatusTransitionHandler(deps: WorkerMessageHandlerDeps) {
  const syncEffects = resolveWorkerMessageHandlerSyncEffects(deps);
  function consumeKernelRpcResumeOutcome(
    handle: ProcessHandle,
    worker: Worker,
    vmPtr: number,
    outcome: KernelRpcResumeOutcome,
  ): void {
    switch (outcome.kind) {
      case "completed":
      case "queued-in-worker":
      case "process-gone":
      case "stale":
        return;
      case "blocked": {
        const blocked = handle.blockedWorkers.get(worker);
        if (blocked && blocked.vmPtr === vmPtr) {
          blocked.status = outcome.status;
        }
        return;
      }
      default: {
        const exhaustive: never = outcome;
        throw new Error(`unhandled kernel-rpc outcome: ${JSON.stringify(exhaustive)}`);
      }
    }
  }

  return function handleStatusTransition({
    handle,
    worker,
    thread,
    msg,
    status,
    wroteToPipe,
    drainedPipe,
    completedSyncEffects,
  }: StatusTransitionArgs): void {
    const vforkWaitStatus = handle.kernel.abi_status_vfork_wait?.();
    const execveSpawnStatus = handle.kernel.abi_status_execve_spawn?.();
    const assertHandleProcessIdentity = (context: string): void => {
      assertHandleWorkerProcessIdentity(handle, deps.threadWorkerStates, context, {
        status,
        threadVmPtr: thread.vmPtr,
        workerLogTail: handle.workerDebugLog.slice(-16),
        skipVmPtrs: [handle.vforkSnapshot?.vmPtr ?? 0].filter((vmPtr) => vmPtr !== 0),
      });
    };
    const workerStillOwnedByHandle = createWorkerOwnershipGuard(
      handle,
      worker,
      thread,
      deps.threadWorkerStates,
    );
    if (
      status !== vforkWaitStatus
      && status !== execveSpawnStatus
    ) {
      assertHandleProcessIdentity("handleStatusTransition:start");
    }
    const supervisorFdControlDiagnostic = pollSupervisorFdControlDiagnostic(handle);
    if (supervisorFdControlDiagnostic !== undefined) {
      deps.appendWorkerDebugLog(handle, supervisorFdControlDiagnostic);
    }
    const supervisorFsControlDiagnostic = pollSupervisorFsControlDiagnostic(handle);
    if (supervisorFsControlDiagnostic !== undefined) {
      deps.appendWorkerDebugLog(handle, supervisorFsControlDiagnostic);
    }
    const supervisorSignalControlDiagnostic = pollSupervisorSignalControlDiagnostic(handle);
    if (supervisorSignalControlDiagnostic !== undefined) {
      deps.appendWorkerDebugLog(handle, supervisorSignalControlDiagnostic);
    }

    const enqueueWorkerAfterOk = (): void => {
      if (hasPendingBlockedRpc(handle)) {
        setTimeout(() => {
          if (!workerStillOwnedByHandle()) {
            deps.pumpProcess(handle);
            return;
          }
          enqueueWorkerAfterOk();
        }, 1);
        return;
      }
      deps.enqueueWorker(handle, worker);
    };

    const syncSocketAfterNetworkPump = (networkChanged: boolean): Promise<void> => {
      if (!networkChanged) {
        return Promise.resolve();
      }
      return deps.requestSocketStateSync(handle, true);
    };

    if (status === handle.kernel.abi_status_ok()) {
      const shouldSyncPipeState = wroteToPipe
        || drainedPipe
        || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
      const shouldSyncSocketState = (completedSyncEffects & syncEffects.syncEffectSocketState) !== 0;
      const shouldSyncLifecycle = (completedSyncEffects & syncEffects.syncEffectProcessLifecycle) !== 0;
      const shouldWakeSignalWaiters = (completedSyncEffects & syncEffects.syncEffectSignalWaiters) !== 0;
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && (shouldSyncPipeState || shouldSyncSocketState || shouldSyncLifecycle)
      ) {
        deps.appendWorkerDebugLog(handle, `ok-sync-start:0x${thread.vmPtr.toString(16)}`);
        thread.ownerAsyncStatus = status;
        const syncPipeState = shouldSyncPipeState
          ? deps.requestPipeStateSync(handle, true)
          : Promise.resolve();
        const syncSocketState = shouldSyncSocketState
          ? deps.requestSocketStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-pipe-sync");
          })
          .then(() => syncSocketState)
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-socket-sync");
          })
          .then(() => (shouldSyncLifecycle
            ? deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
            : Promise.resolve()))
          .then(() => {
            assertHandleProcessIdentity("handleStatusTransition:after-lifecycle-sync");
            if (!workerStillOwnedByHandle()) {
              return;
            }
            if (shouldSyncLifecycle) {
              deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
              assertHandleProcessIdentity("handleStatusTransition:after-prune-detached");
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-done:0x${thread.vmPtr.toString(16)}`);
            return handle.networkConnections.size > 0
              ? deps.pumpNetworkConnections(handle, thread.vmPtr)
              : Promise.resolve(false);
          })
          .then((networkChanged) => syncSocketAfterNetworkPump(networkChanged ?? false))
          .then(() => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              deps.pumpProcess(handle);
              return;
            }
            if (shouldWakeSignalWaiters) {
              wakeRuntimeBridge(handle.runtimeBridge);
            }
            deps.kickBlockedWorkers(handle);
            if (shouldSyncPipeState) {
              for (const member of kernelShareOtherMembers(handle)) {
                deps.kickBlockedWorkers(member);
                deps.pumpProcess(member);
              }
            }
            assertHandleProcessIdentity("handleStatusTransition:before-enqueue");
            enqueueWorkerAfterOk();
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              deps.pumpProcess(handle);
              return;
            }
            deps.appendWorkerDebugLog(handle, `ok-sync-err:0x${thread.vmPtr.toString(16)}`);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (shouldSyncLifecycle) {
        deps.pruneDetachedProcessWorkers(handle, thread.vmPtr, worker);
      }
      if (shouldWakeSignalWaiters) {
        wakeRuntimeBridge(handle.runtimeBridge);
      }
      if (handle.networkConnections.size > 0) {
        void deps.pumpNetworkConnections(handle, thread.vmPtr)
          .then((networkChanged) => syncSocketAfterNetworkPump(networkChanged))
          .then(() => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.kickBlockedWorkers(handle);
            enqueueWorkerAfterOk();
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      deps.kickBlockedWorkers(handle);
      enqueueWorkerAfterOk();
      return;
    }

    if (status === handle.kernel.abi_status_fault()) {
      if (
        handle.vforkSnapshot !== null
        && handle.vforkWorker === worker
        && handle.vforkSnapshot.vmPtr === thread.vmPtr
      ) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-child-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:child=${handle.vforkSnapshot.childPid ?? "none"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid?.() ?? "n/a"}`
            + `:exit=${msg.exitCode}:${msg.termSignal}`
            + formatCompactDebugSuffix(msg.debug, VFORK_CHILD_FAULT_DEBUG_KEYS),
        );
      }
      if (deps.resumeVforkParentAfterChildExit(handle, worker, thread.vmPtr, msg.exitCode, msg.termSignal)) {
        return;
      }
      if (msg.debug) {
        deps.appendWorkerDebugLog(handle, `fault_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`);
      }
      const supervisorExitDecision = readSupervisorFaultExitDecision(handle, thread.vmPtr);
      const currentPid = handle.kernel.process_current_pid(thread.vmPtr);
      const faultStateLooksStale = currentPid === 0 || currentPid > 0xffff;
      const remainingThreads = supervisorExitDecision?.remainingThreads
        ?? (faultStateLooksStale ? 0 : handle.kernel.thread_process_thread_count(thread.vmPtr));
      const hasLiveThreads = supervisorExitDecision !== undefined
        ? supervisorExitDecision.liveThreads !== 0
        : !faultStateLooksStale
          && remainingThreads !== 0
          && processHasLiveThreads(handle.kernel, thread.vmPtr, thread.vmPtr);
      const shouldExitProcess = supervisorExitDecision !== undefined
        ? supervisorExitDecision.kind === supervisorFaultExitKindProcessExit(handle.kernel)
        : remainingThreads <= 1 || !hasLiveThreads;
      const exitCode = supervisorExitDecision?.exitCode ?? msg.exitCode;
      const termSignal = supervisorExitDecision?.termSignal ?? msg.termSignal;
      const faultProcessPtr = handle.kernel.thread_process_ptr(thread.vmPtr);
      const ownerPid = handle.kernel.process_current_pid(handle.vmPtr);
      const ownerProcessPtr = handle.kernel.thread_process_ptr(handle.vmPtr);
      const ownerThreadCount = handle.kernel.thread_process_thread_count(handle.vmPtr);
      const processTableDebug = `:fault_pid=${currentPid}:fault_process=0x${faultProcessPtr.toString(16)}`
        + `:fault_count=${remainingThreads}`
        + `:owner_pid=${ownerPid}:owner_process=0x${ownerProcessPtr.toString(16)}`
        + `:owner_count=${ownerThreadCount}:owner_threads=[${formatProcessThreadSlots(handle, handle.vmPtr)}]`;
      deps.appendWorkerDebugLog(handle, `fault:0x${thread.vmPtr.toString(16)} remaining=${remainingThreads}`);
      if (shouldExitProcess) {
        const exitVmPtr = supervisorExitDecision?.exitVmPtr
          ?? selectFaultExitVmPtr(handle, thread.vmPtr, deps.threadWorkerStates);
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `complete-exit-request:transitions-fault:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:exitVm=0x${exitVmPtr.toString(16)}`
            + `:priority=${supervisorExitDecision === undefined ? faultExitVmPriority(handle, exitVmPtr) : "supervisor"}`
            + `:status=${status}:msg=${msg.status}:exit=${exitCode}:${termSignal}`
            + `:current=${handle.kernel.thread_status?.(thread.vmPtr) ?? -1}:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
            + processTableDebug
            + `:${formatFaultThreadSummary(handle, "faultVm", thread.vmPtr)}`
            + `:${formatFaultThreadSummary(handle, "exitVm", exitVmPtr)}`,
        );
        void deps.completeProcessExit(handle, exitCode, termSignal, exitVmPtr).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      } else {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `fault-worker-delete:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:status=${status}:msg=${msg.status}:exit=${msg.exitCode}:${msg.termSignal}`
            + `:current=${handle.kernel.thread_status?.(thread.vmPtr) ?? -1}`
            + `:remaining=${remainingThreads}:live=${hasLiveThreads ? 1 : 0}`
            + processTableDebug
            + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
        );
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        handle.workers.delete(worker);
        deps.threadWorkerStates.delete(worker);
        releaseWorker(worker);
        if (wroteToPipe || drainedPipe || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0) {
          void deps.requestPipeStateSync(handle, true)
            .then(() => {
              deps.kickBlockedWorkers(handle);
              deps.pumpProcess(handle);
            })
            .catch((error) => {
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        deps.kickBlockedWorkers(handle);
        deps.pumpProcess(handle);
      }
      return;
    }

    const shouldSyncPipeState = wroteToPipe
      || drainedPipe
      || (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
    const pendingHostResume = status === handle.kernel.abi_status_fs();
    if (shouldSyncPipeState && !pendingHostResume) {
      void deps.requestPipeStateSync(handle, true).catch((error) => {
        deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
      });
    }
    const networkRelevantStatus = status === handle.kernel.abi_status_connect()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_epoll_wait();
    if (networkRelevantStatus) {
      restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr);
    }

    if (
      handle.networkConnections.size > 0
      && status !== handle.kernel.abi_status_connect()
    ) {
      void deps.pumpNetworkConnections(handle, thread.vmPtr)
        .then(async (changed) => {
          if (!changed) {
            return;
          }
          await deps.requestSocketStateSync(handle, true);
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.kickBlockedWorkers(handle);
          deps.pumpProcess(handle);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
    }

    if (status === handle.kernel.abi_status_sleep()) {
      const sec = handle.kernel.thread_sleep_seconds(thread.vmPtr);
      const nsec = handle.kernel.thread_sleep_nanoseconds(thread.vmPtr);
      if (sec === 0n && nsec === 0n) {
        thread.yieldedReady = true;
        deps.appendWorkerDebugLog(handle, `yield:0x${thread.vmPtr.toString(16)}`);
        const resumeZeroDurationSleep = (): void => {
          handle.kernel.thread_resume_sleep?.(thread.vmPtr);
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        };
        const enqueueAfterYield = (): void => {
          if (hasPendingBlockedRpc(handle)) {
            setTimeout(() => {
              if (workerStillOwnedByHandle()) {
                enqueueAfterYield();
              }
            }, 1);
            return;
          }
          if (hasBlockedIoPressure(handle)) {
            setTimeout(() => {
              if (workerStillOwnedByHandle()) {
                resumeZeroDurationSleep();
                deps.enqueueWorker(handle, worker);
              }
            }, 1);
            return;
          }
          if (handle.readyWorkers.length > 0) {
            deps.pumpProcess(handle);
            setTimeout(() => {
              if (workerStillOwnedByHandle()) {
                resumeZeroDurationSleep();
                deps.enqueueWorker(handle, worker);
              }
            }, 0);
            return;
          }
          deps.enqueueWorker(handle, worker);
        };
        const shouldSyncPipeState = (completedSyncEffects & syncEffects.syncEffectPipeState) !== 0;
        if (handle.kernelWorkerRequest && handle.pid > 0 && shouldSyncPipeState) {
          void deps.requestPipeStateSync(handle, false)
            .then(() => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              resumeZeroDurationSleep();
              deps.kickBlockedWorkers(handle);
              enqueueAfterYield();
            })
            .catch((error) => {
              if (!workerStillOwnedByHandle()) {
                return;
              }
              deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
            });
          return;
        }
        resumeZeroDurationSleep();
        deps.kickBlockedWorkers(handle);
        enqueueAfterYield();
        return;
      }
      thread.yieldedReady = false;
      deps.appendWorkerDebugLog(handle, `sleep:0x${thread.vmPtr.toString(16)}@${sec.toString()}s+${nsec.toString()}ns`);
      deps.blockWorker(handle, worker, status, thread.vmPtr);
      deps.kickBlockedWorkers(handle);
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_fs()) {
      deps.appendWorkerDebugLog(handle, `fs:0x${thread.vmPtr.toString(16)}`);
      if (shouldRefreshPipeStateBeforeFsResume(handle, thread.vmPtr)) {
        thread.ownerAsyncStatus = status;
        void deps.requestPipeStateSync(handle, false)
          .then(() => {
            refreshPipeControlFromKernelShare(handle);
            const pipeSlotsForNextStep = snapshotProcessPipeSlotsForThreadStep(handle, thread.vmPtr);
            if (pipeSlotsForNextStep.length > 0) {
              thread.pendingPipeSlotsForNextStep.push(...pipeSlotsForNextStep);
            }
            if (!workerStillOwnedByHandle()) {
              thread.ownerAsyncStatus = null;
              return;
            }
            const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
            return resumeFs(handle, worker, thread.vmPtr)
              .then((outcome) => {
                if (outcome !== undefined) {
                  consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                }
              })
              .then(() => undefined)
              .finally(() => {
                thread.ownerAsyncStatus = null;
                deps.pumpProcess(handle);
              });
          })
          .catch((error) => {
            thread.ownerAsyncStatus = null;
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (!handle.kernelWorkerRequest || handle.pid <= 0) {
        const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
        void resumeFs(handle, worker, thread.vmPtr)
          .then((outcome) => {
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
          })
          .catch((error) => {
            if (!workerStillOwnedByHandle()) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      thread.ownerAsyncStatus = status;
      const resumeFs = deps.resumeFsViaKernelWorkerOutcome ?? deps.resumeFsViaKernelWorker;
      void resumeFs(handle, worker, thread.vmPtr)
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        })
        .finally(() => {
          thread.ownerAsyncStatus = null;
          deps.pumpProcess(handle);
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    if (status === handle.kernel.abi_status_connect()) {
      const socketTimeoutMs = handle.kernel.thread_socket_wait_timeout === undefined
        ? -1
        : Number(handle.kernel.thread_socket_wait_timeout(thread.vmPtr));
      if (socketTimeoutMs >= 0) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        deps.pumpProcess(handle);
        return;
      }
      if (restoreRuntimeNetworkSidecarForHandle(handle, deps.processHandles, thread.vmPtr)) {
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        const connectResult = deps.resolveNetworkConnect(handle, thread.vmPtr);
        void connectResult
          .then(async (result) => {
            await deps.requestSocketStateSync(handle, false);
            const outcome = await deps.resumeConnectViaKernelWorker(handle, worker, thread.vmPtr, result);
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
            await deps.commitNetworkConnect(handle, thread.vmPtr, result);
          })
          .then(() => {
            deps.kickBlockedWorkers(handle);
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        deps.pumpProcess(handle);
        return;
      }
      if (!handle.kernelWorkerRequest || handle.pid <= 0) {
        void deps.resumeConnectViaKernelWorker(
          handle,
          worker,
          thread.vmPtr,
          BigInt(-handle.kernel.abi_econnrefused()),
        ).then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
        }).catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
        deps.kickBlockedWorkers(handle);
        deps.enqueueWorker(handle, worker);
        return;
      }
      void deps.requestSocketStateSync(handle, false)
        .then(() => deps.resumeConnectViaKernelWorker(
          handle,
          worker,
          thread.vmPtr,
          BigInt(-handle.kernel.abi_econnrefused()),
        ))
        .then((outcome) => {
          if (outcome !== undefined) {
            consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
          }
          deps.kickBlockedWorkers(handle);
          deps.enqueueWorker(handle, worker);
        })
        .catch((error) => {
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      deps.pumpProcess(handle);
      return;
    }

    if (status === handle.kernel.abi_status_clone_spawn()) {
      if (msg.debug) {
        deps.appendWorkerDebugLog(
          handle,
          `clone_spawn_debug:0x${thread.vmPtr.toString(16)}:${JSON.stringify(msg.debug)}`,
        );
      }
      if (CLONE_SPAWN_DEBUG_ENABLED) {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `clone-spawn-enter:${handle.pid}:0x${thread.vmPtr.toString(16)}:status=${status}`,
        );
      }
      let childVmPtr = 0;
      const spawnCloneChild = (): boolean => {
        const hostOp = drainSupervisorCloneSpawnHostOp(handle, thread.vmPtr);
        if (hostOp === undefined) {
          childVmPtr = handle.kernel.thread_spawn_from_clone(thread.vmPtr);
        } else if (hostOp === null) {
          childVmPtr = 0;
        } else {
          childVmPtr = hostOp.childVmPtr;
          deps.appendWorkerDebugLog(
            handle,
            `clone_spawn_hostop:parent=0x${hostOp.parentVmPtr.toString(16)}`
              + `:child=0x${hostOp.childVmPtr.toString(16)}`
              + `:tid=${hostOp.tid}:seq=${hostOp.seq.toString()}`,
          );
        }
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-after-kernel:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
        if (childVmPtr !== 0) {
          handle.kernelState = readKernelRuntimeState(handle.kernel);
          return true;
        }
        const currentStatus = handle.kernel.thread_status?.(thread.vmPtr);
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-zero:${handle.pid}:0x${thread.vmPtr.toString(16)}`
              + `:status=${status}:current=${currentStatus ?? "n/a"}`,
          );
        }
        if (currentStatus !== undefined && currentStatus !== status) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-stale-current:${handle.pid}:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
          );
          deps.appendWorkerDebugLog(
            handle,
            `clone_spawn_stale:0x${thread.vmPtr.toString(16)}:${status}->${currentStatus}`,
          );
          deps.kickBlockedWorkers(handle);
          deps.pumpProcess(handle);
          return false;
        }
        if (hostOp !== undefined) {
          throw new Error(`supervisor clone spawn HostOp missing for pid=${handle.pid}`);
        }
        throw new Error(`thread_spawn_from_clone failed for pid=${handle.pid}`);
      };
      const finishCloneSpawn = (): void => {
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-finish:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
        handle.kernelState = readKernelRuntimeState(handle.kernel);
        deps.assertProcessStandardFdIntegrity(handle, "clone-spawn");
        publishKernelRuntimeState(handle.runtimeBridge, handle.kernelState, true);
        const childWorker = acquireWorker();
        deps.attachThreadWorker(handle, { worker: childWorker, vmPtr: childVmPtr });
        const childThread = deps.threadWorkerStates.get(childWorker);
        if (!childThread) {
          throw new Error(`child thread state missing for pid=${handle.pid} vm=0x${childVmPtr.toString(16)}`);
        }
        thread.ownerAsyncStatus = status;
        childThread.resumeWorkersOnReady.push(worker);
        deps.kickBlockedWorkers(handle);
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-attached:${handle.pid}:parent=0x${thread.vmPtr.toString(16)}`
              + `:child=0x${childVmPtr.toString(16)}:workers=${handle.workers.size}`
              + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
          );
        }
      };

      if (handle.kernelWorkerRequest && handle.pid > 0) {
        thread.ownerAsyncStatus = status;
        const cloneSpawnAsyncToken = (cloneSpawnAsyncTokens.get(thread) ?? 0) + 1;
        cloneSpawnAsyncTokens.set(thread, cloneSpawnAsyncToken);
        const isCurrentCloneSpawnAsync = (): boolean =>
          cloneSpawnAsyncTokens.get(thread) === cloneSpawnAsyncToken
          && (thread.ownerAsyncStatus ?? null) === status;
        if (CLONE_SPAWN_DEBUG_ENABLED) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `clone-spawn-sync-start:${handle.pid}:0x${thread.vmPtr.toString(16)}`
              + `:child=pending`,
          );
        }
        void deps.syncProcessLifecycleViaKernelWorker(handle, thread.vmPtr)
          .then(() => {
            if (CLONE_SPAWN_DEBUG_ENABLED) {
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `clone-spawn-sync-done:${handle.pid}:0x${thread.vmPtr.toString(16)}`
                  + `:owned=${workerStillOwnedByHandle() ? 1 : 0}`
                  + `:threads=[${formatProcessThreadSlots(handle, thread.vmPtr)}]`,
              );
            }
            if (!workerStillOwnedByHandle()) {
              return;
            }
            if (!isCurrentCloneSpawnAsync()) {
              if (CLONE_SPAWN_DEBUG_ENABLED) {
                deps.appendRuntimeLifecycleEvent(
                  handle.runtimeId,
                  `clone-spawn-sync-stale:${handle.pid}:0x${thread.vmPtr.toString(16)}`
                    + `:token=${cloneSpawnAsyncToken}`,
                );
              }
              return;
            }
            if (!spawnCloneChild()) {
              thread.ownerAsyncStatus = null;
              return;
            }
            finishCloneSpawn();
          })
          .catch((error) => {
            const currentCloneSpawnAsync = isCurrentCloneSpawnAsync();
            if (currentCloneSpawnAsync) {
              thread.ownerAsyncStatus = null;
            }
            if (CLONE_SPAWN_DEBUG_ENABLED) {
              deps.appendRuntimeLifecycleEvent(
                handle.runtimeId,
                `clone-spawn-sync-error:${handle.pid}:0x${thread.vmPtr.toString(16)}`
                  + `:${error instanceof Error ? error.message : String(error)}`,
              );
            }
            if (!workerStillOwnedByHandle()) {
              return;
            }
            if (!currentCloneSpawnAsync) {
              return;
            }
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }

      if (!spawnCloneChild()) {
        return;
      }
      finishCloneSpawn();
      return;
    }

    if (status === handle.kernel.abi_status_execve_spawn()) {
      thread.ownerAsyncStatus = status;
      void deps.handleExecveSpawn(handle, worker, thread)
        .finally(() => {
          if (workerStillOwnedByHandle()) {
            thread.ownerAsyncStatus = null;
          }
        })
        .catch((error) => {
          if (!workerStillOwnedByHandle()) {
            return;
          }
          deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
      return;
    }

    if (status === handle.kernel.abi_status_vfork_wait()) {
      if (handle.vforkSnapshot === null) {
        handle.vforkSnapshot = deps.captureVforkSnapshot(handle, thread);
        handle.vforkWorker = worker;
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-wait-snapshot:capture:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:child=${handle.vforkSnapshot.childPid ?? "none"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid()}`,
        );
      } else {
        deps.appendRuntimeLifecycleEvent(
          handle.runtimeId,
          `vfork-wait-snapshot:reuse:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
            + `:snapshot=0x${handle.vforkSnapshot.vmPtr.toString(16)}`
            + `:child=${handle.vforkSnapshot.childPid ?? "none"}`
            + `:worker=${handle.vforkWorker === worker ? "match" : "mismatch"}`
            + `:kernel_child=${handle.kernel.thread_vfork_child_pid()}`,
        );
      }
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
        deps.pumpProcess(handle);
        void deps.syncForkSharedStateViaKernelWorker(
          handle,
          kernelShareOtherMembers(handle),
          {
            cause: { kind: "syscall", nr: -1n, name: "vfork-wait-preserve-source" },
            includeMemfs: false,
            preserveSourceRefcounts: true,
            sourceVmPtr: thread.vmPtr,
          },
        )
          .then(() => {
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              return;
            }
            if (!handle.vforkSnapshot || handle.vforkSnapshot.vmPtr !== thread.vmPtr) {
              return;
            }
            if (handle.vforkSnapshot.childPid === null) {
              const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
              if (provisionalChildPid > 0) {
                deps.appendRuntimeLifecycleEvent(
                  handle.runtimeId,
                  `vfork-wait-canonicalize:start:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
                    + `:child=${provisionalChildPid}`,
                );
                void deps.canonicalizeActiveVforkChildViaKernelWorker(
                  handle,
                  worker,
                  thread.vmPtr,
                  provisionalChildPid,
                ).then((outcome) => {
                  deps.appendRuntimeLifecycleEvent(
                    handle.runtimeId,
                    `vfork-wait-canonicalize:done:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
                      + `:child=${provisionalChildPid}:outcome=${outcome?.kind ?? "none"}`,
                  );
                  if (outcome !== undefined) {
                    consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
                  }
                });
                return;
              }
            }
            deps.enqueueWorker(handle, worker);
          })
          .catch((error) => {
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        handle.kernelWorkerRequest
        && handle.pid > 0
        && handle.vforkSnapshot.childPid === null
      ) {
        const provisionalChildPid = handle.kernel.thread_vfork_child_pid();
        if (provisionalChildPid > 0) {
          deps.appendRuntimeLifecycleEvent(
            handle.runtimeId,
            `vfork-wait-canonicalize:start:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
              + `:child=${provisionalChildPid}`,
          );
          void deps.canonicalizeActiveVforkChildViaKernelWorker(
            handle,
            worker,
            thread.vmPtr,
            provisionalChildPid,
          ).then((outcome) => {
            deps.appendRuntimeLifecycleEvent(
              handle.runtimeId,
              `vfork-wait-canonicalize:done:${handle.pid}:vm=0x${thread.vmPtr.toString(16)}`
                + `:child=${provisionalChildPid}:outcome=${outcome?.kind ?? "none"}`,
            );
            if (outcome !== undefined) {
              consumeKernelRpcResumeOutcome(handle, worker, thread.vmPtr, outcome);
            }
          });
          return;
        }
      }
      deps.enqueueWorker(handle, worker);
      return;
    }

    if (status === handle.kernel.abi_status_futex_wait()) {
      const hostTimer = drainSupervisorArmHostTimerHostOp(handle, thread.vmPtr);
      if (hostTimer !== undefined) {
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        thread.waitingInWorkerStatus = status;
        thread.waitingInWorkerReconcileDeferred = false;
        if (hostTimer !== null) {
          armSupervisorHostTimer(handle, hostTimer, deps);
        }
        deps.pumpProcess(handle);
        return;
      }
    }

    if (
      status === handle.kernel.abi_status_futex_wait()
      || status === handle.kernel.abi_status_wait4()
      || status === handle.kernel.abi_status_pipe_wait()
      || status === handle.kernel.abi_status_epoll_wait()
      || status === handle.kernel.abi_status_socket_recv()
      || status === handle.kernel.abi_status_socket_send()
      || status === handle.kernel.abi_status_flock_wait()
      || status === handle.kernel.abi_status_connect()
    ) {
      const finitePollTimeout = status === handle.kernel.abi_status_pipe_wait()
        && handle.kernel.thread_poll_wait_timeout !== undefined
        && Number(handle.kernel.thread_poll_wait_timeout(thread.vmPtr)) >= 0;
      const pipeWaitHasSocket = status === handle.kernel.abi_status_pipe_wait()
        && handle.networkConnections.size > 0
        && handle.kernel.thread_poll_wait_has_socket?.(thread.vmPtr) === 1;
      if (
        pipeWaitHasSocket
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.blockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        void deps.requestSocketStateSync(handle, true)
          .then(() => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        status === handle.kernel.abi_status_wait4()
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        const blocked = handle.blockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        void deps.requestPipeStateSync(handle, true)
          .then(() => {
            const latestBlocked = finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            if (!latestBlocked) {
              return;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
          });
        return;
      }
      if (
        blockedStatusUsesForkSharedStateSync(handle, status)
        && !pipeWaitHasSocket
        && !finitePollTimeout
        && handle.kernelWorkerRequest
        && handle.pid > 0
      ) {
        deps.blockWorker(handle, worker, status, thread.vmPtr);
        const blocked = handle.blockedWorkers.get(worker);
        const rpcGeneration = blocked ? beginStatusTransitionRpc(blocked) : -1;
        const syncPipeState = status === handle.kernel.abi_status_pipe_wait()
          ? deps.requestPipeStateSync(handle, false)
          : Promise.resolve();
        const socketWaitStatus = status === handle.kernel.abi_status_socket_recv()
          || status === handle.kernel.abi_status_socket_send();
        const syncSocketState = socketWaitStatus
          ? deps.requestSocketStateSync(handle, true)
          : Promise.resolve();
        void syncPipeState
          .then(() => syncSocketState)
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-start:${handle.pid}:status=${status}`);
            return deps.syncBlockedForkSharedStateViaKernelWorker(
              handle,
              thread.vmPtr,
              { kind: "blocked-rpc", status },
            );
          })
          .then(() => {
            deps.appendRuntimeLifecycleEvent(handle.runtimeId, `blocked-rpc-sync-done:${handle.pid}:status=${status}`);
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            if (!handle.workers.has(worker) || !deps.threadWorkerStates.has(worker)) {
              handle.blockedWorkers.delete(worker);
              return;
            }
            if (
              status === handle.kernel.abi_status_pipe_wait()
              && (handle.kernel.thread_poll_wait_ready_count?.(thread.vmPtr) ?? 0) <= 0
            ) {
              deps.pumpProcess(handle);
              return;
            }
            deps.kickBlockedWorkers(handle);
            deps.pumpProcess(handle);
          })
          .catch((error) => {
            finishStatusTransitionRpc(handle, worker, thread.vmPtr, rpcGeneration);
            deps.failProcess(handle, error instanceof Error ? error : new Error(String(error)));
        });
        return;
      }
      deps.blockWorker(handle, worker, status, thread.vmPtr);
      deps.pumpProcess(handle);
      return;
    }

    deps.enqueueWorker(handle, worker);
  };
}
