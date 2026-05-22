import {
  setWorkerErrorHandler,
  setWorkerMessageErrorHandler,
  setWorkerMessageHandler,
} from "../../bridge/compat.js";
import { publishRuntimeBridgeEpoch } from "../../bridge/runtime-state.js";
import type {
  AttachThreadWorkerRequest,
  ProcessHandle,
  ThreadWorkerState,
} from "../types.js";
import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
  KernelWorkerSocketStateSnapshot,
  ThreadWorkerOutbound,
} from "../../messages.js";
import { kernelStackSlotForWorker } from "../pool.js";
import type {
  ThreadWorkerInbound,
  WorkerSchedulerQueueHelpers,
} from "./types.js";

const DEFAULT_NODE_SCHEDULER_STEP_BUDGET = 5_000_000;
const DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET = 10_000;
const MIN_SCHEDULER_STEP_BUDGET = 1_000;
const MAX_SCHEDULER_STEP_BUDGET = 5_000_000;
const SCHEDULER_STEP_BUDGET = readSchedulerStepBudget();
const SCHEDULER_QUEUE_DEBUG_LOG_ENABLED = false;
let nextThreadWorkerGeneration = 1;
const HOT_PC_COUNTER_LIMIT = 32;
const PIPE_HEADER_BYTES = 32;
const PIPE_READ_OPEN_OFFSET = 8;
const PIPE_WRITE_OPEN_OFFSET = 12;
const OFD_OFFSET_OFFSET = 16;

function snapshotSocketStateForThread(handle: ProcessHandle): KernelWorkerSocketStateSnapshot | undefined {
  if (
    handle.kernelState.socketUsed === 0
    && handle.kernelState.netBufUsed === 0n
    && (handle.networkConnections?.size ?? 0) === 0
    && (handle.networkPendingConnectFds?.size ?? 0) === 0
    && (handle.networkClosedFds?.size ?? 0) === 0
    && (handle.networkClosedSocketSlots?.size ?? 0) === 0
    && (handle.networkSocketSlotWatermarks?.size ?? 0) === 0
    && (handle.networkRecvPayloadWatermarks?.size ?? 0) === 0
  ) {
    return undefined;
  }
  const socketTableBytes = handle.kernel.kernel_socket_table_bytes?.() ?? 0;
  const netBufPoolBytes = handle.kernel.kernel_net_buf_pool_bytes?.() ?? 0;
  if (socketTableBytes <= 0 || netBufPoolBytes <= 0) {
    return undefined;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  return {
    socketTable: memory.slice(
      handle.kernelState.socketTablePtr,
      handle.kernelState.socketTablePtr + socketTableBytes,
    ),
    netBufPool: memory.slice(
      handle.kernelState.netBufBase,
      handle.kernelState.netBufBase + netBufPoolBytes,
    ),
    socketUsed: handle.kernelState.socketUsed | handle.kernel.kernel_socket_used(),
    netBufUsed: handle.kernelState.netBufUsed | handle.kernel.kernel_net_buf_used(),
  };
}

function runtimeNetworkFds(handle: ProcessHandle): number[] {
  return [...new Set([
    ...(handle.networkConnections?.keys() ?? []),
    ...(handle.networkPendingConnectFds ?? []),
    ...(handle.networkClosedFds ?? []),
    ...(handle.networkClosedSocketSlots?.keys() ?? []),
    ...(handle.networkSocketSlotWatermarks?.keys() ?? []),
    ...(handle.networkRecvPayloadWatermarks?.keys() ?? []),
  ])].sort((left, right) => left - right);
}

function snapshotRuntimeNetworkFdEntriesForThread(handle: ProcessHandle): KernelWorkerFdEntrySnapshot[] | undefined {
  if (handle.vmPtr === 0) {
    return undefined;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fdEntries: KernelWorkerFdEntrySnapshot[] = [];
  for (const fd of runtimeNetworkFds(handle)) {
    if (fd < 0 || fd >= maxFds) {
      continue;
    }
    const ofdId = handle.kernel.process_fd_ofd_id(handle.vmPtr, fd);
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    const hasWatermark = handle.networkOfdSlotWatermarks?.has(ofdId) ?? false;
    if (handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_socket() && !hasWatermark) {
      continue;
    }
    fdEntries.push({
      fd,
      ofdId,
      fdFlags: handle.kernel.process_fd_flags(handle.vmPtr, fd),
    });
  }
  return fdEntries.length > 0 ? fdEntries : undefined;
}

function snapshotOfdSlotsForThread(
  handle: ProcessHandle,
  fdEntries: readonly KernelWorkerFdEntrySnapshot[] | undefined,
): KernelWorkerOfdSlotSnapshot[] | undefined {
  if (!fdEntries || fdEntries.length === 0) {
    return undefined;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (!Number.isInteger(slotBytes) || slotBytes <= 0) {
    return undefined;
  }
  const memory = new Uint8Array(handle.memory.buffer);
  const ofdSlots: KernelWorkerOfdSlotSnapshot[] = [];
  for (const ofdId of [...new Set(fdEntries.map((entry) => entry.ofdId))]) {
    if (ofdId <= 0 || ofdId >= maxFds) {
      continue;
    }
    const watermarked = handle.networkOfdSlotWatermarks?.get(ofdId);
    if (watermarked && watermarked.length === slotBytes) {
      ofdSlots.push({
        ofdId,
        slot: watermarked.slice(),
      });
      continue;
    }
    const offset = handle.kernelState.ofdTablePtr + ofdId * slotBytes;
    ofdSlots.push({
      ofdId,
      slot: memory.slice(offset, offset + slotBytes),
    });
  }
  return ofdSlots.length > 0 ? ofdSlots : undefined;
}

function readSchedulerStepBudget(): number {
  const hostConfigured = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
    TIDEMARK_SCHEDULER_STEP_BUDGET?: unknown;
  }).TIDEMARK_SCHEDULER_STEP_BUDGET;
  const envConfigured = (globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }).process?.env?.TIDEMARK_SCHEDULER_STEP_BUDGET;
  const parsed = Number.parseInt(String(hostConfigured ?? envConfigured ?? ""), 10);
  if (Number.isFinite(parsed)) {
    return Math.min(MAX_SCHEDULER_STEP_BUDGET, Math.max(MIN_SCHEDULER_STEP_BUDGET, parsed));
  }
  return typeof window === "undefined"
    ? DEFAULT_NODE_SCHEDULER_STEP_BUDGET
    : DEFAULT_BROWSER_SCHEDULER_STEP_BUDGET;
}

function formatChildExitRecords(
  handle: ProcessHandle,
): Array<{ childPid: number; waitStatus: number; parentPid: number }> {
  return [...handle.childExitSyncRecords].map(([childPid, waitStatus]) => ({
    childPid,
    waitStatus,
    parentPid: handle.pid,
  }));
}

function composeHostStdioPipeUsed(handle: ProcessHandle, pipeUsed: bigint): bigint {
  let composed = pipeUsed | 1n;
  const owner = handle.stdioOwner;
  for (const pipeSlot of [
    handle.stdinPipe,
    handle.stdoutPipe,
    handle.stderrPipe,
    handle.hostStdio.stdinPipe,
    handle.hostStdio.stdoutPipe,
    handle.hostStdio.stderrPipe,
    owner?.stdinPipe ?? 0,
    owner?.stdoutPipe ?? 0,
    owner?.stderrPipe ?? 0,
    owner?.hostStdio.stdinPipe ?? 0,
    owner?.hostStdio.stdoutPipe ?? 0,
    owner?.hostStdio.stderrPipe ?? 0,
  ]) {
    if (pipeSlot > 0) {
      composed |= 1n << BigInt(pipeSlot);
    }
  }
  return composed;
}

function hostStdioPipeSlotsForFd(handle: ProcessHandle, fd: 0 | 1 | 2): Set<number> {
  const owner = handle.stdioOwner;
  const slotsByFd = fd === 0
    ? [
        handle.stdinPipe,
        handle.hostStdio.stdinPipe,
        owner?.stdinPipe ?? 0,
        owner?.hostStdio.stdinPipe ?? 0,
      ]
    : fd === 1
      ? [
          handle.stdoutPipe,
          handle.hostStdio.stdoutPipe,
          owner?.stdoutPipe ?? 0,
          owner?.hostStdio.stdoutPipe ?? 0,
        ]
      : [
          handle.stderrPipe,
          handle.hostStdio.stderrPipe,
          owner?.stderrPipe ?? 0,
          owner?.hostStdio.stderrPipe ?? 0,
        ];
  return new Set(slotsByFd.filter((slot) => slot > 0));
}

function preserveHostStdioPipeControlForStep(handle: ProcessHandle, vmPtr: number): void {
  const maxPipes = handle.kernel.abi_max_pipes?.() ?? 0;
  const pipeRegionBytes = handle.kernel.kernel_pipe_region_bytes?.() ?? 0;
  const slotBytes = maxPipes > 0 ? pipeRegionBytes / maxPipes : 0;
  if (!Number.isInteger(slotBytes) || slotBytes <= PIPE_HEADER_BYTES) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  const preserve = (pipeSlot: number, readOpenMin: number, writeOpenMin: number): void => {
    if (pipeSlot <= 0 || pipeSlot >= maxPipes) {
      return;
    }
    const offset = handle.kernelState.pipeBase + pipeSlot * slotBytes;
    if (readOpenMin > 0) {
      const readOpen = view.getInt32(offset + PIPE_READ_OPEN_OFFSET, true);
      if (readOpen < readOpenMin) {
        view.setInt32(offset + PIPE_READ_OPEN_OFFSET, readOpenMin, true);
      }
    }
    if (writeOpenMin > 0) {
      const writeOpen = view.getInt32(offset + PIPE_WRITE_OPEN_OFFSET, true);
      if (writeOpen < writeOpenMin) {
        view.setInt32(offset + PIPE_WRITE_OPEN_OFFSET, writeOpenMin, true);
      }
    }
  };

  for (const [fd, readOpenMin, writeOpenMin] of [
    [0, 0, 1],
    [1, 1, 0],
    [2, 1, 0],
  ] as const) {
    const ofdId = handle.kernel.process_fd_ofd_id(vmPtr, fd);
    if (
      ofdId <= 0
      || ofdId >= handle.kernel.abi_max_fds()
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== handle.kernel.abi_fd_type_pipe()
    ) {
      continue;
    }
    const pipeSlot = handle.kernel.kernel_ofd_ino(ofdId);
    if (hostStdioPipeSlotsForFd(handle, fd).has(pipeSlot)) {
      preserve(pipeSlot, readOpenMin, writeOpenMin);
    }
  }
}

function restoreRegularFileOffsetsForStep(handle: ProcessHandle): void {
  const offsets = handle.regularFileOfdOffsets;
  if (!offsets || offsets.size === 0) {
    return;
  }
  const maxFds = handle.kernel.abi_max_fds();
  const fileType = handle.kernel.abi_fd_type_file?.();
  const tableBytes = handle.kernel.kernel_ofd_table_bytes();
  const slotBytes = tableBytes / maxFds;
  if (fileType === undefined || !Number.isInteger(slotBytes) || slotBytes <= 0) {
    return;
  }
  const view = new DataView(handle.memory.buffer);
  for (const [ofdId, snapshot] of offsets) {
    if (
      ofdId <= 0
      || ofdId >= maxFds
      || handle.kernel.kernel_ofd_fd_type(ofdId) !== fileType
      || handle.kernel.kernel_ofd_refcount(ofdId) <= 0
      || handle.kernel.kernel_ofd_ino(ofdId) !== snapshot.ino
      || handle.kernel.kernel_ofd_host_fd(ofdId) !== snapshot.hostFd
    ) {
      continue;
    }
    if (handle.kernel.kernel_ofd_offset(ofdId) >= snapshot.offset) {
      continue;
    }
    view.setBigInt64(handle.kernelState.ofdTablePtr + ofdId * slotBytes + OFD_OFFSET_OFFSET, snapshot.offset, true);
  }
}

interface WorkerSchedulerQueueDeps {
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>;
  appendWorkerDebugLog(handle: ProcessHandle, event: string): void;
  failProcess(handle: ProcessHandle, error: Error): void;
  handleWorkerMessage(handle: ProcessHandle, worker: Worker, msg: ThreadWorkerOutbound): void;
}

function formatStepDebugSuffix(handle: ProcessHandle, vmPtr: number): string {
  const parts: string[] = [];
  const pid = handle.kernel.process_current_pid?.(vmPtr);
  if (pid !== undefined) {
    parts.push(`pid=${pid}`);
  }
  const tid = handle.kernel.thread_tid?.(vmPtr);
  if (tid !== undefined) {
    parts.push(`tid=${tid}`);
  }
  const pc = handle.kernel.thread_pc?.(vmPtr);
  if (pc !== undefined) {
    parts.push(`pc=0x${pc.toString(16)}`);
  }
  const ra = handle.kernel.thread_ra?.(vmPtr);
  if (ra !== undefined) {
    parts.push(`ra=0x${ra.toString(16)}`);
  }
  const sp = handle.kernel.thread_sp?.(vmPtr);
  if (sp !== undefined) {
    parts.push(`sp=0x${sp.toString(16)}`);
  }
  for (let index = 0; index <= 32; index += 1) {
    const stackWord = handle.kernel.thread_stack_word?.(vmPtr, index);
    if (stackWord !== undefined) {
      parts.push(`stk${index}=0x${stackWord.toString(16)}`);
    }
  }
  const a0 = handle.kernel.thread_a0?.(vmPtr);
  if (a0 !== undefined) {
    parts.push(`a0=${a0.toString()}`);
  }
  const a1 = handle.kernel.thread_a1?.(vmPtr);
  if (a1 !== undefined) {
    parts.push(`a1=${a1.toString()}`);
  }
  const a2 = handle.kernel.thread_reg?.(vmPtr, 12);
  if (a2 !== undefined) {
    parts.push(`a2=${a2.toString()}`);
  }
  const a3 = handle.kernel.thread_reg?.(vmPtr, 13);
  if (a3 !== undefined) {
    parts.push(`a3=${a3.toString()}`);
  }
  const gp = handle.kernel.thread_gp?.(vmPtr);
  if (gp !== undefined) {
    parts.push(`gp=0x${gp.toString(16)}`);
  }
  const tp = handle.kernel.thread_tp?.(vmPtr);
  if (tp !== undefined) {
    parts.push(`tp=0x${tp.toString(16)}`);
  }
  for (const [label, reg] of [["s0", 8], ["s1", 9], ["s2", 18], ["s3", 19], ["s4", 20], ["s5", 21], ["s6", 22]] as const) {
    const value = handle.kernel.thread_reg?.(vmPtr, reg);
    if (value !== undefined) {
      parts.push(`${label}=0x${value.toString(16)}`);
    }
  }
  const s11 = handle.kernel.thread_s11?.(vmPtr);
  if (s11 !== undefined) {
    parts.push(`s11=0x${s11.toString(16)}`);
  }
  return parts.length > 0 ? `:${parts.join(":")}` : "";
}

function hasBrokenThreadState(handle: ProcessHandle, vmPtr: number): boolean {
  const pc = handle.kernel.thread_pc?.(vmPtr) ?? 0n;
  const sp = handle.kernel.thread_sp?.(vmPtr) ?? 0n;
  const s11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
  return pc < 0x1000n || sp === 0n || s11 === 0n;
}

function removeReadyWorker(handle: ProcessHandle, worker: Worker): void {
  for (let index = handle.readyWorkers.length - 1; index >= 0; index -= 1) {
    if (handle.readyWorkers[index] === worker) {
      handle.readyWorkers.splice(index, 1);
    }
  }
}

function clearBlockedWorker(handle: ProcessHandle, worker: Worker): void {
  const blocked = handle.blockedWorkers.get(worker);
  if (!blocked) {
    return;
  }
  if (blocked.retryTimer !== null) {
    clearTimeout(blocked.retryTimer);
  }
  handle.blockedWorkers.delete(worker);
}

function incrementBoundedCounter(
  counters: Record<string, number>,
  key: string,
  maxEntries = HOT_PC_COUNTER_LIMIT,
): void {
  const existing = counters[key];
  if (existing !== undefined) {
    counters[key] = existing + 1;
    return;
  }
  const entries = Object.keys(counters);
  if (entries.length < maxEntries) {
    counters[key] = 1;
    return;
  }
  for (const entry of entries) {
    const next = (counters[entry] ?? 0) - 1;
    if (next <= 0) {
      delete counters[entry];
      continue;
    }
    counters[entry] = next;
  }
}

export function createWorkerSchedulerQueueHelpers(
  deps: WorkerSchedulerQueueDeps,
): WorkerSchedulerQueueHelpers {
  const observedForkSharedSyncGates = new WeakMap<ProcessHandle, Promise<void>>();

  function observeForkSharedSyncGate(handle: ProcessHandle, promise: Promise<void>): void {
    if (observedForkSharedSyncGates.get(handle) === promise) {
      return;
    }
    observedForkSharedSyncGates.set(handle, promise);
    const onSettled = (): void => {
      if (observedForkSharedSyncGates.get(handle) === promise) {
        observedForkSharedSyncGates.delete(handle);
      }
      if (handle.kernelShare.forkSharedSyncPromise === promise) {
        handle.kernelShare.forkSharedSyncPromise = null;
      }
      if (handle.forkSharedSyncPromise === promise) {
        handle.forkSharedSyncPromise = null;
      }
      pumpProcess(handle);
    };
    promise.then(onSettled, onSettled);
  }

  function sanitizeRunningWorker(handle: ProcessHandle): Worker | null {
    const runningWorker = handle.runningWorker;
    if (runningWorker === null) {
      return null;
    }
    const thread = deps.threadWorkerStates.get(runningWorker);
    if (
      handle.workers.has(runningWorker)
      && thread
      && (thread.runningGeneration ?? thread.generation) === thread.generation
    ) {
      return runningWorker;
    }
    handle.runningWorker = null;
    if (thread) {
      thread.runningGeneration = null;
    }
    deps.appendWorkerDebugLog(handle, "running-worker-cleared:stale");
    return null;
  }

  function hasExclusiveVforkWorker(handle: ProcessHandle): boolean {
    const worker = handle.vforkWorker;
    if (handle.vforkSnapshot === null || worker === null) {
      return false;
    }
    if (sanitizeRunningWorker(handle) === worker) {
      return true;
    }
    if (handle.readyWorkers.includes(worker)) {
      return true;
    }
    if (handle.blockedWorkers.has(worker)) {
      return true;
    }
    const thread = deps.threadWorkerStates.get(worker);
    return thread?.waitingInWorkerStatus !== null;
  }

  function assertThreadOwnershipInvariant(
    handle: ProcessHandle,
    vmPtr: number,
    context: string,
  ): void {
    if (vmPtr === 0 || handle.pid <= 0) {
      return;
    }
    const currentPid = handle.kernel.process_current_pid(vmPtr);
    if (currentPid !== handle.pid) {
      throw new Error(
        `${context} worker/process ownership mismatch vm=0x${vmPtr.toString(16)}`
          + ` expected_pid=${handle.pid} actual_pid=${currentPid}`,
      );
    }
  }

  function detachTransferredWorker(handle: ProcessHandle, worker: Worker): void {
    clearBlockedWorker(handle, worker);
    removeReadyWorker(handle, worker);
    if (handle.runningWorker === worker) {
      handle.runningWorker = null;
    }
    if (handle.vforkWorker === worker) {
      handle.vforkWorker = null;
    }
    handle.workers.delete(worker);
  }

  function attachThreadWorker(handle: ProcessHandle, request: AttachThreadWorkerRequest): void {
    const { worker, vmPtr, forcePrepare, transferFrom, preserveMemoryBytes, preserveMemoryRanges } = request;
    const generation = nextThreadWorkerGeneration++;
    const existingThread = deps.threadWorkerStates.get(worker);
    const reinitWithinHandle = existingThread !== undefined
      && transferFrom === undefined
      && existingThread.ownerPid === handle.pid;
    if (existingThread) {
      if (reinitWithinHandle) {
        clearBlockedWorker(handle, worker);
        removeReadyWorker(handle, worker);
        if (handle.runningWorker === worker) {
          handle.runningWorker = null;
        }
        if (handle.vforkWorker === worker) {
          handle.vforkWorker = null;
        }
      } else {
        const sourceHandle = transferFrom;
        if (!sourceHandle) {
          throw new Error(
            `attachThreadWorker requires transferFrom for existing worker owner=${existingThread.ownerPid}`
              + ` target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (sourceHandle === handle) {
          throw new Error(
            `attachThreadWorker transferFrom must differ from target pid=${handle.pid}`
              + ` vm=0x${vmPtr.toString(16)}`,
          );
        }
        if (existingThread.ownerPid !== sourceHandle.pid) {
          throw new Error(
            `attachThreadWorker owner mismatch worker_owner=${existingThread.ownerPid}`
              + ` transfer_from=${sourceHandle.pid} target=${handle.pid}`,
          );
        }
        detachTransferredWorker(sourceHandle, worker);
      }
    } else if (transferFrom) {
      throw new Error(
        `attachThreadWorker received transferFrom for unattached worker`
          + ` source=${transferFrom.pid} target=${handle.pid} vm=0x${vmPtr.toString(16)}`,
      );
    }

    assertThreadOwnershipInvariant(handle, vmPtr, "attachThreadWorker");
    handle.workers.add(worker);
    deps.appendWorkerDebugLog(
      handle,
      `${reinitWithinHandle ? "reinit" : "attach"}:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
    );
    deps.threadWorkerStates.set(worker, {
      vmPtr,
      ownerPid: handle.pid,
      generation,
      runningGeneration: null,
      reinitCount: reinitWithinHandle ? (existingThread?.reinitCount ?? 0) + 1 : 0,
      stepCount: 0,
      totalStepBudget: 0,
      totalStepWallMs: 0,
      maxStepWallMs: 0,
      lastStepDurationMs: null,
      statusCount: 0,
      statusCounts: {},
      lastStatus: null,
      lastStepAtMs: null,
      lastStatusAtMs: null,
      lastStepProcessPtr: null,
      lastStepPc: null,
      lastStepSp: null,
      lastStepS11: null,
      hotStepPcs: {},
      lastStatusPc: null,
      lastStatusSp: null,
      lastStatusS11: null,
      hotStatusPcs: {},
      initialized: false,
      waitingInWorkerStatus: null,
      ownerAsyncStatus: null,
      resumeBlockedOnNextStep: false,
      pendingPipeSlotsForNextStep: [],
      pendingGuestMemoryWrites: [],
      resumeWorkersOnReady: [],
    });
    setWorkerMessageHandler(worker, (event: MessageEvent<ThreadWorkerOutbound>) => {
      deps.handleWorkerMessage(handle, worker, event.data);
    });
    setWorkerErrorHandler(worker, (event) => {
      const detail = [
        event.message,
        event.filename ? `at ${event.filename}:${event.lineno}:${event.colno}` : null,
        event.error instanceof Error ? (event.error.stack ?? event.error.message) : null,
      ].filter((part): part is string => !!part).join(" ");
      deps.appendWorkerDebugLog(handle, `worker-error:0x${vmPtr.toString(16)}:${JSON.stringify(detail || "unknown worker error")}`);
      deps.failProcess(handle, new Error(
        `thread worker error vm=0x${vmPtr.toString(16)} ${detail || "unknown worker error"}`,
      ));
      event.preventDefault?.();
    });
    setWorkerMessageErrorHandler(worker, (event) => {
      deps.appendWorkerDebugLog(handle, `worker-messageerror:0x${vmPtr.toString(16)}`);
      deps.failProcess(handle, new Error(
        `thread worker messageerror vm=0x${vmPtr.toString(16)}`,
      ));
      event.preventDefault?.();
    });
    const runtimeEpoch = publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const networkFdEntries = snapshotRuntimeNetworkFdEntriesForThread(handle);
    const initMessage: ThreadWorkerInbound = {
      type: "init",
      wasmModule: handle.wasmModule,
      memory: handle.memory,
      forcePrepare,
      vmPtr,
      guestBase: handle.guestBase,
      kernelStackSlot: kernelStackSlotForWorker(worker),
      generation,
      runtimeEpoch,
      preserveMemoryBytes,
      preserveMemoryRanges,
      guestWriteWatchRegions: handle.guestWriteWatchRegions,
      kernelState: handle.kernelState,
      sabPageCache: handle.sabPageCache,
      runtimeNotify: handle.runtimeBridge.notifySab,
      runtimeState: handle.runtimeBridge.stateSab,
    };
    const initSocketState = snapshotSocketStateForThread(handle);
    const initOfdSlots = snapshotOfdSlotsForThread(handle, networkFdEntries);
    if (initSocketState !== undefined) {
      initMessage.socketState = initSocketState;
    }
    if (networkFdEntries !== undefined) {
      initMessage.fdEntries = networkFdEntries;
    }
    if (initOfdSlots !== undefined) {
      initMessage.ofdSlots = initOfdSlots;
    }
    const initChildExitRecords = formatChildExitRecords(handle);
    if (initChildExitRecords.length > 0) {
      initMessage.childExitRecords = initChildExitRecords;
    }
    worker.postMessage(initMessage);
  }

  function enqueueWorker(handle: ProcessHandle, worker: Worker): void {
    if (handle.exitInProgress) {
      return;
    }
    if (
      handle.runningWorker === worker
      && handle.workers.has(worker)
      && deps.threadWorkerStates.has(worker)
    ) {
      return;
    }
    if (!handle.readyWorkers.includes(worker)) {
      const vmPtr = deps.threadWorkerStates.get(worker)?.vmPtr;
      if (vmPtr !== undefined) {
        const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
        if (SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
          deps.appendWorkerDebugLog(handle, `ready:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
        }
        if (brokenThreadState) {
          deps.appendWorkerDebugLog(
            handle,
            `ready_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
          );
        }
      }
      handle.readyWorkers.push(worker);
      if (handle.vforkSnapshot !== null && handle.vforkWorker === worker) {
      }
    }
    pumpProcess(handle);
  }

  function recoverRunnableIdleWorkers(handle: ProcessHandle): void {
    const exclusiveWorker = handle.vforkSnapshot !== null ? handle.vforkWorker : null;
    for (const worker of handle.workers) {
      if (handle.runningWorker === worker || handle.readyWorkers.includes(worker) || handle.blockedWorkers.has(worker)) {
        continue;
      }
      if (exclusiveWorker !== null && worker !== exclusiveWorker) {
        continue;
      }
      const thread = deps.threadWorkerStates.get(worker);
      if (
        !thread?.initialized
        || thread.waitingInWorkerStatus !== null
        || thread.ownerAsyncStatus !== null
      ) {
        continue;
      }
      if (handle.kernel.thread_status(thread.vmPtr) !== handle.kernel.abi_status_ok()) {
        continue;
      }
      deps.appendWorkerDebugLog(handle, `ready_recover:0x${thread.vmPtr.toString(16)}${formatStepDebugSuffix(handle, thread.vmPtr)}`);
      handle.readyWorkers.push(worker);
    }
  }

  function pumpProcess(handle: ProcessHandle): void {
    if (handle.exitInProgress) {
      return;
    }
    const forkSharedSyncPromise = handle.kernelShare.forkSharedSyncPromise;
    if (forkSharedSyncPromise !== null) {
      observeForkSharedSyncGate(handle, forkSharedSyncPromise);
      return;
    }
    const runningWorker = sanitizeRunningWorker(handle);
    if (runningWorker !== null) {
      return;
    }
    recoverRunnableIdleWorkers(handle);
    let next: Worker | undefined;
    if (hasExclusiveVforkWorker(handle)) {
      const index = handle.readyWorkers.indexOf(handle.vforkWorker!);
      if (index === -1) {
        return;
      }
      const candidate = handle.readyWorkers[index];
      const candidateThread = candidate ? deps.threadWorkerStates.get(candidate) : undefined;
      if ((candidateThread?.ownerAsyncStatus ?? null) !== null) {
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    } else {
      const index = handle.readyWorkers.findIndex((candidate) => {
        const candidateThread = deps.threadWorkerStates.get(candidate);
        return (candidateThread?.ownerAsyncStatus ?? null) === null;
      });
      if (index === -1) {
        return;
      }
      next = handle.readyWorkers.splice(index, 1)[0];
    }
    if (!next) {
      return;
    }
    handle.runningWorker = next;
    const vmPtr = deps.threadWorkerStates.get(next)?.vmPtr;
    const thread = deps.threadWorkerStates.get(next);
    if (thread && vmPtr !== undefined) {
      restoreRegularFileOffsetsForStep(handle);
      preserveHostStdioPipeControlForStep(handle, vmPtr);
      thread.stepCount += 1;
      thread.runningGeneration = thread.generation;
      thread.totalStepBudget += SCHEDULER_STEP_BUDGET;
      thread.lastStepAtMs = Date.now();
      thread.lastStepProcessPtr = handle.kernel.thread_process_ptr(vmPtr);
      thread.lastStepPc = handle.kernel.thread_pc(vmPtr);
      thread.lastStepSp = handle.kernel.thread_sp(vmPtr);
      thread.lastStepS11 = handle.kernel.thread_s11?.(vmPtr) ?? 0n;
      incrementBoundedCounter(thread.hotStepPcs, `0x${thread.lastStepPc.toString(16)}`);
      const brokenThreadState = hasBrokenThreadState(handle, vmPtr);
      if (SCHEDULER_QUEUE_DEBUG_LOG_ENABLED) {
        deps.appendWorkerDebugLog(handle, `step:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`);
      }
      if (brokenThreadState) {
        deps.appendWorkerDebugLog(
          handle,
          `step_break:0x${vmPtr.toString(16)}${formatStepDebugSuffix(handle, vmPtr)}`,
        );
      }
    }
    publishRuntimeBridgeEpoch(handle.runtimeBridge);
    const networkFdEntries = snapshotRuntimeNetworkFdEntriesForThread(handle);
    const stepMessage: ThreadWorkerInbound = {
      type: "step",
      budget: SCHEDULER_STEP_BUDGET,
      kernelState: handle.kernelState,
    };
    const stepSocketState = snapshotSocketStateForThread(handle);
    const stepOfdSlots = snapshotOfdSlotsForThread(handle, networkFdEntries);
    if (stepSocketState !== undefined) {
      stepMessage.socketState = stepSocketState;
    }
    if (networkFdEntries !== undefined) {
      stepMessage.fdEntries = networkFdEntries;
    }
    if (stepOfdSlots !== undefined) {
      stepMessage.ofdSlots = stepOfdSlots;
    }
    if (thread?.pendingPipeSlotsForNextStep?.length) {
      stepMessage.pipeSlots = thread.pendingPipeSlotsForNextStep.splice(0);
    }
    const stepChildExitRecords = formatChildExitRecords(handle);
    if (stepChildExitRecords.length > 0) {
      stepMessage.childExitRecords = stepChildExitRecords;
      stepMessage.kernelState = {
        ...stepMessage.kernelState,
        pipeUsed: composeHostStdioPipeUsed(handle, stepMessage.kernelState.pipeUsed),
      };
    }
    if (thread && (thread.pendingGuestMemoryWrites?.length ?? 0) > 0) {
      stepMessage.guestMemoryWrites = thread.pendingGuestMemoryWrites.splice(0);
    }
    if (thread?.resumeBlockedOnNextStep) {
      thread.resumeBlockedOnNextStep = false;
      stepMessage.resumeBlockedSyscall = true;
    }
    next.postMessage(stepMessage);
  }

  return {
    attachThreadWorker,
    enqueueWorker,
    pumpProcess,
  };
}
