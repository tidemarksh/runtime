import type {
  KernelExports,
  KernelRuntimeState,
} from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type { GuestMemoryPolicyLimitInput } from "./memory-policy.js";
import type {
  KernelWorkerOutbound,
  GuestWriteWatchRegion,
  KernelWorkerRequest,
  KernelWorkerPipeSlotSnapshot,
  GuestMemoryWriteSnapshot,
  ThreadWorkerMemoryPreserveRange,
} from "../messages.js";
import type { RuntimeStateBridge } from "../bridge/runtime-state.js";
import type { RuntimeNetworkBridge, RuntimeNetworkConnection } from "../bridge/network.js";

export interface SharedHostStdio {
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  ttyPtySlot?: number;
  refCount: number;
}

export interface VforkSnapshot {
  vmPtr: number;
  childPid: number | null;
  nextPid: number | null;
  pgid: number | null;
  sid: number | null;
  parentPid: number;
  parentProcessPtr: number;
  parentPc: bigint;
  parentSp: bigint;
  parentS11: bigint;
  inheritedSharedOfdIds: number[];
}

export interface ThreadWorkerState {
  vmPtr: number;
  ownerPid: number;
  generation: number;
  runningGeneration?: number | null;
  reinitCount: number;
  stepCount: number;
  totalStepBudget: number;
  totalStepWallMs: number;
  maxStepWallMs: number;
  lastStepDurationMs: number | null;
  statusCount: number;
  statusCounts: Record<string, number>;
  lastStatus: number | null;
  recentStatuses?: ThreadWorkerStatusSnapshot[];
  lastStepAtMs: number | null;
  lastStatusAtMs: number | null;
  lastStepProcessPtr: number | null;
  lastStepPc: bigint | null;
  lastStepSp: bigint | null;
  lastStepS11: bigint | null;
  hotStepPcs: Record<string, number>;
  recentSteps?: ThreadWorkerStepSnapshot[];
  lastStatusPc: bigint | null;
  lastStatusSp: bigint | null;
  lastStatusS11: bigint | null;
  execveProcessState?: Uint8Array;
  hotStatusPcs: Record<string, number>;
  initialized: boolean;
  waitingInWorkerStatus: number | null;
  ownerAsyncStatus: number | null;
  pipeCanonicalEofReadyOnNextBlock?: boolean;
  pendingKernelStateForNextStep?: KernelRuntimeState;
  pendingPipeSlotsForNextStep: KernelWorkerPipeSlotSnapshot[];
  pendingGuestMemoryWrites: GuestMemoryWriteSnapshot[];
  resumeWorkersOnReady: Worker[];
}

export interface ThreadWorkerStepSnapshot {
  pc: bigint;
  sp: bigint;
  s11: bigint;
  atMs: number;
}

export interface ThreadWorkerStatusSnapshot {
  status: number;
  pc: bigint;
  syscallNr: bigint | null;
  fdTraceSyscallNr: bigint | null;
  fdTraceResult: bigint | null;
  futexWaitAddr: bigint | null;
  futexBlockedAddr: bigint | null;
  atMs: number;
}

export interface WorkerMessageDebugStats {
  received: number;
  statusReceived: number;
  controlReceived: number;
  rerouted: number;
  inactiveHandleDrops: number;
  exitInProgressDrops: number;
  ownershipInvariantDrops: number;
  staleGenerationDrops: number;
  staleProcessStatusDrops: number;
  readyRehydrateAdopts: number;
  statusHandlerNoThreadDrops: number;
  ownerAsyncStatusReturns: number;
  statusHandled: number;
  statusClearedRunning: number;
  statusNotRunning: number;
  lastEvents: string[];
}

export function createWorkerMessageDebugStats(): WorkerMessageDebugStats {
  return {
    received: 0,
    statusReceived: 0,
    controlReceived: 0,
    rerouted: 0,
    inactiveHandleDrops: 0,
    exitInProgressDrops: 0,
    ownershipInvariantDrops: 0,
    staleGenerationDrops: 0,
    staleProcessStatusDrops: 0,
    readyRehydrateAdopts: 0,
    statusHandlerNoThreadDrops: 0,
    ownerAsyncStatusReturns: 0,
    statusHandled: 0,
    statusClearedRunning: 0,
    statusNotRunning: 0,
    lastEvents: [],
  };
}

export function recordWorkerMessageDebugEvent(
  stats: WorkerMessageDebugStats,
  key: Exclude<keyof WorkerMessageDebugStats, "lastEvents">,
  event?: string,
): void {
  stats[key] += 1;
  if (event === undefined) {
    return;
  }
  stats.lastEvents.push(event);
  if (stats.lastEvents.length > 32) {
    stats.lastEvents.splice(0, stats.lastEvents.length - 32);
  }
}

// Host-executor parking state for non-futex host effects that still need
// TypeScript I/O/RPC completion. Supervisor-owned futex/sleep waits must not
// create entries here.
export interface HostBlockedWorkerState {
  vmPtr: number;
  status: number;
  rpcPending: boolean;
  rpcGeneration: number;
  skipNextNetworkPump: boolean;
  pipeCanonicalEofReady?: boolean;
  lastPipeRetryLifecycleEvent?: string;
  lastWait4RetryLifecycleEvent?: string;
}

export interface RuntimeFsState {
  version: number;
  entries: FsSnapshotEntry[];
}

export interface TransferDebugStats {
  fsResumeCalls: number;
  fsKernelWorkerCalls: number;
  fsLocalCalls: number;
  fsReadCalls: number;
  fsReadBytes: number;
  fsWriteCalls: number;
  fsWriteBytes: number;
  packagePathWriteCalls: number;
  packagePathWriteBytes: number;
  memfsMutationOps: number;
  memfsFileDataOps: number;
  pageCachePublishCandidates: number;
  pageCachePublishCalls: number;
  forkSharedMemfsSyncCalls: number;
  networkPumpCalls: number;
  networkPumpChangedCalls: number;
  networkPumpElapsedMs: number;
  networkBridgeReadCalls: number;
  networkBridgeReadBytes: number;
  networkBridgeReadQueued: number;
  networkInboundInjectCalls: number;
  networkInboundInjectedBytes: number;
  networkInboundBlockedCalls: number;
  networkPendingInboundBytesMax: number;
  networkOutboundDrainCalls: number;
  networkOutboundDrainedBytes: number;
  networkGuestSocketReadCalls: number;
  networkGuestSocketReadBytes: number;
  pipeSyncRequests: number;
  pipeSyncKernelCalls: number;
  pipeSyncVisibleDrainCalls: number;
  socketSyncRequests: number;
  socketSyncKernelCalls: number;
}

export function createTransferDebugStats(): TransferDebugStats {
  return {
    fsResumeCalls: 0,
    fsKernelWorkerCalls: 0,
    fsLocalCalls: 0,
    fsReadCalls: 0,
    fsReadBytes: 0,
    fsWriteCalls: 0,
    fsWriteBytes: 0,
    packagePathWriteCalls: 0,
    packagePathWriteBytes: 0,
    memfsMutationOps: 0,
    memfsFileDataOps: 0,
    pageCachePublishCandidates: 0,
    pageCachePublishCalls: 0,
    forkSharedMemfsSyncCalls: 0,
    networkPumpCalls: 0,
    networkPumpChangedCalls: 0,
    networkPumpElapsedMs: 0,
    networkBridgeReadCalls: 0,
    networkBridgeReadBytes: 0,
    networkBridgeReadQueued: 0,
    networkInboundInjectCalls: 0,
    networkInboundInjectedBytes: 0,
    networkInboundBlockedCalls: 0,
    networkPendingInboundBytesMax: 0,
    networkOutboundDrainCalls: 0,
    networkOutboundDrainedBytes: 0,
    networkGuestSocketReadCalls: 0,
    networkGuestSocketReadBytes: 0,
    pipeSyncRequests: 0,
    pipeSyncKernelCalls: 0,
    pipeSyncVisibleDrainCalls: 0,
    socketSyncRequests: 0,
    socketSyncKernelCalls: 0,
  };
}

export interface CanonicalizedChildLifecycle {
  childPid: number;
  ppid: number;
  nextPid: number;
  pgid: number;
  sid: number;
}

export type KernelWorkerRequestFn = (msg: KernelWorkerRequest) => Promise<KernelWorkerOutbound>;

export interface RuntimeMemoryOwnership {
  guestAddressProfile: number | null;
  guestAddressLimitBytes: string | null;
  guestMemoryPolicyLimitBytes: bigint;
  processMaxMemoryBytes: number;
  forkProcessMaxMemoryBytes: number;
  pageCacheDataCapacityBytes: number;
  publishedMetadataCapacityBytes: number;
  fsRpcRingCapacityBytes: number;
  kernelWorkerPrivateMemoryBytes: number | null;
  artifactMaterializationBudgetBytes: number | null;
  artifactMaterializedBytes: number;
}

export interface ProcessHandle {
  runtimeId: number;
  pid: number;
  parentPid: number | null;
  vmPtr: number;
  guestMemoryPolicyLimitBytes: bigint;
  maxMemoryPages: number;
  forkMaxMemoryPages: number;
  guestBase: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  wasmModule: WebAssembly.Module;
  kernel: KernelExports;
  kernelState: KernelRuntimeState;
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  ttyPtySlot: number;
  hostStdio: SharedHostStdio;
  stdinClosed?: boolean;
  stdioOwner: ProcessHandle | null;
  kernelShare: KernelShareGroup;
  stdinQueue: Uint8Array[];
  stdinFlushPromise: Promise<void> | null;
  outputBacklog: Uint8Array[];
  debugCommand: string;
  lastDebugMessage?: string;
  runtimeBridge: RuntimeStateBridge;
  sabPageCache: SharedArrayBuffer | null;
  kernelWorkerRequest?: KernelWorkerRequestFn;
  networkBridge?: RuntimeNetworkBridge;
  networkConnections: Map<number, RuntimeNetworkConnection>;
  networkConnectionOfds?: Map<number, number>;
  networkOfdSlotWatermarks?: Map<number, Uint8Array>;
  networkPendingConnectFds: Set<number>;
  networkClosedFds: Set<number>;
  networkClosedSocketSlots: Map<number, { offset: number; data: Uint8Array }>;
  networkSocketSlotWatermarks: Map<number, { offset: number; data: Uint8Array }>;
  networkRecvHeadWatermarks: Map<number, number>;
  networkRecvTailWatermarks: Map<number, number>;
  networkRecvPayloadWatermarks: Map<number, {
    recvBufIdx: number;
    head: number;
    tail: number;
    capacity: number;
    readBytes?: number;
    availableBytes?: number;
    deliveredBytes?: number;
    data: Uint8Array;
  }>;
  workers: Set<Worker>;
  // Worker host queues. Runnable/thread authority comes from the supervisor;
  // these collections only track host execution, parking, and diagnostics.
  // vforkReadyWorkers is the vfork host-hold queue, not the general runnable set.
  vforkReadyWorkers: Worker[];
  // runningWorker is the browser Worker currently executing a guest step.
  // It is a host execution mutex, not a runnable selection authority.
  runningWorker: Worker | null;
  // hostBlockedWorkers tracks waits whose host-side effect bridge is still live.
  // Futex/runnable/dead-thread decisions remain supervisor-owned.
  hostBlockedWorkers: Map<Worker, HostBlockedWorkerState>;
  pendingChildSpawnRecords: number;
  lastSupervisorFdControlSeq?: bigint;
  lastSupervisorFsControlSeq?: bigint;
  lastSupervisorSignalControlSeq?: bigint;
  pipeSyncPromise: Promise<void> | null;
  pendingPipeSlotSnapshots: KernelWorkerPipeSlotSnapshot[];
  epollSyncPromise: Promise<void> | null;
  forkSharedSyncPromise: Promise<void> | null;
  processMemfsDirty: boolean;
  processMemfsPublishPending: boolean;
  processMemfsDeletedPaths?: Set<string>;
  regularFileOfdOffsets?: Map<number, { ino: number; hostFd: number; offset: bigint }>;
  ofdSyncPromise: Promise<void> | null;
  ofdSyncQueuedIds: Set<number>;
  socketSyncPromise: Promise<void> | null;
  vforkSnapshot: VforkSnapshot | null;
  vforkWorker: Worker | null;
  exitInProgress?: boolean;
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
  workerDebugLog: string[];
  workerMessageDebugStats: WorkerMessageDebugStats;
  transferDebugStats: TransferDebugStats;
  guestWriteWatchRegions: GuestWriteWatchRegion[];
  memoryOwnership?: RuntimeMemoryOwnership;
}

export interface AttachThreadWorkerRequest {
  worker: Worker;
  vmPtr: number;
  forcePrepare?: boolean;
  transferFrom?: ProcessHandle;
  preserveMemoryBytes?: number;
  preserveMemoryRanges?: ThreadWorkerMemoryPreserveRange[];
}

export interface KernelShareGroup {
  members: Set<ProcessHandle>;
  sharedOfdIds: Set<number>;
  ofdSlotSnapshots: Map<number, Uint8Array>;
  forkSharedSyncPromise: Promise<void> | null;
}

export type KernelSyncHandle = Pick<
  ProcessHandle,
  "memory" | "kernel" | "kernelState" | "pid" | "kernelWorkerRequest"
>;

export type KernelOwnershipHandle = Pick<ProcessHandle, "pid" | "kernelWorkerRequest">;

export interface CreateProcessOptions {
  runtimeId?: number;
  pid?: number;
  parentPid?: number | null;
  guestMemoryPolicyLimitBytes?: GuestMemoryPolicyLimitInput;
  maxMemoryPages?: number;
  forkMaxMemoryPages?: number;
  stackSize?: number;
  argv?: string[];
  env?: string[];
  cwd?: string;
  preloadEntries?: FsSnapshotEntry[];
  sabPageCache?: SharedArrayBuffer | null;
  fsRpcRingCapacityBytes?: number;
  kernelWorkerPrivateMemoryBytes?: number;
  artifactMaterializationBudgetBytes?: number;
  hydrateFsFromKernelWorker?: boolean;
  kernelWorkerRequest?: KernelWorkerRequestFn;
  networkBridge?: RuntimeNetworkBridge;
  stdio?: "pipe" | "pty";
  stdin?: "pipe" | "closed";
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
}

export type KernelWorkerMemfsSeedMode = "full" | "metadata" | "none";
export type KernelWorkerPipeSeedMode = "canonical" | "process";
