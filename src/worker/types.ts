import type {
  KernelExports,
  KernelRuntimeState,
} from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type {
  KernelWorkerOutbound,
  ForkStackSnapshot,
  GuestWriteWatchRegion,
  KernelWorkerRequest,
  KernelWorkerPipeSlotSnapshot,
  GuestMemoryWriteSnapshot,
  ThreadWorkerMemoryPreserveRange,
} from "../messages.js";
import type { RuntimeStateBridge } from "../bridge/runtime-state.js";
import type { RuntimeNetworkBridge, RuntimeNetworkConnection } from "../network.js";

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
  statusCount: number;
  statusCounts: Record<string, number>;
  lastStatus: number | null;
  lastStepAtMs: number | null;
  lastStatusAtMs: number | null;
  lastStepProcessPtr: number | null;
  lastStepPc: bigint | null;
  lastStepSp: bigint | null;
  lastStepS11: bigint | null;
  hotStepPcs: Record<string, number>;
  lastStatusPc: bigint | null;
  lastStatusSp: bigint | null;
  lastStatusS11: bigint | null;
  forkStackSnapshot?: ForkStackSnapshot;
  execveProcessState?: Uint8Array;
  hotStatusPcs: Record<string, number>;
  initialized: boolean;
  waitingInWorkerStatus: number | null;
  ownerAsyncStatus: number | null;
  resumeBlockedOnNextStep: boolean;
  pendingPipeSlotsForNextStep: KernelWorkerPipeSlotSnapshot[];
  pendingGuestMemoryWrites: GuestMemoryWriteSnapshot[];
  resumeWorkersOnReady: Worker[];
}

export interface BlockedWorkerState {
  vmPtr: number;
  status: number;
  retryTimer: ReturnType<typeof setTimeout> | null;
  timeoutExpired: boolean;
  timeoutDeadlineMs: number | null;
  sleepDeadlineTicks: bigint | null;
  rpcPending: boolean;
  rpcGeneration: number;
  skipNextNetworkPump: boolean;
  pipeSyncedBeforePassive: boolean;
  pipeChildExitResumeKey: string | null;
  pipeEofChildExitDeferrals: number;
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
  };
}

export interface CanonicalizedChildLifecycle {
  childPid: number;
  nextPid: number;
  pgid: number;
  sid: number;
}

export type KernelWorkerRequestFn = (msg: KernelWorkerRequest) => Promise<KernelWorkerOutbound>;

export interface ProcessHandle {
  runtimeId: number;
  pid: number;
  parentPid: number | null;
  vmPtr: number;
  ramSize: number;
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
  readyWorkers: Worker[];
  runningWorker: Worker | null;
  blockedWorkers: Map<Worker, BlockedWorkerState>;
  pendingChildExitRecords: number;
  pendingChildSpawnRecords: number;
  childExitSyncRecords: Map<number, number>;
  pipeSyncPromise: Promise<void> | null;
  pipeSyncQueued: boolean;
  pipeWaiterWakeQueued: boolean;
  pendingPipeSlotSnapshots: KernelWorkerPipeSlotSnapshot[];
  epollSyncPromise: Promise<void> | null;
  epollSyncQueued: boolean;
  epollWaiterWakeQueued: boolean;
  forkSharedSyncPromise: Promise<void> | null;
  processMemfsDirty: boolean;
  regularFileOfdOffsets?: Map<number, { ino: number; hostFd: number; offset: bigint }>;
  ofdSyncPromise: Promise<void> | null;
  ofdSyncQueuedIds: Set<number>;
  socketSyncPromise: Promise<void> | null;
  socketSyncQueued: boolean;
  socketWaiterWakeQueued: boolean;
  vforkSnapshot: VforkSnapshot | null;
  vforkWorker: Worker | null;
  exitInProgress?: boolean;
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
  workerDebugLog: string[];
  transferDebugStats: TransferDebugStats;
  guestWriteWatchRegions: GuestWriteWatchRegion[];
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
  ramSize?: number;
  maxMemoryPages?: number;
  forkMaxMemoryPages?: number;
  stackSize?: number;
  argv?: string[];
  env?: string[];
  cwd?: string;
  preloadEntries?: FsSnapshotEntry[];
  sabPageCache?: SharedArrayBuffer | null;
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
