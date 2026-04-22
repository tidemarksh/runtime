import type {
  KernelExports,
  KernelRuntimeState,
} from "../abi.js";
import type { FsSnapshotEntry } from "../fs/types.js";
import type {
  KernelWorkerOutbound,
  GuestWriteWatchRegion,
  KernelWorkerRequest,
} from "../messages.js";
import type { RuntimeStateBridge } from "../bridge/runtime-state.js";

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
  reinitCount: number;
  lastStepProcessPtr: number | null;
  lastStepPc: bigint | null;
  lastStepSp: bigint | null;
  lastStepS11: bigint | null;
  initialized: boolean;
  waitingInWorkerStatus: number | null;
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
}

export interface RuntimeFsState {
  version: number;
  entries: FsSnapshotEntry[];
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
  workers: Set<Worker>;
  readyWorkers: Worker[];
  runningWorker: Worker | null;
  blockedWorkers: Map<Worker, BlockedWorkerState>;
  pendingChildExitRecords: number;
  pendingChildSpawnRecords: number;
  pipeSyncPromise: Promise<void> | null;
  pipeSyncQueued: boolean;
  pipeWaiterWakeQueued: boolean;
  epollSyncPromise: Promise<void> | null;
  epollSyncQueued: boolean;
  epollWaiterWakeQueued: boolean;
  forkSharedSyncPromise: Promise<void> | null;
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
  guestWriteWatchRegions: GuestWriteWatchRegion[];
}

export interface AttachThreadWorkerRequest {
  worker: Worker;
  vmPtr: number;
  forcePrepare?: boolean;
  transferFrom?: ProcessHandle;
}

export interface KernelShareGroup {
  members: Set<ProcessHandle>;
  sharedOfdIds: Set<number>;
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
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
}

export type KernelWorkerMemfsSeedMode = "full" | "metadata" | "none";
export type KernelWorkerPipeSeedMode = "canonical" | "process";
