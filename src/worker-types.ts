import type {
  KernelExports,
  KernelRuntimeState,
} from "./abi.js";
import type { FsSnapshotEntry } from "./fs-types.js";
import type {
  KernelWorkerOutbound,
  KernelWorkerRequest,
} from "./messages.js";
import type { RuntimeStateBridge } from "./runtime-state-bridge.js";

export interface SharedHostStdio {
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  refCount: number;
}

export interface VforkSnapshot {
  vmPtr: number;
  childPid: number | null;
  nextPid: number | null;
  pgid: number | null;
  sid: number | null;
  inheritedSharedOfdIds: number[];
}

export interface ThreadWorkerState {
  vmPtr: number;
  initialized: boolean;
  waitingInWorkerStatus: number | null;
}

export interface BlockedWorkerState {
  vmPtr: number;
  status: number;
  retryTimer: ReturnType<typeof setTimeout> | null;
  timeoutExpired: boolean;
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
  guestBase: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  wasmModule: WebAssembly.Module;
  kernel: KernelExports;
  kernelState: KernelRuntimeState;
  stdinPipe: number;
  stdoutPipe: number;
  stderrPipe: number;
  hostStdio: SharedHostStdio;
  kernelShare: KernelShareGroup;
  stdinQueue: Uint8Array[];
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
  pipeSyncPromise: Promise<void> | null;
  pipeSyncQueued: boolean;
  pipeWaiterWakeQueued: boolean;
  epollSyncPromise: Promise<void> | null;
  epollSyncQueued: boolean;
  epollWaiterWakeQueued: boolean;
  ofdSyncPromise: Promise<void> | null;
  ofdSyncQueuedIds: Set<number>;
  socketSyncPromise: Promise<void> | null;
  socketSyncQueued: boolean;
  socketWaiterWakeQueued: boolean;
  vforkSnapshot: VforkSnapshot | null;
  vforkWorker: Worker | null;
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
  workerDebugLog: string[];
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
