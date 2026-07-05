import type {
  KernelExports,
  KernelRuntimeState,
} from "../../abi.js";
import type { RuntimeStateBridge } from "../../bridge/runtime-state.js";
import type { RuntimeNetworkBridge, RuntimeNetworkConnection } from "../../bridge/network.js";
import type { FsSnapshotEntry } from "../../fs/types.js";
import type {
  GuestMemoryPolicyLimitInput,
} from "../memory-policy.js";
import type {
  GuestWriteWatchRegion,
  KernelWorkerOutbound,
  KernelWorkerRequest,
  ThreadWorkerMemoryPreserveRange,
} from "../../messages.js";
import type {
  TransferDebugStats,
  WorkerMessageDebugStats,
} from "./debug.js";

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

// Host-executor parking state for non-futex host effects that still need
// TypeScript I/O/RPC completion. Supervisor-owned futex/sleep waits must not
// create entries here.
export interface HostBlockedWorkerState {
  vmPtr: number;
  status: number;
}

export interface RuntimeFsState {
  version: number;
  entries: FsSnapshotEntry[];
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
  processMemfsDirty: boolean;
  processMemfsPublishPending: boolean;
  processMemfsDeletedPaths?: Set<string>;
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
