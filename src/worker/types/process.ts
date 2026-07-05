import type {
  KernelExports,
  KernelRuntimeState,
} from "../../abi.js";
import type { RuntimeStateBridge } from "../../bridge/runtime-state.js";
import type { RuntimeNetworkBridge } from "../../bridge/network.js";
import type { FsSnapshotEntry } from "../../fs/types.js";
import type {
  GuestMemoryPolicyLimitInput,
} from "../memory-policy.js";
import type {
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
  workers: Set<Worker>;
  exitInProgress?: boolean;
  onExit?: (code: number) => void;
  onError?: (error: Error) => void;
  onStdout?: (data: Uint8Array) => void;
  workerDebugLog: string[];
  workerMessageDebugStats: WorkerMessageDebugStats;
  transferDebugStats: TransferDebugStats;
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
