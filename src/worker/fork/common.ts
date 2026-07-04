import { createRuntimeStateBridge } from "../../bridge/runtime-state.js";
import { createInheritedErrorSink, retainSharedHostStdio } from "../stdio.js";
import type { KernelExports } from "../../abi.js";
import { createTransferDebugStats, createWorkerMessageDebugStats } from "../types.js";
import type { ProcessHandle } from "../types.js";
import type { WorkerForkDeps } from "./types.js";
export { normalizeGuestMemoryPolicyLimitBytes } from "../memory-policy.js";

interface CreateForkChildHandleArgs {
  deps: WorkerForkDeps;
  parent: ProcessHandle;
  childPid: number;
  childPpid: number;
  vmPtr: number;
  worker: Worker;
  memory: WebAssembly.Memory;
  kernel: KernelExports;
  kernelState: ProcessHandle["kernelState"];
  guestMemoryPolicyLimitBytes: bigint;
  maxMemoryPages: number;
  debugCommand: string;
  workerDebugLabel: string;
}

export function createForkChildHandle({
  deps,
  parent,
  childPid,
  childPpid,
  vmPtr,
  worker,
  memory,
  kernel,
  kernelState,
  guestMemoryPolicyLimitBytes,
  maxMemoryPages,
  debugCommand,
  workerDebugLabel,
}: CreateForkChildHandleArgs): ProcessHandle {
  return {
    runtimeId: parent.runtimeId,
    pid: childPid,
    parentPid: childPpid,
    vmPtr,
    guestMemoryPolicyLimitBytes,
    maxMemoryPages,
    forkMaxMemoryPages: parent.forkMaxMemoryPages,
    guestBase: parent.guestBase,
    worker,
    memory,
    wasmModule: parent.wasmModule,
    kernel,
    kernelState,
    stdinPipe: kernel.process_stdin_pipe(vmPtr),
    stdoutPipe: kernel.process_stdout_pipe(vmPtr),
    stderrPipe: kernel.process_stderr_pipe(vmPtr),
    ttyPtySlot: kernel.process_tty_pty_slot?.(vmPtr) ?? parent.ttyPtySlot,
    hostStdio: retainSharedHostStdio(parent.hostStdio),
    stdinClosed: parent.stdinClosed,
    stdioOwner: parent,
    kernelShare: parent.kernelShare,
    stdinQueue: [],
    stdinFlushPromise: null,
    outputBacklog: [],
    debugCommand,
    lastDebugMessage: undefined,
    runtimeBridge: createRuntimeStateBridge(),
    sabPageCache: parent.sabPageCache,
    kernelWorkerRequest: parent.kernelWorkerRequest,
    networkBridge: parent.networkBridge,
    networkConnections: new Map(parent.networkConnections),
    networkConnectionOfds: new Map(parent.networkConnectionOfds),
    networkOfdSlotWatermarks: new Map(
      [...(parent.networkOfdSlotWatermarks ?? new Map())].map(([ofdId, slot]) => [ofdId, slot.slice()]),
    ),
    networkPendingConnectFds: new Set(parent.networkPendingConnectFds),
    networkClosedFds: new Set(parent.networkClosedFds),
    networkClosedSocketSlots: new Map(
      [...parent.networkClosedSocketSlots].map(([fd, slot]) => [fd, {
        offset: slot.offset,
        data: slot.data.slice(),
      }]),
    ),
    networkSocketSlotWatermarks: new Map(
      [...parent.networkSocketSlotWatermarks].map(([fd, slot]) => [fd, {
        offset: slot.offset,
        data: slot.data.slice(),
      }]),
    ),
    networkRecvHeadWatermarks: new Map(parent.networkRecvHeadWatermarks),
    networkRecvTailWatermarks: new Map(parent.networkRecvTailWatermarks),
    networkRecvPayloadWatermarks: new Map(
      [...parent.networkRecvPayloadWatermarks].map(([fd, snapshot]) => [fd, {
        recvBufIdx: snapshot.recvBufIdx,
        head: snapshot.head,
        tail: snapshot.tail,
        capacity: snapshot.capacity,
        readBytes: snapshot.readBytes,
        availableBytes: snapshot.availableBytes,
        deliveredBytes: snapshot.deliveredBytes,
        data: snapshot.data.slice(),
      }]),
    ),
    workers: new Set(),
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    pendingChildSpawnRecords: 0,
    pendingPipeSlotSnapshots: [],
    forkSharedSyncPromise: null,
    processMemfsDirty: false,
    processMemfsPublishPending: false,
    processMemfsDeletedPaths: new Set(),
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    vforkSnapshot: null,
    vforkWorker: null,
    exitInProgress: false,
    onError: createInheritedErrorSink(
      parent,
      childPid,
      (pid) => deps.processHandles.get(pid),
      deps.failProcess,
    ),
    workerDebugLog: [],
    workerMessageDebugStats: createWorkerMessageDebugStats(),
    transferDebugStats: createTransferDebugStats(),
    guestWriteWatchRegions: parent.guestWriteWatchRegions.map((region) => ({ ...region })),
    ...(parent.memoryOwnership ? { memoryOwnership: { ...parent.memoryOwnership } } : {}),
  };
}

export function detachInheritedHostStdioFromChild(child: ProcessHandle): void {
  if (child.stdioOwner === null) {
    return;
  }
  child.kernel.process_set_host_stdio(child.vmPtr, 0, 0, 0);
}
