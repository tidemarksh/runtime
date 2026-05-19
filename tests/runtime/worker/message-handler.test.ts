import { expect, test, vi } from "vite-plus/test";
import { createWorkerMessageHandler } from "../../../src/worker/message-handler.js";
import type { ThreadWorkerOutbound } from "../../../src/messages.js";
import type { WorkerMessageHandlerDeps } from "../../../src/worker/message-handler/types.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../src/worker/types.js";

function makeThreadState(overrides: Partial<ThreadWorkerState> = {}): ThreadWorkerState {
  return {
    vmPtr: 0x1234,
    ownerPid: 3,
    generation: 2,
    reinitCount: 0,
    stepCount: 0,
    totalStepBudget: 0,
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
    initialized: true,
    waitingInWorkerStatus: null,
    resumeWorkersOnReady: [],
    ...overrides,
  };
}

function makeDeps(
  threadWorkerStates: WeakMap<Worker, ThreadWorkerState>,
  appendWorkerDebugLog: ReturnType<typeof vi.fn>,
  failProcess: ReturnType<typeof vi.fn>,
): WorkerMessageHandlerDeps {
  const processHandles = new Map<number, ProcessHandle>();
  return {
    processHandles,
    threadWorkerStates,
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 0,
    syncEffectSocketState: 0,
    syncEffectEpollState: 0,
    syncEffectEpollDependentOfdState: 0,
    syncEffectProcessLifecycle: 0,
    syncEffectSignalWaiters: 0,
    syncEffectForkSharedOfdState: 0,
    syncEffectProcessMemfs: 0,
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog,
    assertProcessStandardFdIntegrity: () => {},
    attachThreadWorker: () => {},
    blockWorker: () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    captureVforkSnapshot: () => ({
      vmPtr: 0,
      childPid: null,
      nextPid: null,
      pgid: null,
      sid: null,
      inheritedSharedOfdIds: [],
    }),
    clearVforkSnapshot: () => {},
    clearBlockedWorker: () => {},
    completeProcessExit: async () => {},
    commitNetworkConnect: async () => {},
    enqueueWorker: () => {},
    failProcess,
    handleExecveSpawn: async () => {},
    kickBlockedWorkers: () => {},
    pumpProcess: () => {},
    pruneDetachedProcessWorkers: () => {},
    pumpNetworkConnections: async () => false,
    requestEpollDependentOfdStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    resolveNetworkConnect: async () => 0n,
    resumeConnectViaKernelWorker: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    statusDefersOwnedStateSync: () => false,
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    syncProcessMemfsViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
  };
}

test("stale generated messages are dropped before transferred worker ownership is checked", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState());
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const handle = {
    pid: 1,
    exitInProgress: false,
    workerDebugLog: [],
    workers: new Set<Worker>(),
  } as unknown as ProcessHandle;
  deps.processHandles.set(1, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, worker, {
    type: "status",
    generation: 1,
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0x1000n,
    vmPtr: 0x1234,
    kernelState: {} as ThreadWorkerOutbound extends { kernelState: infer State } ? State : never,
  } as ThreadWorkerOutbound);

  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "stale-message:0x1234:status:1->2",
  );
});

test("status from a removed process state is dropped as stale", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>();
  threadWorkerStates.set(worker, makeThreadState({ ownerPid: 4 }));
  const appendWorkerDebugLog = vi.fn();
  const failProcess = vi.fn();
  const deps = makeDeps(threadWorkerStates, appendWorkerDebugLog, failProcess);
  const handle = {
    pid: 4,
    exitInProgress: false,
    workerDebugLog: [],
    workers: new Set<Worker>(),
    vforkSnapshot: null,
    kernel: {
      abi_status_fault: () => 31,
      abi_status_vfork_wait: () => 11,
      abi_status_execve_spawn: () => 13,
      process_current_pid: () => 0,
      thread_process_thread_count: () => 0,
      thread_process_ptr: () => 0x48000,
      process_current_ppid: () => 0,
    },
  } as unknown as ProcessHandle;
  deps.processHandles.set(4, handle);
  const handleWorkerMessage = createWorkerMessageHandler(deps);

  handleWorkerMessage(handle, worker, {
    type: "status",
    generation: 2,
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0x1000n,
    vmPtr: 0x1234,
    kernelState: {} as ThreadWorkerOutbound extends { kernelState: infer State } ? State : never,
  } as ThreadWorkerOutbound);

  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "stale-process-status:0x1234:0:pid=4",
  );
});
