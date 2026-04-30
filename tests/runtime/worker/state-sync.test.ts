import { expect, test, vi } from "vite-plus/test";
import { writePipe } from "../../../src/bridge/pipe.js";
import { createWorkerSchedulerStateSyncHelpers } from "../../../src/worker/scheduler/state-sync.js";

function makeHandle(overrides: Record<string, unknown> = {}) {
  let runtimeState = {
    heapBase: 0,
    heapPtr: 0,
    ofdTablePtr: 0,
    memfsPtr: 0,
    memStatePtr: 0,
    epollTablePtr: 0,
    socketTablePtr: 0,
    socketUsed: 0,
    pipeBase: 0,
    pipeUsed: 7n,
    netBufBase: 0,
    netBufUsed: 0n,
    sigSlotsPtr: 0,
    processPendingPtr: 0,
    processRuntimeStatePtr: 0,
    timeStatePtr: 0,
  };
  return {
    pid: 1,
    kernelWorkerRequest: vi.fn(async () => ({
      type: "pipe-state-synced",
      kernelState: runtimeState,
    })),
    runtimeBridge: {
      notifySab: new SharedArrayBuffer(12),
      stateSab: new SharedArrayBuffer(176),
    },
    memory: new WebAssembly.Memory({ initial: 2, maximum: 2, shared: true }),
    hostStdio: {
      stdinPipe: 0,
      stdoutPipe: 2,
      stderrPipe: 3,
      refCount: 1,
    },
    outputBacklog: [],
    onStdout: null,
    kernel: {
      abi_max_fds: () => 0,
      abi_max_threads: () => 0,
      kernel_attach_runtime_state(
        heapBase: number,
        heapPtr: number,
        ofdTablePtr: number,
        memfsPtr: number,
        memStatePtr: number,
        epollTablePtr: number,
        socketTablePtr: number,
        socketUsed: number,
        pipeBase: number,
        pipeUsed: bigint,
        netBufBase: number,
        netBufUsed: bigint,
        sigSlotsPtr: number,
        processPendingPtr: number,
        processRuntimeStatePtr: number,
        timeStatePtr: number,
      ) {
        runtimeState = {
          heapBase,
          heapPtr,
          ofdTablePtr,
          memfsPtr,
          memStatePtr,
          epollTablePtr,
          socketTablePtr,
          socketUsed,
          pipeBase,
          pipeUsed,
          netBufBase,
          netBufUsed,
          sigSlotsPtr,
          processPendingPtr,
          processRuntimeStatePtr,
          timeStatePtr,
        };
      },
      kernel_heap_base: () => runtimeState.heapBase,
      kernel_heap_ptr: () => runtimeState.heapPtr,
      kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
      kernel_memfs_ptr: () => runtimeState.memfsPtr,
      kernel_mem_state_ptr: () => runtimeState.memStatePtr,
      kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
      kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
      kernel_socket_used: () => runtimeState.socketUsed,
      kernel_pipe_base: () => runtimeState.pipeBase,
      kernel_pipe_used: () => runtimeState.pipeUsed,
      kernel_net_buf_base: () => runtimeState.netBufBase,
      kernel_net_buf_used: () => runtimeState.netBufUsed,
      kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
      kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
      kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
      kernel_time_state_ptr: () => runtimeState.timeStatePtr,
      abi_status_pipe_wait: () => 19,
      abi_status_epoll_wait: () => 22,
      abi_fd_type_pipe: () => 6,
      process_fd_ofd_id: () => 0,
      kernel_ofd_fd_type: () => 0,
      kernel_ofd_ino: () => 0,
    },
    kernelState: runtimeState,
    stdioOwner: null,
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    blockedWorkers: new Map(),
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    ...overrides,
  } as never;
}

test("requestPipeStateSync drains the visible stdio owner for hidden-child internal sync", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const parent = makeHandle();
  const child = makeHandle({ pid: 2, stdioOwner: parent });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, false);

  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(parent);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
  expect(child.kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(parent.kernelWorkerRequest).toHaveBeenCalledTimes(2);
  expect(retryBlockedWorker).not.toHaveBeenCalled();
});

test("requestPipeStateSync refreshes the visible stdio owner before draining child output for waking sync", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const parent = makeHandle({ pid: 1 });
  const child = makeHandle({ pid: 2, stdioOwner: parent });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, true);

  expect(child.kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(parent.kernelWorkerRequest).toHaveBeenCalledTimes(2);
  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(parent);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync re-syncs the visible owner after self-drain advances the read head", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const handle = makeHandle({ pid: 1, stdioOwner: null });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(handle, false);

  expect(handle.kernelWorkerRequest).toHaveBeenCalledTimes(2);
  expect(drainProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainProcessOutput).toHaveBeenCalledWith(handle);
  expect(drainHiddenProcessOutput).not.toHaveBeenCalled();
});

test("requestPipeStateSync still drains hidden child output when stdout was remapped away from the visible host pipe", async () => {
  const drainProcessOutput = vi.fn(() => true);
  const drainHiddenProcessOutput = vi.fn(() => true);
  const retryBlockedWorker = vi.fn();
  const parent = makeHandle();
  const child = makeHandle({
    pid: 2,
    stdioOwner: parent,
    stdoutPipe: 9,
    stderrPipe: parent.stderrPipe,
  });

  const helpers = createWorkerSchedulerStateSyncHelpers({
    appendRuntimeLifecycleEvent: () => {},
    drainProcessOutput,
    drainHiddenProcessOutput,
    threadWorkerStates: new WeakMap(),
    retryBlockedWorker,
  });

  await helpers.requestPipeStateSync(child, false);

  expect(drainHiddenProcessOutput).toHaveBeenCalledTimes(1);
  expect(drainHiddenProcessOutput).toHaveBeenCalledWith(child);
});
