import { expect, test, vi } from "vite-plus/test";
import { handleOwnerAsyncStatusReturn } from "../../../../src/worker/message-handler/status/owner-async.js";
import { getThreadOwnerAsyncStatus } from "../../../../src/worker/thread-host-state.js";
import { makeHandle, makeKernel, makeStatusMessage, makeThreadState } from "./helpers.js";
import { writeSupervisorFaultExitDecisionRecord } from "../support/supervisor-abi.js";

test("owner-async status return clears the running generation and records the status", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const pumpProcess = vi.fn();
  const handle = makeHandle({
    runningWorker: worker,
    kernel: makeKernel({
      abi_status_fs: () => 4,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 4,
    runningGeneration: 1,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 4,
      generation: 1,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
      pumpProcess,
    } as never,
  });

  expect(handled).toBe(true);
  expect(handle.runningWorker).toBe(null);
  expect(thread.runningGeneration).toBe(null);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async:0x1234:owner=4:status=4:ownerMatch=1",
  );
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(1);
});

test("owner-async status return does not apply ownership payloads for another process", () => {
  const worker = {} as Worker;
  const setFdEntry = vi.fn();
  const handle = makeHandle({
    kernel: makeKernel({
      process_set_fd_entry: setFdEntry,
    }),
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: makeThreadState({ ownerAsyncStatus: 4 }) as never,
    msg: makeStatusMessage({
      status: 4,
      fdEntries: [{ fd: 3, ofdId: 5, fdFlags: 0 }],
    }) as never,
    statusBelongsToHandle: false,
    deps: {
      appendWorkerDebugLog: () => {},
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
    } as never,
  });

  expect(handled).toBe(true);
  expect(setFdEntry).not.toHaveBeenCalled();
});

test("owner-async STATUS_OK return seeds recovery pump while async remains pending", () => {
  const worker = {} as Worker;
  const pumpProcess = vi.fn();
  const handle = makeHandle({
    kernel: makeKernel({
      abi_status_ok: () => 0,
      abi_status_fs: () => 4,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 0,
    runningGeneration: null,
    stepCount: 8,
    statusCount: 8,
    lastStatus: 0,
    lastStatusAtMs: 7_000,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 0,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog: () => {},
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
      pumpProcess,
    } as never,
  });

  expect(handled).toBe(true);
  expect(getThreadOwnerAsyncStatus(thread as never)).toBe(0);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(1);
});

test("owner-async STATUS_FS return seeds blocking recovery pump while async remains pending", () => {
  const worker = {} as Worker;
  const pumpProcess = vi.fn();
  const handle = makeHandle({
    runningWorker: null,
    kernel: makeKernel({
      abi_status_fs: () => 6,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: null,
    stepCount: 32,
    statusCount: 33,
    lastStatus: 6,
    lastStatusAtMs: 8_950,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 6,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog: () => {},
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
      pumpProcess,
    } as never,
  });

  expect(handled).toBe(true);
  expect(getThreadOwnerAsyncStatus(thread as never)).toBe(6);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(1);
});

test("owner-async STATUS_FS return clears stale blocked wait4 rpc for the same worker", () => {
  const worker = {} as Worker;
  const pumpProcess = vi.fn();
  const clearBlockedWorker = vi.fn((handleArg: ReturnType<typeof makeHandle>, workerArg: Worker) => {
    handleArg.hostBlockedWorkers.delete(workerArg);
  });
  const appendWorkerDebugLog = vi.fn();
  const handle = makeHandle({
    runningWorker: null,
    kernel: makeKernel({
      abi_status_fs: () => 6,
    }),
  });
  handle.hostBlockedWorkers.set(worker, {
    vmPtr: 0x1234,
    status: 31,
    rpcPending: true,
    rpcGeneration: 3,
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: null,
    stepCount: 32,
    statusCount: 33,
    lastStatus: 6,
    lastStatusAtMs: 8_950,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 6,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      clearBlockedWorker,
      failProcess: (_handle: unknown, error: Error) => { throw error; },
      pumpProcess,
    } as never,
  });

  expect(handled).toBe(true);
  expect(clearBlockedWorker).toHaveBeenCalledWith(handle, worker);
  expect(handle.hostBlockedWorkers.has(worker)).toBe(false);
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async-clear-stale-blocked-rpc:0x1234:blocked=31:status=6",
  );
  expect(pumpProcess).toHaveBeenCalledWith(handle);
});

test("owner-async STATUS_FS return preserves matching blocked FS rpc", () => {
  const worker = {} as Worker;
  const pumpProcess = vi.fn();
  const clearBlockedWorker = vi.fn();
  const handle = makeHandle({
    runningWorker: null,
    kernel: makeKernel({
      abi_status_fs: () => 6,
    }),
  });
  const blocked = {
    vmPtr: 0x1234,
    status: 6,
    rpcPending: true,
    rpcGeneration: 3,
  };
  handle.hostBlockedWorkers.set(worker, blocked);
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: null,
    stepCount: 32,
    statusCount: 33,
    lastStatus: 6,
    lastStatusAtMs: 8_950,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 6,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog: () => {},
      appendRuntimeLifecycleEvent: () => {},
      clearBlockedWorker,
      failProcess: (_handle: unknown, error: Error) => { throw error; },
      pumpProcess,
    } as never,
  });

  expect(handled).toBe(true);
  expect(clearBlockedWorker).not.toHaveBeenCalled();
  expect(handle.hostBlockedWorkers.get(worker)).toBe(blocked);
  expect(pumpProcess).toHaveBeenCalledWith(handle);
});

test("owner-async STATUS_FS from an unanswered running step routes to transition", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const handle = makeHandle({
    runningWorker: worker,
    kernel: makeKernel({
      abi_status_fs: () => 6,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: 1,
    stepCount: 3,
    statusCount: 2,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 6,
      generation: 1,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
    } as never,
  });

  expect(handled).toBe(false);
  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(1);
  expect(getThreadOwnerAsyncStatus(thread as never)).toBeNull();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async-stale-unanswered-fs:0x1234:status=6",
  );
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(0);
});

test("owner-async STATUS_FS from an unanswered non-running step routes to transition", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const otherWorker = {} as Worker;
  const handle = makeHandle({
    runningWorker: otherWorker,
    kernel: makeKernel({
      abi_status_fs: () => 6,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: 1,
    stepCount: 3,
    statusCount: 2,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 6,
      generation: 1,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
    } as never,
  });

  expect(handled).toBe(false);
  expect(handle.runningWorker).toBe(otherWorker);
  expect(thread.runningGeneration).toBe(1);
  expect(getThreadOwnerAsyncStatus(thread as never)).toBeNull();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async-stale-unanswered-fs:0x1234:status=6",
  );
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(0);
});

test("owner-async generationless STATUS_FS from an unanswered step routes to transition", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const handle = makeHandle({
    runningWorker: null,
    kernel: makeKernel({
      abi_status_fs: () => 6,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: null,
    stepCount: 65,
    statusCount: 58,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 6,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
    } as never,
  });

  expect(handled).toBe(false);
  expect(handle.runningWorker).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  expect(getThreadOwnerAsyncStatus(thread as never)).toBeNull();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async-stale-unanswered-fs:0x1234:status=6",
  );
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(0);
});

test("owner-async STATUS_OK from an unanswered step routes to transition", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const handle = makeHandle({
    runningWorker: null,
    kernel: makeKernel({
      abi_status_ok: () => 0,
      abi_status_fs: () => 6,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 0,
    runningGeneration: null,
    stepCount: 79,
    statusCount: 76,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 0,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
    } as never,
  });

  expect(handled).toBe(false);
  expect(handle.runningWorker).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  expect(getThreadOwnerAsyncStatus(thread as never)).toBeNull();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async-stale-unanswered-ok:0x1234:status=0",
  );
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(0);
});

test("owner-async STATUS_OK return completes a supervisor-selected faulted process before preserving stale OK", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const lifecycle: string[] = [];
  const completeProcessExit = vi.fn(async () => {});
  const failProcess = vi.fn();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const handle = makeHandle({
    runningWorker: worker,
    kernel: makeKernel({
      memory,
      abi_status_ok: () => 0,
      abi_status_fault: () => 3,
      abi_status_fs: () => 6,
      kernel_heap_ptr: () => 0,
      thread_status: () => 3,
      supervisor_abi_version: () => 1,
      supervisor_fault_exit_decision_bytes: () => 88,
      supervisor_fault_exit_decision_magic: () => 0x5846_5354,
      supervisor_fault_exit_kind_thread_exit: () => 1,
      supervisor_fault_exit_kind_process_exit: () => 2,
      supervisor_fault_exit_decision: vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
        expect(vmPtr).toBe(0x1234);
        expect(outLen).toBe(88);
        writeSupervisorFaultExitDecisionRecord(memory, outPtr, {
          vmPtr,
          exitVmPtr: 0x1234,
          processPtr: 0x5000,
          remainingThreads: 1,
          liveThreads: 0,
          exitCode: 13,
          termSignal: 0,
          status: 3,
        });
        return 88;
      }),
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 0,
    runningGeneration: 4,
    stepCount: 9,
    statusCount: 8,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 0,
      generation: 4,
      exitCode: 0,
      termSignal: 0,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: (_runtimeId: number, event: string) => {
        lifecycle.push(event);
      },
      completeProcessExit,
      failProcess,
    } as never,
  });

  expect(handled).toBe(true);
  expect(handle.runningWorker).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  expect(getThreadOwnerAsyncStatus(thread as never)).toBeNull();
  expect(completeProcessExit).toHaveBeenCalledWith(handle, 13, 0, 0x1234);
  expect(failProcess).not.toHaveBeenCalled();
  expect(appendWorkerDebugLog).toHaveBeenCalledWith(
    handle,
    "status-owner-async-ok-fault-complete:0x1234:exitVm=0x1234:decision=1:exit=13:0:threads=1",
  );
  expect(lifecycle).toContain(
    "complete-exit-request:owner-async-ok-fault:7:vm=0x1234:exitVm=0x1234:decision=1:exit=13:0:threads=1",
  );
  expect(handle.workerMessageDebugStats.ownerAsyncStatusReturns).toBe(0);
});

test("owner-async status return does not consume a fault from resumed guest execution", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
  const handle = makeHandle({
    runningWorker: worker,
    kernel: makeKernel({
      abi_status_fault: () => 3,
      abi_status_fs: () => 6,
    }),
  });
  const thread = makeThreadState({
    ownerAsyncStatus: 6,
    runningGeneration: 1,
  });

  const handled = handleOwnerAsyncStatusReturn({
    handle: handle as never,
    worker,
    thread: thread as never,
    msg: makeStatusMessage({
      status: 3,
      generation: 1,
    }) as never,
    statusBelongsToHandle: true,
    deps: {
      appendWorkerDebugLog,
      appendRuntimeLifecycleEvent: () => {},
      failProcess: (_handle: unknown, error: Error) => { throw error; },
    } as never,
  });

  expect(handled).toBe(false);
  expect(handle.runningWorker).toBe(worker);
  expect(thread.runningGeneration).toBe(1);
  expect(appendWorkerDebugLog).not.toHaveBeenCalled();
});
