import { expect, test, vi } from "vite-plus/test";
import { handleOwnerAsyncStatusReturn } from "../../../../src/worker/message-handler/status/owner-async.js";
import { makeHandle, makeKernel, makeStatusMessage, makeThreadState } from "./helpers.js";

test("owner-async status return clears the running generation and records the status", () => {
  const worker = {} as Worker;
  const appendWorkerDebugLog = vi.fn();
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
    } as never,
  });

  expect(handled).toBe(true);
  expect(handle.runningWorker).toBe(null);
  expect(thread.runningGeneration).toBe(null);
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
