import { hostExecutionState } from "../../../../src/worker/host-execution-state.js";
import { expect, test, vi } from "vite-plus/test";

vi.mock("../../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerSchedulerQueueHelpers } from "../../../../src/worker/host-boundary/thread-worker-queue.js";
import { getThreadWaitingInWorkerStatus } from "../../../../src/worker/thread-host-state.js";
import { getThreadActivityState } from "../../../../src/worker/thread-activity-state.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
  makeHostBoundaryThread,
} from "./helpers.js";

function makeQueue(threadWorkerStates: WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>, debugEvents: string[]) {
  return createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    clearHostBlockedRetryTimer: () => {},
    takeThreadWorkerBlockedResume: () => false,
    isSupervisorFutexWaiter: (handle, worker) => {
      const thread = threadWorkerStates.get(worker);
      return thread !== undefined
        && handle.kernel.thread_status(thread.vmPtr) === handle.kernel.abi_status_futex_wait();
    },
    completeProcessExit: async () => {},
    failProcess: (_handle, error) => {
      throw error;
    },
    handleWorkerMessage: () => {},
    threadWorkerStates,
  });
}

test("host boundary consumes only supervisor-selected futex resume records", () => {
  const selectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const unselectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const selectedThread = makeHostBoundaryThread(0x1234);
  const unselectedThread = makeHostBoundaryThread(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const kernel = makeHostBoundaryKernel();
  let resumePolled = false;
  kernel.thread_status = (vmPtr: number) => vmPtr === selectedThread.vmPtr
    ? kernel.abi_status_ok()
    : kernel.abi_status_futex_wait();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_futex_wake = () => 1;
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    if (resumePolled) {
      return 0;
    }
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 1, true);
    view.setBigUint64(16, 7n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0xfeedn, true);
    resumePolled = true;
    return 48;
  });
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    expect(vmPtr).toBe(0x1234);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 2, true);
    view.setBigUint64(16, 8n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0xfeedn, true);
    return 48;
  });
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  handle.workers.add(unselectedWorker);
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(2);
  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(getThreadWaitingInWorkerStatus(selectedThread as never)).toBeNull();
  expect(getThreadWaitingInWorkerStatus(unselectedThread as never)).toBeNull();
  expect(hostExecutionState(handle).runningWorker).toBe(selectedWorker);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-resume:futex-wake:observed:0x1234:tid=2:seq=7:process=0x2000:detail=0xfeed:status=0",
  );
});

test("host boundary consumes process-scoped supervisor runnable resume decisions", () => {
  const selectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const unselectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const selectedThread = makeHostBoundaryThread(0x1234);
  const unselectedThread = makeHostBoundaryThread(0x5678);
  getThreadActivityState(unselectedThread).lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    expect(vmPtr).toBe(0x1234);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 2, true);
    view.setBigUint64(16, 13n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 1n, true);
    return 48;
  });
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  handle.workers.add(unselectedWorker);
  hostExecutionState(handle).vforkReadyWorkers = [unselectedWorker];
  hostExecutionState(handle).hostBlockedWorkers.set(selectedWorker, {
    vmPtr: selectedThread.vmPtr,
    status: kernel.abi_status_futex_wait(),
  });
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_process_resume).toHaveBeenCalledTimes(1);
  expect(hostExecutionState(handle).runningWorker).toBe(selectedWorker);
  expect(hostExecutionState(handle).vforkReadyWorkers).toEqual([unselectedWorker]);
  expect(hostExecutionState(handle).hostBlockedWorkers.has(selectedWorker)).toBe(false);
  expect(getThreadWaitingInWorkerStatus(selectedThread as never)).toBeNull();
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:selected:0x1234:tid=2:seq=13:process=0x2000:status=0:detail=0x1",
  );
});

test("host boundary includes supervisor thread snapshot in resume diagnostics", () => {
  const selectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const selectedThread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((_vmPtr: number, _outPtr: number, _outLen: number) => {
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 2, true);
    view.setBigUint64(16, 21n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0n, true);
    return 48;
  });
  kernel.supervisor_thread_state_snapshot_bytes = () => 64;
  kernel.supervisor_thread_state_snapshot = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
    expect(outLen).toBe(64);
    const view = new DataView(kernel.memory.buffer, outPtr, outLen);
    view.setUint32(0, 0x4854_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 64, true);
    view.setUint32(12, 1, true);
    view.setUint32(16, 0x2000, true);
    view.setUint32(20, selectedThread.vmPtr, true);
    view.setUint32(24, 32, true);
    view.setUint32(32, 2, true);
    view.setUint32(36, 2, true);
    view.setUint32(40, selectedThread.vmPtr, true);
    view.setUint32(44, kernel.abi_status_ok(), true);
    return 64;
  });
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  hostExecutionState(handle).vforkReadyWorkers = [selectedWorker];
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_thread_state_snapshot).toHaveBeenCalledWith(0x1234, 0, 64);
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:selected:0x1234:tid=2:seq=21:process=0x2000:status=0:detail=0x0"
      + ":snapshot=process=0x2000:owner=0x1234:threads=2/2/0x1234/0/0x0",
  );
});
