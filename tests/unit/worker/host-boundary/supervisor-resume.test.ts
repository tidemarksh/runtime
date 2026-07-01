import { expect, test, vi } from "vite-plus/test";

vi.mock("../../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerSchedulerQueueHelpers } from "../../../../src/worker/scheduler/queue.js";
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
  selectedThread.waitingInWorkerStatus = 20;
  unselectedThread.waitingInWorkerStatus = 20;
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
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  handle.workers.add(unselectedWorker);
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(2);
  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(selectedThread.waitingInWorkerStatus).toBeNull();
  expect(unselectedThread.waitingInWorkerStatus).toBe(kernel.abi_status_futex_wait());
  expect(handle.runningWorker).toBe(selectedWorker);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-resume:futex-wake:ready:0x1234:tid=2:seq=7:addr=0xfeed:status=0",
  );
});

test("host boundary asks supervisor for runnable selection without runtime candidates", () => {
  const selectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const unselectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const selectedThread = makeHostBoundaryThread(0x1234);
  const unselectedThread = makeHostBoundaryThread(0x5678);
  selectedThread.lastStatus = 26;
  selectedThread.yieldedReady = true;
  unselectedThread.lastStatus = 0;
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  threadWorkerStates.set(unselectedWorker, unselectedThread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_runnable_selection_bytes = () => 48;
  kernel.supervisor_select_runnable = vi.fn((
    vmPtr: number,
    candidatesPtr: number,
    candidateCount: number,
    outPtr: number,
    outLen: number,
  ) => {
    expect(vmPtr).toBe(0x1234);
    expect(candidatesPtr).toBe(0);
    expect(candidateCount).toBe(0);
    expect(outPtr).toBe(0);
    expect(outLen).toBe(48);
    const view = new DataView(kernel.memory.buffer);
    view.setUint32(0, 0x4e52_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 0, true);
    view.setBigUint64(16, 11n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, 2, true);
    view.setUint32(40, kernel.abi_status_ok(), true);
    view.setUint32(44, 1, true);
    return 48;
  });
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  handle.workers.add(unselectedWorker);
  handle.readyWorkers = [selectedWorker, unselectedWorker];
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_select_runnable).toHaveBeenCalledTimes(1);
  expect(handle.runningWorker).toBe(selectedWorker);
  expect(handle.readyWorkers).toEqual([unselectedWorker]);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-runnable:selected:0x1234:tid=2:seq=11:slot=1:state=2:status=0",
  );
});
