import { hostExecutionState } from "../../../../src/worker/host-execution-state.js";
import {
  addProcessWorker,
} from "../../../../src/worker/process-worker-state.js";
import { expect, test, vi } from "vite-plus/test";

vi.mock("../../../../src/worker/pool.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../../src/worker/pool.js")>();
  return {
    ...actual,
    kernelStackSlotForWorker: () => 2,
  };
});

import { createWorkerSchedulerQueueHelpers } from "../../../../src/worker/host-boundary/thread-worker-queue.js";
import {
  clearThreadOwnerAsyncStatus,
  getThreadWaitingInWorkerStatus,
  setThreadOwnerAsyncStatus,
} from "../../../../src/worker/thread-host-state.js";
import { getThreadActivityState } from "../../../../src/worker/thread-activity-state.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
  makeHostBoundaryThread,
} from "./helpers.js";

type QueueDeps = Parameters<typeof createWorkerSchedulerQueueHelpers>[0];

function makeQueue(
  threadWorkerStates: WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>,
  debugEvents: string[],
  overrides: Partial<Pick<QueueDeps, "takeThreadWorkerBlockedResume">> = {},
) {
  return createWorkerSchedulerQueueHelpers({
    appendRuntimeLifecycleEvent: () => {},
    appendWorkerDebugLog: (_handle, event) => {
      debugEvents.push(event);
    },
    blockWorker: () => {},
    clearHostBlockedRetryTimer: () => {},
    takeThreadWorkerBlockedResume: overrides.takeThreadWorkerBlockedResume ?? (() => false),
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

function writeResumeRecord(
  memory: WebAssembly.Memory,
  outPtr: number,
  record: {
    kind: number;
    seq: bigint;
    vmPtr: number;
    tid: number;
    processPtr: number;
    status: number;
    detail0: bigint;
  },
): number {
  const view = new DataView(memory.buffer, outPtr, 48);
  view.setUint32(0, 0x5255_5354, true);
  view.setUint32(4, 1, true);
  view.setUint32(8, 48, true);
  view.setUint32(12, record.kind, true);
  view.setBigUint64(16, record.seq, true);
  view.setUint32(24, record.vmPtr, true);
  view.setUint32(28, record.tid, true);
  view.setUint32(32, record.processPtr, true);
  view.setUint32(36, record.status, true);
  view.setBigUint64(40, record.detail0, true);
  return 48;
}

test("host boundary selects supervisor futex wake records for thread-worker completion", () => {
  const notify = vi.spyOn(Atomics, "notify");
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
    ? kernel.abi_status_futex_wait()
    : kernel.abi_status_futex_wait();
  kernel.thread_record_blocked_addr = (vmPtr: number) => vmPtr === selectedThread.vmPtr ? 0n : 0xbeefn;
  kernel.thread_futex_wait_addr = (vmPtr: number) => vmPtr === selectedThread.vmPtr ? 0xfee0n : 0xbeefn;
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
    view.setBigUint64(40, 0xfee0n, true);
    resumePolled = true;
    return 48;
  });
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn(() => 0);
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  addProcessWorker(handle, unselectedWorker);
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(1);
  expect(kernel.supervisor_process_resume).not.toHaveBeenCalled();
  expect(kernel.supervisor_complete_futex_wait).toHaveBeenCalledWith(
    selectedThread.vmPtr,
    kernel.supervisor_futex_wait_completion_woken(),
  );
  expect(notify).toHaveBeenCalledWith(expect.any(Int32Array), 0xfee0 / 4, 1);
  expect(getThreadWaitingInWorkerStatus(selectedThread as never)).toBeNull();
  expect(getThreadWaitingInWorkerStatus(unselectedThread as never)).toBeNull();
  expect(hostExecutionState(handle).runningWorker).toBe(selectedWorker);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    resumeBlockedSyscall: true,
  }));
  expect(unselectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-resume:futex-wake:observed:0x1234:tid=2:seq=7:process=0x2000:detail=0xfee0:status=0",
  );
  expect(debugEvents).toContain(
    "supervisor-resume:futex-wake:selected:0x1234:tid=2:seq=7:process=0x2000:status=0:detail=0xfee0",
  );
});

test("host boundary preserves one-shot futex wake records while owner async status is active", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const thread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(worker, thread);
  const kernel = makeHostBoundaryKernel();
  let resumePolled = false;
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_futex_wake = () => 1;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    if (resumePolled) {
      return 0;
    }
    resumePolled = true;
    return writeResumeRecord(kernel.memory, outPtr, {
      kind: 1,
      seq: 8n,
      vmPtr: thread.vmPtr,
      tid: 2,
      processPtr: 0x2000,
      status: kernel.abi_status_ok(),
      detail0: 0xfeedn,
    }) <= outLen ? 48 : 0;
  });
  kernel.supervisor_process_resume = vi.fn(() => 0);
  const handle = makeHostBoundaryHandle(kernel, worker);
  setThreadOwnerAsyncStatus(thread as never, kernel.abi_status_ok());
  const debugEvents: string[] = [];
  const queue = makeQueue(threadWorkerStates, debugEvents);

  queue.pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(1);
  expect(hostExecutionState(handle).runningWorker).toBeNull();
  expect(worker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toEqual(expect.arrayContaining([
    expect.stringContaining(
      "supervisor-resume:futex-wake:defer-owner-async:0x1234:tid=2:seq=8:status=0:owner=0:ownerDetail=status:0:ownerAgeMs=",
    ),
  ]));

  clearThreadOwnerAsyncStatus(thread as never);
  queue.pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(1);
  expect(hostExecutionState(handle).runningWorker).toBe(worker);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    resumeBlockedSyscall: true,
  }));
});

test("host boundary keeps later runnable resume records for later pumps", () => {
  const firstWorker = { postMessage: vi.fn() } as unknown as Worker;
  const secondWorker = { postMessage: vi.fn() } as unknown as Worker;
  const firstThread = makeHostBoundaryThread(0x1234);
  const secondThread = makeHostBoundaryThread(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(firstWorker, firstThread);
  threadWorkerStates.set(secondWorker, secondThread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  const queued = [
    { vmPtr: firstThread.vmPtr, tid: 2, seq: 31n },
    { vmPtr: secondThread.vmPtr, tid: 3, seq: 32n },
  ];
  kernel.supervisor_next_resume = vi.fn((outPtr: number, outLen: number) => {
    expect(outLen).toBe(48);
    const next = queued.shift();
    if (next === undefined) {
      return 0;
    }
    return writeResumeRecord(kernel.memory, outPtr, {
      kind: 2,
      seq: next.seq,
      vmPtr: next.vmPtr,
      tid: next.tid,
      processPtr: 0x2000,
      status: kernel.abi_status_ok(),
      detail0: 0n,
    });
  });
  const handle = makeHostBoundaryHandle(kernel, firstWorker);
  addProcessWorker(handle, secondWorker);
  const debugEvents: string[] = [];
  const queue = makeQueue(threadWorkerStates, debugEvents);

  queue.pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(1);
  expect(hostExecutionState(handle).runningWorker).toBe(firstWorker);
  expect(firstWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(secondWorker.postMessage).not.toHaveBeenCalled();
  expect(queued).toHaveLength(1);

  hostExecutionState(handle).runningWorker = null;
  queue.pumpProcess(handle);

  expect(kernel.supervisor_next_resume).toHaveBeenCalledTimes(2);
  expect(hostExecutionState(handle).runningWorker).toBe(secondWorker);
  expect(secondWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({ type: "step" }));
  expect(queued).toHaveLength(0);
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:selected:0x1234:tid=2:seq=31:process=0x2000:status=0:detail=0x0",
  );
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:selected:0x5678:tid=3:seq=32:process=0x2000:status=0:detail=0x0",
  );
});

test("host boundary holds worker selection while regular-file fs is in flight", () => {
  const inFlightWorker = { postMessage: vi.fn() } as unknown as Worker;
  const readyWorker = { postMessage: vi.fn() } as unknown as Worker;
  const inFlightThread = makeHostBoundaryThread(0x1234);
  const readyThread = makeHostBoundaryThread(0x5678);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(inFlightWorker, inFlightThread);
  threadWorkerStates.set(readyWorker, readyThread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
    return writeResumeRecord(kernel.memory, outPtr, {
      kind: 2,
      seq: 41n,
      vmPtr: readyThread.vmPtr,
      tid: 3,
      processPtr: 0x2000,
      status: kernel.abi_status_ok(),
      detail0: 0n,
    }) <= outLen ? 48 : 0;
  });
  const handle = makeHostBoundaryHandle(kernel, inFlightWorker);
  addProcessWorker(handle, readyWorker);
  hostExecutionState(handle).vforkReadyWorkers = [readyWorker];
  hostExecutionState(handle).regularFileFsInFlight = {
    worker: inFlightWorker,
    vmPtr: inFlightThread.vmPtr,
    fd: 3,
    ofdId: 4,
  };
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_process_resume).not.toHaveBeenCalled();
  expect(hostExecutionState(handle).runningWorker).toBeNull();
  expect(hostExecutionState(handle).vforkReadyWorkers).toEqual([readyWorker]);
  expect(inFlightWorker.postMessage).not.toHaveBeenCalled();
  expect(readyWorker.postMessage).not.toHaveBeenCalled();
});

test("host boundary marks Runtime blocked-resume fallback steps", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const thread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(worker, thread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_process_resume = vi.fn(() => 0);
  const handle = makeHostBoundaryHandle(kernel, worker);
  const debugEvents: string[] = [];
  const takeThreadWorkerBlockedResume = vi.fn((_handle, candidate: Worker) => candidate === worker);

  makeQueue(threadWorkerStates, debugEvents, { takeThreadWorkerBlockedResume }).pumpProcess(handle);

  expect(kernel.supervisor_process_resume).toHaveBeenCalledTimes(1);
  expect(takeThreadWorkerBlockedResume).toHaveBeenCalledWith(handle, worker);
  expect(hostExecutionState(handle).runningWorker).toBe(worker);
  expect(worker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    resumeBlockedSyscall: true,
  }));
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:blocked-resume:0x1234:status=0",
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
  addProcessWorker(handle, unselectedWorker);
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

test("host boundary defers supervisor runnable selection while owner async status is active", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const thread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(worker, thread);
  const kernel = makeHostBoundaryKernel();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
    return writeResumeRecord(kernel.memory, outPtr, {
      kind: 2,
      seq: 17n,
      vmPtr: thread.vmPtr,
      tid: 2,
      processPtr: 0x2000,
      status: kernel.abi_status_ok(),
      detail0: 0n,
    }) <= outLen ? 48 : 0;
  });
  const handle = makeHostBoundaryHandle(kernel, worker);
  setThreadOwnerAsyncStatus(thread as never, kernel.abi_status_ok());
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(kernel.supervisor_process_resume).toHaveBeenCalledTimes(1);
  expect(hostExecutionState(handle).runningWorker).toBeNull();
  expect(worker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toEqual(expect.arrayContaining([
    expect.stringContaining(
      "supervisor-resume:runnable:defer-owner-async:0x1234:tid=2:seq=17:status=0:owner=0:ownerDetail=status:0:ownerAgeMs=",
    ),
  ]));
});

test("host boundary routes host-wait runnable resume through blocked-syscall retry", () => {
  const selectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const selectedThread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  const kernel = makeHostBoundaryKernel();
  kernel.thread_status = () => kernel.abi_status_ok();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
    const view = new DataView(kernel.memory.buffer, outPtr, outLen);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 2, true);
    view.setBigUint64(16, 14n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0n, true);
    return 48;
  });
  kernel.supervisor_thread_state_snapshot_bytes = () => 64;
  kernel.supervisor_thread_state_snapshot = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
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
    view.setUint32(44, kernel.abi_status_pipe_wait(), true);
    return 64;
  });
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  hostExecutionState(handle).hostBlockedWorkers.set(selectedWorker, {
    vmPtr: selectedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
  });
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(hostExecutionState(handle).runningWorker).toBe(selectedWorker);
  expect(hostExecutionState(handle).hostBlockedWorkers.has(selectedWorker)).toBe(false);
  expect(selectedWorker.postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "step",
    resumeBlockedSyscall: true,
  }));
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:selected:0x1234:tid=2:seq=14:process=0x2000:status=0:detail=0x0"
      + ":snapshot=process=0x2000:owner=0x1234:threads=2/2/0x1234/6/0x0",
  );
});

test("host boundary still defers non-host-wait stale runnable records", () => {
  const selectedWorker = { postMessage: vi.fn() } as unknown as Worker;
  const selectedThread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(selectedWorker, selectedThread);
  const kernel = makeHostBoundaryKernel();
  kernel.thread_status = () => kernel.abi_status_futex_wait();
  kernel.supervisor_resume_record_bytes = () => 48;
  kernel.supervisor_resume_kind_runnable_thread = () => 2;
  kernel.supervisor_process_resume = vi.fn((_vmPtr: number, outPtr: number, outLen: number) => {
    const view = new DataView(kernel.memory.buffer, outPtr, outLen);
    view.setUint32(0, 0x5255_5354, true);
    view.setUint32(4, 1, true);
    view.setUint32(8, 48, true);
    view.setUint32(12, 2, true);
    view.setBigUint64(16, 15n, true);
    view.setUint32(24, selectedThread.vmPtr, true);
    view.setUint32(28, 2, true);
    view.setUint32(32, 0x2000, true);
    view.setUint32(36, kernel.abi_status_ok(), true);
    view.setBigUint64(40, 0n, true);
    return 48;
  });
  const handle = makeHostBoundaryHandle(kernel, selectedWorker);
  hostExecutionState(handle).hostBlockedWorkers.set(selectedWorker, {
    vmPtr: selectedThread.vmPtr,
    status: kernel.abi_status_futex_wait(),
  });
  const debugEvents: string[] = [];

  makeQueue(threadWorkerStates, debugEvents).pumpProcess(handle);

  expect(hostExecutionState(handle).runningWorker).toBeNull();
  expect(hostExecutionState(handle).hostBlockedWorkers.has(selectedWorker)).toBe(true);
  expect(selectedWorker.postMessage).not.toHaveBeenCalled();
  expect(debugEvents).toContain(
    "supervisor-resume:runnable:defer-blocked:0x1234:tid=2:seq=15:status=0:current=20:blocked=20",
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
