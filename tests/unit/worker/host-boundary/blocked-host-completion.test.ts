import { afterEach, expect, test, vi } from "vite-plus/test";
import {
  handleFutexWait,
} from "../../../../src/thread-worker/blocking.js";
import { createWorkerWaitInWorkerStatusHandler } from "../../../../src/worker/message-handler/wait-in-worker.js";
import { createWorkerScheduler } from "../../../../src/worker/scheduler.js";
import {
  armSupervisorHostTimer,
  drainSupervisorArmHostTimerHostOp,
} from "../../../../src/worker/supervisor-host-op.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
  makeHostBoundaryThread,
} from "./helpers.js";
import {
  writeSupervisorArmHostTimerHostOpRecord,
  writeSupervisorBlockedRetryHostOpRecord,
} from "../support/supervisor-abi.js";

afterEach(() => {
  vi.useRealTimers();
});

test("host boundary observes supervisor-owned futex timeout completion state", () => {
  const waitSpy = vi.spyOn(Atomics, "wait");
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);
  const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
  const waitAddr = 0x100;
  const timeoutPtr = 0x200;
  const expected = 7;
  let status = kernel.abi_status_futex_wait();
  let blockedAddr = BigInt(waitAddr);
  let waitCalls = 0;
  const postStatus = vi.fn();

  new Int32Array(memory.buffer)[waitAddr >>> 2] = expected;
  const view = new DataView(memory.buffer);
  view.setBigUint64(timeoutPtr, 0n, true);
  view.setBigUint64(timeoutPtr + 8, 0n, true);
  waitSpy.mockImplementation((view, index, value) => {
    waitCalls += 1;
    expect(view.buffer).toBe(memory.buffer);
    expect(index).toBe(waitAddr >>> 2);
    expect(value).toBe(expected);
    if (waitCalls === 2) {
      status = kernel.abi_status_ok();
      blockedAddr = 0n;
    }
    return "timed-out";
  });

  kernel.memory = memory;
  handle.memory = memory;
  kernel.thread_status = () => status;
  kernel.thread_record_blocked_addr = () => blockedAddr;
  kernel.thread_futex_wait_addr = () => BigInt(waitAddr);
  kernel.thread_futex_wait_timeout = () => BigInt(timeoutPtr);
  kernel.thread_futex_wait_expected = () => expected;
  kernel.supervisor_signal_pending_for_blocked_wait = () => 0;
  kernel.supervisor_complete_futex_wait = vi.fn(() => 0);

  const handled = handleFutexWait({
    exports: kernel,
    statusCodes: { FUTEX_WAIT: kernel.abi_status_futex_wait() } as never,
    vmPtr: 0x1234,
    guestBase: 0,
    runtimeBridge: null,
    threadStepBudget: 1000,
    eintrErrno: 4,
    futexWaitSliceMs: 0,
    postStatus,
    reattachPublishedKernelRuntimeState: () => {},
    step: () => {},
  }, kernel.abi_status_futex_wait());

  expect(handled).toBe(false);
  expect(waitCalls).toBe(2);
  expect(kernel.supervisor_complete_futex_wait).not.toHaveBeenCalled();
  expect(postStatus).toHaveBeenNthCalledWith(1, {
    type: "status",
    status: kernel.abi_status_futex_wait(),
    waitInWorker: true,
  });
  expect(postStatus).toHaveBeenNthCalledWith(2, {
    type: "status",
    status: kernel.abi_status_ok(),
  });
  waitSpy.mockRestore();
});

test("host boundary executes supervisor arm timer HostOp through completion ABI", async () => {
  vi.useFakeTimers();
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);
  const memory = new WebAssembly.Memory({ initial: 6, maximum: 6, shared: true });
  kernel.memory = memory;
  handle.memory = memory;
  const completionViews: DataView[] = [];
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_host_op_record_bytes = () => 64;
  kernel.supervisor_host_op_kind_arm_host_timer = () => 12;
  kernel.supervisor_host_completion_outcome_success = () => 1;
  kernel.supervisor_host_completion_outcome_failure = () => 2;
  kernel.supervisor_next_host_op = vi.fn((outPtr: number, outLen: number) => {
    expect(outLen).toBe(64);
    return writeSupervisorArmHostTimerHostOpRecord(kernel.memory!, outPtr, {
      vmPtr: 0x1234,
      delayMs: 5n,
      seq: 7n,
      tid: 2,
      processPtr: 0x2000,
      status: 22,
    });
  });
  kernel.supervisor_complete_host_op = vi.fn((completionPtr: number, completionLen: number) => {
    completionViews.push(new DataView(kernel.memory!.buffer, completionPtr, completionLen));
    return 0;
  });
  const runtimeEvents: string[] = [];
  const workerEvents: string[] = [];
  const hostTimer = drainSupervisorArmHostTimerHostOp(handle, 0x1234);

  expect(hostTimer).not.toBeNull();
  expect(hostTimer).not.toBeUndefined();
  armSupervisorHostTimer(handle, hostTimer!, {
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    appendWorkerDebugLog: (_handle, event) => workerEvents.push(event),
    failProcess: (_handle, error) => {
      throw error;
    },
    pumpProcess: vi.fn(),
  });

  await vi.advanceTimersByTimeAsync(5);

  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(completionViews).toHaveLength(1);
  expect(completionViews[0].getUint32(0, true)).toBe(0x4348_5354);
  expect(completionViews[0].getUint32(12, true)).toBe(12);
  expect(completionViews[0].getBigUint64(16, true)).toBe(7n);
  expect(completionViews[0].getUint32(24, true)).toBe(0x1234);
  expect(completionViews[0].getUint32(36, true)).toBe(1);
  expect(runtimeEvents[0]).toContain("supervisor-host-timer-arm");
  expect(workerEvents[0]).toContain("supervisor-host-timer-fired");
});

test("host boundary drains timer HostOp by kind without consuming pending clone", () => {
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);
  const memory = new WebAssembly.Memory({ initial: 6, maximum: 6, shared: true });
  kernel.memory = memory;
  handle.memory = memory;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_host_op_record_bytes = () => 64;
  kernel.supervisor_host_op_kind_arm_host_timer = () => 12;
  const pendingKinds = [1, 12];
  kernel.supervisor_next_host_op = vi.fn(() => {
    throw new Error("unfiltered HostOp drain should not be used");
  });
  kernel.supervisor_next_host_op_kind = vi.fn((kind: number, outPtr: number, outLen: number) => {
    expect(outLen).toBe(64);
    const index = pendingKinds.indexOf(kind);
    if (index < 0) {
      return 0;
    }
    pendingKinds.splice(index, 1);
    if (kind !== 12) {
      throw new Error(`unexpected HostOp kind request: ${kind}`);
    }
    return writeSupervisorArmHostTimerHostOpRecord(kernel.memory!, outPtr, {
      vmPtr: 0x1234,
      delayMs: 5n,
      seq: 7n,
      tid: 2,
      processPtr: 0x2000,
      status: 22,
    });
  });

  const hostTimer = drainSupervisorArmHostTimerHostOp(handle, 0x1234);

  expect(hostTimer?.kind).toBe(12);
  expect(kernel.supervisor_next_host_op_kind).toHaveBeenCalledWith(12, expect.any(Number), 64);
  expect(kernel.supervisor_next_host_op).not.toHaveBeenCalled();
  expect(pendingKinds).toEqual([1]);
});

test("host boundary arms supervisor timer for wait-in-worker futex waits", async () => {
  vi.useFakeTimers();
  const worker = { postMessage: vi.fn() } as unknown as Worker;
  const kernel = makeHostBoundaryKernel();
  const handle = makeHostBoundaryHandle(kernel, worker);
  const thread = makeHostBoundaryThread(0x1234);
  const threadWorkerStates = new WeakMap<Worker, typeof thread>();
  const memory = new WebAssembly.Memory({ initial: 6, maximum: 6, shared: true });
  const completionViews: DataView[] = [];
  const runtimeEvents: string[] = [];
  const workerEvents: string[] = [];
  const notifyWorkerStatusProgress = vi.fn();
  const pumpProcess = vi.fn();
  const parkSupervisorFutexWait = vi.fn();

  threadWorkerStates.set(worker, thread);
  kernel.memory = memory;
  handle.memory = memory;
  handle.runningWorker = worker;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_host_op_record_bytes = () => 64;
  kernel.supervisor_host_op_kind_arm_host_timer = () => 12;
  kernel.supervisor_host_completion_outcome_success = () => 1;
  kernel.supervisor_host_completion_outcome_failure = () => 2;
  kernel.supervisor_next_host_op = vi.fn((outPtr: number, outLen: number) => {
    expect(outLen).toBe(64);
    return writeSupervisorArmHostTimerHostOpRecord(memory, outPtr, {
      vmPtr: 0x1234,
      delayMs: 5n,
      seq: 11n,
      tid: 2,
      processPtr: 0x2000,
      status: kernel.abi_status_futex_wait(),
    });
  });
  kernel.supervisor_complete_host_op = vi.fn((completionPtr: number, completionLen: number) => {
    completionViews.push(new DataView(memory.buffer, completionPtr, completionLen));
    return 0;
  });

  const handler = createWorkerWaitInWorkerStatusHandler({
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeEvents.push(event),
    appendWorkerDebugLog: (_handle, event) => workerEvents.push(event),
    clearBlockedWorker: vi.fn(),
    notifyWorkerStatusProgress,
    parkSupervisorFutexWait,
    pumpProcess,
    blockWorker: vi.fn(),
    failProcess: (_handle, error) => {
      throw error;
    },
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    threadWorkerStates,
  } as never);

  handler(
    handle,
    worker,
    thread,
    {
      type: "status",
      status: kernel.abi_status_futex_wait(),
      vmPtr: 0x1234,
      generation: thread.generation,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      kernelState: handle.kernelState,
      waitInWorker: true,
    },
    true,
  );

  expect(handle.runningWorker).toBeNull();
  expect(thread.runningGeneration).toBeNull();
  expect(thread.waitingInWorkerStatus).toBeNull();
  expect(parkSupervisorFutexWait).toHaveBeenCalledWith(
    handle,
    worker,
    0x1234,
    "wait-in-worker-futex-supervisor-owned",
  );
  expect(kernel.supervisor_next_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(notifyWorkerStatusProgress).toHaveBeenCalledWith(handle);
  expect(pumpProcess).toHaveBeenCalledWith(handle);

  await vi.advanceTimersByTimeAsync(5);

  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(completionViews[0]?.getUint32(12, true)).toBe(12);
  expect(completionViews[0]?.getBigUint64(16, true)).toBe(11n);
  expect(completionViews[0]?.getUint32(24, true)).toBe(0x1234);
  expect(completionViews[0]?.getUint32(36, true)).toBe(1);
  expect(runtimeEvents[0]).toContain("supervisor-host-timer-arm");
  expect(workerEvents).toEqual(expect.arrayContaining([
    expect.stringContaining("supervisor-host-timer-fired:vm=0x1234"),
  ]));
});

test("host boundary reports blocked retry timer expiry to supervisor before retrying", async () => {
  vi.useFakeTimers();
  const runningWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const blockedWorker = {
    postMessage: vi.fn(),
  } as unknown as Worker;
  const runningThread = makeHostBoundaryThread(0x5860320);
  const blockedThread = makeHostBoundaryThread(0x6286130);
  const threadWorkerStates = new WeakMap<Worker, ReturnType<typeof makeHostBoundaryThread>>();
  threadWorkerStates.set(runningWorker, runningThread);
  threadWorkerStates.set(blockedWorker, blockedThread);
  const kernel = makeHostBoundaryKernel();
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 16, shared: true });
  kernel.memory = memory;
  const handle = makeHostBoundaryHandle(kernel, runningWorker);
  handle.memory = memory;
  handle.workers.add(blockedWorker);
  handle.runningWorker = runningWorker;
  const submittedEvents: Array<{
    kind: number;
    vmPtr: number;
    tid: number;
    status: number;
    blockedReason: number;
    detail: bigint;
  }> = [];
  const supervisorSubmitThreadEvent = vi.fn((eventPtr: number, eventLen: number) => {
    expect(eventLen).toBe(80);
    const view = new DataView(kernel.memory!.buffer);
    submittedEvents.push({
      kind: view.getUint32(eventPtr + 12, true),
      vmPtr: view.getUint32(eventPtr + 24, true),
      tid: view.getUint32(eventPtr + 36, true),
      status: view.getUint32(eventPtr + 40, true),
      blockedReason: view.getUint32(eventPtr + 48, true),
      detail: view.getBigUint64(eventPtr + 72, true),
    });
    return 0;
  });
  const pendingRetryHostOps: Array<{ vmPtr: number; delayMs: bigint; reason: bigint; seq: bigint; status: number }> = [];
  Object.assign(kernel, {
    supervisor_abi_version: () => 1,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_retry_blocked_worker: () => 15,
    supervisor_next_host_op: vi.fn((_outPtr: number, outLen: number) => {
      expect(outLen).toBe(64);
      return 0;
    }),
    supervisor_next_host_op_kind: vi.fn((kind: number, outPtr: number, outLen: number) => {
      expect(outLen).toBe(64);
      if (kind !== 15) {
        return 0;
      }
      const hostOp = pendingRetryHostOps.shift();
      if (hostOp === undefined) {
        return 0;
      }
      return writeSupervisorBlockedRetryHostOpRecord(kernel.memory!, outPtr, hostOp);
    }),
    supervisor_enqueue_blocked_wait_timer: vi.fn(() => 0),
    supervisor_enqueue_blocked_retry_timer: vi.fn((vmPtr: number, flags: number) => {
      const reason = kernel.supervisor_blocked_wait_retry_reason!(vmPtr, flags);
      const delayMs = kernel.supervisor_blocked_wait_retry_delay_ms!(vmPtr, reason);
      pendingRetryHostOps.push({
        vmPtr,
        delayMs: BigInt(delayMs),
        reason: BigInt(reason),
        seq: 17n,
        status: kernel.thread_status(vmPtr),
      });
      return 1;
    }),
    supervisor_blocked_wait_timer_required: vi.fn(() => 0),
    supervisor_blocked_wait_retry_reason: () => 0,
    supervisor_blocked_wait_retry_delay_ms: () => 1,
    supervisor_blocked_wait_retry_reason_default: () => 0,
    supervisor_blocked_wait_retry_flag_host_progress: () => 1,
    supervisor_blocked_wait_retry_flag_active_child: () => 2,
    supervisor_blocked_wait_retry_flag_passive_wait: () => 4,
    supervisor_blocked_wait_retry_flag_process_gate: () => 8,
    supervisor_blocked_wait_retry_flag_thread_init_pending: () => 16,
    thread_status: (vmPtr: number) => vmPtr === blockedThread.vmPtr
      ? kernel.abi_status_pipe_wait()
      : kernel.abi_status_ok(),
    thread_tid: (vmPtr: number) => vmPtr === blockedThread.vmPtr ? 42 : 1,
    thread_pc: (vmPtr: number) => vmPtr === blockedThread.vmPtr ? 0x4444n : 0x1000n,
    thread_sp: (vmPtr: number) => vmPtr === blockedThread.vmPtr ? 0x8888n : 0x8000n,
    supervisor_thread_event_record_bytes: () => 80,
    supervisor_thread_event_record_magic: () => 0x5645_5354,
    supervisor_thread_event_kind_timer_fired: () => 6,
    supervisor_submit_thread_event: supervisorSubmitThreadEvent,
  });
  handle.hostBlockedWorkers.set(blockedWorker, {
    vmPtr: blockedThread.vmPtr,
    status: kernel.abi_status_pipe_wait(),
    rpcPending: false,
    rpcGeneration: 3,
  });

  const scheduler = createWorkerScheduler({
    appendRuntimeLifecycleEvent: () => {},
    threadWorkerStates,
    completeProcessExit: async () => {},
    failProcess: () => {},
    pumpNetworkConnections: async () => false,
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    resumeWait4ViaKernelWorker: async () => ({ kind: "completed" }),
    handleWorkerMessage: () => {},
  });

  scheduler.notifySchedulerProgress(handle);
  expect(supervisorSubmitThreadEvent).not.toHaveBeenCalled();

  await vi.advanceTimersByTimeAsync(1);

  expect(supervisorSubmitThreadEvent).toHaveBeenCalledWith(expect.any(Number), 80);
  expect(submittedEvents[0]).toEqual({
    kind: 6,
    vmPtr: blockedThread.vmPtr,
    tid: 42,
    status: kernel.abi_status_pipe_wait(),
    blockedReason: 0,
    detail: 1n,
  });
});
