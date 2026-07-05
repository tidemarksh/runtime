import { expect, test } from "vite-plus/test";
import { assertHandleWorkerProcessIdentity } from "../../../../src/worker/process-identity.js";
import type { ProcessHandle, ThreadWorkerState } from "../../../../src/worker/types.js";
import {
  setThreadHostIdentity,
  setThreadInitialized,
  setThreadReinitCount,
} from "../../../../src/worker/thread-host-state.js";

function makeWorkerState(vmPtr: number): ThreadWorkerState {
  const thread = {
    vmPtr,
    stepCount: 0,
    totalStepBudget: 0,
    totalStepWallMs: 0,
    maxStepWallMs: 0,
    lastStepDurationMs: null,
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
    waitingInWorkerStatus: null,
    ownerAsyncStatus: null,
  };
  setThreadHostIdentity(thread, 3, 0);
  setThreadInitialized(thread, true);
  setThreadReinitCount(thread, 0);
  return thread;
}

function makeHandle(options: {
  actualPidByVm: Map<number, number>;
  processPtrByVm: Map<number, number>;
  liveVmPtrs: number[];
}): ProcessHandle {
  return {
    pid: 3,
    pendingChildSpawnRecords: 0,
    vmPtr: 0,
    workers: new Set<Worker>(),
    kernel: {
      abi_max_threads: () => options.liveVmPtrs.length,
      process_thread_vm_ptr: (_vmPtr: number, slot: number) => options.liveVmPtrs[slot] ?? 0,
      process_current_pid: (vmPtr: number) => options.actualPidByVm.get(vmPtr) ?? 0,
      supervisor_process_owner_matches: (vmPtr: number, runtimePid: number) =>
        options.actualPidByVm.get(vmPtr) === runtimePid ? 1 : 0,
      thread_process_ptr: (vmPtr: number) => options.processPtrByVm.get(vmPtr) ?? 0,
    },
    lastDebugMessage: "unit",
  } as unknown as ProcessHandle;
}

test("process identity ignores detached live-thread slots with no process pointer", () => {
  const anchorWorker = {} as Worker;
  const detachedWorker = {} as Worker;
  const anchorVmPtr = 0x1000;
  const detachedVmPtr = 0x2000;
  const handle = makeHandle({
    actualPidByVm: new Map([
      [anchorVmPtr, 3],
      [detachedVmPtr, 1],
    ]),
    processPtrByVm: new Map([
      [anchorVmPtr, 0x3000],
      [detachedVmPtr, 0],
    ]),
    liveVmPtrs: [anchorVmPtr, detachedVmPtr],
  });
  handle.workers.add(anchorWorker);
  handle.workers.add(detachedWorker);

  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>([
    [anchorWorker, makeWorkerState(anchorVmPtr)],
    [detachedWorker, makeWorkerState(detachedVmPtr)],
  ]);

  expect(() => {
    assertHandleWorkerProcessIdentity(handle, threadWorkerStates, "unit");
  }).not.toThrow();
});

test("process identity still rejects an owned live thread with a different pid", () => {
  const anchorWorker = {} as Worker;
  const driftWorker = {} as Worker;
  const anchorVmPtr = 0x1000;
  const driftVmPtr = 0x2000;
  const handle = makeHandle({
    actualPidByVm: new Map([
      [anchorVmPtr, 3],
      [driftVmPtr, 1],
    ]),
    processPtrByVm: new Map([
      [anchorVmPtr, 0x3000],
      [driftVmPtr, 0x4000],
    ]),
    liveVmPtrs: [anchorVmPtr, driftVmPtr],
  });
  handle.workers.add(anchorWorker);
  handle.workers.add(driftWorker);

  const threadWorkerStates = new WeakMap<Worker, ThreadWorkerState>([
    [anchorWorker, makeWorkerState(anchorVmPtr)],
    [driftWorker, makeWorkerState(driftVmPtr)],
  ]);

  expect(() => {
    assertHandleWorkerProcessIdentity(handle, threadWorkerStates, "unit");
  }).toThrow(/process identity drift expected_pid=3 actual_pid=1/);
});
