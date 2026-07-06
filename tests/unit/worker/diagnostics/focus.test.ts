import { expect, test } from "vite-plus/test";
import { summarizeProcessWorkerLogFocus } from "../../../../src/worker/diagnostics/debug.js";
import { seedProcessHandleStateFixture } from "../support/process-handle-state-fixture.js";

test("summarizeProcessWorkerLogFocus keeps the last credible state ahead of low-pc postmortem entries", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const handle = {
    workers: new Set([worker]),
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog: [
      "status:0x1234@0:pc=0x67c592:ra=0x67cdc6:sp=0x197d2a0:s11=0x1",
      "ready:0x1234:pc=0x67c592:ra=0x67cdc6:sp=0x197d2a0:s11=0x1",
      "status:0x1234@3:pc=0x2:ra=0x0:sp=0x0:s11=0x0:process_thread_count=0",
      "fault_debug:0x1234:\"fault_pc=0x2 fault_addr=0x2 pc=0x2 sp=0x0 s11=0x0\"",
    ],
    kernel: {},
  } as never;
  seedProcessHandleStateFixture(handle);

  const [focus] = summarizeProcessWorkerLogFocus(handle, threadWorkerStates);
  expect(focus?.lastStatus).toBe("status:0x1234@3:pc=0x2:ra=0x0:sp=0x0:s11=0x0:process_thread_count=0");
  expect(focus?.lastCredibleState).toBe("ready:0x1234:pc=0x67c592:ra=0x67cdc6:sp=0x197d2a0:s11=0x1");
});

test("summarizeProcessWorkerLogFocus finds the last credible state outside the short tail window", () => {
  const worker = {} as Worker;
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  threadWorkerStates.set(worker, {
    vmPtr: 0x1234,
    initialized: true,
    waitingInWorkerStatus: null,
  });

  const workerDebugLog = [
    "status:0x1234@0:pc=0x67c5be:ra=0x67cda6:sp=0x197d2a0:s11=0x1",
  ];
  for (let index = 0; index < 32; index += 1) {
    workerDebugLog.push(`write:0x1234:pc=0x2:ra=0x0:sp=0x0:s11=0x0:seq=${index}`);
  }
  workerDebugLog.push("status:0x1234@3:pc=0x2:ra=0x0:sp=0x0:s11=0x0:process_thread_count=0");
  workerDebugLog.push("fault_debug:0x1234:\"fault_pc=0x2 fault_addr=0x2 pc=0x2 sp=0x0 s11=0x0\"");

  const handle = {
    workers: new Set([worker]),
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog,
    kernel: {},
  } as never;
  seedProcessHandleStateFixture(handle);

  const [focus] = summarizeProcessWorkerLogFocus(handle, threadWorkerStates);
  expect(focus?.lastStatus).toBe("status:0x1234@3:pc=0x2:ra=0x0:sp=0x0:s11=0x0:process_thread_count=0");
  expect(focus?.lastCredibleState).toBe("status:0x1234@0:pc=0x67c5be:ra=0x67cda6:sp=0x197d2a0:s11=0x1");
});

test("summarizeProcessWorkerLogFocus keeps supervisor retry cause-chain entries for detached workers", () => {
  const threadWorkerStates = new WeakMap<Worker, {
    vmPtr: number;
    initialized: boolean;
    waitingInWorkerStatus: number | null;
  }>();
  const workerDebugLog = [
    "supervisor-blocked-retry-timer-arm:vm=0x1234:seq=17:delay=1:reason=0",
    "supervisor-blocked-retry-timer-fired:vm=0x1234:seq=17:delay=1:reason=0:completion=0",
    "supervisor-timer-fired-event:submit:vm=0x1234:seq=3:hostSeq=17:kind=6:result=0:status=12:reason=0:delay=1",
    "blocked-rpc-start:12:0x1234",
    "blocked-rpc-stage:12:start:pid=1:0x1234",
    "blocked-rpc-done:0:0x1234",
    "supervisor-resume:runnable:selected:0x1234:tid=42:seq=18:process=0x2000:status=0",
  ];
  const handle = {
    workers: new Set<Worker>(),
    runningWorker: null,
    vforkReadyWorkers: [],
    hostBlockedWorkers: new Map(),
    workerDebugLog,
    kernel: {},
  } as never;
  seedProcessHandleStateFixture(handle);

  const [focus] = summarizeProcessWorkerLogFocus(handle, threadWorkerStates);
  expect(focus?.vmPtr).toBe(0x1234);
  expect(focus?.ownerState).toBe("detached");
  expect(focus?.eventTail).toEqual(workerDebugLog);
  expect(focus?.statusTail).toEqual(workerDebugLog);
});
