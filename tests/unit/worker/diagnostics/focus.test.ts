import { expect, test } from "vite-plus/test";
import { summarizeProcessWorkerLogFocus } from "../../../../src/worker/diagnostics/debug.js";

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
    blockedWorkers: new Map(),
    workerDebugLog: [
      "status:0x1234@0:pc=0x67c592:ra=0x67cdc6:sp=0x197d2a0:s11=0x1",
      "ready:0x1234:pc=0x67c592:ra=0x67cdc6:sp=0x197d2a0:s11=0x1",
      "status:0x1234@3:pc=0x2:ra=0x0:sp=0x0:s11=0x0:process_thread_count=0",
      "fault_debug:0x1234:\"fault_pc=0x2 fault_addr=0x2 pc=0x2 sp=0x0 s11=0x0\"",
    ],
    kernel: {},
  } as never;

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
    blockedWorkers: new Map(),
    workerDebugLog,
    kernel: {},
  } as never;

  const [focus] = summarizeProcessWorkerLogFocus(handle, threadWorkerStates);
  expect(focus?.lastStatus).toBe("status:0x1234@3:pc=0x2:ra=0x0:sp=0x0:s11=0x0:process_thread_count=0");
  expect(focus?.lastCredibleState).toBe("status:0x1234@0:pc=0x67c5be:ra=0x67cda6:sp=0x197d2a0:s11=0x1");
});
