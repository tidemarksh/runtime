import { expect, test } from "vite-plus/test";
import {
  continueBlockedRpcResumeAfterEffects,
  kernelRpcOutcomeNeedsResumeContinuation,
} from "../../../../src/worker/kernel-rpc/effects.js";
import type { KernelRpcResumeOutcome } from "../../../../src/worker/kernel-rpc/outcomes.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";

function makeBlockedHandle(worker: Worker): ProcessHandle {
  return {
    workers: new Set([worker]),
    blockedWorkers: new Map([[worker, {
      vmPtr: 0x1234,
      status: 21,
      timeoutExpired: true,
    }]]),
    kernel: {
      thread_status: () => 21,
    },
  } as unknown as ProcessHandle;
}

test("kernel-rpc effects own blocked resume continuation classification", () => {
  expect(kernelRpcOutcomeNeedsResumeContinuation({
    kind: "blocked",
    status: 21,
    effects: [{ kind: "request-pipe-state-sync" }],
  })).toBe(true);
  expect(kernelRpcOutcomeNeedsResumeContinuation({
    kind: "blocked",
    status: 21,
    effects: [{ kind: "enqueue-worker" }],
  })).toBe(false);
  expect(kernelRpcOutcomeNeedsResumeContinuation({ kind: "completed" })).toBe(false);
});

test("continueBlockedRpcResumeAfterEffects returns status for scheduler retry after sync effects", async () => {
  const worker = {} as Worker;
  const handle = makeBlockedHandle(worker);
  const threadWorkerStates = new WeakMap<Worker, unknown>([[worker, {}]]);
  const outcome: KernelRpcResumeOutcome = {
    kind: "blocked",
    status: 21,
    effects: [{ kind: "sync-blocked-socket-state", status: 21 }],
  };

  await expect(continueBlockedRpcResumeAfterEffects(
    handle,
    worker,
    0x1234,
    outcome,
    { kind: "blocked", status: 21 },
    { threadWorkerStates },
  )).resolves.toEqual({ kind: "completed", status: 21 });

  expect(handle.blockedWorkers.get(worker)?.rpcPending).toBe(false);
});

test("continueBlockedRpcResumeAfterEffects leaves non-continuation outcomes unchanged", async () => {
  const worker = {} as Worker;
  const handle = makeBlockedHandle(worker);
  const appliedOutcome: KernelRpcResumeOutcome = { kind: "completed", status: 0 };

  await expect(continueBlockedRpcResumeAfterEffects(
    handle,
    worker,
    0x1234,
    { kind: "completed" },
    appliedOutcome,
    { threadWorkerStates: new WeakMap([[worker, {}]]) },
  )).resolves.toBe(appliedOutcome);
});
