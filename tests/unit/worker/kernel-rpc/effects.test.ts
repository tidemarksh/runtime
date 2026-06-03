import { expect, test, vi } from "vite-plus/test";
import {
  applyKernelRpcResumeEffects,
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

test("continueBlockedRpcResumeAfterEffects resumes through kernel-rpc ports after sync effects", async () => {
  const worker = {} as Worker;
  const handle = makeBlockedHandle(worker);
  const threadWorkerStates = new WeakMap<Worker, unknown>([[worker, {}]]);
  const resumeBlockedSyscallLocally = vi.fn(async () => {
    handle.blockedWorkers.delete(worker);
  });
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
    { threadWorkerStates, resumeBlockedSyscallLocally },
  )).resolves.toEqual({ kind: "queued-in-worker" });

  expect(resumeBlockedSyscallLocally).toHaveBeenCalledWith(handle, worker, 0x1234, 21, true);
});

test("continueBlockedRpcResumeAfterEffects leaves non-continuation outcomes unchanged", async () => {
  const worker = {} as Worker;
  const handle = makeBlockedHandle(worker);
  const appliedOutcome: KernelRpcResumeOutcome = { kind: "completed", status: 0 };
  const resumeBlockedSyscallLocally = vi.fn(async () => {});

  await expect(continueBlockedRpcResumeAfterEffects(
    handle,
    worker,
    0x1234,
    { kind: "completed" },
    appliedOutcome,
    { threadWorkerStates: new WeakMap([[worker, {}]]), resumeBlockedSyscallLocally },
  )).resolves.toBe(appliedOutcome);
  expect(resumeBlockedSyscallLocally).not.toHaveBeenCalled();
});

test("publish-memfs-after-fs does not refresh pipe state", async () => {
  const worker = {} as Worker;
  const handle = {
    runtimeBridge: {
      stateSab: new SharedArrayBuffer(176),
      notifySab: new SharedArrayBuffer(12),
    },
    kernelState: {},
  } as unknown as ProcessHandle;
  const target = {} as ProcessHandle;
  const syncForkSharedStateViaKernelWorker = vi.fn(async () => {});
  const syncPipeStateViaKernelWorker = vi.fn(async () => 0n);

  await applyKernelRpcResumeEffects(
    handle,
    worker,
    0x1234,
    {
      kind: "completed",
      effects: [{
        kind: "publish-memfs-after-fs",
        targets: [target],
        preservePublishedRegularFileData: true,
        sourceVmPtr: 0x5678,
      }],
    },
    {
      blockWorker: vi.fn(),
      enqueueWorker: vi.fn(),
      handleBlockedRetryResult: vi.fn(),
      handleExecveSpawn: vi.fn(async () => {}),
      kickBlockedWorkers: vi.fn(),
      markRuntimeFsDirty: vi.fn(),
      requestPipeStateSync: vi.fn(async () => {}),
      settleVisibleProcessOutput: vi.fn(async () => {}),
      syncBlockedForkSharedStateViaKernelWorker: vi.fn(async () => {}),
      syncForkSharedStateLocally: vi.fn(),
      syncForkSharedStateViaKernelWorker,
      syncPipeStateViaKernelWorker,
      syncSocketStateViaKernelWorker: vi.fn(async () => ({ socketUsed: 0, netBufUsed: 0n })),
      threadWorkerStates: new WeakMap(),
    },
  );

  expect(syncForkSharedStateViaKernelWorker).toHaveBeenCalledWith(handle, [target], {
    cause: { kind: "syscall", nr: -1n, name: "fs-memfs-publication" },
    includeMemfs: true,
    trackHeldOfds: false,
    memfsOnly: true,
    preservePublishedRegularFileData: true,
    sourceVmPtr: 0x5678,
  });
  expect(syncPipeStateViaKernelWorker).not.toHaveBeenCalled();
});
