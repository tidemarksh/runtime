import { expect, test, vi } from "vite-plus/test";
import { createWorkerControlMessageHandler } from "../../../src/worker/message-handler/control.js";
import { processLastDebugMessage } from "../../../src/worker/process-debug-state.js";
import type { ProcessHandle } from "../../../src/worker/types.js";
import type { WorkerMessageHandlerDeps } from "../../../src/worker/message-handler/types.js";

function makeDeps(failProcess: ReturnType<typeof vi.fn>): WorkerMessageHandlerDeps {
  return {
    processHandles: new Map(),
    threadWorkerStates: new WeakMap(),
    statusOkCompletedSyncEffects: 0,
    syncEffectPipeState: 0,
    syncEffectSocketState: 0,
    syncEffectEpollState: 0,
    syncEffectEpollDependentOfdState: 0,
    syncEffectProcessLifecycle: 0,
    syncEffectSignalWaiters: 0,
    syncEffectForkSharedOfdState: 0,
    syncEffectProcessMemfs: 0,
    appendWorkerDebugLog: () => {},
    assertProcessStandardFdIntegrity: () => {},
    attachThreadWorker: () => {},
    blockWorker: () => {},
    canonicalizeActiveVforkChildViaKernelWorker: async () => {},
    captureVforkSnapshot: () => ({ vmPtr: 0, childPid: null, nextPid: null, pgid: null, sid: null, inheritedSharedOfdIds: [] }),
    clearBlockedWorker: () => {},
    completeProcessExit: async () => {},
    enqueueWorker: () => {},
    failProcess,
    handleExecveSpawn: async () => {},
    notifyWorkerStatusProgress: () => {},
    pumpProcess: () => {},
    requestEpollDependentOfdStateSync: async () => {},
    requestEpollStateSync: async () => {},
    requestPipeStateSync: async () => {},
    requestSocketStateSync: async () => {},
    requestThreadWorkerBlockedResume: () => {},
    resumeConnectViaKernelWorker: async () => {},
    resumeFsViaKernelWorker: async () => {},
    resumeVforkParentAfterChildExit: () => false,
    statusDefersOwnedStateSync: () => false,
    syncBlockedForkSharedStateViaKernelWorker: async () => {},
    syncForkSharedStateViaKernelWorker: async () => {},
    syncProcessLifecycleViaKernelWorker: async () => {},
  };
}

test("worker control error path degrades gracefully when thread debug access traps", () => {
  const failProcess = vi.fn();
  const handleWorkerControlMessage = createWorkerControlMessageHandler(makeDeps(failProcess));
  const worker = {} as Worker;
  const handle = {
    pid: 7,
    runningWorker: worker,
    hostBlockedWorkers: new Map(),
    workers: new Set<Worker>([worker]),
    kernel: {
      abi_max_threads: () => 8,
      thread_process_thread_count: () => {
        throw new WebAssembly.RuntimeError("operation does not support unaligned accesses");
      },
      process_thread_vm_ptr: () => 0,
    },
  } as unknown as ProcessHandle;

  expect(() => handleWorkerControlMessage(handle, worker, {
    type: "error",
    vmPtr: 0x1234,
    message: "worker boom",
  })).not.toThrow();

  expect(processLastDebugMessage(handle)).toBe("worker boom");
  expect(failProcess).toHaveBeenCalledTimes(1);

  const error = failProcess.mock.calls[0]?.[1];
  expect(error).toBeInstanceOf(Error);
  expect((error as Error).message).toContain("worker boom");
  expect((error as Error).message).toContain("thread_count=unavailable:");
});
