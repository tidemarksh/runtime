import { expect, test, vi } from "vite-plus/test";
import type { KernelExports } from "../../../../src/abi.js";
import { createWorkerSchedulerQueueHelpers } from "../../../../src/worker/host-boundary/thread-worker-queue.js";
import {
  acquireWorker,
  destroyWorkerPool,
} from "../../../../src/worker/pool.js";
import { processHasWorker } from "../../../../src/worker/process-worker-state.js";
import { drainSupervisorCloneSpawnHostOp } from "../../../../src/worker/supervisor-host-op.js";
import type { ThreadWorkerState } from "../../../../src/worker/types.js";
import {
  getThreadGeneration,
  getThreadOwnerPid,
  isThreadInitialized,
} from "../../../../src/worker/thread-host-state.js";
import type { ThreadWorkerInbound } from "../../../../src/worker/host-boundary/worker-scheduler-types.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
} from "./helpers.js";

type MockWorker = Worker & {
  onmessage: ((event: MessageEvent<unknown>) => void) | null;
  onerror: ((event: ErrorEvent) => void) | null;
  onmessageerror: ((event: MessageEvent<unknown>) => void) | null;
  postMessage: ReturnType<typeof vi.fn>;
  terminate: ReturnType<typeof vi.fn>;
};

function makeMockWorker(): MockWorker {
  return {
    onmessage: null,
    onerror: null,
    onmessageerror: null,
    postMessage: vi.fn(),
    terminate: vi.fn(),
  } as unknown as MockWorker;
}

function installSupervisorHostOpStubs(
  kernel: ReturnType<typeof makeHostBoundaryKernel>,
  memory: WebAssembly.Memory,
): void {
  kernel.memory = memory;
  Object.assign(kernel, makeSupervisorAbiStubs({
    memory,
    currentExports: () => kernel as KernelExports,
    abi_status_ok: () => 0,
    abi_status_clone_spawn: () => 25,
    kernel_scratch_base: () => 0,
    supervisor_host_op_record_bytes: () => 64,
    supervisor_host_op_kind_spawn_thread_worker: () => 1,
    thread_process_ptr: () => 0x5000,
    thread_status: (vmPtr: number) => vmPtr === 0x1234 ? 25 : 0,
    thread_tid: (vmPtr: number) => vmPtr === 0x2000 ? 2 : 1,
  }));
}

test("thread host boundary drains supervisor SpawnThreadWorker HostOp by kind", () => {
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 8, shared: true });
  const parentWorker = makeMockWorker();
  const kernel = makeHostBoundaryKernel();
  installSupervisorHostOpStubs(kernel, memory);
  const handle = makeHostBoundaryHandle(kernel, parentWorker);
  handle.memory = memory;
  handle.vmPtr = 0x1234;

  const pendingKinds = [12, 1];
  kernel.supervisor_next_host_op = vi.fn(() => {
    throw new Error("thread HostOp drain must use kind-filtered supervisor polling");
  });
  kernel.supervisor_next_host_op_kind = vi.fn((kind, outPtr, outLen) => {
    expect(outLen).toBe(64);
    const index = pendingKinds.indexOf(kind);
    if (index < 0) {
      return 0;
    }
    pendingKinds.splice(index, 1);
    if (kind !== kernel.supervisor_host_op_kind_spawn_thread_worker?.()) {
      return 0;
    }
    return makeSupervisorAbiStubs({
      memory,
      currentExports: () => kernel as KernelExports,
      cloneSpawnParentVmPtr: () => 0x1234,
      thread_spawn_from_clone: () => 0x2000,
      thread_tid: () => 2,
      thread_process_ptr: () => 0x5000,
      thread_status: () => 25,
      abi_status_clone_spawn: () => 25,
      abi_status_ok: () => 0,
    }).supervisor_next_host_op_kind?.call(kernel as KernelExports, kind, outPtr, outLen) ?? 0;
  });

  const hostOp = drainSupervisorCloneSpawnHostOp(handle, 0x1234);

  expect(kernel.supervisor_next_host_op_kind).toHaveBeenCalledWith(1, expect.any(Number), 64);
  expect(kernel.supervisor_next_host_op).not.toHaveBeenCalled();
  expect(hostOp).toMatchObject({
    kind: 1,
    parentVmPtr: 0x1234,
    childVmPtr: 0x2000,
    tid: 2,
    processPtr: 0x5000,
    status: 0,
  });
  expect(pendingKinds).toEqual([12]);
});

test("thread host boundary attaches supervisor-requested worker and posts init diagnostics", () => {
  const memory = new WebAssembly.Memory({ initial: 2, maximum: 8, shared: true });
  const parentWorker = makeMockWorker();
  vi.stubGlobal("Worker", vi.fn(function WorkerMock(this: MockWorker) {
    Object.assign(this, makeMockWorker());
  }));
  const childWorker = acquireWorker() as MockWorker;
  const kernel = makeHostBoundaryKernel();
  installSupervisorHostOpStubs(kernel, memory);
  const handle = makeHostBoundaryHandle(kernel, parentWorker);
  handle.memory = memory;
  handle.vmPtr = 0x1234;

  const workerStates = new WeakMap<Worker, ThreadWorkerState>();
  const workerDebugLog: string[] = [];
  const runtimeLifecycleLog: string[] = [];
  const queue = createWorkerSchedulerQueueHelpers({
    threadWorkerStates: workerStates,
    appendRuntimeLifecycleEvent: (_runtimeId, event) => runtimeLifecycleLog.push(event),
    appendWorkerDebugLog: (_handle, event) => workerDebugLog.push(event),
    blockWorker: () => {},
    clearHostBlockedRetryTimer: () => {},
    takeThreadWorkerBlockedResume: () => false,
    isSupervisorFutexWaiter: () => false,
    completeProcessExit: async () => {},
    failProcess: vi.fn(),
    handleWorkerMessage: vi.fn(),
  });

  try {
    queue.attachThreadWorker(handle, {
      worker: childWorker,
      vmPtr: 0x2000,
    });

    const thread = workerStates.get(childWorker);
    expect(processHasWorker(handle, childWorker)).toBe(true);
    expect(thread?.vmPtr).toBe(0x2000);
    expect(getThreadOwnerPid(thread)).toBe(handle.pid);
    expect(getThreadGeneration(thread)).toBeGreaterThan(0);
    expect(isThreadInitialized(thread)).toBe(false);
    expect(childWorker.postMessage).toHaveBeenCalledTimes(1);
    expect(childWorker.postMessage.mock.calls[0]?.[0]).toMatchObject({
      type: "init",
      memory,
      vmPtr: 0x2000,
      kernelState: handle.kernelState,
    } satisfies Partial<ThreadWorkerInbound>);
    expect(workerDebugLog.some((entry) => entry.startsWith("attach:0x2000"))).toBe(true);
    expect(runtimeLifecycleLog.some((entry) =>
      entry.startsWith("clone-child-init-post:1:vm=0x2000:generation=")
        && entry.endsWith(":force=0:workers=2:ready=0")
    )).toBe(true);
  } finally {
    destroyWorkerPool();
    vi.unstubAllGlobals();
  }
});
