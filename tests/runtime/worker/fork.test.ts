import { expect, test, vi } from "vite-plus/test";
import type { ProcessHandle, ThreadWorkerState, WorkerForkDeps } from "../../../src/worker/fork/types.js";

const plainForkMock = vi.fn(async () => {});
const vforkExecveMock = vi.fn(async () => {});

vi.mock("../../../src/worker/fork/plain-fork.js", () => ({
  handlePlainFork: plainForkMock,
}));

vi.mock("../../../src/worker/fork/vfork-execve.js", () => ({
  handleVforkExecve: vforkExecveMock,
}));

async function loadForkHandlers() {
  return await import("../../../src/worker/fork.js");
}

function makeDeps(): WorkerForkDeps & {
  appendRuntimeLifecycleEvent: ReturnType<typeof vi.fn>;
  enqueueWorker: ReturnType<typeof vi.fn>;
} {
  return {
    processHandles: new Map(),
    appendRuntimeLifecycleEvent: vi.fn(),
    appendWorkerDebugLog: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    attachThreadWorker: vi.fn(),
    clearVforkSnapshot: vi.fn(),
    enqueueWorker: vi.fn(),
    failProcess: vi.fn(),
    initializeProcessLifecycleViaKernelWorker: vi.fn(),
    kickBlockedWorkers: vi.fn(),
    recordChildExitViaKernelWorker: vi.fn(),
    requestPipeStateSync: vi.fn(),
    rehydrateForkSharedStateViaKernelWorker: vi.fn(),
    registerProcessWithKernelWorker: vi.fn(),
    resumeVforkExecveParentViaKernelWorker: vi.fn(),
    syncForkSharedStateLocally: vi.fn(),
    syncForkSharedStateViaKernelWorker: vi.fn(),
    canonicalizeChildLifecycleViaKernelWorker: vi.fn(),
    bootStrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  };
}

function makeHandle(kind: number, childPid = 77): ProcessHandle {
  return {
    runtimeId: 1,
    pid: 11,
    kernel: {
      abi_spawn_kind_execve: () => 1,
      abi_spawn_kind_fork: () => 2,
      thread_spawn_kind: () => kind,
      thread_spawn_child_pid: () => childPid,
      thread_spawn_path: () => 0n,
      thread_spawn_argv: () => 0n,
      thread_spawn_envp: () => 0n,
      thread_spawn_ram_size: () => 0,
      thread_clear_spawn_handoff: vi.fn(),
    },
  } as unknown as ProcessHandle;
}

function makeThread(): ThreadWorkerState {
  return {
    vmPtr: 0x1234,
  } as ThreadWorkerState;
}

test("handleExecveSpawn routes execve spawn handoffs into the vfork-execve ownership path", async () => {
  plainForkMock.mockClear();
  vforkExecveMock.mockClear();
  const { createWorkerForkHandlers } = await loadForkHandlers();
  const deps = makeDeps();
  const handlers = createWorkerForkHandlers(deps);
  const worker = {} as Worker;

  await handlers.handleExecveSpawn(makeHandle(1, 91), worker, makeThread());

  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:1:91");
  expect(vforkExecveMock).toHaveBeenCalledTimes(1);
  expect(vforkExecveMock).toHaveBeenCalledWith(deps, expect.anything(), worker, expect.objectContaining({ vmPtr: 0x1234 }), 91);
  expect(plainForkMock).not.toHaveBeenCalled();
});

test("handleExecveSpawn routes fork spawn handoffs into the plain-fork ownership path", async () => {
  plainForkMock.mockClear();
  vforkExecveMock.mockClear();
  const { createWorkerForkHandlers } = await loadForkHandlers();
  const deps = makeDeps();
  const handlers = createWorkerForkHandlers(deps);
  const worker = {} as Worker;

  await handlers.handleExecveSpawn(makeHandle(2, 92), worker, makeThread());

  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:2:92");
  expect(plainForkMock).toHaveBeenCalledTimes(1);
  expect(plainForkMock).toHaveBeenCalledWith(deps, expect.anything(), worker, expect.objectContaining({ vmPtr: 0x1234 }), 92);
  expect(vforkExecveMock).not.toHaveBeenCalled();
});

test("handleExecveSpawn clears the handoff and re-enqueues the worker when no child ownership path is requested", async () => {
  plainForkMock.mockClear();
  vforkExecveMock.mockClear();
  const { createWorkerForkHandlers } = await loadForkHandlers();
  const deps = makeDeps();
  const handlers = createWorkerForkHandlers(deps);
  const worker = {} as Worker;
  const handle = makeHandle(0, 0);
  const clearSpawnHandoff = handle.kernel.thread_clear_spawn_handoff as ReturnType<typeof vi.fn>;

  await handlers.handleExecveSpawn(handle, worker, makeThread());

  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:0:0");
  expect(clearSpawnHandoff).toHaveBeenCalledWith(0x1234);
  expect(deps.enqueueWorker).toHaveBeenCalledWith(handle, worker);
  expect(plainForkMock).not.toHaveBeenCalled();
  expect(vforkExecveMock).not.toHaveBeenCalled();
});
