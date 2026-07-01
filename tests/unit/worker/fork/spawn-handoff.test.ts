import { expect, test, vi } from "vite-plus/test";
import type { ProcessHandle, ThreadWorkerState, WorkerForkDeps } from "../../../../src/worker/fork/types.js";

const plainForkMock = vi.fn(async () => {});
const vforkExecveMock = vi.fn(async () => {});

vi.mock("../../../../src/worker/fork/plain.js", () => ({
  handlePlainFork: plainForkMock,
}));

vi.mock("../../../../src/worker/fork/vfork-execve.js", () => ({
  handleVforkExecve: vforkExecveMock,
}));

function writeU32(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value, true);
}

function writeU64(view: DataView, offset: number, value: bigint): void {
  view.setBigUint64(offset, value, true);
}

function writeProcessHostOpRecord(
  memory: WebAssembly.Memory,
  ptr: number,
  parentVmPtr: number,
  spawnKind: number,
  childPid: number,
  seq = 1n,
): void {
  const view = new DataView(memory.buffer, ptr, 96);
  writeU32(view, 0, 0x4850_5354);
  writeU32(view, 4, 1);
  writeU32(view, 8, 96);
  writeU32(view, 12, 1);
  writeU64(view, 16, seq);
  writeU32(view, 24, parentVmPtr);
  writeU32(view, 28, spawnKind);
  writeU32(view, 32, childPid);
  writeU32(view, 36, 0x5000);
  writeU32(view, 40, 7);
  writeU32(view, 44, 2);
  writeU64(view, 48, 8n * 1024n * 1024n);
  writeU32(view, 56, 128);
  writeU32(view, 60, 0);
  writeU64(view, 64, 0x6000n);
  writeU64(view, 72, 0x7000n);
  writeU64(view, 80, 0x8000n);
  writeU64(view, 88, 0n);
}

async function loadForkHandlers() {
  return await import("../../../../src/worker/fork.js");
}

function makeDeps(): WorkerForkDeps & {
  appendRuntimeLifecycleEvent: ReturnType<typeof vi.fn>;
  enqueueWorker: ReturnType<typeof vi.fn>;
  notifyForkProgress: ReturnType<typeof vi.fn>;
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
    notifyForkProgress: vi.fn(),
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
    pendingChildSpawnRecords: 0,
    kernel: {
      abi_spawn_kind_execve: () => 1,
      abi_spawn_kind_fork: () => 2,
      thread_spawn_kind: () => kind,
      thread_spawn_child_pid: () => childPid,
      thread_spawn_path: () => 0n,
      thread_spawn_argv: () => 0n,
      thread_spawn_envp: () => 0n,
      thread_spawn_guest_memory_policy_limit: () => 0,
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
  const handle = makeHandle(2, 92);
  const completeHostOp = vi.fn();
  (handle.kernel as typeof handle.kernel & {
    supervisor_complete_host_op: (completionPtr: number, completionLen: number) => number;
  }).supervisor_complete_host_op = completeHostOp;

  await handlers.handleExecveSpawn(handle, worker, makeThread());

  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:2:92");
  expect(completeHostOp).not.toHaveBeenCalled();
  expect(plainForkMock).toHaveBeenCalledTimes(1);
  expect(plainForkMock).toHaveBeenCalledWith(deps, expect.anything(), worker, expect.objectContaining({ vmPtr: 0x1234 }), 92);
  expect(vforkExecveMock).not.toHaveBeenCalled();
});

test("handleExecveSpawn prefers supervisor process HostOp over legacy spawn handoff", async () => {
  plainForkMock.mockClear();
  vforkExecveMock.mockClear();
  const { createWorkerForkHandlers } = await loadForkHandlers();
  const deps = makeDeps();
  const handlers = createWorkerForkHandlers(deps);
  const worker = {} as Worker;
  const handle = makeHandle(1, 91);
  const memory = new WebAssembly.Memory({ initial: 4, maximum: 4, shared: true });
  const kernel = handle.kernel as typeof handle.kernel & {
    memory: WebAssembly.Memory;
    kernel_heap_ptr: () => number;
    supervisor_abi_version: () => number;
    supervisor_process_host_op_record_bytes: () => number;
    supervisor_process_host_op_kind_spawn_process_worker: () => number;
    supervisor_process_host_op: (vmPtr: number, outPtr: number, outLen: number) => number;
    supervisor_complete_host_op: (completionPtr: number, completionLen: number) => number;
  };
  kernel.memory = memory;
  kernel.kernel_heap_ptr = () => 0;
  kernel.supervisor_abi_version = () => 1;
  kernel.supervisor_process_host_op_record_bytes = () => 96;
  kernel.supervisor_process_host_op_kind_spawn_process_worker = () => 1;
  kernel.supervisor_process_host_op = vi.fn((vmPtr: number, outPtr: number, outLen: number) => {
    expect(vmPtr).toBe(0x1234);
    expect(outLen).toBe(96);
    writeProcessHostOpRecord(memory, outPtr, 0x1234, 2, 92, 42n);
    return 96;
  });
  kernel.supervisor_complete_host_op = vi.fn((completionPtr: number, completionLen: number) => {
    expect(completionLen).toBe(64);
    const completion = new DataView(memory.buffer, completionPtr, completionLen);
    expect(completion.getUint32(0, true)).toBe(0x4348_5354);
    expect(completion.getUint32(4, true)).toBe(1);
    expect(completion.getUint32(8, true)).toBe(64);
    expect(completion.getUint32(12, true)).toBe(1);
    expect(completion.getBigUint64(16, true)).toBe(42n);
    expect(completion.getUint32(24, true)).toBe(0x1234);
    expect(completion.getUint32(28, true)).toBe(2);
    expect(completion.getInt32(32, true)).toBe(92);
    expect(completion.getUint32(36, true)).toBe(1);
    expect(completion.getUint32(40, true)).toBe(0);
    expect(completion.getBigUint64(48, true)).toBe(0x6000n);
    return 0;
  });

  await handlers.handleExecveSpawn(handle, worker, makeThread());

  expect(kernel.supervisor_process_host_op).toHaveBeenCalledWith(0x1234, expect.any(Number), 96);
  expect(kernel.supervisor_complete_host_op).toHaveBeenCalledWith(expect.any(Number), 64);
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:2:92");
  expect(plainForkMock).toHaveBeenCalledTimes(1);
  expect(plainForkMock).toHaveBeenCalledWith(deps, handle, worker, expect.objectContaining({ vmPtr: 0x1234 }), 92);
  expect(vforkExecveMock).not.toHaveBeenCalled();
});

test("handleExecveSpawn rejects an unknown spawn handoff kind", async () => {
  plainForkMock.mockClear();
  vforkExecveMock.mockClear();
  const { createWorkerForkHandlers } = await loadForkHandlers();
  const deps = makeDeps();
  const handlers = createWorkerForkHandlers(deps);
  const worker = {} as Worker;
  const handle = makeHandle(0, 0);
  const clearSpawnHandoff = handle.kernel.thread_clear_spawn_handoff as ReturnType<typeof vi.fn>;

  await expect(handlers.handleExecveSpawn(handle, worker, makeThread())).rejects.toThrow(
    "unexpected spawn handoff kind",
  );

  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:0:0");
  expect(clearSpawnHandoff).not.toHaveBeenCalled();
  expect(deps.enqueueWorker).not.toHaveBeenCalled();
  expect(plainForkMock).not.toHaveBeenCalled();
  expect(vforkExecveMock).not.toHaveBeenCalled();
});

test("handleExecveSpawn ignores a cleared handoff while a child spawn is pending", async () => {
  plainForkMock.mockClear();
  vforkExecveMock.mockClear();
  const { createWorkerForkHandlers } = await loadForkHandlers();
  const deps = makeDeps();
  const handlers = createWorkerForkHandlers(deps);
  const worker = {} as Worker;
  const handle = makeHandle(0, 0);
  handle.pendingChildSpawnRecords = 1;

  await expect(handlers.handleExecveSpawn(handle, worker, makeThread())).resolves.toBeUndefined();

  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff:11:0:0");
  expect(deps.appendRuntimeLifecycleEvent).toHaveBeenCalledWith(1, "spawn-handoff-stale:11:0x1234:1");
  expect(deps.notifyForkProgress).toHaveBeenCalledWith(handle);
  expect(deps.enqueueWorker).not.toHaveBeenCalled();
  expect(plainForkMock).not.toHaveBeenCalled();
  expect(vforkExecveMock).not.toHaveBeenCalled();
});
