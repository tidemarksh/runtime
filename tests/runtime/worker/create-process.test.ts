import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { readPublishedKernelRuntimeState } from "../../../src/bridge/runtime-state.js";
import { createWorkerProcessFactory } from "../../../src/worker/create-process.js";
import { destroyWorkerPool } from "../../../src/worker/pool.js";
import type { ProcessHandle } from "../../../src/worker/types.js";

const decoder = new TextDecoder();
const encoder = new TextEncoder();
const originalWorker = globalThis.Worker;

class FakeWorker {
  onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
  terminate = vi.fn();

  constructor(
    readonly _url: URL,
    readonly _options: WorkerOptions,
  ) {}

  postMessage(message: { type: string }): void {
    if (message.type === "prepare") {
      queueMicrotask(() => {
        this.onmessage?.({ data: { type: "prepared", wasmFeatures: {} } } as MessageEvent<unknown>);
      });
    }
  }
}

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memStatePtr: 0x240000,
    epollTablePtr: 0x250000,
    socketTablePtr: 0x260000,
    socketUsed: 0,
    pipeBase: 0x270000,
    pipeUsed: 0n,
    netBufBase: 0x280000,
    netBufUsed: 0n,
    sigSlotsPtr: 0xc00000,
    processPendingPtr: 0xc10000,
    processRuntimeStatePtr: 0xc20000,
    timeStatePtr: 0xc30000,
  };
}

function readCString(memory: WebAssembly.Memory, ptr: number): string {
  const bytes = new Uint8Array(memory.buffer);
  let end = ptr;
  while (bytes[end] !== 0) {
    end += 1;
  }
  return decoder.decode(bytes.subarray(ptr, end));
}

type FakeProcessKernel = KernelExports & {
  argPtrs: number[];
  envPtrs: number[];
  processSetCwdIno: ReturnType<typeof vi.fn>;
  memfsPaths: Map<string, number>;
  callLog: string[];
};

function makeFakeProcessKernel(memory: WebAssembly.Memory): FakeProcessKernel {
  let runtimeState = makeKernelState();
  const argPtrs: number[] = [];
  const envPtrs: number[] = [];
  const processSetCwdIno = vi.fn();
  const callLog: string[] = [];
  const pathToIno = new Map<string, number>([["/", 1]]);
  const inoToPath = new Map<number, string>([[1, "/"]]);
  const inoMode = new Map<number, number>([[1, 0o040755]]);
  const fileData = new Map<number, Uint8Array>();
  let nextIno = 2;
  const readBytes = (ptr: number, len: number): Uint8Array =>
    new Uint8Array(memory.buffer.slice(ptr, ptr + len));
  const readString = (ptr: number, len: number): string => decoder.decode(readBytes(ptr, len));
  const childPath = (parentIno: number, namePtr: number, nameLen: number): string => {
    const parentPath = inoToPath.get(parentIno) ?? "/";
    const name = readString(namePtr, nameLen);
    return parentPath === "/" ? `/${name}` : `${parentPath}/${name}`;
  };
  const ensureIno = (path: string, mode: number): number => {
    const existing = pathToIno.get(path);
    if (existing !== undefined) {
      return existing;
    }
    const ino = nextIno++;
    pathToIno.set(path, ino);
    inoToPath.set(ino, path);
    inoMode.set(ino, mode);
    return ino;
  };
  return {
    memory,
    abi_max_fds: () => 64,
    abi_max_threads: () => 8,
    kernel_init: vi.fn(),
    set_base_time: vi.fn(),
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 64 * 32,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_attach_runtime_state: (
      heapBase: number,
      heapPtr: number,
      ofdTablePtr: number,
      memfsPtr: number,
      memStatePtr: number,
      epollTablePtr: number,
      socketTablePtr: number,
      socketUsed: number,
      pipeBase: number,
      pipeUsed: bigint,
      netBufBase: number,
      netBufUsed: bigint,
      sigSlotsPtr: number,
      processPendingPtr: number,
      processRuntimeStatePtr: number,
      timeStatePtr: number,
    ) => {
      runtimeState = {
        heapBase,
        heapPtr,
        ofdTablePtr,
        memfsPtr,
        memStatePtr,
        epollTablePtr,
        socketTablePtr,
        socketUsed,
        pipeBase,
        pipeUsed,
        netBufBase,
        netBufUsed,
        sigSlotsPtr,
        processPendingPtr,
        processRuntimeStatePtr,
        timeStatePtr,
      };
    },
    process_inspect_elf_brk: vi.fn(() => {
      callLog.push("process_inspect_elf_brk");
      return 0x2000;
    }),
    process_clear_args: vi.fn(),
    process_push_arg: vi.fn((ptr: number) => {
      argPtrs.push(ptr);
    }),
    process_push_env: vi.fn((ptr: number) => {
      envPtrs.push(ptr);
    }),
    process_guest_base: vi.fn(() => {
      callLog.push("process_guest_base");
      return 0x100000;
    }),
    process_create: vi.fn(() => 0x1234),
    process_stdin_pipe: () => 1,
    process_stdout_pipe: () => 2,
    process_stderr_pipe: () => 3,
    process_set_cwd_ino: processSetCwdIno,
    process_current_pgid: () => 77,
    process_current_sid: () => 88,
    memfs_resolve_path: (ptr: number, len: number) => {
      callLog.push("memfs_resolve_path");
      return pathToIno.get(readString(ptr, len)) ?? 0;
    },
    memfs_ensure_dir: (parentIno: number, namePtr: number, nameLen: number, mode: number) => {
      callLog.push("memfs_ensure_dir");
      return ensureIno(childPath(parentIno, namePtr, nameLen), 0o040000 | mode);
    },
    memfs_create_file: (parentIno: number, namePtr: number, nameLen: number) => {
      callLog.push("memfs_create_file");
      return ensureIno(childPath(parentIno, namePtr, nameLen), 0o100644);
    },
    memfs_reserve_inode_until: () => 0n,
    memfs_truncate_file: (ino: number, size: bigint) => {
      callLog.push("memfs_truncate_file");
      fileData.set(ino, new Uint8Array(Number(size)));
      return 0n;
    },
    memfs_set_file_size_shallow: (ino: number, size: bigint) => {
      callLog.push("memfs_set_file_size_shallow");
      fileData.set(ino, new Uint8Array(Number(size)));
      return 0n;
    },
    memfs_write_file_at: (ino: number, offset: bigint, dataPtr: number, dataLen: number) => {
      callLog.push("memfs_write_file_at");
      const current = fileData.get(ino) ?? new Uint8Array();
      const start = Number(offset);
      const needed = start + dataLen;
      const next = needed > current.length ? new Uint8Array(needed) : new Uint8Array(current);
      if (next !== current) {
        next.set(current);
      }
      next.set(readBytes(dataPtr, dataLen), start);
      fileData.set(ino, next);
      return BigInt(dataLen);
    },
    memfs_set_mode: (ino: number, mode: number) => {
      callLog.push("memfs_set_mode");
      const current = inoMode.get(ino) ?? 0o100000;
      inoMode.set(ino, (current & 0o170000) | mode);
      return 0n;
    },
    memfs_inode_mode: (ino: number) => inoMode.get(ino) ?? 0,
    memfs_file_size: (ino: number) => BigInt(fileData.get(ino)?.length ?? 0),
    memfs_read_file_to: (ino: number, offset: bigint, bufPtr: number, bufLen: number) => {
      const bytes = fileData.get(ino) ?? new Uint8Array();
      const start = Number(offset);
      if (start >= bytes.length) {
        return 0n;
      }
      const slice = bytes.subarray(start, start + bufLen);
      new Uint8Array(memory.buffer).set(slice, bufPtr);
      return BigInt(slice.length);
    },
    argPtrs,
    envPtrs,
    processSetCwdIno,
    memfsPaths: pathToIno,
    callLog,
  } as unknown as FakeProcessKernel;
}

function makeParentHandle(): ProcessHandle {
  const parentKernel = {
    process_current_pgid: () => 77,
    process_current_sid: () => 88,
  } as unknown as ProcessHandle["kernel"];
  return {
    runtimeId: 7,
    pid: 1,
    parentPid: null,
    vmPtr: 0x4321,
    ramSize: 0,
    guestBase: 0,
    worker: {} as Worker,
    memory: new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true }),
    wasmModule: {} as WebAssembly.Module,
    kernel: parentKernel,
    kernelState: makeKernelState(),
    stdinPipe: 0,
    stdoutPipe: 0,
    stderrPipe: 0,
    hostStdio: { stdinPipe: 0, stdoutPipe: 0, stderrPipe: 0, refCount: 1 },
    kernelShare: { members: new Set(), sharedOfdIds: new Set() },
    stdinQueue: [],
    outputBacklog: [],
    debugCommand: "parent",
    runtimeBridge: {} as ProcessHandle["runtimeBridge"],
    sabPageCache: null,
    workers: new Set(),
    readyWorkers: [],
    runningWorker: null,
    blockedWorkers: new Map(),
    pipeSyncPromise: null,
    pipeSyncQueued: false,
    pipeWaiterWakeQueued: false,
    epollSyncPromise: null,
    epollSyncQueued: false,
    epollWaiterWakeQueued: false,
    ofdSyncPromise: null,
    ofdSyncQueuedIds: new Set(),
    socketSyncPromise: null,
    socketSyncQueued: false,
    socketWaiterWakeQueued: false,
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
}

beforeEach(() => {
  globalThis.Worker = class extends FakeWorker {} as unknown as typeof globalThis.Worker;
});

afterEach(() => {
  destroyWorkerPool();
  globalThis.Worker = originalWorker;
  vi.restoreAllMocks();
});

test("createProcess stages argv/env strings and seeds full memfs for standalone processes", async () => {
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const registerProcessWithKernelWorker = vi.fn(async () => {});
  const attachThreadWorker = vi.fn();
  const assertProcessStandardFdIntegrity = vi.fn();
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker,
    assertProcessStandardFdIntegrity,
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker,
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 0),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  const handle = await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      argv: ["/bin/test", "--flag"],
      env: ["KEY=VALUE", "LANG=C"],
      preloadEntries: [],
    },
  );

  expect(fakeKernel.argPtrs.map((ptr) => readCString(kernelMemory, ptr))).toEqual(["/bin/test", "--flag"]);
  expect(fakeKernel.envPtrs.map((ptr) => readCString(kernelMemory, ptr))).toEqual(["KEY=VALUE", "LANG=C"]);
  expect(fakeKernel.processSetCwdIno).toHaveBeenCalledWith(0x1234, 1);
  expect(registerProcessWithKernelWorker).toHaveBeenCalledWith(handle, { seedMemfsMode: "full" });
  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledWith(handle, "create-process");
  expect(handle.stdinPipe).toBe(1);
  expect(handle.stdoutPipe).toBe(2);
  expect(handle.stderrPipe).toBe(3);
  expect(handle.hostStdio).toMatchObject({
    stdinPipe: 1,
    stdoutPipe: 2,
    stderrPipe: 3,
    refCount: 1,
  });
  expect(handle.kernelShare.members.has(handle)).toBe(true);
  expect(handle.debugCommand).toBe("/bin/test");
  expect(processHandles.size).toBe(0);
  expect(attachThreadWorker).toHaveBeenCalledWith(handle, handle.worker, 0x1234);
});

test("createProcess delegates cwd resolution and lifecycle seeding for runtime-owned children", async () => {
  const processHandles = new Map<number, ProcessHandle>([[1, makeParentHandle()]]);
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const registerProcessWithKernelWorker = vi.fn(async () => {});
  const initializeProcessLifecycleViaKernelWorker = vi.fn(async () => {});
  const resolveProcessCwdInoViaKernelWorker = vi.fn(async () => 33);
  const kernelWorkerRequest = vi.fn(async () => {
    throw new Error("unexpected kernel worker request");
  });
  const assertProcessStandardFdIntegrity = vi.fn();
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity,
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker,
    registerProcessWithKernelWorker,
    resolveProcessCwdInoViaKernelWorker,
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  const handle = await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      pid: 9,
      parentPid: 1,
      argv: ["/bin/test"],
      cwd: "/work/child",
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest,
    },
  );

  expect(resolveProcessCwdInoViaKernelWorker).toHaveBeenCalledWith(kernelWorkerRequest, "/work/child");
  expect(registerProcessWithKernelWorker).toHaveBeenCalledWith(handle, { seedMemfsMode: "metadata" });
  expect(fakeKernel.processSetCwdIno).toHaveBeenCalledWith(0x1234, 33);
  expect(assertProcessStandardFdIntegrity).toHaveBeenCalledWith(handle, "create-process");
  expect(handle.stdinPipe).toBe(1);
  expect(handle.stdoutPipe).toBe(2);
  expect(handle.stderrPipe).toBe(3);
  expect(initializeProcessLifecycleViaKernelWorker).toHaveBeenCalledWith(
    handle,
    0x1234,
    1,
    10,
    77,
    88,
    false,
  );
  expect(processHandles.get(9)).toBe(handle);
});

test("createProcess publishes kernel runtime state before attaching the first worker", async () => {
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const bridgeStatesAtAttach: KernelRuntimeState[] = [];
  const attachThreadWorker = vi.fn((handle: ProcessHandle) => {
    bridgeStatesAtAttach.push(readPublishedKernelRuntimeState(handle.runtimeBridge));
  });
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker,
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 1),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  const handle = await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      argv: ["/bin/test"],
    },
  );

  expect(attachThreadWorker).toHaveBeenCalledWith(handle, handle.worker, 0x1234);
  expect(bridgeStatesAtAttach).toEqual([handle.kernelState]);
});

test("createProcess publishes lifecycle-updated kernel state before attaching runtime-owned children", async () => {
  const processHandles = new Map<number, ProcessHandle>([[1, makeParentHandle()]]);
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const bridgeStatesAtAttach: KernelRuntimeState[] = [];
  const attachThreadWorker = vi.fn((handle: ProcessHandle) => {
    bridgeStatesAtAttach.push(readPublishedKernelRuntimeState(handle.runtimeBridge));
  });
  const registerProcessWithKernelWorker = vi.fn(async (handle: ProcessHandle) => {
    handle.kernelState = {
      ...handle.kernelState,
      heapPtr: handle.kernelState.heapPtr + 0x20,
      processRuntimeStatePtr: handle.kernelState.processRuntimeStatePtr + 0x80,
    };
  });
  const initializeProcessLifecycleViaKernelWorker = vi.fn(async (handle: ProcessHandle) => {
    handle.kernelState = {
      ...handle.kernelState,
      heapPtr: handle.kernelState.heapPtr + 0x40,
      processPendingPtr: handle.kernelState.processPendingPtr + 0x20,
    };
  });
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker,
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker,
    registerProcessWithKernelWorker,
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 33),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  const handle = await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      pid: 9,
      parentPid: 1,
      argv: ["/bin/test"],
      cwd: "/work/child",
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest: vi.fn(async () => {
        throw new Error("unexpected kernel worker request");
      }),
    },
  );

  expect(attachThreadWorker).toHaveBeenCalledWith(handle, handle.worker, 0x1234);
  expect(bridgeStatesAtAttach).toEqual([handle.kernelState]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x60);
  expect(handle.kernelState.processRuntimeStatePtr).toBe(makeKernelState().processRuntimeStatePtr + 0x80);
  expect(handle.kernelState.processPendingPtr).toBe(makeKernelState().processPendingPtr + 0x20);
});

test("createProcess fetches a full kernel-worker snapshot when runtime fs entries are metadata-only", async () => {
  const processHandles = new Map<number, ProcessHandle>([[1, makeParentHandle()]]);
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const getRuntimeFsEntries = vi.fn(() => [{
    kind: "file" as const,
    path: "/bin/app",
    size: 18,
    mode: 0o755,
  }]);
  const kernelWorkerRequest = vi.fn(async (msg: { type: string; includeData?: boolean }) => {
    expect(msg).toEqual({
      type: "snapshot-fs",
      includeData: true,
    });
    return {
      type: "snapshot-fs-result" as const,
      requestId: 1,
      entries: [{
        kind: "file" as const,
        path: "/bin/app",
        data: encoder.encode("materialized-app"),
        mode: 0o755,
      }],
    };
  });
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries,
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 1),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      pid: 2,
      parentPid: 1,
      argv: ["app"],
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest,
    },
  );

  expect(getRuntimeFsEntries).toHaveBeenCalledTimes(1);
  expect(kernelWorkerRequest).toHaveBeenCalledTimes(1);
  expect(fakeKernel.memfsPaths.get("/bin/app")).toBeGreaterThan(0);
  const ino = fakeKernel.memfsPaths.get("/bin/app")!;
  const fileBuf = new Uint8Array(Number(fakeKernel.memfs_file_size(ino)));
  const read = Number(fakeKernel.memfs_read_file_to(ino, 0n, 0x4000, fileBuf.length));
  expect(read).toBe("materialized-app".length);
  expect(decoder.decode(new Uint8Array(kernelMemory.buffer, 0x4000, read))).toBe("materialized-app");
});

test("createProcess computes guest base after preloading runtime fs entries", async () => {
  const processHandles = new Map<number, ProcessHandle>([[1, makeParentHandle()]]);
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  const processGuestBase = vi.mocked(fakeKernel.process_guest_base);
  processGuestBase.mockImplementation(() => {
    fakeKernel.callLog.push("process_guest_base");
    return fakeKernel.memfsPaths.has("/bin/app") ? 0x200000 : 0x100000;
  });
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: vi.fn(() => [{
      kind: "file" as const,
      path: "/bin/app",
      data: encoder.encode("app"),
      mode: 0o755,
    }]),
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 1),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      pid: 2,
      parentPid: 1,
      argv: ["app"],
    },
  );

  expect(fakeKernel.callLog.some((entry) => entry.startsWith("memfs_"))).toBe(true);
  const guestBaseIndex = fakeKernel.callLog.indexOf("process_guest_base");
  const inspectIndex = fakeKernel.callLog.indexOf("process_inspect_elf_brk");
  const preloadIndex = fakeKernel.callLog.findIndex((entry) => entry.startsWith("memfs_"));
  expect(preloadIndex).toBeGreaterThanOrEqual(0);
  expect(guestBaseIndex).toBeGreaterThan(preloadIndex);
  expect(inspectIndex).toBeGreaterThan(preloadIndex);
  expect(inspectIndex).toBeGreaterThan(guestBaseIndex);
  expect(processGuestBase).toHaveReturnedWith(0x200000);
});

test("createProcess rebuilds argv, env, and cwd independently for sequential runtime-owned children", async () => {
  const parent = makeParentHandle();
  const processHandles = new Map<number, ProcessHandle>([[1, parent]]);
  const firstKernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const secondKernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const firstKernel = makeFakeProcessKernel(firstKernelMemory);
  const secondKernel = makeFakeProcessKernel(secondKernelMemory);
  const instantiate = vi.spyOn(WebAssembly, "instantiate");
  instantiate
    .mockResolvedValueOnce({ exports: firstKernel } as unknown as WebAssembly.Instance)
    .mockResolvedValueOnce({ exports: secondKernel } as unknown as WebAssembly.Instance);
  const registerProcessWithKernelWorker = vi.fn(async () => {});
  const initializeProcessLifecycleViaKernelWorker = vi.fn(async () => {});
  const resolveProcessCwdInoViaKernelWorker = vi.fn()
    .mockResolvedValueOnce(33)
    .mockResolvedValueOnce(44);
  const assertProcessStandardFdIntegrity = vi.fn();
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity,
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker,
    registerProcessWithKernelWorker,
    resolveProcessCwdInoViaKernelWorker,
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  const firstChild = await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      pid: 9,
      parentPid: 1,
      argv: ["/bin/first", "--alpha"],
      env: ["KEY=FIRST"],
      cwd: "/work/first",
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest: vi.fn(async () => {
        throw new Error("unexpected kernel worker request");
      }),
    },
  );
  const secondChild = await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      runtimeId: 7,
      pid: 10,
      parentPid: 1,
      argv: ["/bin/second", "--beta"],
      env: ["KEY=SECOND", "LANG=C"],
      cwd: "/work/second",
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest: vi.fn(async () => {
        throw new Error("unexpected kernel worker request");
      }),
    },
  );

  expect(firstKernel.argPtrs.map((ptr) => readCString(firstKernelMemory, ptr))).toEqual([
    "/bin/first",
    "--alpha",
  ]);
  expect(firstKernel.envPtrs.map((ptr) => readCString(firstKernelMemory, ptr))).toEqual([
    "KEY=FIRST",
  ]);
  expect(firstKernel.processSetCwdIno).toHaveBeenCalledWith(0x1234, 33);

  expect(secondKernel.argPtrs.map((ptr) => readCString(secondKernelMemory, ptr))).toEqual([
    "/bin/second",
    "--beta",
  ]);
  expect(secondKernel.envPtrs.map((ptr) => readCString(secondKernelMemory, ptr))).toEqual([
    "KEY=SECOND",
    "LANG=C",
  ]);
  expect(secondKernel.processSetCwdIno).toHaveBeenCalledWith(0x1234, 44);

  expect(resolveProcessCwdInoViaKernelWorker).toHaveBeenNthCalledWith(1, expect.any(Function), "/work/first");
  expect(resolveProcessCwdInoViaKernelWorker).toHaveBeenNthCalledWith(2, expect.any(Function), "/work/second");
  expect(processHandles.get(1)).toBe(parent);
  expect(processHandles.get(9)).toBe(firstChild);
  expect(processHandles.get(10)).toBe(secondChild);
});
