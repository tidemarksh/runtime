import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../../src/abi.js";
import { readPublishedKernelRuntimeState } from "../../../../src/bridge/runtime-state.js";
import {
  DATA_REGION_OFFSET,
  publishInode,
  readResidentFileData,
} from "../../../../src/fs/sab-page-cache.js";
import { createWorkerProcessFactory } from "../../../../src/worker/create-process.js";
import { guestWriteWatchRegions } from "../../../../src/worker/guest-write-watch-state.js";
import { destroyWorkerPool } from "../../../../src/worker/pool.js";
import type { ProcessHandle } from "../../../../src/worker/types.js";
import { AutoPreparedFakeWorker } from "../../../support/fake-worker.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";

const decoder = new TextDecoder();
const encoder = new TextEncoder();
const originalWorker = globalThis.Worker;

function makeKernelState(): KernelRuntimeState {
  return {
    heapBase: 0x200000,
    heapPtr: 0x210000,
    ofdTablePtr: 0x220000,
    memfsPtr: 0x230000,
    memfsHeaderBytes: 32,
    memfsInodesBytes: 4096,
    memfsFirstChildBytes: 1024,
    memfsDirEntriesBytes: 4096,
    memfsDataBytes: 16384,
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
  processUsePipeStdio: ReturnType<typeof vi.fn>;
  debugClearGuestWriteWatchTrace: ReturnType<typeof vi.fn>;
  debugConfigureGuestWriteWatch: ReturnType<typeof vi.fn>;
  memfsPaths: Map<string, number>;
  callLog: string[];
  externalizedInos: Set<number>;
};

function makeFakeProcessKernel(memory: WebAssembly.Memory): FakeProcessKernel {
  let runtimeState = makeKernelState();
  const argPtrs: number[] = [];
  const envPtrs: number[] = [];
  const processSetCwdIno = vi.fn();
  const processUsePipeStdio = vi.fn(() => 0);
  const callLog: string[] = [];
  const debugClearGuestWriteWatchTrace = vi.fn(() => {
    callLog.push("debug_clear_guest_write_watch_trace");
  });
  const debugConfigureGuestWriteWatch = vi.fn((slot: number, start: bigint, end: bigint) => {
    callLog.push(`debug_configure_guest_write_watch:${slot}:0x${start.toString(16)}:0x${end.toString(16)}`);
  });
  const pathToIno = new Map<string, number>([["/", 1]]);
  const inoToPath = new Map<number, string>([[1, "/"]]);
  const inoMode = new Map<number, number>([[1, 0o040755]]);
  const fileData = new Map<number, Uint8Array>();
  const fileSizes = new Map<number, number>();
  const externalizedInos = new Set<number>();
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
    ...makeSupervisorAbiStubs(),
    memory,
    __stack_pointer: new WebAssembly.Global({ value: "i32", mutable: true }, 0),
    abi_max_fds: () => 64,
    abi_max_threads: () => 8,
    kernel_init: vi.fn(),
    set_base_time: vi.fn(),
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 64 * 32,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => runtimeState.memfsHeaderBytes,
    kernel_memfs_inodes_bytes: () => runtimeState.memfsInodesBytes,
    kernel_memfs_first_child_bytes: () => runtimeState.memfsFirstChildBytes,
    kernel_memfs_dir_entries_bytes: () => runtimeState.memfsDirEntriesBytes,
    kernel_memfs_data_bytes: () => runtimeState.memfsDataBytes,
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
        ...runtimeState,
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
    abi_guest_address_profile_target: vi.fn(() => 2),
    process_inspect_elf_brk_with_memory_policy: vi.fn(() => {
      callLog.push("process_inspect_elf_brk_with_memory_policy");
      return 0x2000;
    }),
    process_inspect_elf_brk: vi.fn(() => 0x2000),
    process_clear_args: vi.fn(),
    process_push_arg: vi.fn((ptr: number) => {
      argPtrs.push(ptr);
    }),
    process_push_env: vi.fn((ptr: number) => {
      envPtrs.push(ptr);
    }),
    process_guest_base_for_address_profile: vi.fn(() => {
      callLog.push("process_guest_base_for_address_profile");
      return 0x100000n;
    }),
    process_guest_base: vi.fn(() => 0x100000),
    process_create_with_memory_policy: vi.fn(() => 0x1234),
    process_create: vi.fn(() => 0x1234),
    debug_clear_guest_write_watch_trace: debugClearGuestWriteWatchTrace,
    debug_configure_guest_write_watch: debugConfigureGuestWriteWatch,
    processUsePipeStdio,
    debugClearGuestWriteWatchTrace,
    debugConfigureGuestWriteWatch,
    thread_set_guest_memory_policy_limit: vi.fn(),
    process_stdin_pipe: () => 1,
    process_stdout_pipe: () => 2,
    process_stderr_pipe: () => 3,
    process_use_pipe_stdio: processUsePipeStdio,
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
      const nextSize = Number(size);
      fileData.set(ino, new Uint8Array(nextSize));
      fileSizes.set(ino, nextSize);
      externalizedInos.delete(ino);
      return 0n;
    },
    memfs_set_file_size_shallow: (ino: number, size: bigint) => {
      callLog.push("memfs_set_file_size_shallow");
      const nextSize = Number(size);
      fileData.set(ino, new Uint8Array(nextSize));
      fileSizes.set(ino, nextSize);
      return 0n;
    },
    memfs_set_file_external: (ino: number, size: bigint) => {
      callLog.push("memfs_set_file_external");
      fileSizes.set(ino, Number(size));
      fileData.delete(ino);
      externalizedInos.add(ino);
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
      fileSizes.set(ino, next.length);
      externalizedInos.delete(ino);
      return BigInt(dataLen);
    },
    memfs_set_mode: (ino: number, mode: number) => {
      callLog.push("memfs_set_mode");
      const current = inoMode.get(ino) ?? 0o100000;
      inoMode.set(ino, (current & 0o170000) | mode);
      return 0n;
    },
    memfs_inode_mode: (ino: number) => inoMode.get(ino) ?? 0,
    memfs_file_size: (ino: number) => BigInt(fileSizes.get(ino) ?? fileData.get(ino)?.length ?? 0),
    memfs_read_file_to: (ino: number, offset: bigint, bufPtr: number, bufLen: number) => {
      if (externalizedInos.has(ino)) {
        return 0n;
      }
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
    externalizedInos,
  } as unknown as FakeProcessKernel;
}

function makeDynamicElfWithInterp(interpPath: string): Uint8Array {
  const interpBytes = encoder.encode(`${interpPath}\0`);
  const elf = new Uint8Array(0x78 + interpBytes.length);
  const view = new DataView(elf.buffer);
  elf.set([0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00], 0);
  view.setBigUint64(32, 0x40n, true);
  view.setUint16(54, 56, true);
  view.setUint16(56, 1, true);
  view.setUint32(0x40, 3, true);
  view.setBigUint64(0x40 + 8, 0x78n, true);
  view.setBigUint64(0x40 + 32, BigInt(interpBytes.length), true);
  view.setBigUint64(0x40 + 40, BigInt(interpBytes.length), true);
  elf.set(interpBytes, 0x78);
  return elf;
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
    guestMemoryPolicyLimitBytes: 0,
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
    vforkReadyWorkers: [],
    runningWorker: null,
    hostBlockedWorkers: new Map(),
    vforkSnapshot: null,
    vforkWorker: null,
    workerDebugLog: [],
  } as unknown as ProcessHandle;
}

beforeEach(() => {
  globalThis.Worker = class extends AutoPreparedFakeWorker {} as unknown as typeof globalThis.Worker;
});

afterEach(() => {
  destroyWorkerPool();
  globalThis.Worker = originalWorker;
  vi.unstubAllEnvs();
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
  expect(fakeKernel.processUsePipeStdio).toHaveBeenCalledWith(0x1234);
  expect(fakeKernel.processSetCwdIno).toHaveBeenCalledWith(0x1234, 1);
  expect(registerProcessWithKernelWorker).not.toHaveBeenCalled();
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
  expect(attachThreadWorker).toHaveBeenCalledWith(handle, {
    worker: handle.worker,
    vmPtr: 0x1234,
  });
});

test("createProcess configures debug guest write watch before attaching the worker", async () => {
  vi.stubEnv("TIDEMARK_DEBUG_GUEST_WRITE_WATCH", "0x1000:0x1020,0x2000:0x2010 0x3000:0x3010");
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const attachThreadWorker = vi.fn(() => {
    fakeKernel.callLog.push("attachThreadWorker");
  });
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker,
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
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
      argv: ["/bin/test"],
      env: [],
      preloadEntries: [],
    },
  );

  expect(guestWriteWatchRegions(handle)).toEqual([
    { slot: 0, start: 0x1000, end: 0x1020 },
    { slot: 1, start: 0x2000, end: 0x2010 },
  ]);
  expect(fakeKernel.debugClearGuestWriteWatchTrace).toHaveBeenCalledTimes(1);
  expect(fakeKernel.debugConfigureGuestWriteWatch).toHaveBeenCalledTimes(2);
  expect(fakeKernel.debugConfigureGuestWriteWatch).toHaveBeenNthCalledWith(1, 0, 0x1000n, 0x1020n);
  expect(fakeKernel.debugConfigureGuestWriteWatch).toHaveBeenNthCalledWith(2, 1, 0x2000n, 0x2010n);
  expect(fakeKernel.callLog.indexOf("debug_configure_guest_write_watch:1:0x2000:0x2010"))
    .toBeLessThan(fakeKernel.callLog.indexOf("attachThreadWorker"));
});

test("createProcess uses target guest address profile for process memory policy", async () => {
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 0),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });
  const guestPolicyLimitBytes = 512 * 1024 * 1024;

  await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      guestMemoryPolicyLimitBytes: guestPolicyLimitBytes,
      maxMemoryPages: 1024,
      preloadEntries: [],
    },
  );

  expect(fakeKernel.process_create_with_memory_policy).toHaveBeenCalledWith(
    expect.any(Number),
    expect.any(Number),
    2,
    BigInt(guestPolicyLimitBytes),
    1024,
    8 * 1024 * 1024,
  );
  expect(fakeKernel.process_inspect_elf_brk_with_memory_policy).toHaveBeenCalledWith(
    expect.any(Number),
    expect.any(Number),
    2,
    BigInt(guestPolicyLimitBytes),
  );
  expect(fakeKernel.thread_set_guest_memory_policy_limit).toHaveBeenCalledWith(
    0x1234,
    BigInt(guestPolicyLimitBytes),
  );
});

test("createProcess preserves Memory64 process backing page requests above the wasm32 page ceiling", async () => {
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({
    initial: 1024n,
    maximum: 70000n,
    shared: true,
    address: "i64",
  } as unknown as WebAssembly.MemoryDescriptor);
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 0),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 131072,
  });

  await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      maxMemoryPages: 70000,
      preloadEntries: [],
    },
  );

  expect(fakeKernel.process_create_with_memory_policy).toHaveBeenCalledWith(
    expect.any(Number),
    expect.any(Number),
    2,
    expect.any(BigInt),
    70000,
    8 * 1024 * 1024,
  );
});

test("createProcess rejects an unusable target guest profile instead of falling back", async () => {
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.mocked(fakeKernel.process_guest_base_for_address_profile).mockImplementation((profile: number) => {
    fakeKernel.callLog.push("process_guest_base_for_address_profile");
    return profile === 2 ? 0n : 0x100000n;
  });
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 0),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  await expect(factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      preloadEntries: [],
    },
  )).rejects.toThrow("kernel ABI returned invalid target guest address base");

  expect(fakeKernel.process_create_with_memory_policy).not.toHaveBeenCalled();
  expect(fakeKernel.process_inspect_elf_brk_with_memory_policy).not.toHaveBeenCalled();
});

test("createProcess preserves PTY stdio only when requested", async () => {
  const processHandles = new Map<number, ProcessHandle>();
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries: () => [],
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker: vi.fn(async () => {}),
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 0),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  await factory.createProcess(
    new Uint8Array([0]),
    new Uint8Array([0x7f, 0x45, 0x4c, 0x46]),
    {
      argv: ["/bin/sh"],
      preloadEntries: [],
      stdio: "pty",
    },
  );

  expect(fakeKernel.processUsePipeStdio).not.toHaveBeenCalled();
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
  expect(registerProcessWithKernelWorker).toHaveBeenCalledWith(handle, { seedMemfsMode: "none" });
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

  expect(attachThreadWorker).toHaveBeenCalledWith(handle, {
    worker: handle.worker,
    vmPtr: 0x1234,
  });
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

  expect(attachThreadWorker).toHaveBeenCalledWith(handle, {
    worker: handle.worker,
    vmPtr: 0x1234,
  });
  expect(bridgeStatesAtAttach).toEqual([handle.kernelState]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x60);
  expect(handle.kernelState.processRuntimeStatePtr).toBe(makeKernelState().processRuntimeStatePtr + 0x80);
  expect(handle.kernelState.processPendingPtr).toBe(makeKernelState().processPendingPtr + 0x20);
});

test("createProcess fetches a metadata-only kernel-worker snapshot when runtime fs entries are metadata-only", async () => {
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
      includeData: false,
    });
    return {
      type: "snapshot-fs-result" as const,
      requestId: 1,
      entries: [{
        kind: "file" as const,
        path: "/bin/app",
        size: "materialized-app".length,
        mode: 0o755,
      }],
    };
  });
  const registerProcessWithKernelWorker = vi.fn(async () => {});
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries,
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker,
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
  expect(Number(fakeKernel.memfs_file_size(ino))).toBe("materialized-app".length);
  expect(fakeKernel.externalizedInos.has(ino)).toBe(true);
  expect(fakeKernel.callLog).toContain("memfs_set_file_external");
  expect(registerProcessWithKernelWorker).toHaveBeenCalledWith(expect.anything(), { seedMemfsMode: "metadata" });
});

test("createProcess publishes PT_INTERP through backing ranges for dynamic ELF metadata-only snapshots", async () => {
  const processHandles = new Map<number, ProcessHandle>([[1, makeParentHandle()]]);
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  const sabPageCache = new SharedArrayBuffer(DATA_REGION_OFFSET + 8192);
  const interpIno = 42;
  const interpSize = 30;
  publishInode(sabPageCache, interpIno, {
    mode: 0o100755,
    size: interpSize,
    parentIno: 2,
    name: encoder.encode("ld-linux-riscv64-lp64d.so.1"),
    nextSibling: 0,
    fileObjectId: interpIno,
    generation: 1,
  });
  vi.spyOn(WebAssembly, "instantiate").mockResolvedValue({
    exports: fakeKernel,
  } as unknown as WebAssembly.Instance);
  const getRuntimeFsEntries = vi.fn(() => [{
    kind: "file" as const,
    path: "/lib/ld-linux-riscv64-lp64d.so.1",
    ino: interpIno,
    size: interpSize,
    mode: 0o755,
  }]);
  const interpBytes = encoder.encode("dynamic-loader-backing-range!!");
  expect(interpBytes.length).toBe(interpSize);
  const kernelWorkerRequest = vi.fn(async (msg: {
    type: string;
    includeData?: boolean;
    ino?: number;
    offset?: number;
    length?: number;
    expectedSize?: number;
  }) => {
    if (msg.type === "snapshot-fs") {
      expect(msg).toEqual({
        type: "snapshot-fs",
        includeData: false,
      });
      return {
        type: "snapshot-fs-result" as const,
        requestId: 1,
        entries: [{
          kind: "file" as const,
          path: "/lib/ld-linux-riscv64-lp64d.so.1",
          ino: interpIno,
          size: interpSize,
          mode: 0o755,
        }],
      };
    }
    if (msg.type === "read-file-range-by-inode") {
      expect(msg).toEqual({
        type: "read-file-range-by-inode",
        ino: interpIno,
        offset: 0,
        length: interpSize,
        expectedSize: interpSize,
      });
      return {
        type: "file-range-read" as const,
        requestId: 2,
        data: interpBytes,
      };
    }
    throw new Error(`unexpected kernel worker request: ${msg.type}`);
  });
  const registerProcessWithKernelWorker = vi.fn(async () => {});
  const factory = createWorkerProcessFactory({
    processHandles,
    attachThreadWorker: vi.fn(),
    assertProcessStandardFdIntegrity: vi.fn(),
    ensureCompiledModule: async () => ({}) as WebAssembly.Module,
    getRuntimeFsEntries,
    initializeProcessLifecycleViaKernelWorker: vi.fn(async () => {}),
    registerProcessWithKernelWorker,
    resolveProcessCwdInoViaKernelWorker: vi.fn(async () => 1),
    bootstrapMemoryBytes: 64 * 1024 * 1024,
    wasmPageSize: 65536,
    wasmMaxPages: 65536,
  });

  await factory.createProcess(
    new Uint8Array([0]),
    makeDynamicElfWithInterp("/lib/ld-linux-riscv64-lp64d.so.1"),
    {
      runtimeId: 7,
      pid: 2,
      parentPid: 1,
      argv: ["app"],
      hydrateFsFromKernelWorker: true,
      sabPageCache,
      kernelWorkerRequest,
    },
  );

  expect(getRuntimeFsEntries).toHaveBeenCalledTimes(1);
  expect(kernelWorkerRequest).toHaveBeenCalledTimes(2);
  expect(kernelWorkerRequest.mock.calls.some(([msg]) => msg.type === "read-file")).toBe(false);
  const ino = fakeKernel.memfsPaths.get("/lib/ld-linux-riscv64-lp64d.so.1");
  expect(ino).toBeGreaterThan(0);
  expect(fakeKernel.externalizedInos.has(ino!)).toBe(true);
  expect(Number(fakeKernel.memfs_file_size(ino!))).toBe(interpSize);
  expect(fakeKernel.callLog).toContain("memfs_set_file_external");
  expect(fakeKernel.callLog).not.toContain("memfs_write_file_at");
  expect(readResidentFileData(sabPageCache, interpIno, 0, interpSize)).toEqual(interpBytes);
});

test("createProcess computes guest base after preloading runtime fs entries", async () => {
  const processHandles = new Map<number, ProcessHandle>([[1, makeParentHandle()]]);
  const kernelMemory = new WebAssembly.Memory({ initial: 1024, maximum: 2048, shared: true });
  const fakeKernel = makeFakeProcessKernel(kernelMemory);
  const processGuestBase = vi.mocked(fakeKernel.process_guest_base_for_address_profile);
  processGuestBase.mockImplementation(() => {
    fakeKernel.callLog.push("process_guest_base_for_address_profile");
    return fakeKernel.memfsPaths.has("/bin/app") ? 0x200000n : 0x100000n;
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
  const guestBaseIndex = fakeKernel.callLog.indexOf("process_guest_base_for_address_profile");
  const inspectIndex = fakeKernel.callLog.indexOf("process_inspect_elf_brk_with_memory_policy");
  const preloadIndex = fakeKernel.callLog.findIndex((entry) => entry.startsWith("memfs_"));
  expect(preloadIndex).toBeGreaterThanOrEqual(0);
  expect(guestBaseIndex).toBeGreaterThan(preloadIndex);
  expect(inspectIndex).toBeGreaterThan(preloadIndex);
  expect(inspectIndex).toBeGreaterThan(guestBaseIndex);
  expect(processGuestBase).toHaveReturnedWith(0x200000n);
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
