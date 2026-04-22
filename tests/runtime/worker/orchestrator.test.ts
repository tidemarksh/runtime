import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../../../src/abi.js";
import { writePipe } from "../../../src/bridge/pipe.js";
import { readPublishedKernelRuntimeState } from "../../../src/bridge/runtime-state.js";
import type {
  KernelWorkerOutbound,
  KernelWorkerRequest,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "../../../src/messages.js";

type WorkerModule = typeof import("../../../src/worker.js");

let workerModule: WorkerModule | null = null;

function clearRuntimeFsState(runtimeId: number): void {
  workerModule?.clearRuntimeFsState(runtimeId);
}

function clearModuleCache(): void {
  workerModule?.clearModuleCache();
}

function createProcess(...args: Parameters<WorkerModule["createProcess"]>) {
  return workerModule!.createProcess(...args);
}

function destroyProcessTree(...args: Parameters<WorkerModule["destroyProcessTree"]>): void {
  workerModule?.destroyProcessTree(...args);
}

function injectHttpRequest(...args: Parameters<WorkerModule["injectHttpRequest"]>) {
  return workerModule!.injectHttpRequest(...args);
}

function mirrorRuntimeFsEntry(...args: Parameters<WorkerModule["mirrorRuntimeFsEntry"]>): void {
  workerModule?.mirrorRuntimeFsEntry(...args);
}

function primeRuntimeFsState(...args: Parameters<WorkerModule["primeRuntimeFsState"]>) {
  return workerModule!.primeRuntimeFsState(...args);
}

function replaceRuntimeFsSnapshot(...args: Parameters<WorkerModule["replaceRuntimeFsSnapshot"]>): void {
  workerModule?.replaceRuntimeFsSnapshot(...args);
}

function writeProcessStdin(...args: Parameters<WorkerModule["writeProcessStdin"]>): void {
  workerModule?.writeProcessStdin(...args);
}

class FakeWorker {
  onmessage:
    | ((event: MessageEvent<ThreadWorkerOutbound>) => void)
    | null = null;
  readonly postedMessages: ThreadWorkerInbound[] = [];
  terminated = false;

  constructor(
    readonly _url: URL,
    readonly _options: WorkerOptions,
  ) {}

  postMessage(message: ThreadWorkerInbound): void {
    if (message.type === "prepare") {
      queueMicrotask(() => {
        this.onmessage?.({ data: { type: "prepared", wasmFeatures: {} } as ThreadWorkerOutbound } as MessageEvent<ThreadWorkerOutbound>);
      });
      return;
    }
    this.postedMessages.push(message);
  }

  terminate(): void {
    this.terminated = true;
  }

  emit(message: ThreadWorkerOutbound): void {
    this.onmessage?.({ data: message } as MessageEvent<ThreadWorkerOutbound>);
  }

  clearPostedMessages(): void {
    this.postedMessages.length = 0;
  }
}

const createdWorkers: FakeWorker[] = [];
const createdKernelExports: Array<
  KernelExports & {
    recordedChildExits: Array<[number, number]>;
    retainedOfds: number[];
    liveThreadVmPtrs: number[];
    fsProcessCalls: number;
    memfsCreateCalls: Array<[number, number]>;
    memfsTruncateCalls: Array<[number, number]>;
    memfsSetFileSizeShallowCalls: Array<[number, number]>;
    memfsWriteCalls: Array<[number, number]>;
    memfsCreateWithDataCalls: Array<[number, number]>;
  }
> = [];
const threadCountQueue: number[] = [];
const injectedHttpRequests: Array<{ port: number; request: string }> = [];
const httpResponseChunks: Uint8Array[] = [];
const closedHttpConnections: number[] = [];
const originalWorker = globalThis.Worker;

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

const SYNC_EFFECT_PIPE_STATE = 1 << 0;
const SYNC_EFFECT_SOCKET_STATE = 1 << 1;
const SYNC_EFFECT_EPOLL_STATE = 1 << 2;
const SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE = 1 << 3;
const SYNC_EFFECT_PROCESS_LIFECYCLE = 1 << 4;
const SYNC_EFFECT_FORK_SHARED_OFD_STATE = 1 << 6;

function emitReady(
  worker: FakeWorker,
  vmPtr = 0x1234,
  kernelState = makeKernelState(),
): void {
  worker.emit({
    type: "ready",
    vmPtr,
    kernelState,
    wasmFeatures: {
      memory64: true,
      tailCalls: true,
      exceptionHandling: true,
    },
  });
}

async function flushMicrotasks(turns = 20): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await Promise.resolve();
  }
}

function makeKernelExports(memory: WebAssembly.Memory): KernelExports {
  let nextVmPtr = 0x2000;
  let runtimeState = makeKernelState();
  const threadCount = threadCountQueue.shift() ?? 1;
  const recordedChildExits: Array<[number, number]> = [];
  const retainedOfds: number[] = [];
  const liveThreadVmPtrs = Array.from({ length: threadCount }, (_, index) => 0x1234 + index * 0x10);
  let fsProcessCalls = 0;
  const memfsCreateCalls: Array<[number, number]> = [];
  const memfsTruncateCalls: Array<[number, number]> = [];
  const memfsSetFileSizeShallowCalls: Array<[number, number]> = [];
  const memfsWriteCalls: Array<[number, number]> = [];
  const memfsCreateWithDataCalls: Array<[number, number]> = [];
  let nextIno = 10;
  let currentPid = 2;
  let currentPpid = 1;
  let currentPgid = 1;
  let currentSid = 1;
  let currentDidExec = 0;
  function initPipeSlot(slot: number): void {
    const base = runtimeState.pipeBase + slot * (32 + 4096);
    const view = new DataView(memory.buffer);
    view.setInt32(base + 8, 1, true);
    view.setInt32(base + 12, 1, true);
  }
  const exports: KernelExports & {
    recordedChildExits: Array<[number, number]>;
    fsProcessCalls: number;
    memfsCreateCalls: Array<[number, number]>;
    memfsTruncateCalls: Array<[number, number]>;
    memfsSetFileSizeShallowCalls: Array<[number, number]>;
    memfsWriteCalls: Array<[number, number]>;
    memfsCreateWithDataCalls: Array<[number, number]>;
  } = {
    memory,
    abi_status_ok: () => 0,
    abi_status_fault: () => 3,
    abi_status_fs: () => 4,
    abi_status_futex_wait: () => 5,
    abi_status_pipe_wait: () => 6,
    abi_status_epoll_wait: () => 7,
    abi_status_vfork_wait: () => 8,
    abi_status_execve_spawn: () => 9,
    abi_status_clone_spawn: () => 10,
    abi_status_sleep: () => 11,
    abi_status_jit_ready: () => 12,
    abi_status_wait4: () => 31,
    abi_status_connect: () => 28,
    abi_status_socket_send: () => 29,
    abi_status_socket_recv: () => 30,
    abi_status_flock_wait: () => 32,
    abi_spawn_kind_none: () => 0,
    abi_spawn_kind_execve: () => 1,
    abi_spawn_kind_fork: () => 2,
    abi_page_size: () => 4096,
    abi_max_fds: () => 256,
    abi_max_threads: () => 64,
    abi_max_pipes: () => 64,
    abi_fd_type_none: () => 0,
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => 2,
    abi_fd_type_epoll: () => 3,
    abi_fd_type_socket: () => 4,
    abi_sigkill: () => 9,
    abi_sigterm: () => 15,
    abi_sigchld: () => 17,
    abi_sigpipe: () => 13,
    abi_clone_parent: () => 0x00008000,
    abi_o_rdonly: () => 0,
    abi_o_wronly: () => 1,
    abi_o_rdwr: () => 2,
    abi_o_creat: () => 0x40,
    abi_o_trunc: () => 0x200,
    abi_o_append: () => 0x400,
    abi_o_cloexec: () => 0x80000,
    abi_enoent: () => 2,
    abi_ebadf: () => 9,
    abi_einval: () => 22,
    abi_enomem: () => 12,
    abi_enosys: () => 38,
    abi_eintr: () => 4,
    abi_eagain: () => 11,
    abi_epipe: () => 32,
    abi_econnrefused: () => 111,
    set_base_time: () => {},
    kernel_init: () => {},
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
    kernel_ofd_table_bytes: () => 256 * 32,
    kernel_memfs_ptr: () => runtimeState.memfsPtr,
    kernel_memfs_header_bytes: () => 32,
    kernel_memfs_inodes_ptr: () => runtimeState.memfsPtr + 0x100,
    kernel_memfs_inodes_bytes: () => 16384 * 32,
    kernel_memfs_first_child_ptr: () => runtimeState.memfsPtr + 0x200000,
    kernel_memfs_first_child_bytes: () => 16384 * 4,
    kernel_memfs_dir_entries_ptr: () => runtimeState.memfsPtr + 0x220000,
    kernel_memfs_dir_entries_bytes: () => 16384 * 264,
    kernel_memfs_data_ptr: () => runtimeState.memfsPtr + 0x900000,
    kernel_memfs_data_bytes: () => 256 * 1024,
    kernel_mem_state_ptr: () => runtimeState.memStatePtr,
    kernel_epoll_table_ptr: () => runtimeState.epollTablePtr,
    kernel_epoll_table_bytes: () => 64 * 4,
    kernel_epoll_instance_bytes: () => 64 * 16 + 4,
    kernel_socket_table_ptr: () => runtimeState.socketTablePtr,
    kernel_socket_table_bytes: () => 64 * 128,
    kernel_socket_used: () => runtimeState.socketUsed,
    kernel_pipe_base: () => runtimeState.pipeBase,
    kernel_pipe_region_bytes: () => 64 * (32 + 4096),
    kernel_pipe_used: () => runtimeState.pipeUsed,
    kernel_net_buf_base: () => runtimeState.netBufBase,
    kernel_net_buf_pool_bytes: () => 128 * (16 + 8192),
    kernel_net_buf_used: () => runtimeState.netBufUsed,
    kernel_sig_slots_ptr: () => runtimeState.sigSlotsPtr,
    kernel_sig_slots_bytes: () => 64 * 4,
    kernel_signal_action_table_bytes: () => 64 * 24,
    kernel_process_pending_ptr: () => runtimeState.processPendingPtr,
    kernel_process_pending_bytes: () => 64 * 8,
    kernel_process_runtime_state_ptr: () => runtimeState.processRuntimeStatePtr,
    kernel_process_runtime_state_bytes: () => 4096,
    kernel_time_state_ptr: () => runtimeState.timeStatePtr,
    kernel_time_state_bytes: () => 4096,
    kernel_reset_memfs: () => {},
    kernel_attach_runtime_state: (
      heapBase, heapPtr, ofdTablePtr, memfsPtr, memStatePtr,
      epollTablePtr, socketTablePtr, socketUsed, pipeBase, pipeUsed,
      netBufBase, netBufUsed, sigSlotsPtr, processPendingPtr,
      processRuntimeStatePtr, timeStatePtr,
    ) => {
      runtimeState = {
        heapBase, heapPtr, ofdTablePtr, memfsPtr, memStatePtr,
        epollTablePtr, socketTablePtr, socketUsed, pipeBase, pipeUsed,
        netBufBase, netBufUsed, sigSlotsPtr, processPendingPtr,
        processRuntimeStatePtr, timeStatePtr,
      };
    },
    process_push_arg: () => {},
    process_push_env: () => {},
    process_clear_args: () => {},
    process_guest_base: () => 0x1000000,
    process_inspect_elf_brk: () => 0x40_0000n,
    process_create: () => {
      initPipeSlot(1);
      initPipeSlot(2);
      initPipeSlot(3);
      return 0x1234;
    },
    thread_step: () => 0,
    thread_status: () => 0,
    thread_exit_code: () => 0,
    thread_term_signal: () => 0,
    thread_pc: () => 0n,
    thread_ra: () => 0n,
    thread_sp: () => 0n,
    thread_s11: () => 0n,
    thread_tls_base: () => 0n,
    thread_a0: () => 0n,
    thread_pending_fs_effects: () => 0,
    thread_pending_fs_target_ino: () => 0,
    thread_pending_fs_opcode: () => 0,
    thread_process_fs: () => {
      fsProcessCalls += 1;
      runtimeState = { ...runtimeState, heapPtr: runtimeState.heapPtr + 0x10 };
    },
    thread_resume_fs: () => 0,
    thread_futex_wait_addr: () => 0n,
    thread_futex_wait_timeout: () => 0n,
    thread_futex_wait_expected: () => 0,
    thread_resume_futex_wait: () => {},
    thread_resume_futex_wait_result: () => {},
    thread_sleep_seconds: () => 0n,
    thread_sleep_nanoseconds: () => 0n,
    thread_record_blocked_addr: () => 0,
    thread_resume_wait4: () => 0n,
    thread_epoll_wait_timeout: () => 0n,
    thread_resume_epoll_wait_timeout: () => {},
    thread_resume_blocked_syscall: () => 0n,
    thread_resume_connect_result: () => {},
    thread_vfork_child_pid: () => 0,
    thread_blocked_fd: () => 0,
    thread_blocked_ofd_id: () => 0,
    thread_clone_flags: () => 0n,
    thread_spawn_kind: () => 0,
    thread_spawn_child_pid: () => 0,
    thread_spawn_path: () => 0n,
    thread_spawn_argv: () => 0n,
    thread_spawn_envp: () => 0n,
    thread_spawn_ram_size: () => 0,
    thread_set_execve_handoff: () => {},
    thread_set_guest_ram_limit: () => {},
    thread_clear_spawn_handoff: () => {},
    thread_resume_vfork_parent: () => {},
    thread_restore_vfork_parent_without_cleanup: () => {},
    thread_complete_execve_handoff: () => 0n,
    thread_spawn_from_clone: () => nextVmPtr++,
    thread_process_thread_count: () => threadCount,
    thread_process_ptr: () => 0x5000,
    process_state_bytes: () => 0x1000,
    process_thread_tid: (_vmPtr, slot) => (liveThreadVmPtrs[slot] ?? 0) === 0 ? 0 : slot + 1,
    process_thread_state: (_vmPtr, slot) => liveThreadVmPtrs[slot] ? 2 : 0,
    process_thread_vm_ptr: (_vmPtr, slot) => liveThreadVmPtrs[slot] ?? 0,
    process_record_child_exit: (pid, waitStatus) => {
      recordedChildExits.push([pid, waitStatus]);
    },
    process_current_pid: () => currentPid,
    process_current_ppid: () => currentPpid,
    process_current_pgid: () => currentPgid,
    process_current_sid: () => currentSid,
    process_current_did_exec: () => currentDidExec,
    process_rewrite_child_pid: (_vmPtr, oldPid, newPid, _nextPid) => {
      if (currentPid === oldPid) {
        currentPid = newPid;
      }
    },
    process_reset_lifecycle: (_vmPtr, pid, ppid, _nextPid, pgid, sid, didExec) => {
      currentPid = pid;
      currentPpid = ppid;
      currentPgid = pgid;
      currentSid = sid;
      currentDidExec = didExec;
    },
    process_upsert_process_entry: (_pid, _ppid, _pgid, _sid, _didExec) => {},
    process_remove_process_entry: () => {},
    process_stdin_pipe: () => 1,
    process_stdout_pipe: () => 2,
    process_stderr_pipe: () => 3,
    process_set_host_stdio: () => {},
    process_cwd_ino: () => 1,
    process_set_cwd_ino: () => {},
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    process_set_fd_entry: () => {},
    kernel_retain_ofd: (ofdId) => {
      retainedOfds.push(ofdId);
    },
    kernel_release_ofd: () => {},
    kernel_ofd_fd_type: (ofdId) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32, true),
    kernel_ofd_host_fd: (ofdId) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 4, true),
    kernel_ofd_status_flags: (ofdId) => new DataView(memory.buffer)
      .getInt32(runtimeState.ofdTablePtr + ofdId * 32 + 8, true),
    kernel_ofd_ino: (ofdId) => new DataView(memory.buffer)
      .getUint32(runtimeState.ofdTablePtr + ofdId * 32 + 24, true),
    kernel_ofd_refcount: () => 0,
    kernel_ofd_offset: () => 0n,
    process_release_host_stdio: () => {},
    process_inject_http: (_vmPtr, port, reqPtr, reqLen) => {
      const req = new TextDecoder().decode(new Uint8Array(memory.buffer, reqPtr, reqLen));
      injectedHttpRequests.push({ port, request: req });
      return 17;
    },
    process_read_http_response: (_vmPtr, _connId, dstPtr, dstLen) => {
      const chunk = httpResponseChunks.shift();
      if (!chunk) {
        return -1;
      }
      const copied = chunk.subarray(0, dstLen);
      new Uint8Array(memory.buffer).set(copied, dstPtr);
      return copied.length;
    },
    process_close_http_connection: (_vmPtr, connId) => {
      closedHttpConnections.push(connId);
    },
    memfs_resolve_path: () => 0,
    memfs_create_file: (parentIno, _namePtr, nameLen) => {
      memfsCreateCalls.push([parentIno, nameLen]);
      return nextIno++;
    },
    memfs_reserve_inode_until: (targetIno) => {
      nextIno = Math.max(nextIno, targetIno);
      return 0n;
    },
    memfs_write_file_at: (ino, offset, _dataPtr, dataLen) => {
      memfsWriteCalls.push([ino, dataLen]);
      return offset + BigInt(dataLen);
    },
    memfs_read_file_to: () => 0n,
    memfs_file_size: () => 0n,
    memfs_truncate_file: (ino, size) => {
      memfsTruncateCalls.push([ino, Number(size)]);
      return 0n;
    },
    memfs_set_file_size_shallow: (ino, size) => {
      memfsSetFileSizeShallowCalls.push([ino, Number(size)]);
      return 0n;
    },
    memfs_inode_mode: () => 0,
    memfs_inode_nlink: () => 0,
    memfs_inode_uid: () => 0,
    memfs_inode_gid: () => 0,
    memfs_ensure_dir: () => nextIno++,
    memfs_set_mode: () => 0n,
    memfs_create_file_with_data: (parentIno, _namePtr, nameLen, _dataPtr, dataLen) => {
      memfsCreateWithDataCalls.push([parentIno, nameLen + dataLen]);
      return nextIno++;
    },
    memfs_readdir_names: () => 0n,
    recordedChildExits,
    retainedOfds,
    liveThreadVmPtrs,
    get fsProcessCalls() {
      return fsProcessCalls;
    },
    memfsCreateCalls,
    memfsTruncateCalls,
    memfsSetFileSizeShallowCalls,
    memfsWriteCalls,
    memfsCreateWithDataCalls,
  };
  createdKernelExports.push(exports);
  return exports;
}

beforeEach(async () => {
  vi.resetModules();
  workerModule = await import("../../../src/worker.js");
  createdWorkers.length = 0;
  createdKernelExports.length = 0;
  threadCountQueue.length = 0;
  injectedHttpRequests.length = 0;
  httpResponseChunks.length = 0;
  closedHttpConnections.length = 0;
  globalThis.Worker = class extends FakeWorker {
    constructor(url: URL, options: WorkerOptions) {
      super(url, options);
      createdWorkers.push(this);
    }
  } as unknown as typeof Worker;

  const fakeModule = {} as WebAssembly.Module;
  vi.spyOn(WebAssembly, "compile").mockResolvedValue(fakeModule);
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    return {
      exports: makeKernelExports(memory),
    } as WebAssembly.WebAssemblyInstantiatedSource;
  });
});

afterEach(() => {
  workerModule?.destroyWorkerPool();
  clearModuleCache();
  workerModule = null;
  vi.restoreAllMocks();
  globalThis.Worker = originalWorker;
  vi.resetModules();
});

test("createProcess spawns a child worker on STATUS_CLONE_SPAWN", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  expect(handle.vmPtr).toBe(0x1234);
  expect(worker).toBeDefined();

  emitReady(worker, 0x1234, refreshedState);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages).toEqual([
    {
      type: "init",
      wasmModule: expect.any(Object),
      memory: handle.memory,
      vmPtr: 0x2000,
      guestBase: 0x1000000,
      guestBytes: Math.min(handle.ramSize, handle.memory.buffer.byteLength - 0x1000000),
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: makeKernelState(),
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    },
  ]);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
});

test("ready adopts worker kernel state before first step", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const readyState = {
    ...makeKernelState(),
    heapPtr: makeKernelState().heapPtr + 0x88,
    pipeUsed: 0x40n,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr + 0x20,
  };

  worker.clearPostedMessages();
  emitReady(worker, 0x1234, readyState);

  expect(handle.kernelState).toEqual(readyState);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: readyState },
  ]);
});

test("clone spawn refreshes kernel state before child init and parent resume", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const refreshedState = {
    ...makeKernelState(),
    heapPtr: makeKernelState().heapPtr + 0x80,
    processRuntimeStatePtr: makeKernelState().processRuntimeStatePtr + 0x40,
  };

  kernel.kernel_attach_runtime_state(
    refreshedState.heapBase,
    refreshedState.heapPtr,
    refreshedState.ofdTablePtr,
    refreshedState.memfsPtr,
    refreshedState.memStatePtr,
    refreshedState.epollTablePtr,
    refreshedState.socketTablePtr,
    refreshedState.socketUsed,
    refreshedState.pipeBase,
    refreshedState.pipeUsed,
    refreshedState.netBufBase,
    refreshedState.netBufUsed,
    refreshedState.sigSlotsPtr,
    refreshedState.processPendingPtr,
    refreshedState.processRuntimeStatePtr,
    refreshedState.timeStatePtr,
  );

  emitReady(worker, 0x1234, refreshedState);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(handle.kernelState).toEqual(refreshedState);
  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages).toEqual([
    {
      type: "init",
      wasmModule: expect.any(Object),
      memory: handle.memory,
      vmPtr: 0x2000,
      guestBase: 0x1000000,
      guestBytes: Math.min(handle.ramSize, handle.memory.buffer.byteLength - 0x1000000),
      preserveKernelHeapBase: refreshedState.heapBase,
      preserveKernelHeapBytes: refreshedState.heapPtr - refreshedState.heapBase,
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: refreshedState,
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    },
  ]);
  expect(worker.postedMessages).toEqual([]);
});


test("clone child init caps preserved guest bytes to ramSize", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  handle.ramSize = 0x2000;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages[0]).toMatchObject({
    type: "init",
    guestBase: 0x1000000,
    guestBytes: 0x2000,
  });
});

test("clone child init preserves only live guest bytes when kernel debug memory state is narrower", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  handle.ramSize = 0x200000;
  handle.kernel.debug_memory_brk_current = () => 0x4000n;
  handle.kernel.debug_memory_mmap_next = () => 0x9000n;
  handle.kernel.debug_memory_vma_len = () => 2;
  handle.kernel.debug_memory_vma_end = (index: number) => (index === 0 ? 0x6000n : 0x8000n);

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages[0]).toMatchObject({
    type: "init",
    guestBase: 0x1000000,
    guestBytes: 0x9000,
  });
});

test("clone child first ready->step uses refreshed canonical kernel state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const staleState = makeKernelState();
  const refreshedState = {
    ...staleState,
    heapPtr: staleState.heapPtr + 0x80,
    processRuntimeStatePtr: staleState.processRuntimeStatePtr + 0x40,
  };

  kernel.kernel_attach_runtime_state(
    refreshedState.heapBase,
    refreshedState.heapPtr,
    refreshedState.ofdTablePtr,
    refreshedState.memfsPtr,
    refreshedState.memStatePtr,
    refreshedState.epollTablePtr,
    refreshedState.socketTablePtr,
    refreshedState.socketUsed,
    refreshedState.pipeBase,
    refreshedState.pipeUsed,
    refreshedState.netBufBase,
    refreshedState.netBufUsed,
    refreshedState.sigSlotsPtr,
    refreshedState.processPendingPtr,
    refreshedState.processRuntimeStatePtr,
    refreshedState.timeStatePtr,
  );

  emitReady(parentWorker, 0x1234, refreshedState);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: staleState,
  });

  expect(readPublishedKernelRuntimeState(handle.runtimeBridge)).toEqual(refreshedState);

  const childWorker = createdWorkers[1];
  childWorker.clearPostedMessages();
  emitReady(childWorker, 0x2000);

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: refreshedState,
  });

  expect(childWorker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: refreshedState },
  ]);
});

test("clone child ready syncs owned epoll state before scheduling", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const epollOfdId = 7;

  new DataView(kernel.memory.buffer).setInt32(
    handle.kernelState.ofdTablePtr + epollOfdId * 32,
    kernel.abi_fd_type_epoll(),
    true,
  );
  new DataView(kernel.memory.buffer).setUint32(
    handle.kernelState.ofdTablePtr + epollOfdId * 32 + 24,
    1,
    true,
  );
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? epollOfdId : 0);

  emitReady(parentWorker, 0x1234, refreshedState);
  parentWorker.clearPostedMessages();
  requests.length = 0;
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  const childWorker = createdWorkers[1];
  requests.length = 0;
  childWorker.clearPostedMessages();
  emitReady(childWorker, 0x2000);
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(true);
  expect(childWorker.postedMessages).toEqual([]);
});

test("clone child ready drains pending output and syncs pipe state before first step", async () => {
  const requests: KernelWorkerRequest[] = [];
  let resolvePipeSync: (() => void) | null = null;
  const stdoutChunks: Uint8Array[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onStdout: (data) => {
      stdoutChunks.push(new Uint8Array(data));
    },
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          await new Promise<void>((resolve) => {
            resolvePipeSync = resolve;
          });
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  const childWorker = createdWorkers[1];
  childWorker.clearPostedMessages();
  requests.length = 0;

  const warningBytes = new TextEncoder().encode("[java] cloned child warning\n");
  expect(writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdoutPipe, warningBytes).written).toBe(
    warningBytes.length,
  );

  emitReady(childWorker, 0x2000);
  await flushMicrotasks();

  expect(stdoutChunks.map((chunk) => new TextDecoder().decode(chunk))).toEqual(["[java] cloned child warning\n"]);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(childWorker.postedMessages).toEqual([]);

  resolvePipeSync?.();
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(childWorker.postedMessages).toEqual([]);

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(childWorker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
});

test("STATUS_OK local stdout drain does not re-sync stale pipe state from kernel-worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  const stdoutChunks: Uint8Array[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onStdout: (data) => {
      stdoutChunks.push(new Uint8Array(data));
    },
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  requests.length = 0;

  const bannerBytes = new TextEncoder().encode("openjdk banner\n");
  expect(writePipe(handle.memory, handle.kernelState.pipeBase, handle.stdoutPipe, bannerBytes).written).toBe(
    bannerBytes.length,
  );

  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_PIPE_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(stdoutChunks.map((chunk) => new TextDecoder().decode(chunk))).toEqual(["openjdk banner\n"]);
  expect(requests.filter((msg) => msg.type === "sync-pipe-state")).toHaveLength(1);
});

test("running worker prunes stale blocked workers after the live thread table shrinks", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  const childWorker = createdWorkers[1];
  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length, 0x1234, 0x2000);
  emitReady(childWorker, 0x2000);
  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
  });

  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length, 0x1234);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect((handle as { workers: Set<Worker> }).workers.has(childWorker as unknown as Worker)).toBe(false);
});

test("fault exits the process when the live thread table is empty despite a stale positive thread count", async () => {
  const onExit = vi.fn();
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { onExit });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_process_thread_count = () => 1;
  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length);

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 139,
    termSignal: 11,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(onExit).toHaveBeenCalledWith(139);
  expect((handle as { workers: Set<Worker> }).workers.size).toBe(0);
  expect(worker.terminated).toBe(true);
});

test("createProcess processes STATUS_FS and resumes worker with refreshed kernel state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x10,
      },
    },
  ]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("createProcess handles STATUS_FS locally before publishing refreshed kernel state", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  expect(requests[0]).toMatchObject({
    type: "register-process",
    pid: 1,
    memory: handle.memory,
    kernelState: makeKernelState(),
  });

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "process-fs")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-fork-shared-state")).toBe(false);
  expect(kernel.fsProcessCalls).toBe(1);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x10,
      },
    },
  ]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("STATUS_FS only syncs fork-shared OFD state when the local kernel reports shared effects", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  handle.kernel.thread_pending_fs_effects = () => 1 << 1;
  handle.kernelShare.sharedOfdIds.add(7);
  handle.kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 3 ? 7 : 0);
  new DataView(handle.memory.buffer).setInt32(handle.kernelState.ofdTablePtr + 7 * 32, 1, true);

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));

  const syncRequest = requests.find((msg) => msg.type === "sync-fork-shared-state");
  expect(syncRequest).toMatchObject({
    type: "sync-fork-shared-state",
    pid: 1,
    targetPids: [],
    includeMemfs: false,
  });
});

test("createProcess resumes STATUS_CONNECT from the process owner", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeConnect = vi.fn();
  kernel.thread_resume_connect_result = resumeConnect;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 28,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(resumeConnect).toHaveBeenCalledWith(0x1234, -111n);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: makeKernelState(),
    },
  ]);
});

test("createProcess delegates STATUS_CONNECT to kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "resume-connect":
          return {
            type: "connect-resumed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeConnect = vi.fn();
  kernel.thread_resume_connect_result = resumeConnect;

  expect(requests[0]).toMatchObject({
    type: "register-process",
    pid: 1,
    memory: handle.memory,
    kernelState: makeKernelState(),
  });

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 28,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "resume-connect")).toBe(true);
  expect(resumeConnect).not.toHaveBeenCalled();
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x20);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x20,
      },
    },
  ]);
});

test("createProcess blocks STATUS_CONNECT until socket timeout expires", async () => {
  vi.useFakeTimers();
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let status = 28;
  const resumeConnect = vi.fn();
  const resumeSocketTimeout = vi.fn(() => {
    status = 0;
  });
  kernel.thread_resume_connect_result = resumeConnect;
  kernel.thread_resume_socket_wait_timeout = resumeSocketTimeout;
  kernel.thread_socket_wait_timeout = () => 25n;
  kernel.thread_status = () => status;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 28,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(resumeConnect).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(30);
  expect(resumeSocketTimeout).toHaveBeenCalledWith(0x1234);
  vi.useRealTimers();
});

test("kernel-worker child exit bookkeeping tears down the child and resumes parent wait4", async () => {
  const requests: KernelWorkerRequest[] = [];
  threadCountQueue.push(1, 0);

  const parent = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "record-child-exit":
          return {
            type: "child-exit-recorded",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x80,
            },
          };
        case "resume-wait4":
          return {
            type: "wait4-resumed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x40,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected parent kernel-worker request: ${msg.type}`);
      }
    },
  });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 2,
    parentPid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected child kernel-worker request: ${msg.type}`);
      }
    },
  });

  const parentWorker = createdWorkers[0];
  const childWorker = createdWorkers[1];
  const parentKernel = createdKernelExports[0];
  const resumeWait4 = vi.fn(() => 0n);
  parentKernel.thread_resume_wait4 = resumeWait4;

  emitReady(parentWorker);
  emitReady(childWorker);
  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(
    requests.some((msg) => msg.type === "record-child-exit" && msg.pid === 1 && msg.childPid === 2),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.pid === 2),
  ).toBe(true);
  expect(parentKernel.recordedChildExits).toEqual([[2, 7 << 8]]);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);

  vi.useFakeTimers();
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: parent.kernelState,
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(15);
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "resume-wait4" && msg.pid === 1)).toBe(false);
  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);
  expect(parentWorker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x80,
      },
    },
  ]);
  vi.useRealTimers();
});

test("createProcess does not grow linear memory to requested ramSize during spawn staging", async () => {
  const requestedRam = 512 * 1024 * 1024;
  const handle = await createProcess(
    new Uint8Array([0]),
    new Uint8Array(512 * 1024),
    { ramSize: requestedRam },
  );

  expect(handle.ramSize).toBe(requestedRam);
  expect(handle.memory.buffer.byteLength).toBeLessThan(128 * 1024 * 1024);
});

test("createProcess preloads snapshot files by resizing then writing payload bytes", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    preloadEntries: [
      {
        kind: "file",
        path: "/workspace/bin/tool",
        data: new Uint8Array(8 * 1024 * 1024),
        mode: 0o755,
      },
    ],
  });

  expect(createdKernelExports[0].memfsCreateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsTruncateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsTruncateCalls[0]?.[1]).toBe(8 * 1024 * 1024);
  expect(createdKernelExports[0].memfsWriteCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsWriteCalls[0]?.[1]).toBe(8 * 1024 * 1024);
  expect(createdKernelExports[0].memfsCreateWithDataCalls).toEqual([]);
});

test("createProcess seeds local memfs from runtime FS state when preloadEntries are omitted", async () => {
  const runtimeId = 77;
  primeRuntimeFsState(runtimeId, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/workspace", mode: 0o755 },
    {
      kind: "file",
      path: "/workspace/app.bin",
      data: new Uint8Array(4096),
      mode: 0o644,
    },
  ]);

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      runtimeId,
    });
  } finally {
    clearRuntimeFsState(runtimeId);
  }

  expect(createdKernelExports[0].memfsCreateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsTruncateCalls).toEqual([[11, 4096]]);
  expect(createdKernelExports[0].memfsWriteCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsWriteCalls[0]?.[1]).toBe(4096);
});

test("createProcess seeds local memfs before requesting canonical metadata hydration", async () => {
  const runtimeId = 78;
  primeRuntimeFsState(runtimeId, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/workspace", mode: 0o755 },
    {
      kind: "file",
      path: "/workspace/app.bin",
      data: new Uint8Array(4096),
      mode: 0o644,
    },
  ]);

  const kernelWorkerRequest = vi.fn(async (msg: KernelWorkerRequest): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "register-process":
        return {
          type: "process-registered",
          requestId: 0,
          kernelState: msg.kernelState,
        };
      case "initialize-process-lifecycle":
        return {
          type: "process-lifecycle-initialized",
          requestId: 0,
          kernelState: msg.kernelState,
        };
      default:
        throw new Error(`unexpected kernel worker request: ${msg.type}`);
    }
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      runtimeId,
      pid: 1,
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest,
    });
  } finally {
    clearRuntimeFsState(runtimeId);
  }

  expect(createdKernelExports[0].memfsCreateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsSetFileSizeShallowCalls).toEqual([]);
  expect(createdKernelExports[0].memfsTruncateCalls).toEqual([[11, 4096]]);
  expect(createdKernelExports[0].memfsWriteCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsWriteCalls[0]?.[1]).toBe(4096);
  expect(
    kernelWorkerRequest.mock.calls.some(([msg]) => msg.type === "snapshot-fs"),
  ).toBe(false);
  expect(kernelWorkerRequest).toHaveBeenCalledWith(expect.objectContaining({
    type: "register-process",
    seedMemfsMode: "none",
  }));
});

test("createProcess materializes runtime fs entries before process_create under kernel-worker ownership", async () => {
  const runtimeId = 79;
  primeRuntimeFsState(runtimeId, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/lib", mode: 0o755 },
    {
      kind: "file",
      path: "/lib/ld-linux-riscv64-lp64d.so.1",
      data: new Uint8Array(4096),
      mode: 0o755,
    },
  ]);

  vi.spyOn(WebAssembly, "instantiate").mockImplementationOnce(async (_module, imports) => {
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    const processCreate = vi.fn(() => {
      expect(exports.memfsCreateCalls).toHaveLength(1);
      expect(exports.memfsTruncateCalls).toEqual([[11, 4096]]);
      expect(exports.memfsWriteCalls).toHaveLength(1);
      return 0x1234;
    });
    exports.process_create = processCreate;
    createdKernelExports.push(exports);
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  const kernelWorkerRequest = vi.fn(async (msg: KernelWorkerRequest): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "register-process":
        return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
      case "initialize-process-lifecycle":
        return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
      default:
        throw new Error(`unexpected kernel worker request: ${msg.type}`);
    }
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      runtimeId,
      pid: 1,
      hydrateFsFromKernelWorker: true,
      kernelWorkerRequest,
    });
  } finally {
    clearRuntimeFsState(runtimeId);
  }

  expect(
    kernelWorkerRequest.mock.calls.some(([msg]) => msg.type === "snapshot-fs"),
  ).toBe(false);
});

test("STATUS_EXECVE_SPAWN with unknown spawn kind clears handoff and resumes", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  expect(worker).toBeDefined();

  emitReady(worker);
  worker.clearPostedMessages();
  // Mock returns spawn_kind=0 (NONE) by default, so handleExecveSpawn
  // hits the unknown-kind path: clear handoff + enqueue parent
  worker.emit({
    type: "status",
    status: 9, // EXECVE_SPAWN
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
});

test("plain fork retains shared OFDs before spawning the child worker", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_spawn_kind = () => kernel.abi_spawn_kind_fork();
  kernel.thread_spawn_child_pid = () => 77;
  kernel.process_fd_ofd_id = (_vmPtr, fd) => {
    switch (fd) {
      case 0:
        return 11;
      case 1:
        return 12;
      case 2:
        return 13;
      case 3:
        return 12;
      default:
        return 0;
    }
  };

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  expect(kernel.retainedOfds).toEqual([11, 12, 13]);
  expect(createdWorkers).toHaveLength(2);
});

test("plain fork canonicalizes child pid and reinitializes child lifecycle through kernel-worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 177,
            nextPid: 178,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: msg.kernelState,
          };
        case "rehydrate-fork-shared-state":
          return {
            type: "fork-shared-state-rehydrated",
            requestId: 0,
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_spawn_kind = () => kernel.abi_spawn_kind_fork();
  kernel.thread_spawn_child_pid = () => 77;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(
    requests.some((msg) => msg.type === "canonicalize-child-pid" && msg.provisionalChildPid === 77),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "register-process" && msg.pid === 177),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.pid === 1),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "initialize-process-lifecycle" && msg.pid === 177),
  ).toBe(true);
});

test("plain fork spawn path records child exit and resumes parent wait4", async () => {
  const requests: KernelWorkerRequest[] = [];
  threadCountQueue.push(1, 0);
  const parent = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 177,
            nextPid: 178,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        case "record-child-exit":
          return {
            type: "child-exit-recorded",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x80,
            },
          };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: msg.kernelState,
          };
        case "rehydrate-fork-shared-state":
          return {
            type: "fork-shared-state-rehydrated",
            requestId: 0,
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const resumeWait4 = vi.fn(() => 0n);

  parentKernel.thread_resume_wait4 = resumeWait4;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 77;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(createdWorkers).toHaveLength(2);
  const childWorker = createdWorkers[1];
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(100);

  expect(
    requests.some((msg) => msg.type === "canonicalize-child-pid" && msg.provisionalChildPid === 77),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "record-child-exit" && msg.pid === 1 && msg.childPid === 177),
  ).toBe(true);
  expect(parentKernel.recordedChildExits).toEqual([[177, 7 << 8]]);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);

  vi.useFakeTimers();
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: parent.kernelState,
  });
  await flushMicrotasks();
  vi.advanceTimersByTime(15);
  await flushMicrotasks();

  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);
  expect(parentWorker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x80,
      },
    },
  ]);
  vi.useRealTimers();
});

test("plain fork rehydrates shared kernel objects after child memory scrub", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      const epollTableBytes = Number(exports.kernel_epoll_table_bytes());
      const epollInstanceBytes = Number(exports.kernel_epoll_instance_bytes());
      const epollTable = new Uint32Array(memory.buffer, state.epollTablePtr, epollTableBytes >>> 2);
      const epollInstancePtrs = [...epollTable].filter((ptr) => ptr !== 0);
      const bytes = new Uint8Array(memory.buffer);
      bytes.fill(0, state.ofdTablePtr, state.ofdTablePtr + Number(exports.kernel_ofd_table_bytes()));
      bytes.fill(0, state.memfsPtr, state.memfsPtr + Number(exports.kernel_memfs_header_bytes()));
      bytes.fill(0, exports.kernel_memfs_inodes_ptr(), exports.kernel_memfs_inodes_ptr() + Number(exports.kernel_memfs_inodes_bytes()));
      bytes.fill(
        0,
        exports.kernel_memfs_first_child_ptr(),
        exports.kernel_memfs_first_child_ptr() + Number(exports.kernel_memfs_first_child_bytes()),
      );
      bytes.fill(
        0,
        exports.kernel_memfs_dir_entries_ptr(),
        exports.kernel_memfs_dir_entries_ptr() + Number(exports.kernel_memfs_dir_entries_bytes()),
      );
      bytes.fill(
        0,
        exports.kernel_memfs_data_ptr(),
        exports.kernel_memfs_data_ptr() + Number(exports.kernel_memfs_data_bytes()),
      );
      bytes.fill(0, state.pipeBase, state.pipeBase + Number(exports.kernel_pipe_region_bytes()));
      bytes.fill(0, state.socketTablePtr, state.socketTablePtr + Number(exports.kernel_socket_table_bytes()));
      bytes.fill(0, state.netBufBase, state.netBufBase + Number(exports.kernel_net_buf_pool_bytes()));
      bytes.fill(0, state.epollTablePtr, state.epollTablePtr + epollTableBytes);
      for (const ptr of epollInstancePtrs) {
        bytes.fill(0, ptr, ptr + epollInstanceBytes);
      }
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const state = makeKernelState();
    const ofdId = 1;
    const ofdOffset = state.ofdTablePtr + ofdId * 32;
    const parentBytes = new Uint8Array(parentKernel.memory.buffer);

    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? ofdId : 0;
    parentBytes[ofdOffset] = 0x01;
    parentBytes[ofdOffset + 12] = 0x21;
    parentBytes[state.memfsPtr + 4] = 0x31;
    parentBytes[parentKernel.kernel_memfs_inodes_ptr() + 8] = 0x32;
    parentBytes[parentKernel.kernel_memfs_data_ptr() + 16] = 0x33;
    parentBytes[state.pipeBase + 64] = 0x41;
    parentBytes[state.socketTablePtr + 16] = 0x51;
    parentBytes[state.netBufBase + 32] = 0x61;
    new DataView(parentKernel.memory.buffer).setUint32(state.epollTablePtr, 0x250100, true);
    parentBytes[0x250100] = 0x71;

    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
    parentKernel.thread_spawn_child_pid = () => 88;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);

    expect(childBytes[ofdOffset + 12]).toBe(0x21);
    expect(childBytes[state.memfsPtr + 4]).toBe(0x31);
    expect(childBytes[childKernel.kernel_memfs_inodes_ptr() + 8]).toBe(0x32);
    expect(childBytes[childKernel.kernel_memfs_data_ptr() + 16]).toBe(0x33);
    expect(childBytes[state.pipeBase + 64]).toBe(0x41);
    expect(new DataView(childKernel.memory.buffer).getUint32(state.epollTablePtr, true)).toBe(0x250100);
    expect(childBytes[0x250100]).toBe(0x71);
    expect(childBytes[state.socketTablePtr + 16]).toBe(0x51);
    expect(childBytes[state.netBufBase + 32]).toBe(0x61);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("plain fork does not keep signal disposition state synchronized after the initial copy", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const signalSlotsPtr = makeKernelState().sigSlotsPtr;
  const processPendingPtr = makeKernelState().processPendingPtr;
  const actionTablePtr = 0xc40000;
  const initialAction = new Uint8Array(24).fill(0x11);
  const initialPending = new Uint8Array(8).fill(0x33);

  new Uint32Array(parentKernel.memory.buffer, signalSlotsPtr, 1)[0] = actionTablePtr;
  new Uint8Array(parentKernel.memory.buffer).set(initialAction, actionTablePtr);
  new Uint8Array(parentKernel.memory.buffer).set(initialPending, processPendingPtr);

  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 77;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  const childWorker = createdWorkers[1];
  const childKernel = createdKernelExports[1];
  emitReady(childWorker);

  expect(new Uint32Array(childKernel.memory.buffer, signalSlotsPtr, 1)[0]).toBe(actionTablePtr);
  expect(
    [...new Uint8Array(childKernel.memory.buffer, actionTablePtr, initialAction.length)],
  ).toEqual([...initialAction]);
  expect(
    [...new Uint8Array(childKernel.memory.buffer, processPendingPtr, initialPending.length)],
  ).toEqual([...initialPending]);

  const updatedAction = new Uint8Array(24).fill(0x44);
  const updatedPending = new Uint8Array(8).fill(0x55);
  new Uint8Array(parentKernel.memory.buffer).set(updatedAction, actionTablePtr);
  new Uint8Array(parentKernel.memory.buffer).set(updatedPending, processPendingPtr);

  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(
    [...new Uint8Array(childKernel.memory.buffer, actionTablePtr, initialAction.length)],
  ).toEqual([...initialAction]);
  expect(
    [...new Uint8Array(childKernel.memory.buffer, processPendingPtr, initialPending.length)],
  ).toEqual([...initialPending]);
});

test("plain fork does not keep time state synchronized after the initial copy", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const timeStatePtr = makeKernelState().timeStatePtr;
  const initialTimeState = new Uint8Array(32).fill(0x21);

  new Uint8Array(parentKernel.memory.buffer).set(initialTimeState, timeStatePtr);
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 78;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  const childWorker = createdWorkers[1];
  const childKernel = createdKernelExports[1];
  emitReady(childWorker);

  expect(
    [...new Uint8Array(childKernel.memory.buffer, timeStatePtr, initialTimeState.length)],
  ).toEqual([...initialTimeState]);

  const updatedTimeState = new Uint8Array(32).fill(0x66);
  new Uint8Array(parentKernel.memory.buffer).set(updatedTimeState, timeStatePtr);

  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(
    [...new Uint8Array(childKernel.memory.buffer, timeStatePtr, initialTimeState.length)],
  ).toEqual([...initialTimeState]);
});

test("plain fork does not keep process lifecycle state synchronized after the initial copy", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const processRuntimeStatePtr = makeKernelState().processRuntimeStatePtr;
  const initialLifecycleState = new Uint8Array(32).fill(0x72);

  new Uint8Array(parentKernel.memory.buffer).set(initialLifecycleState, processRuntimeStatePtr);
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 79;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await Promise.resolve();

  const childWorker = createdWorkers[1];
  const childKernel = createdKernelExports[1];
  emitReady(childWorker);

  expect(
    [...new Uint8Array(childKernel.memory.buffer, processRuntimeStatePtr, initialLifecycleState.length)],
  ).toEqual([...initialLifecycleState]);

  const updatedLifecycleState = new Uint8Array(32).fill(0x93);
  new Uint8Array(parentKernel.memory.buffer).set(updatedLifecycleState, processRuntimeStatePtr);

  childWorker.clearPostedMessages();
  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(
    [...new Uint8Array(childKernel.memory.buffer, processRuntimeStatePtr, initialLifecycleState.length)],
  ).toEqual([...initialLifecycleState]);
});

test("plain fork child teardown opens the parent wait4 retry gate", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const resumeWait4 = vi.fn(() => 0n);

  parentKernel.thread_resume_wait4 = resumeWait4;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 77;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(40);
  await new Promise((resolve) => setTimeout(resolve, 0));
  for (let i = 0; i < 10 && createdWorkers.length < 2; i++) {
    await Promise.resolve();
  }

  expect(createdWorkers).toHaveLength(2);

  vi.useFakeTimers();
  destroyProcessTree(77, handle.runtimeId);
  parentKernel.process_record_child_exit(77, 7 << 8);

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(15);
  await flushMicrotasks();

  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
  vi.useRealTimers();
});

test("plain fork child fault records child exit on the parent state", async () => {
  threadCountQueue.push(1, 0);
  const onError = vi.fn();
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onError,
  });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_fork();
  parentKernel.thread_spawn_child_pid = () => 77;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(400);
  await new Promise((resolve) => setTimeout(resolve, 20));
  await flushMicrotasks(400);

  const childWorker = createdWorkers[1];
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(100);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(100);

  expect(onError).not.toHaveBeenCalled();
  expect(parentKernel.recordedChildExits).toEqual([[77, 7 << 8]]);
  expect(handle.lastDebugMessage).toContain("child-exit:77");
});

test("child exit records wait status on parent kernel state", async () => {
  threadCountQueue.push(1, 0);

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 2,
    parentPid: 1,
  });

  emitReady(createdWorkers[0]);
  const childWorker = createdWorkers[1];
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 7,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdKernelExports[0].recordedChildExits).toEqual([[2, 7 << 8]]);
});

test("runtime-owned file writes are mirrored into live process memfs for the same runtime", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 7,
    pid: 1,
  });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 7,
    pid: 2,
  });

  mirrorRuntimeFsEntry(7, {
    kind: "file",
    path: "/workspace/note.txt",
    data: new Uint8Array([1, 2, 3, 4]),
    mode: 0o644,
  });

  expect(createdKernelExports[0].memfsTruncateCalls.at(-1)?.[1]).toBe(4);
  expect(createdKernelExports[0].memfsWriteCalls.at(-1)?.[1]).toBe(4);
  expect(createdKernelExports[1].memfsTruncateCalls.at(-1)?.[1]).toBe(4);
  expect(createdKernelExports[1].memfsWriteCalls.at(-1)?.[1]).toBe(4);
});

test("runtime-owned snapshot replacement is mirrored into live processes for the same runtime", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 8,
    pid: 1,
  });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 8,
    pid: 2,
  });

  replaceRuntimeFsSnapshot(8, [
    { kind: "dir", path: "/", mode: 0o755 },
    { kind: "dir", path: "/workspace", mode: 0o755 },
    {
      kind: "file",
      path: "/workspace/config.txt",
      data: new Uint8Array([9, 8, 7]),
      mode: 0o600,
    },
  ]);

  expect(createdKernelExports[0].memfsCreateCalls.length).toBeGreaterThan(0);
  expect(createdKernelExports[0].memfsTruncateCalls.at(-1)?.[1]).toBe(3);
  expect(createdKernelExports[1].memfsCreateCalls.length).toBeGreaterThan(0);
  expect(createdKernelExports[1].memfsTruncateCalls.at(-1)?.[1]).toBe(3);
});

test("runtime-owned fs mirroring preserves kernel-worker-owned pipe, epoll, and socket state", async () => {
  const kernelWorkerRequest = async (msg: KernelWorkerRequest): Promise<KernelWorkerOutbound> => {
    switch (msg.type) {
      case "register-process":
        return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
      case "initialize-process-lifecycle":
        return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
      case "sync-process-lifecycle":
        return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-epoll-state":
        return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-socket-state":
        return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "sync-pipe-state":
        return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
      case "release-host-stdio":
        return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
      case "unregister-process":
        return { type: "process-unregistered", requestId: 0 };
      default:
        throw new Error(`unexpected kernel-worker request: ${msg.type}`);
    }
  };
  const canonical = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 9,
    pid: 1,
    kernelWorkerRequest,
  });
  const member = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 9,
    pid: 2,
    kernelWorkerRequest,
  });
  canonical.kernelState = {
    ...canonical.kernelState,
    pipeUsed: 0x11n,
    socketUsed: 1,
    netBufUsed: 0x12n,
  };
  member.kernelState = {
    ...member.kernelState,
    pipeUsed: 0x21n,
    socketUsed: 2,
    netBufUsed: 0x22n,
  };

  const canonicalBytes = new Uint8Array(canonical.memory.buffer);
  const memberBytes = new Uint8Array(member.memory.buffer);
  canonicalBytes[canonical.kernelState.pipeBase + 64] = 0x11;
  memberBytes[member.kernelState.pipeBase + 64] = 0x22;
  canonicalBytes[canonical.kernelState.socketTablePtr + 16] = 0x31;
  memberBytes[member.kernelState.socketTablePtr + 16] = 0x32;
  canonicalBytes[canonical.kernelState.netBufBase + 32] = 0x41;
  memberBytes[member.kernelState.netBufBase + 32] = 0x42;
  new DataView(canonical.memory.buffer).setUint32(canonical.kernelState.epollTablePtr, 0x250100, true);
  new DataView(member.memory.buffer).setUint32(member.kernelState.epollTablePtr, 0x250200, true);

  mirrorRuntimeFsEntry(9, {
    kind: "file",
    path: "/workspace/runtime-owned.txt",
    data: new Uint8Array([1, 2, 3]),
    mode: 0o644,
  });

  expect(member.kernelState.pipeUsed).toBe(0x21n);
  expect(member.kernelState.socketUsed).toBe(2);
  expect(member.kernelState.netBufUsed).toBe(0x22n);
  expect(memberBytes[member.kernelState.pipeBase + 64]).toBe(0x22);
  expect(new DataView(member.memory.buffer).getUint32(member.kernelState.epollTablePtr, true)).toBe(0x250200);
  expect(memberBytes[member.kernelState.socketTablePtr + 16]).toBe(0x32);
  expect(memberBytes[member.kernelState.netBufBase + 32]).toBe(0x42);
});

test("signaled child exit records signaled wait status on parent kernel state", async () => {
  threadCountQueue.push(1, 0);

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 2,
    parentPid: 1,
  });

  emitReady(createdWorkers[0]);
  const childWorker = createdWorkers[1];
  emitReady(childWorker);
  childWorker.emit({
    type: "status",
    status: 3,
    exitCode: 0,
    termSignal: 9,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(createdKernelExports[0].recordedChildExits).toEqual([[2, 9]]);
});

test("STATUS_WAIT4 retries through thread_resume_wait4 instead of generic blocked resume", async () => {
  vi.useFakeTimers();

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeWait4 = vi.fn(() => 0n);
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_wait4 = resumeWait4;
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  vi.advanceTimersByTime(15);

  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);

  vi.useRealTimers();
});

test("STATUS_WAIT4 delegates retries through kernel-worker when available", async () => {
  vi.useFakeTimers();

  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "resume-wait4":
          return {
            type: "wait4-resumed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x30,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeWait4 = vi.fn(() => 0n);
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_wait4 = resumeWait4;
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  requests.length = 0;
  worker.emit({
    type: "status",
    status: 31,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(15);
  await flushMicrotasks();

  expect(requests.map((msg) => msg.type)).toEqual([
    "resume-wait4",
    "sync-pipe-state",
  ]);
  expect(resumeWait4).toHaveBeenCalledWith(0x1234);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x30);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x30,
        pipeUsed: 15n,
      },
    },
  ]);

  vi.useRealTimers();
});

test("waitInWorker STATUS_EPOLL_WAIT skips epoll sync when the process holds no epoll OFDs", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x40,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "resume-epoll-wait")).toBe(false);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr);
  expect(Atomics.load(notify, 0)).toBe(notifyBefore);
  expect(worker.postedMessages).toEqual([]);
});

test("waitInWorker STATUS_EPOLL_WAIT republishes synced kernel state and wakes bridge waiters", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x40,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const notify = new Int32Array(handle.runtimeBridge.notifySab);

  kernel.thread_epoll_wait_timeout = () => -1n;
  emitReady(worker);
  worker.clearPostedMessages();
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);
  kernel.kernel_ofd_fd_type = (ofdId) => (ofdId === 7 ? kernel.abi_fd_type_epoll() : 0);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks();

  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x40);
  expect(readPublishedKernelRuntimeState(handle.runtimeBridge).heapPtr).toBe(makeKernelState().heapPtr + 0x40);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toEqual([]);
});

test("STATUS_PIPE_WAIT syncs fork-shared state to kernel-worker even without peer members", async () => {
  vi.useFakeTimers();

  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  kernel.thread_resume_blocked_syscall = () => 0n;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();

  expect(
    requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.targetPids.length === 0),
  ).toBe(true);

  vi.useRealTimers();
});

test("STATUS_PIPE_WAIT re-syncs fork-shared state after local blocked resume", async () => {
  vi.useFakeTimers();

  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(5);
  await flushMicrotasks();

  expect(resumeBlocked).toHaveBeenCalledWith(0x1234);
  expect(requests.filter((msg) => msg.type === "sync-fork-shared-state")).toHaveLength(2);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);

  vi.useRealTimers();
});

test("STATUS_PIPE_WAIT local blocked resume preserves thread register context before re-step", async () => {
  vi.useFakeTimers();

  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let currentStatus = 6;
  let currentPc = 0x83336n;
  let currentSp = 0x37d6c000n;
  let currentS11 = 0xbfdc80n;
  kernel.thread_status = () => currentStatus;
  kernel.thread_pc = () => currentPc;
  kernel.thread_sp = () => currentSp;
  kernel.thread_s11 = () => currentS11;
  kernel.thread_ra = () => 0x4d9b4n;

  const resumeBlocked = vi.fn(() => {
    currentStatus = 0;
    return 0n;
  });
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    exitCode: 0,
    termSignal: 0,
    pc: currentPc,
    ra: 0x4d9b4n,
    sp: currentSp,
    s11: currentS11,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(5);
  await flushMicrotasks();

  expect(resumeBlocked).toHaveBeenCalledWith(0x1234);
  expect(kernel.thread_pc(0x1234)).toBe(0x83336n);
  expect(kernel.thread_sp(0x1234)).toBe(0x37d6c000n);
  expect(kernel.thread_s11(0x1234)).toBe(0xbfdc80n);
  expect(worker.postedMessages).toContainEqual({
    type: "step",
    budget: 20_000,
    kernelState: makeKernelState(),
  });
  expect(requests.filter((msg) => msg.type === "sync-fork-shared-state")).toHaveLength(2);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);

  vi.useRealTimers();
});

test("pipe sync wakes waitInWorker epoll waits through runtime bridge", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_epoll_wait_timeout = () => -1n;

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(requests.some((msg) => msg.type === "resume-epoll-wait")).toBe(false);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toEqual([]);
});

test("pipe sync with held epoll OFDs refreshes epoll state and wakes waitInWorker bridge waiters", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x24,
            },
          };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? 7 : 0);
  kernel.kernel_ofd_fd_type = (ofdId) => (ofdId === 7 ? kernel.abi_fd_type_epoll() : 0);

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(requests.filter((msg) => msg.type === "sync-epoll-state")).toHaveLength(2);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x48);
  expect(readPublishedKernelRuntimeState(handle.runtimeBridge).heapPtr).toBe(makeKernelState().heapPtr + 0x48);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toContainEqual(expect.objectContaining({ type: "step" }));
});

test("pipe sync refreshes epoll-dependent OFD state before retrying waitInWorker epoll waits", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-ofd-state":
          return {
            type: "ofd-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
          };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.process_fd_ofd_id = (_vmPtr, fd) => {
    if (fd === 3) {
      return 9;
    }
    if (fd === 4) {
      return 7;
    }
    return 0;
  };
  kernel.kernel_ofd_fd_type = (ofdId) => {
    if (ofdId === 7) {
      return kernel.abi_fd_type_epoll();
    }
    if (ofdId === 9) {
      return 9;
    }
    return 0;
  };

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);
  worker.emit({
    type: "status",
    status: 7,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks();

  const pipeSyncIndex = requests.findIndex((msg) => msg.type === "sync-pipe-state");
  const ofdSyncIndex = requests.findIndex((msg) => msg.type === "sync-ofd-state");
  const epollSyncIndex = requests.findLastIndex((msg) => msg.type === "sync-epoll-state");
  expect(pipeSyncIndex).toBeGreaterThanOrEqual(0);
  expect(ofdSyncIndex).toBeGreaterThan(pipeSyncIndex);
  expect(ofdSyncIndex).toBeGreaterThanOrEqual(0);
  expect(epollSyncIndex).toBeGreaterThan(ofdSyncIndex);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x50);
  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
  expect(worker.postedMessages).toContainEqual(expect.objectContaining({ type: "step" }));
});

test("signal syscalls wake waitInWorker threads through the runtime bridge", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  const notify = new Int32Array(handle.runtimeBridge.notifySab);
  const notifyBefore = Atomics.load(notify, 0);

  worker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
    syncEffects: 1 << 5,
  });

  await flushMicrotasks();

  expect(Atomics.load(notify, 0)).toBeGreaterThan(notifyBefore);
});

test("STATUS_OK syncs epoll state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const kernel = handle.kernel;
  const ofdId = 7;
  new DataView(kernel.memory.buffer).setInt32(
    handle.kernelState.ofdTablePtr + ofdId * 32,
    kernel.abi_fd_type_epoll(),
    true,
  );
  new DataView(kernel.memory.buffer).setUint32(
    handle.kernelState.ofdTablePtr + ofdId * 32 + 24,
    1,
    true,
  );
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 4 ? ofdId : 0);
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_EPOLL_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: makeKernelState(),
    },
  ]);
});

test("STATUS_OK syncs eventfd-backed OFD state before epoll retries", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-ofd-state":
          return {
            type: "ofd-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
          };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const kernel = handle.kernel;
  const ofdId = 7;
  new DataView(kernel.memory.buffer).setInt32(handle.kernelState.ofdTablePtr + ofdId * 32, 9, true);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 3 ? ofdId : 0);

  const worker = createdWorkers[0];
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_EPOLL_DEPENDENT_OFD_STATE | SYNC_EFFECT_EPOLL_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-ofd-state" && msg.ofdIds.includes(ofdId))).toBe(true);
  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("STATUS_OK syncs fork-shared OFD state through kernel-worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const kernel = handle.kernel;
  const ofdId = 5;
  new DataView(kernel.memory.buffer).setInt32(handle.kernelState.ofdTablePtr + ofdId * 32, 6, true);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => (fd === 3 ? ofdId : 0);
  handle.kernelShare.sharedOfdIds.add(ofdId);

  const worker = createdWorkers[0];
  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_FORK_SHARED_OFD_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.ofdIds.includes(ofdId))).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x20);
});

test("STATUS_OK syncs socket state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x18,
            },
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: SYNC_EFFECT_SOCKET_STATE | SYNC_EFFECT_EPOLL_STATE,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x18);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x18,
      },
    },
  ]);
});

test("STATUS_OK skips owner-side sync for thread-local returns", async () => {
  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    syncEffects: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-process-lifecycle")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(false);
  expect(requests.some((msg) => msg.type === "sync-ofd-state")).toBe(false);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
});

test("waitInWorker status skips owner-side blocked retry scheduling", async () => {
  vi.useFakeTimers();

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 6,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  vi.advanceTimersByTime(20);

  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);

  vi.useRealTimers();
});

test("running sibling waitInWorker FUTEX_WAIT kicks blocked EPOLL_WAIT retries", async () => {
  vi.useFakeTimers();

  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-ofd-state":
          return {
            type: "ofd-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x10,
            },
          };
        case "sync-epoll-state":
          return {
            type: "epoll-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x20,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  let parentStatus = kernel.abi_status_epoll_wait();
  const resumeBlocked = vi.fn((vmPtr: number) => {
    if (vmPtr === 0x1234) {
      parentStatus = kernel.abi_status_ok();
    }
    return 0n;
  });

  kernel.thread_process_thread_count = () => 2;
  kernel.thread_status = (vmPtr) => vmPtr === 0x1234 ? parentStatus : kernel.abi_status_ok();
  kernel.thread_resume_blocked_syscall = resumeBlocked;
  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.liveThreadVmPtrs.splice(0, kernel.liveThreadVmPtrs.length, 0x1234, 0x2000);
  kernel.process_fd_ofd_id = (_vmPtr, fd) => {
    if (fd === 3) {
      return 9;
    }
    if (fd === 4) {
      return 7;
    }
    return 0;
  };
  kernel.kernel_ofd_fd_type = (ofdId) => {
    if (ofdId === 7) {
      return kernel.abi_fd_type_epoll();
    }
    if (ofdId === 9) {
      return 9;
    }
    return 0;
  };

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: kernel.abi_status_clone_spawn(),
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  const siblingWorker = createdWorkers[1];
  emitReady(siblingWorker, 0x2000);
  siblingWorker.clearPostedMessages();
  parentWorker.clearPostedMessages();
  requests.length = 0;

  parentWorker.emit({
    type: "status",
    status: kernel.abi_status_epoll_wait(),
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  handle.runningWorker = siblingWorker as unknown as Worker;
  handle.readyWorkers.length = 0;

  siblingWorker.emit({
    type: "status",
    status: kernel.abi_status_futex_wait(),
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  const requestTypes = requests.map((msg) => msg.type);
  expect(requestTypes).toContain("sync-ofd-state");
  expect(requestTypes.slice(-2)).toEqual([
    "sync-ofd-state",
    "sync-epoll-state",
  ]);
  expect(resumeBlocked).toHaveBeenCalledWith(0x1234);
  expect(parentWorker.postedMessages.at(-1)).toEqual(expect.objectContaining({
    type: "step",
    budget: 20_000,
    kernelState: expect.objectContaining({
      heapPtr: makeKernelState().heapPtr + 0x70,
    }),
  }));

  vi.useRealTimers();
});

test("waitInWorker EPOLL_WAIT timeout=0 falls back to owner-side blocked retry scheduling", async () => {
  vi.useFakeTimers();

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeTimeout = vi.fn(() => 0);

  kernel.thread_epoll_wait_timeout = () => 0n;
  kernel.thread_resume_epoll_wait_timeout = resumeTimeout;
  kernel.thread_status = () => 0;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: kernel.abi_status_epoll_wait(),
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  vi.advanceTimersByTime(0);
  await flushMicrotasks();

  expect(resumeTimeout).toHaveBeenCalledWith(0x1234);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);

  vi.useRealTimers();
});

test("waitInWorker STATUS_SOCKET_RECV syncs socket state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x11,
            },
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 30,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x11);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);
});

test("waitInWorker STATUS_SOCKET_SEND syncs socket state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x12,
            },
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 29,
    waitInWorker: true,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x12);
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(worker.postedMessages).toEqual([]);
});

test("ready and stale non-running status do not regress kernel runtime state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const parentWorker = createdWorkers[0];

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  const advancedHeapPtr = makeKernelState().heapPtr + 0x10;
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: {
      ...makeKernelState(),
      heapPtr: advancedHeapPtr,
    },
  });

  const childWorker = createdWorkers[1];
  emitReady(childWorker, 0x2000);
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);

  childWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
  });
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);
});

test("stale running-worker kernel state does not move heapPtr backwards", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  const advancedHeapPtr = makeKernelState().heapPtr + 0x10;
  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(handle.kernelState.heapPtr).toBe(advancedHeapPtr);
});

test("vfork child fault records wait status and resumes parent on the same worker", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const resumeVforkParent = vi.fn();

  kernel.thread_vfork_child_pid = () => 55;
  kernel.thread_resume_vfork_parent = resumeVforkParent;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(handle.vforkSnapshot?.vmPtr).toBe(0x1234);

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 253,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(kernel.recordedChildExits).toEqual([[55, 253 << 8]]);
  expect(resumeVforkParent).toHaveBeenCalledWith(0x1234, 55);
  expect(handle.vforkSnapshot).toBeNull();
  expect(worker.terminated).toBe(false);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
});

test("vfork child fault delegates wait status bookkeeping and parent resume to kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-process-lifecycle":
          return { type: "process-lifecycle-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 56,
            nextPid: 57,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-parent":
          return {
            type: "vfork-parent-resumed",
            requestId: 0,
            childPid: 56,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x24,
            },
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const readVforkChildPid = vi.fn(() => 56);
  const resumeVforkParent = vi.fn();

  kernel.thread_vfork_child_pid = readVforkChildPid;
  kernel.thread_resume_vfork_parent = resumeVforkParent;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(handle.vforkSnapshot?.vmPtr).toBe(0x1234);

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 252,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  expect(
    requests.some((msg) => msg.type === "resume-vfork-parent" && msg.waitStatus === (252 << 8)),
  ).toBe(true);
  expect(kernel.recordedChildExits).toEqual([[56, 252 << 8]]);
  expect(readVforkChildPid).toHaveBeenCalledTimes(1);
  expect(resumeVforkParent).toHaveBeenCalledWith(0x1234, 56);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x24);
  expect(handle.vforkSnapshot).toBeNull();
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x24,
      },
    },
  ]);
});

test("STATUS_VFORK_WAIT syncs pre-vfork fork-shared OFD state before child canonicalization", async () => {
  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 56,
            nextPid: 57,
            pgid: 1,
            sid: 1,
            kernelState: msg.kernelState,
          };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];

  kernel.thread_vfork_child_pid = () => 55;

  emitReady(worker);
  requests.length = 0;
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
    syncEffects: SYNC_EFFECT_FORK_SHARED_OFD_STATE,
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  const syncIndex = requests.findIndex((msg) => msg.type === "sync-fork-shared-state");
  const canonicalizeIndex = requests.findIndex((msg) => msg.type === "canonicalize-child-pid");
  expect(syncIndex).toBeGreaterThanOrEqual(0);
  expect(canonicalizeIndex).toBeGreaterThan(syncIndex);
  const syncRequest = requests.find((msg) => msg.type === "sync-fork-shared-state");
  expect(syncRequest?.type).toBe("sync-fork-shared-state");
  if (syncRequest?.type !== "sync-fork-shared-state") {
    throw new Error("expected sync-fork-shared-state request");
  }
  expect(syncRequest.includeMemfs).toBe(false);
  expect(syncRequest.preserveSourceRefcounts).toBe(true);
});

test("vfork execve resumes the parent and initializes the child lifecycle through kernel-worker", async () => {
  const requests: KernelWorkerRequest[] = [];
  const childRegisteredState = {
    ...makeKernelState(),
    heapPtr: makeKernelState().heapPtr + 0x40,
  };
  const childLifecycleState = {
    ...childRegisteredState,
    processRuntimeStatePtr: childRegisteredState.processRuntimeStatePtr + 0x20,
  };
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return {
            type: "process-registered",
            requestId: 0,
            kernelState: msg.pid === 156 ? childRegisteredState : msg.kernelState,
          };
        case "initialize-process-lifecycle":
          return {
            type: "process-lifecycle-initialized",
            requestId: 0,
            kernelState: msg.pid === 156 ? childLifecycleState : msg.kernelState,
          };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 156,
            nextPid: 201,
            pgid: 33,
            sid: 44,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-execve-parent":
          return {
            type: "vfork-execve-parent-resumed",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x18,
            },
          };
        case "sync-fork-shared-state":
          return {
            type: "fork-shared-state-synced",
            requestId: 0,
            kernelState: msg.kernelState,
          };
        case "rehydrate-fork-shared-state":
          return {
            type: "fork-shared-state-rehydrated",
            requestId: 0,
          };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  const resumeVforkParent = vi.fn();
  const restoreVforkParentWithoutCleanup = vi.fn();
  const clearSpawnHandoff = vi.fn();

  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  parentKernel.thread_spawn_child_pid = () => 156;
  parentKernel.thread_resume_vfork_parent = resumeVforkParent;
  parentKernel.thread_restore_vfork_parent_without_cleanup = restoreVforkParentWithoutCleanup;
  parentKernel.thread_clear_spawn_handoff = clearSpawnHandoff;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(20);
  await new Promise((resolve) => setTimeout(resolve, 0));
  await flushMicrotasks(20);

  expect(
    requests.some((msg) => msg.type === "resume-vfork-execve-parent" && msg.childPid === 156),
  ).toBe(true);
  expect(
    requests.some(
      (msg) => msg.type === "rehydrate-fork-shared-state"
        && msg.targetPids.length === 1
        && msg.targetPids[0] === 1
        && msg.includeMemfs === false,
    ),
  ).toBe(false);
  expect(
    requests.some((msg) => msg.type === "register-process" && msg.pid === 156),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "sync-epoll-state"),
  ).toBe(false);
  expect(
    requests.some(
      (msg) => msg.type === "initialize-process-lifecycle"
        && msg.pid === 156
        && msg.ppid === 1
        && msg.nextPid === 201
        && msg.pgid === 33
        && msg.sid === 44
        && msg.didExec,
    ),
  ).toBe(true);
  expect(restoreVforkParentWithoutCleanup).toHaveBeenCalledWith(0x1234, 156);
  expect(resumeVforkParent).not.toHaveBeenCalled();
  expect(clearSpawnHandoff).toHaveBeenCalledWith(0x1234);
  expect(createdWorkers).toHaveLength(2);
  expect(parentWorker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x18,
      },
    },
  ]);
  expect(createdWorkers[1].postedMessages).toEqual([
    {
      type: "init",
      wasmModule: expect.any(Object),
      memory: expect.any(WebAssembly.Memory),
      vmPtr: 0x1234,
      guestBase: 0x1000000,
      guestBytes: expect.any(Number),
      preserveKernelHeapBase: childLifecycleState.heapBase,
      preserveKernelHeapBytes: childLifecycleState.heapPtr - childLifecycleState.heapBase,
      runtimeEpoch: expect.any(Number),
      guestWriteWatchRegions: [],
      kernelState: childLifecycleState,
      sabPageCache: null,
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    },
  ]);
});

test("vfork execve preserves child-private OFDs across kernel-worker scrub", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      const childPrivateOfds = [7, 8, 9];
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        if (fd >= 0 && fd <= 2) {
          return childPrivateOfds[fd] ?? 0;
        }
        return fd === 3 ? 1 : 0;
      };
      exports.thread_complete_execve_handoff = () => {
        const bytes = new Uint8Array(memory.buffer);
        childPrivateOfds.forEach((ofdId, index) => {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          bytes[slotOffset] = 0x01;
          bytes[slotOffset + 12] = 0x20 + index;
          bytes[slotOffset + 13] = 0x40 + index;
        });
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? 1 : 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);
    for (const [index, ofdId] of [7, 8, 9].entries()) {
      const slotOffset = makeKernelState().ofdTablePtr + ofdId * 32;
      expect(childBytes[slotOffset]).toBe(0x01);
      expect(childBytes[slotOffset + 12]).toBe(0x20 + index);
      expect(childBytes[slotOffset + 13]).toBe(0x40 + index);
    }
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve keeps CLOEXEC-style parent-only OFDs out of the child without mutating the parent", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        switch (fd) {
          case 0:
            return 1;
          case 1:
            return 2;
          case 2:
            return 3;
          case 3:
            return 4;
          case 4:
            return 0;
          default:
            return 0;
        }
      };
      exports.thread_complete_execve_handoff = () => 0n;
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const state = makeKernelState();
    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const parentBytes = new Uint8Array(parentKernel.memory.buffer);
    const cloexecOfdId = 12;
    const cloexecSlotOffset = state.ofdTablePtr + cloexecOfdId * 32;
    parentBytes[cloexecSlotOffset] = 0x01;
    parentBytes[cloexecSlotOffset + 12] = 0xc1;
    parentBytes[cloexecSlotOffset + 13] = 0xd1;
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      switch (fd) {
        case 0:
          return 1;
        case 1:
          return 2;
        case 2:
          return 3;
        case 3:
          return 4;
        case 4:
          return cloexecOfdId;
        default:
          return 0;
      }
    };
    parentKernel.process_fd_flags = (_vmPtr, fd) => fd === 4 ? parentKernel.abi_o_cloexec() : 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(20);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await flushMicrotasks(20);

    const childKernel = createdKernelExports[1];
    expect(parentKernel.process_fd_ofd_id(0x1234, 4)).toBe(cloexecOfdId);
    expect(childKernel.process_fd_ofd_id(0x1234, 4)).toBe(0);
    expect(parentBytes[cloexecSlotOffset + 12]).toBe(0xc1);
    expect(parentBytes[cloexecSlotOffset + 13]).toBe(0xd1);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve preserves dup-style child aliases across fork-shared scrub", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        switch (fd) {
          case 0:
            return 1;
          case 1:
            return 2;
          case 2:
            return 3;
          case 4:
          case 5:
            return 11;
          default:
            return 0;
        }
      };
      exports.thread_complete_execve_handoff = () => {
        const view = new DataView(memory.buffer);
        const seedPipeOfd = (ofdId: number, pipeSlot: number, hostFd: number, marker: number) => {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          view.setInt32(slotOffset, 2, true);
          view.setInt32(slotOffset + 4, hostFd, true);
          view.setUint32(slotOffset + 24, pipeSlot, true);
          view.setUint8(slotOffset + 12, marker);
          view.setUint8(slotOffset + 13, marker + 1);
          const pipeBase = state.pipeBase + pipeSlot * (32 + 4096);
          view.setInt32(pipeBase + 8, 1, true);
          view.setInt32(pipeBase + 12, 1, true);
        };
        seedPipeOfd(1, 1, 0, 0x11);
        seedPipeOfd(2, 2, -1, 0x22);
        seedPipeOfd(3, 3, -1, 0x33);
        const aliasedSlotOffset = state.ofdTablePtr + 11 * 32;
        view.setInt32(aliasedSlotOffset, 1, true);
        view.setInt32(aliasedSlotOffset + 4, 17, true);
        view.setUint32(aliasedSlotOffset + 24, 99, true);
        view.setUint8(aliasedSlotOffset + 12, 0xa1);
        view.setUint8(aliasedSlotOffset + 13, 0xb1);
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      switch (fd) {
        case 0:
          return 1;
        case 1:
          return 2;
        case 2:
          return 3;
        case 3:
          return 4;
        default:
          return 0;
      }
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(20);
    await new Promise((resolve) => setTimeout(resolve, 0));

    const childKernel = createdKernelExports[1];
    expect(childKernel.process_fd_ofd_id(0x1234, 4)).toBe(11);
    expect(childKernel.process_fd_ofd_id(0x1234, 5)).toBe(11);
    const childBytes = new Uint8Array(childKernel.memory.buffer);
    const aliasedSlotOffset = makeKernelState().ofdTablePtr + 11 * 32;
    expect(childBytes[aliasedSlotOffset]).toBe(0x01);
    expect(childBytes[aliasedSlotOffset + 12]).toBe(0xa1);
    expect(childBytes[aliasedSlotOffset + 13]).toBe(0xb1);

    const rehydrate = [...requests].reverse().find((msg) => msg.type === "rehydrate-fork-shared-state");
    expect(rehydrate?.type).toBe("rehydrate-fork-shared-state");
    if (rehydrate?.type !== "rehydrate-fork-shared-state") {
      throw new Error("expected rehydrate-fork-shared-state request");
    }
    expect(rehydrate.ofdIds).not.toContain(11);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure does not publish a partially spawned child", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_enoent());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const onError = vi.fn();
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      onError,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(40);
    vi.advanceTimersByTime(0);
    await flushMicrotasks();

    expect(onError).not.toHaveBeenCalled();
    expect(createdWorkers).toHaveLength(1);
    expect(
      requests.some((msg) => msg.type === "record-child-exit" && msg.childPid === 156 && msg.waitStatus === (127 << 8)),
    ).toBe(true);
    expect(
      requests.some((msg) => msg.type === "register-process" && msg.pid === 156),
    ).toBe(false);
    expect(
      requests.some((msg) => msg.type === "initialize-process-lifecycle" && msg.pid === 156),
    ).toBe(false);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure restores the parent without surfacing a fatal runtime error", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_enoent());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const onError = vi.fn();
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x60,
      processPendingPtr: makeKernelState().processPendingPtr + 0x20,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      onError,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(40);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(createdWorkers[0]?.terminated).toBe(false);
    expect(createdWorkers).toHaveLength(1);
    expect(onError).not.toHaveBeenCalled();
    expect(
      requests.some((msg) => msg.type === "record-child-exit" && msg.childPid === 156 && msg.waitStatus === (127 << 8)),
    ).toBe(true);
    expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
    expect(handle.kernelState.processPendingPtr).toBe(resumedState.processPendingPtr);
    expect(readPublishedKernelRuntimeState(handle.runtimeBridge)).toEqual(handle.kernelState);
    expect(parentWorker.postedMessages).toEqual([
      { type: "step", budget: 20_000, kernelState: handle.kernelState },
    ]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve status-style handoff failure restores the parent and surfaces the handoff marker", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_status_vfork_wait());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const onError = vi.fn();
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x20,
      processPendingPtr: makeKernelState().processPendingPtr + 0x10,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      onError,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(40);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(createdWorkers[0]?.terminated).toBe(false);
    expect(createdWorkers).toHaveLength(1);
    expect(onError).not.toHaveBeenCalled();
    expect(
      parentKernel.recordedChildExits,
    ).toEqual([[156, 127 << 8]]);
    expect(handle.kernelState.heapPtr).toBe(resumedState.heapPtr);
    expect(handle.kernelState.processPendingPtr).toBe(resumedState.processPendingPtr);
    expect(handle.lastDebugMessage).toContain("vfork-execve:handoff-failed child=156 ret=-8");
    expect(parentWorker.postedMessages).toEqual([
      { type: "step", budget: 20_000, kernelState: handle.kernelState },
    ]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure makes synthetic child exit visible to a later STATUS_WAIT4 retry", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_status_vfork_wait());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x20,
      processPendingPtr: makeKernelState().processPendingPtr + 0x10,
    };
    const wait4ResumedState = {
      ...resumedState,
      heapPtr: resumedState.heapPtr + 0x40,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "resume-wait4":
            return { type: "wait4-resumed", requestId: 0, kernelState: wait4ResumedState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const resumeWait4 = vi.fn(() => 0n);
    parentKernel.thread_resume_wait4 = resumeWait4;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(40);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(
      requests.some((msg) => msg.type === "record-child-exit" && msg.childPid === 156 && msg.waitStatus === (127 << 8)),
    ).toBe(true);
    expect(parentKernel.recordedChildExits).toEqual([[156, 127 << 8]]);
    expect(parentWorker.postedMessages).toEqual([
      { type: "step", budget: 20_000, kernelState: resumedState },
    ]);

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 31,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: resumedState,
    });

    await flushMicrotasks();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await flushMicrotasks();

    expect(requests.some((msg) => msg.type === "resume-wait4")).toBe(true);
    expect(resumeWait4).toHaveBeenCalledWith(0x1234);
    expect(handle.kernelState.heapPtr).toBe(wait4ResumedState.heapPtr);
    expect(parentWorker.postedMessages).toEqual([
      { type: "step", budget: 20_000, kernelState: wait4ResumedState },
    ]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve handoff failure leaves a ready parent that is stepped once a running sibling enters waitInWorker FUTEX_WAIT", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 3) {
      exports.thread_complete_execve_handoff = () => -BigInt(exports.abi_status_vfork_wait());
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const resumedState = {
      ...makeKernelState(),
      heapPtr: makeKernelState().heapPtr + 0x20,
      processPendingPtr: makeKernelState().processPendingPtr + 0x10,
    };
    const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "record-child-exit":
            return { type: "child-exit-recorded", requestId: 0, kernelState: msg.kernelState };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: resumedState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    emitReady(parentWorker);

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 10,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });

    const siblingWorker = createdWorkers[1];
    emitReady(siblingWorker, 0x2000);
    siblingWorker.clearPostedMessages();
    parentWorker.clearPostedMessages();

    handle.runningWorker = siblingWorker as unknown as Worker;
    handle.readyWorkers.length = 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks(40);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(handle.readyWorkers).toEqual([parentWorker as unknown as Worker]);
    expect(handle.runningWorker).toBe(siblingWorker as unknown as Worker);
    expect(parentWorker.postedMessages).toEqual([]);

    siblingWorker.emit({
      type: "status",
      status: parentKernel.abi_status_futex_wait(),
      waitInWorker: true,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x2000,
      kernelState: resumedState,
    });
    await flushMicrotasks();

    expect(handle.runningWorker).toBe(parentWorker as unknown as Worker);
    expect(handle.readyWorkers).toEqual([]);
    expect(parentWorker.postedMessages).toEqual([
      { type: "step", budget: 20_000, kernelState: resumedState },
    ]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve keeps post-vfork stdio rewires child-private across kernel-worker scrub", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      const childPrivateStdioOfds = [7, 8, 9];
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        if (fd >= 0 && fd <= 2) {
          return childPrivateStdioOfds[fd] ?? 0;
        }
        return fd === 3 ? 4 : 0;
      };
      exports.thread_complete_execve_handoff = () => {
        const view = new DataView(memory.buffer);
        childPrivateStdioOfds.forEach((ofdId, index) => {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          view.setInt32(slotOffset, 2, true);
          view.setInt32(slotOffset + 4, index === 0 ? 1 : -1, true);
          view.setInt32(slotOffset + 8, 0, true);
          view.setUint32(slotOffset + 24, 4 + index, true);
          view.setUint8(slotOffset + 12, 0x70 + index);
          view.setUint8(slotOffset + 13, 0x80 + index);
        });
        const stdoutPipeBase = state.pipeBase + 4 * (32 + 4096);
        view.setInt32(stdoutPipeBase + 8, 5, true);
        view.setInt32(stdoutPipeBase + 12, 6, true);
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const state = makeKernelState();
    const writeOfdType = (bytes: Uint8Array, ofdId: number, marker: number): void => {
      const slotOffset = state.ofdTablePtr + ofdId * 32;
      bytes[slotOffset] = 0x01;
      bytes[slotOffset + 12] = marker;
      bytes[slotOffset + 13] = marker + 0x10;
    };

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const parentBytes = new Uint8Array(parentKernel.memory.buffer);

    [1, 2, 3, 4].forEach((ofdId, index) => writeOfdType(parentBytes, ofdId, 0x20 + index));
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      if (fd >= 0 && fd <= 2) {
        return fd + 1;
      }
      return fd === 3 ? 4 : 0;
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    [7, 8, 9].forEach((ofdId, index) => writeOfdType(parentBytes, ofdId, 0x70 + index));
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      if (fd >= 0 && fd <= 2) {
        return 7 + fd;
      }
      return fd === 3 ? 4 : 0;
    };

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    const rehydrate = [...requests].reverse().find((msg) => msg.type === "rehydrate-fork-shared-state");
    expect(rehydrate?.type).toBe("rehydrate-fork-shared-state");
    if (rehydrate?.type !== "rehydrate-fork-shared-state") {
      throw new Error("expected rehydrate-fork-shared-state request");
    }
    expect(rehydrate.ofdIds).toEqual([]);
    const childSync = [...requests].reverse().find((msg) => msg.type === "sync-fork-shared-state");
    expect(childSync?.type).toBe("sync-fork-shared-state");
    if (childSync?.type !== "sync-fork-shared-state") {
      throw new Error("expected sync-fork-shared-state request");
    }
    expect(childSync.ofdIds).toEqual([4]);

    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);
    for (const [index, ofdId] of [7, 8, 9].entries()) {
      const slotOffset = state.ofdTablePtr + ofdId * 32;
      expect(childBytes[slotOffset]).toBe(0x02);
      expect(childBytes[slotOffset + 12]).toBe(0x70 + index);
      expect(childBytes[slotOffset + 13]).toBe(0x80 + index);
    }
    const childView = new DataView(childKernel.memory.buffer);
    const stdoutPipeBase = state.pipeBase + 4 * (32 + 4096);
    expect(childView.getInt32(stdoutPipeBase + 8, true)).toBe(5);
    expect(childView.getInt32(stdoutPipeBase + 12, true)).toBe(6);

    requests.length = 0;
    const childWorker = createdWorkers[1];
    emitReady(childWorker);
    childWorker.clearPostedMessages();
    childWorker.emit({
      type: "status",
      status: 0,
      syncEffects: SYNC_EFFECT_FORK_SHARED_OFD_STATE,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();
    await new Promise((resolve) => setTimeout(resolve, 0));

    const postReadySync = requests.find((msg) => msg.type === "sync-fork-shared-state" && msg.pid === 156);
    expect(postReadySync?.type).toBe("sync-fork-shared-state");
    if (postReadySync?.type !== "sync-fork-shared-state") {
      throw new Error("expected post-ready sync-fork-shared-state request");
    }
    expect(postReadySync.ofdIds).toEqual([4]);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve keeps distinct shared stdout and stderr pipe slots across kernel-worker scrub", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        switch (fd) {
          case 0:
            return 1;
          case 1:
            return 8;
          case 2:
            return 10;
          case 3:
            return 4;
          default:
            return 0;
        }
      };
      exports.thread_complete_execve_handoff = () => {
        const view = new DataView(memory.buffer);
        const writePipeOfdIds = [
          [8, 4, 0x88],
          [10, 5, 0xaa],
        ] as const;
        for (const [ofdId, pipeSlot, marker] of writePipeOfdIds) {
          const slotOffset = state.ofdTablePtr + ofdId * 32;
          view.setInt32(slotOffset, 2, true);
          view.setInt32(slotOffset + 4, -1, true);
          view.setInt32(slotOffset + 8, 0, true);
          view.setUint32(slotOffset + 24, pipeSlot, true);
          view.setUint8(slotOffset + 12, marker);
          view.setUint8(slotOffset + 13, marker + 1);
        }
        const stdoutPipeBase = state.pipeBase + 4 * (32 + 4096);
        view.setInt32(stdoutPipeBase + 8, 1, true);
        view.setInt32(stdoutPipeBase + 12, 1, true);
        const stderrPipeBase = state.pipeBase + 5 * (32 + 4096);
        view.setInt32(stderrPipeBase + 8, 1, true);
        view.setInt32(stderrPipeBase + 12, 1, true);
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    const requests: KernelWorkerRequest[] = [];
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
      pid: 1,
      kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
        requests.push(msg);
        switch (msg.type) {
          case "register-process":
            return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
          case "initialize-process-lifecycle":
            return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
          case "canonicalize-child-pid":
            return {
              type: "child-pid-canonicalized",
              requestId: 0,
              childPid: 156,
              nextPid: 201,
              pgid: 33,
              sid: 44,
              kernelState: msg.kernelState,
            };
          case "resume-vfork-execve-parent":
            return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
          case "sync-fork-shared-state":
            return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
          case "rehydrate-fork-shared-state":
            return { type: "fork-shared-state-rehydrated", requestId: 0 };
          case "unregister-process":
            return { type: "process-unregistered", requestId: 0 };
          default:
            throw new Error(`unexpected kernel-worker request: ${msg.type}`);
        }
      },
    });

    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => {
      switch (fd) {
        case 0:
          return 1;
        case 1:
          return 2;
        case 2:
          return 3;
        case 3:
          return 4;
        default:
          return 0;
      }
    };
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    const childKernel = createdKernelExports[1];
    expect(childKernel.process_fd_ofd_id(0x1234, 1)).toBe(8);
    expect(childKernel.process_fd_ofd_id(0x1234, 2)).toBe(10);
    expect(childKernel.kernel_ofd_ino(8)).toBe(4);
    expect(childKernel.kernel_ofd_ino(10)).toBe(5);

    const childView = new DataView(childKernel.memory.buffer);
    const state = makeKernelState();
    const stdoutPipeBase = state.pipeBase + 4 * (32 + 4096);
    const stderrPipeBase = state.pipeBase + 5 * (32 + 4096);
    expect(childView.getInt32(stdoutPipeBase + 8, true)).toBe(1);
    expect(childView.getInt32(stdoutPipeBase + 12, true)).toBe(1);
    expect(childView.getInt32(stderrPipeBase + 8, true)).toBe(1);
    expect(childView.getInt32(stderrPipeBase + 12, true)).toBe(1);

  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve rehydrates child shared kernel objects without mutating parent memfs bytes", async () => {
  const restoreDefaultInstantiate = (): void => {
    vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
      const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
      return {
        exports: makeKernelExports(memory),
      } as WebAssembly.WebAssemblyInstantiatedSource;
    });
  };

  let instantiateCount = 0;
  vi.spyOn(WebAssembly, "instantiate").mockImplementation(async (_module, imports) => {
    instantiateCount += 1;
    const memory = (imports as { env: { memory: WebAssembly.Memory } }).env.memory;
    const exports = makeKernelExports(memory);
    if (instantiateCount === 2) {
      const state = makeKernelState();
      const ofdId = 1;
      const ofdOffset = state.ofdTablePtr + ofdId * 32;
      exports.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? ofdId : 0;
      exports.thread_complete_execve_handoff = () => {
        const bytes = new Uint8Array(memory.buffer);
        bytes[ofdOffset] = 0x01;
        bytes[ofdOffset + 20] = 0x81;
        bytes[state.memfsPtr + 12] = 0x82;
        bytes[exports.kernel_memfs_inodes_ptr() + 6] = 0x83;
        bytes[exports.kernel_memfs_data_ptr() + 10] = 0x84;
        return 0n;
      };
    }
    return { exports } as WebAssembly.WebAssemblyInstantiatedSource;
  });

  try {
    await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
    const parentWorker = createdWorkers[0];
    const parentKernel = createdKernelExports[0];
    const state = makeKernelState();
    const ofdId = 1;
    const ofdOffset = state.ofdTablePtr + ofdId * 32;

    parentKernel.process_fd_ofd_id = (_vmPtr, fd) => fd === 3 ? ofdId : 0;
    parentKernel.thread_vfork_child_pid = () => 55;
    parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
    parentKernel.thread_spawn_child_pid = () => 156;

    emitReady(parentWorker);
    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 8,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    parentWorker.clearPostedMessages();
    parentWorker.emit({
      type: "status",
      status: 9,
      exitCode: 0,
      termSignal: 0,
      pc: 0n,
      vmPtr: 0x1234,
      kernelState: makeKernelState(),
    });
    await flushMicrotasks();

    const parentBytes = new Uint8Array(parentKernel.memory.buffer);
    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);

    expect(parentBytes[ofdOffset + 20]).toBe(0x81);
    expect(parentBytes[state.memfsPtr + 12]).toBe(0);
    expect(parentBytes[parentKernel.kernel_memfs_inodes_ptr() + 6]).toBe(0);
    expect(parentBytes[parentKernel.kernel_memfs_data_ptr() + 10]).toBe(0);
    expect(childBytes[ofdOffset + 20]).toBe(0x81);
    expect(childBytes[state.memfsPtr + 12]).toBe(0x82);
    expect(childBytes[childKernel.kernel_memfs_inodes_ptr() + 6]).toBe(0x83);
    expect(childBytes[childKernel.kernel_memfs_data_ptr() + 10]).toBe(0x84);
  } finally {
    restoreDefaultInstantiate();
  }
});

test("child runtime errors propagate to the parent process handle", async () => {
  const parentError = vi.fn();
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    onError: parentError,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "canonicalize-child-pid":
          return {
            type: "child-pid-canonicalized",
            requestId: 0,
            childPid: 156,
            nextPid: 201,
            pgid: 33,
            sid: 44,
            kernelState: msg.kernelState,
          };
        case "resume-vfork-execve-parent":
          return { type: "vfork-execve-parent-resumed", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "rehydrate-fork-shared-state":
          return { type: "fork-shared-state-rehydrated", requestId: 0 };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });

  const parentWorker = createdWorkers[0];
  const parentKernel = createdKernelExports[0];
  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  parentKernel.thread_spawn_child_pid = () => 156;

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks();

  parentWorker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  await flushMicrotasks(20);

  const childWorker = createdWorkers[1];
  childWorker.emit({
    type: "error",
    vmPtr: 0x1234,
    message: "boom",
  });
  await flushMicrotasks(20);

  expect(parentError).toHaveBeenCalledTimes(1);
  expect(parentError.mock.calls[0][0]).toBeInstanceOf(Error);
  expect((parentError.mock.calls[0][0] as Error).message).toContain("child pid=156:");
  expect((parentError.mock.calls[0][0] as Error).message).toContain("boom");
});

test("vfork child runs exclusively until the parent thread is resumed", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), { pid: 1 });
  const parentWorker = createdWorkers[0];

  emitReady(parentWorker);
  parentWorker.clearPostedMessages();
  parentWorker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  const siblingWorker = createdWorkers[1];
  emitReady(siblingWorker, 0x2000);
  parentWorker.clearPostedMessages();
  siblingWorker.clearPostedMessages();

  parentWorker.emit({
    type: "status",
    status: 8,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  expect(handle.vforkWorker).toBe(parentWorker);
  expect(parentWorker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);

  siblingWorker.emit({
    type: "status",
    status: 0,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x2000,
    kernelState: makeKernelState(),
  });

  expect(siblingWorker.postedMessages).toEqual([]);
});

test("process exit releases host stdio through kernel-worker when available", async () => {
  threadCountQueue.push(0);

  const requests: KernelWorkerRequest[] = [];
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-socket-state":
          return { type: "socket-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-fork-shared-state":
          return { type: "fork-shared-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });
  const worker = createdWorkers[0];
  const kernel = createdKernelExports[0];
  const releaseHostStdio = vi.fn();

  kernel.process_release_host_stdio = releaseHostStdio;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 3,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-socket-state")).toBe(true);
  expect(requests.some((msg) => msg.type === "sync-fork-shared-state")).toBe(true);
  expect(requests.some((msg) => msg.type === "release-host-stdio")).toBe(true);
  expect(releaseHostStdio).not.toHaveBeenCalled();
});

test("writeProcessStdin writes into the hidden stdin pipe buffer", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));

  writeProcessStdin(handle, new TextEncoder().encode("abc"));

  const slotBase = handle.kernelState.pipeBase + handle.stdinPipe * (32 + 4096);
  const dataBase = slotBase + 32;
  const mem = new Uint8Array(handle.memory.buffer);
  expect(new TextDecoder().decode(mem.subarray(dataBase, dataBase + 3))).toBe("abc");
});

test("writeProcessStdin syncs pipe state through kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    pid: 1,
    kernelWorkerRequest: async (msg): Promise<KernelWorkerOutbound> => {
      requests.push(msg);
      switch (msg.type) {
        case "register-process":
          return { type: "process-registered", requestId: 0, kernelState: msg.kernelState };
        case "initialize-process-lifecycle":
          return { type: "process-lifecycle-initialized", requestId: 0, kernelState: msg.kernelState };
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "sync-pipe-state":
          return { type: "pipe-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "unregister-process":
          return { type: "process-unregistered", requestId: 0 };
        default:
          throw new Error(`unexpected kernel-worker request: ${msg.type}`);
      }
    },
  });

  writeProcessStdin(handle, new TextEncoder().encode("abc"));
  for (let i = 0; i < 4; i++) {
    await Promise.resolve();
  }

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
});

test("injectHttpRequest collects the full virtual HTTP response", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  httpResponseChunks.push(
    new TextEncoder().encode("HTTP/1.1 200 OK\r\n\r\nhe"),
    new TextEncoder().encode("llo"),
  );

  const response = await injectHttpRequest(handle, 8080, "GET / HTTP/1.1\r\n\r\n");

  expect(response).toBe("HTTP/1.1 200 OK\r\n\r\nhello");
  expect(injectedHttpRequests).toEqual([
    { port: 8080, request: "GET / HTTP/1.1\r\n\r\n" },
  ]);
  expect(closedHttpConnections).toEqual([17]);
});

test("destroyProcessTree terminates descendant processes in the same runtime only", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 7,
    pid: 1,
  });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 7,
    pid: 2,
    parentPid: 1,
  });
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    runtimeId: 9,
    pid: 3,
  });

  destroyProcessTree(1, 7);

  expect(createdWorkers[0].terminated).toBe(true);
  expect(createdWorkers[1].terminated).toBe(true);
  expect(createdWorkers[2].terminated).toBe(false);
});
