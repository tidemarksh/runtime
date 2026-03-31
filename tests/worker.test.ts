import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../src/abi.js";
import type {
  KernelWorkerOutbound,
  KernelWorkerRequest,
  ThreadWorkerInbound,
  ThreadWorkerOutbound,
} from "../src/messages.js";
import {
  clearRuntimeFsState,
  clearModuleCache,
  createProcess,
  destroyProcessTree,
  destroyWorkerPool,
  injectHttpRequest,
  mirrorRuntimeFsEntry,
  primeRuntimeFsState,
  replaceRuntimeFsSnapshot,
  writeProcessStdin,
} from "../src/worker.js";

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

function emitReady(worker: FakeWorker, vmPtr = 0x1234): void {
  worker.emit({
    type: "ready",
    vmPtr,
    kernelState: makeKernelState(),
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
    process_guest_base: () => 0x1000,
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
    thread_a0: () => 0n,
    thread_process_fs: () => {
      fsProcessCalls += 1;
      runtimeState = { ...runtimeState, heapPtr: runtimeState.heapPtr + 0x10 };
    },
    thread_resume_fs: () => 0,
    thread_futex_wait_addr: () => 0n,
    thread_futex_wait_timeout: () => 0n,
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
    thread_clear_spawn_handoff: () => {},
    thread_resume_vfork_parent: () => {},
    thread_complete_execve_handoff: () => 0n,
    thread_spawn_from_clone: () => nextVmPtr++,
    thread_process_thread_count: () => threadCount,
    thread_process_ptr: () => 0x5000,
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
    memfsWriteCalls,
    memfsCreateWithDataCalls,
  };
  createdKernelExports.push(exports);
  return exports;
}

beforeEach(() => {
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
  destroyWorkerPool();
  clearModuleCache();
  vi.restoreAllMocks();
  globalThis.Worker = originalWorker;
});

test("createProcess spawns a child worker on STATUS_CLONE_SPAWN", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  expect(handle.vmPtr).toBe(0x1234);
  expect(worker).toBeDefined();

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
  expect(createdWorkers[1].postedMessages).toEqual([
    {
      type: "init",
      wasmModule: expect.any(Object),
      memory: handle.memory,
      vmPtr: 0x2000,
      guestBase: 0x1000,
      kernelState: makeKernelState(),
      runtimeNotify: expect.any(SharedArrayBuffer),
      runtimeState: expect.any(SharedArrayBuffer),
    },
  ]);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 20_000, kernelState: makeKernelState() },
  ]);
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

test("createProcess delegates STATUS_FS to kernel-worker when available", async () => {
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
        case "release-host-stdio":
          return { type: "host-stdio-released", requestId: 0, kernelState: msg.kernelState };
        case "process-fs":
          return {
            type: "fs-processed",
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

  expect(requests.some((msg) => msg.type === "process-fs")).toBe(true);
  expect(kernel.fsProcessCalls).toBe(0);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x40,
      },
    },
  ]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x40);
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

test("child exit bookkeeping is delegated to kernel-worker when available", async () => {
  const requests: KernelWorkerRequest[] = [];
  threadCountQueue.push(1, 0);

  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
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

  expect(
    requests.some((msg) => msg.type === "record-child-exit" && msg.pid === 1 && msg.childPid === 2),
  ).toBe(true);
  expect(
    requests.some((msg) => msg.type === "sync-fork-shared-state" && msg.pid === 2),
  ).toBe(true);
  expect(parentKernel.recordedChildExits).toEqual([]);
  expect(parentKernel.kernel_heap_ptr()).toBe(makeKernelState().heapPtr + 0x80);
});

test("createProcess preserves guest-visible ramSize when staging grows linear memory", async () => {
  const requestedRam = 64 * 1024 * 1024;
  const handle = await createProcess(
    new Uint8Array([0]),
    new Uint8Array(512 * 1024),
    { ramSize: requestedRam },
  );

  expect(handle.ramSize).toBe(requestedRam);
  expect(handle.memory.buffer.byteLength).toBeGreaterThan(requestedRam);
});

test("createProcess preloads snapshot files by truncating before writing payload bytes", async () => {
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]), {
    preloadEntries: [
      {
        kind: "file",
        path: "/usr/local/go/bin/go",
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
      path: "/workspace/main.go",
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
  expect(createdKernelExports[0].memfsTruncateCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsTruncateCalls[0]?.[1]).toBe(4096);
  expect(createdKernelExports[0].memfsWriteCalls).toHaveLength(1);
  expect(createdKernelExports[0].memfsWriteCalls[0]?.[1]).toBe(4096);
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

  expect(requests.some((msg) => msg.type === "resume-wait4")).toBe(true);
  expect(resumeWait4).not.toHaveBeenCalled();
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x30);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x30,
      },
    },
  ]);

  vi.useRealTimers();
});

test("STATUS_EPOLL_WAIT delegates retries through kernel-worker when available", async () => {
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
        case "sync-epoll-state":
          return { type: "epoll-state-synced", requestId: 0, kernelState: msg.kernelState };
        case "resume-epoll-wait":
          return {
            type: "epoll-wait-resumed",
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
  const resumeTimeout = vi.fn();
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_epoll_wait_timeout = () => 10n;
  kernel.thread_resume_epoll_wait_timeout = resumeTimeout;
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 7,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  vi.advanceTimersByTime(15);
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "resume-epoll-wait" && msg.timeoutExpired)).toBe(true);
  expect(resumeTimeout).not.toHaveBeenCalled();
  expect(resumeBlocked).not.toHaveBeenCalled();
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x40);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x40,
      },
    },
  ]);

  vi.useRealTimers();
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

test("pipe sync wakes blocked epoll waits through kernel-worker when available", async () => {
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
        case "resume-epoll-wait":
          return {
            type: "epoll-wait-resumed",
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
  const resumeBlocked = vi.fn(() => 0n);

  kernel.thread_epoll_wait_timeout = () => -1n;
  kernel.thread_resume_blocked_syscall = resumeBlocked;

  emitReady(worker);
  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 7,
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  writeProcessStdin(handle, new TextEncoder().encode("x"));
  await flushMicrotasks();

  expect(requests.some((msg) => msg.type === "sync-pipe-state")).toBe(true);
  expect(requests.some((msg) => msg.type === "resume-epoll-wait" && !msg.timeoutExpired)).toBe(true);
  expect(resumeBlocked).not.toHaveBeenCalled();
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
          return {
            type: "socket-state-synced",
            requestId: 0,
            kernelState: {
              ...msg.kernelState,
              heapPtr: msg.kernelState.heapPtr + 0x14,
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
    exitCode: 0,
    termSignal: 0,
    pc: 0n,
    vmPtr: 0x1234,
    kernelState: makeKernelState(),
  });

  await flushMicrotasks();
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(requests.some((msg) => msg.type === "sync-epoll-state")).toBe(true);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x14);
  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 20_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x14,
      },
    },
  ]);
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
  expect(kernel.recordedChildExits).toEqual([]);
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

test("vfork execve resumes the parent and initializes the child lifecycle through kernel-worker", async () => {
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
  const clearSpawnHandoff = vi.fn();

  parentKernel.thread_vfork_child_pid = () => 55;
  parentKernel.thread_spawn_kind = () => parentKernel.abi_spawn_kind_execve();
  parentKernel.thread_spawn_child_pid = () => 156;
  parentKernel.thread_resume_vfork_parent = resumeVforkParent;
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
  expect(resumeVforkParent).toHaveBeenCalledWith(0x1234, 156);
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
      const childPrivateStdioOfds = [7, 8, 9];
      exports.process_fd_ofd_id = (_vmPtr, fd) => {
        if (fd >= 0 && fd <= 2) {
          return childPrivateStdioOfds[fd] ?? 0;
        }
        return fd === 3 ? 4 : 0;
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

    const rehydrate = requests.find((msg) => msg.type === "rehydrate-fork-shared-state");
    expect(rehydrate?.type).toBe("rehydrate-fork-shared-state");
    if (rehydrate?.type !== "rehydrate-fork-shared-state") {
      throw new Error("expected rehydrate-fork-shared-state request");
    }
    expect(rehydrate.ofdIds).not.toContain(7);
    expect(rehydrate.ofdIds).not.toContain(8);
    expect(rehydrate.ofdIds).not.toContain(9);

    const childKernel = createdKernelExports[1];
    const childBytes = new Uint8Array(childKernel.memory.buffer);
    for (const [index, ofdId] of [7, 8, 9].entries()) {
      const slotOffset = state.ofdTablePtr + ofdId * 32;
      expect(childBytes[slotOffset]).toBe(0x01);
      expect(childBytes[slotOffset + 12]).toBe(0x70 + index);
      expect(childBytes[slotOffset + 13]).toBe(0x80 + index);
    }
  } finally {
    restoreDefaultInstantiate();
  }
});

test("vfork execve rehydrates shared kernel objects after child canonical sync", async () => {
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
    expect(parentBytes[state.memfsPtr + 12]).toBe(0x82);
    expect(parentBytes[parentKernel.kernel_memfs_inodes_ptr() + 6]).toBe(0x83);
    expect(parentBytes[parentKernel.kernel_memfs_data_ptr() + 10]).toBe(0x84);
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
