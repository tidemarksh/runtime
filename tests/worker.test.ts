import { afterEach, beforeEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, KernelRuntimeState } from "../src/abi.js";
import type { WorkerMessage } from "../src/messages.js";
import {
  clearModuleCache,
  createProcess,
  destroyWorkerPool,
} from "../src/worker.js";

class FakeWorker {
  onmessage: ((event: MessageEvent<WorkerMessage>) => void) | null = null;
  readonly postedMessages: WorkerMessage[] = [];
  terminated = false;

  constructor(
    readonly _url: URL,
    readonly _options: WorkerOptions,
  ) {}

  postMessage(message: WorkerMessage): void {
    this.postedMessages.push(message);
  }

  terminate(): void {
    this.terminated = true;
  }

  emit(message: WorkerMessage): void {
    this.onmessage?.({ data: message } as MessageEvent<WorkerMessage>);
  }

  clearPostedMessages(): void {
    this.postedMessages.length = 0;
  }
}

const createdWorkers: FakeWorker[] = [];
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
    sigSlotsPtr: 0x290000,
    processPendingPtr: 0x2a0000,
    processRuntimeStatePtr: 0x2b0000,
    timeStatePtr: 0x2c0000,
  };
}

function makeKernelExports(memory: WebAssembly.Memory): KernelExports {
  let nextVmPtr = 0x2000;
  let runtimeState = makeKernelState();
  return {
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
    kernel_init: () => {},
    kernel_heap_base: () => runtimeState.heapBase,
    kernel_heap_ptr: () => runtimeState.heapPtr,
    kernel_ofd_table_ptr: () => runtimeState.ofdTablePtr,
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
    process_push_arg: () => {},
    process_push_env: () => {},
    process_clear_args: () => {},
    process_create: () => 0x1234,
    thread_step: () => 0,
    thread_status: () => 0,
    thread_exit_code: () => 0,
    thread_pc: () => 0n,
    thread_a0: () => 0n,
    thread_process_fs: () => {
      runtimeState = {
        ...runtimeState,
        heapPtr: runtimeState.heapPtr + 0x10,
      };
    },
    thread_resume_fs: () => 0,
    thread_spawn_from_clone: () => nextVmPtr++,
    thread_process_thread_count: () => 1,
  };
}

beforeEach(() => {
  createdWorkers.length = 0;
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

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 10,
    exitCode: 0,
    pc: 0n,
    kernelState: makeKernelState(),
  });

  expect(createdWorkers).toHaveLength(2);
  expect(createdWorkers[1].postedMessages).toEqual([
    {
      type: "init",
      wasmModule: expect.any(Object),
      memory: handle.memory,
      vmPtr: 0x2000,
      kernelState: makeKernelState(),
    },
  ]);
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 500_000, kernelState: makeKernelState() },
  ]);
});

test("createProcess processes STATUS_FS and resumes worker with refreshed kernel state", async () => {
  const handle = await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 4,
    exitCode: 0,
    pc: 0n,
    kernelState: makeKernelState(),
  });

  expect(worker.postedMessages).toEqual([
    {
      type: "step",
      budget: 500_000,
      kernelState: {
        ...makeKernelState(),
        heapPtr: makeKernelState().heapPtr + 0x10,
      },
    },
  ]);
  expect(handle.kernelState.heapPtr).toBe(makeKernelState().heapPtr + 0x10);
});

test("createProcess resumes current worker on STATUS_EXECVE_SPAWN", async () => {
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  await createProcess(new Uint8Array([0]), new Uint8Array([1, 2, 3]));
  const worker = createdWorkers[0];

  expect(worker).toBeDefined();

  worker.clearPostedMessages();
  worker.emit({
    type: "status",
    status: 9,
    exitCode: 0,
    pc: 0n,
    kernelState: makeKernelState(),
  });

  expect(warn).toHaveBeenCalledWith(
    expect.stringContaining("STATUS_EXECVE_SPAWN"),
  );
  expect(worker.postedMessages).toEqual([
    { type: "step", budget: 500_000, kernelState: makeKernelState() },
  ]);
});
