// Thread-worker tests in this file validate status outbound assembly ownership.
import { afterEach, expect, test, vi } from "vite-plus/test";
import type { KernelExports, StatusCodes } from "../../../src/abi.js";
import {
  OFD_OFFSET_OFFSET,
  OFD_TYPE_OFFSET,
} from "../../../src/layout/ofd.js";
import { applyThreadWorkerInboundState } from "../../../src/thread-worker/inbound-state.js";
import {
  postThreadWorkerStatus,
} from "../../../src/thread-worker/status.js";

afterEach(() => {
  Reflect.deleteProperty(globalThis, "postMessage");
});

function makeStatusCodes(): StatusCodes {
  return {
    OK: 0,
    FAULT: 3,
    FS: 4,
    FUTEX_WAIT: 20,
    PIPE_WAIT: 21,
    EPOLL_WAIT: 22,
    VFORK_WAIT: 23,
    EXECVE_SPAWN: 24,
    CLONE_SPAWN: 25,
    SLEEP: 26,
    JIT_READY: 27,
    WAIT4: 28,
    CONNECT: 29,
    SOCKET_SEND: 30,
    SOCKET_RECV: 31,
    FLOCK_WAIT: 32,
  };
}

function makeKernelExports(): KernelExports {
  return {
    memory: new WebAssembly.Memory({ initial: 1 }),
    abi_max_fds: () => 4,
    abi_max_pipes: () => 4,
    abi_status_ok: () => 0,
    abi_status_fault: () => 3,
    abi_status_fs: () => 4,
    abi_status_futex_wait: () => 20,
    abi_status_pipe_wait: () => 21,
    abi_status_epoll_wait: () => 22,
    abi_status_vfork_wait: () => 23,
    abi_status_execve_spawn: () => 24,
    abi_status_clone_spawn: () => 25,
    abi_status_sleep: () => 26,
    abi_status_jit_ready: () => 27,
    abi_status_wait4: () => 28,
    abi_status_connect: () => 29,
    abi_status_socket_send: () => 30,
    abi_status_socket_recv: () => 31,
    abi_status_flock_wait: () => 32,
    abi_spawn_kind_none: () => 0,
    abi_spawn_kind_execve: () => 1,
    abi_spawn_kind_fork: () => 2,
    abi_page_size: () => 4096,
    abi_max_threads: () => 1,
    abi_fd_type_none: () => 0,
    abi_fd_type_file: () => 1,
    abi_fd_type_pipe: () => 2,
    abi_fd_type_epoll: () => 3,
    abi_fd_type_socket: () => 4,
    abi_sigkill: () => 9,
    abi_sigterm: () => 15,
    abi_sigchld: () => 17,
    abi_sigpipe: () => 13,
    abi_clone_parent: () => 0n,
    abi_o_rdonly: () => 0,
    abi_o_wronly: () => 1,
    abi_o_rdwr: () => 2,
    abi_o_creat: () => 0x40,
    abi_o_trunc: () => 0x200,
    abi_o_append: () => 0x400,
    abi_o_cloexec: () => 0x80000,
    abi_enoent: () => 2,
    kernel_heap_base: () => 0,
    kernel_heap_ptr: () => 0,
    kernel_ofd_table_ptr: () => 0x1000,
    kernel_ofd_table_bytes: () => 32,
    kernel_memfs_ptr: () => 0,
    kernel_mem_state_ptr: () => 0,
    kernel_epoll_table_ptr: () => 0,
    kernel_socket_table_ptr: () => 0,
    kernel_socket_used: () => 0,
    kernel_socket_table_bytes: () => 0,
    kernel_net_buf_pool_bytes: () => 0,
    kernel_pipe_base: () => 0x2000,
    kernel_pipe_used: () => 0n,
    kernel_pipe_region_bytes: () => 128,
    kernel_net_buf_base: () => 0,
    kernel_net_buf_used: () => 0n,
    kernel_sig_slots_ptr: () => 0,
    kernel_process_pending_ptr: () => 0,
    kernel_process_runtime_state_ptr: () => 0,
    kernel_time_state_ptr: () => 0,
    process_fd_ofd_id: () => 0,
    process_fd_flags: () => 0,
    supervisor_process_ofd_for_fd: () => 0,
    supervisor_process_fd_flags_for_fd: () => 0,
    process_set_fd_entry: () => {},
    process_create: () => 0,
    process_destroy: () => {},
    process_get_state: () => 0,
    process_set_state: () => {},
    process_current_pid: () => 1,
    process_record_child_exit: () => {},
    thread_exit_code: () => 0,
    thread_term_signal: () => 0,
    thread_pc: () => 0x1234n,
    thread_a7: () => 0n,
    thread_spawn_kind: () => 0,
    thread_guest_memory_policy_limit: () => 0,
    thread_process_ptr: () => 0x2200,
    thread_status: () => 0,
    thread_step: () => 0,
    thread_init: () => {},
    thread_sp: () => 0n,
    thread_ra: () => 0n,
    thread_s11: () => 0n,
    thread_reg: () => 0n,
    thread_a0: () => 0n,
    thread_a1: () => 0n,
    thread_blocked_syscall_nr: () => 0n,
    thread_blocked_syscall_arg: () => 0n,
    thread_blocked_ofd_id: () => 0,
    thread_fault_pc: () => 0n,
    thread_fault_addr: () => 0n,
    supervisor_thread_take_completed_sync_effects: () => 0,
  } as unknown as KernelExports;
}

test("postThreadWorkerStatus owns status outbound assembly", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  let lastResumeBlockedDebug: string | undefined = "thread-resume-blocked:test";
  let lastFutexWaitDebug: string | undefined;

  postThreadWorkerStatus({
    kernelExports: makeKernelExports(),
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 7,
    stepSequence: 41,
    runtimeBridge: null,
    lastResumeBlockedDebug,
    lastFutexWaitDebug,
    setLastResumeBlockedDebug(value) {
      lastResumeBlockedDebug = value;
    },
    setLastFutexWaitDebug(value) {
      lastFutexWaitDebug = value;
    },
  }, 0, true, 0);

  expect(postMessage).toHaveBeenCalledTimes(1);
  expect(postMessage).toHaveBeenCalledWith(expect.objectContaining({
    type: "status",
    generation: 7,
    stepSequence: 41,
    status: 0,
    waitInWorker: true,
    vmPtr: 0x1234,
    processPtr: 0x2200,
    processPid: 1,
    syncEffects: 0,
  }));
  expect(lastResumeBlockedDebug).toBeUndefined();
  expect(lastFutexWaitDebug).toBeUndefined();
});

test("postThreadWorkerStatus publishes the complete Kernel socket state", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const socketStatePtr = 0x3000;
  const socketTableBytes = 16;
  const socketStateBytes = 32;
  const expected = Uint8Array.from({ length: socketStateBytes }, (_, index) => 0x40 + index);
  new Uint8Array(kernelExports.memory.buffer).set(expected, socketStatePtr);
  Object.assign(kernelExports, {
    abi_max_sockets: () => 1,
    kernel_socket_table_ptr: () => socketStatePtr,
    kernel_socket_table_bytes: () => socketTableBytes,
    kernel_socket_state_ptr: () => socketStatePtr,
    kernel_socket_state_bytes: () => socketStateBytes,
    kernel_socket_used: () => 1,
    kernel_net_buf_base: () => 0x3100,
    kernel_net_buf_pool_bytes: () => 16,
    kernel_net_buf_used: () => 0n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 7,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 0, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    socketState?: { socketTable: Uint8Array; socketState?: Uint8Array };
  };
  expect([...msg.socketState!.socketTable]).toEqual([...expected.subarray(0, socketTableBytes)]);
  expect([...msg.socketState!.socketState!]).toEqual([...expected]);
});

test("postThreadWorkerStatus omits unchanged owned state from routine status", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const snapshotFdTable = vi.fn(() => 4);
  kernelExports.abi_max_fds = snapshotFdTable;
  const statusCodes = makeStatusCodes();

  postThreadWorkerStatus({
    kernelExports,
    statusCodes,
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 8,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, statusCodes.SLEEP, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    fdEntries?: unknown;
    ofdSlots?: unknown;
    socketState?: unknown;
    debug?: string;
  };
  expect(msg.fdEntries).toBeUndefined();
  expect(msg.ofdSlots).toBeUndefined();
  expect(msg.socketState).toBeUndefined();
  expect(msg.debug).toBeUndefined();
  expect(snapshotFdTable).not.toHaveBeenCalled();
});

test("postThreadWorkerStatus owns sync-effect snapshot payload assembly", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const pipeSlotOffset = 0x2000 + 2 * 32;
  memory.set(Uint8Array.from({ length: 32 }, (_, index) => 0x80 + index), pipeSlotOffset);
  Object.assign(kernelExports, {
    thread_take_completed_pipe_sync_slots: () => 1n << 2n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 8,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 0, false, 1);

  const msg = postMessage.mock.calls[0]?.[0] as {
    syncEffects?: number;
    pipeSlots?: Array<{ pipeSlot: number; fresh?: boolean; slot: Uint8Array }>;
  };
  expect(msg.syncEffects).toBe(1);
  expect(msg.pipeSlots?.[0]?.pipeSlot).toBe(2);
  expect(msg.pipeSlots?.[0]?.fresh).toBe(false);
  expect([...(msg.pipeSlots?.[0]?.slot ?? [])]).toEqual(
    Array.from({ length: 32 }, (_, index) => 0x80 + index),
  );
});

test("postThreadWorkerStatus treats completed pipe slots as pipe sync evidence", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const pipeSlotOffset = 0x2000 + 2 * 32;
  memory.set(Uint8Array.from({ length: 32 }, (_, index) => 0x40 + index), pipeSlotOffset);
  Object.assign(kernelExports, {
    thread_take_completed_pipe_sync_slots: () => 1n << 2n,
    thread_take_completed_pipe_sync_read_slots: () => 0n,
    thread_take_completed_pipe_sync_write_slots: () => 1n << 2n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 9,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 0, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    syncEffects?: number;
    pipeSlots?: Array<{ pipeSlot: number; mode?: number; slot: Uint8Array }>;
  };
  expect(msg.syncEffects).toBe(1);
  expect(msg.pipeSlots).toHaveLength(1);
  expect(msg.pipeSlots?.[0]?.pipeSlot).toBe(2);
  expect(msg.pipeSlots?.[0]?.mode).toBe(2);
  expect([...(msg.pipeSlots?.[0]?.slot ?? [])]).toEqual(
    Array.from({ length: 32 }, (_, index) => 0x40 + index),
  );
});

test("postThreadWorkerStatus transports completed pipe endpoint control changes", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  Object.assign(kernelExports, {
    thread_take_completed_pipe_sync_slots: () => 0n,
    thread_take_completed_pipe_sync_read_slots: () => 0n,
    thread_take_completed_pipe_sync_write_slots: () => 0n,
    thread_take_completed_pipe_sync_control_slots: () => 1n << 2n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 10,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 0, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    pipeSlots?: Array<{ pipeSlot: number; mode?: number; controlChanged?: boolean }>;
  };
  expect(msg.pipeSlots).toHaveLength(1);
  expect(msg.pipeSlots?.[0]).toMatchObject({ pipeSlot: 2, mode: 0, controlChanged: true });
});

test("postThreadWorkerStatus snapshots traced pipe writes after fd table closes", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  Object.assign(kernelExports, {
    kernel_pipe_region_bytes: () => 4 * (32 + 64),
  });
  const memory = new Uint8Array(kernelExports.memory.buffer);
  const pipeSlotOffset = 0x2000 + 2 * (32 + 64);
  const view = new DataView(kernelExports.memory.buffer);
  view.setUint32(pipeSlotOffset, 0, true);
  view.setUint32(pipeSlotOffset + 4, 4, true);
  memory.set([0x6e, 0x6f, 0x64, 0x65], pipeSlotOffset + 32);
  Object.assign(kernelExports, {
    kernel_pipe_region_bytes: () => 4 * (32 + 64),
    process_fd_ofd_id: () => 0,
    supervisor_process_ofd_for_fd: () => 0,
    debug_fd_syscall_trace_len: () => 1,
    debug_fd_syscall_trace_vm_ptr: () => 0x1234,
    debug_fd_syscall_trace_nr: () => 64n,
    debug_fd_syscall_trace_arg0_ofd_id: () => 7,
    debug_fd_syscall_trace_arg0_ofd_type: () => 2,
    debug_fd_syscall_trace_arg0_ofd_ino: () => 2,
    debug_fd_syscall_trace_arg1: () => 0x4000n,
    debug_fd_syscall_trace_arg2: () => 4n,
    debug_fd_syscall_trace_result: () => 4n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 10,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 3, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    syncEffects?: number;
    pipeSlots?: Array<{ pipeSlot: number; mode?: number; slot: Uint8Array }>;
  };
  expect(msg.syncEffects).toBe(0);
  expect(msg.pipeSlots).toHaveLength(1);
  expect(msg.pipeSlots?.[0]?.pipeSlot).toBe(2);
  expect(msg.pipeSlots?.[0]?.mode).toBe(2);
  expect([...(msg.pipeSlots?.[0]?.slot.slice(32, 36) ?? [])]).toEqual([0x6e, 0x6f, 0x64, 0x65]);
});

test("postThreadWorkerStatus patches eventfd OFD offset from fd trace", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const maxFds = 8;
  const slotBytes = 32;
  const ofdId = 4;
  const eventfdType = 9;
  const slotOffset = 0x1000 + ofdId * slotBytes;
  const slotView = new DataView(kernelExports.memory.buffer, slotOffset, slotBytes);
  slotView.setInt32(OFD_TYPE_OFFSET, eventfdType, true);
  slotView.setBigInt64(OFD_OFFSET_OFFSET, 0n, true);
  Object.assign(kernelExports, {
    abi_max_fds: () => maxFds,
    kernel_ofd_table_bytes: () => maxFds * slotBytes,
    abi_fd_type_eventfd: () => eventfdType,
    supervisor_process_ofd_for_fd: (_vmPtr: number, fd: number) => fd === 3 ? ofdId : 0,
    debug_fd_syscall_trace_len: () => 1,
    debug_fd_syscall_trace_vm_ptr: () => 0x1234,
    debug_fd_syscall_trace_nr: () => 64n,
    debug_fd_syscall_trace_arg0_ofd_id: () => ofdId,
    debug_fd_syscall_trace_arg0_ofd_type: () => eventfdType,
    debug_fd_syscall_trace_arg0_ofd_offset: () => 1n,
    debug_fd_syscall_trace_result: () => 8n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 11,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 0, false, 1 << 6);

  const msg = postMessage.mock.calls[0]?.[0] as {
    syncEffects?: number;
    fdTraceArg0OfdOffset?: bigint;
    ofdSlots?: Array<{ ofdId: number; slot: Uint8Array }>;
  };
  expect(msg.syncEffects).toBe(1 << 6);
  expect(msg.fdTraceArg0OfdOffset).toBe(1n);
  const snapshot = msg.ofdSlots?.find((entry) => entry.ofdId === ofdId);
  expect(snapshot).toBeDefined();
  expect(new DataView(
    snapshot?.slot.buffer ?? new ArrayBuffer(0),
    snapshot?.slot.byteOffset ?? 0,
    snapshot?.slot.byteLength ?? 0,
  ).getBigInt64(OFD_OFFSET_OFFSET, true)).toBe(1n);
});

test("postThreadWorkerStatus includes futex and inbound guest-write diagnostics", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const statusCodes = makeStatusCodes();
  kernelExports.thread_futex_wait_addr = () => 0x4010n;
  kernelExports.thread_record_blocked_addr = () => 0x4020n;
  applyThreadWorkerInboundState({
    kernelExports,
    guestBase: 0,
  }, {
    kernelState: {
      heapBase: 0,
      heapPtr: 0,
      ofdTablePtr: 0x1000,
      memfsPtr: 0,
      memfsHeaderBytes: 0,
      memfsInodesBytes: 0,
      memfsFirstChildBytes: 0,
      memfsDirEntriesBytes: 0,
      memfsDataBytes: 0,
      memStatePtr: 0,
      epollTablePtr: 0,
      socketTablePtr: 0,
      socketUsed: 0,
      pipeBase: 0x2000,
      pipeUsed: 0n,
      netBufBase: 0,
      netBufUsed: 0n,
      sigSlotsPtr: 0,
      processPendingPtr: 0,
      processRuntimeStatePtr: 0,
      timeStatePtr: 0,
    },
    targetVmPtr: 0x1234,
    guestMemoryWrites: [{ guestPtr: 0x24, bytes: Uint8Array.from([0xaa, 0xbb, 0xcc]) }],
    guestWriteWatchRegions: [{ slot: 2, start: 0x25, end: 0x26 }],
  });
  let lastFutexWaitDebug: string | undefined = "futex-wait-debug:test";

  postThreadWorkerStatus({
    kernelExports,
    statusCodes,
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 10,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug(value) {
      lastFutexWaitDebug = value;
    },
  }, statusCodes.FUTEX_WAIT, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    futexWaitAddr?: bigint;
    futexBlockedAddr?: bigint;
    debug?: string;
  };
  expect(msg.futexWaitAddr).toBe(0x4010n);
  expect(msg.futexBlockedAddr).toBe(0x4020n);
  expect(msg.debug).toContain("futex-wait-debug:test");
  expect(msg.debug).toContain("thread_inbound_guest_writes=[thread-inbound-guest-write:slot=2:ptr=0x24:len=3:overlap=0x25..0x26:sample=bb]");
  expect(lastFutexWaitDebug).toBeUndefined();
});

test("postThreadWorkerStatus snapshots completed regular-file read guest writes", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  const bytes = Uint8Array.from([0x69, 0x6d, 0x70, 0x6f, 0x72, 0x74]);
  new Uint8Array(kernelExports.memory.buffer).set(bytes, 0x500);
  Object.assign(kernelExports, {
    thread_completed_pipe_read_guest_ptr: () => 0x500n,
    thread_take_completed_pipe_read_len: () => bytes.length,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 10,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 0, false, 0);

  const msg = postMessage.mock.calls[0]?.[0] as {
    guestMemoryWrites?: Array<{ guestPtr: number; bytes: Uint8Array }>;
  };
  expect(msg.guestMemoryWrites).toEqual([{ guestPtr: 0x500, bytes }]);
});

test("postThreadWorkerStatus carries Kernel blocked syscall state for supervisor adoption", () => {
  const postMessage = vi.fn();
  Object.defineProperty(globalThis, "postMessage", {
    configurable: true,
    value: postMessage,
  });
  const kernelExports = makeKernelExports();
  Object.assign(kernelExports, {
    thread_blocked_fd: () => 12,
    thread_blocked_ofd_id: () => 23,
    thread_blocked_syscall_nr: () => 63n,
    thread_blocked_syscall_arg: (_vmPtr: number, index: number) => [12n, 0x4000n, 80n, 0n, 0n, 0n][index] ?? 0n,
  });

  postThreadWorkerStatus({
    kernelExports,
    statusCodes: makeStatusCodes(),
    vmPtr: 0x1234,
    guestBase: 0,
    generation: 11,
    runtimeBridge: null,
    lastResumeBlockedDebug: undefined,
    lastFutexWaitDebug: undefined,
    setLastResumeBlockedDebug() {},
    setLastFutexWaitDebug() {},
  }, 21, false, 0);

  expect(postMessage.mock.calls[0]?.[0]).toEqual(expect.objectContaining({
    blockedFd: 12,
    blockedHasOfdPin: true,
    blockedSyscallNr: 63n,
    blockedSyscallArgs: [12n, 0x4000n, 80n, 0n, 0n, 0n],
  }));
});
